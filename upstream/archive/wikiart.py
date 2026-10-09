"""WikiArt crawl: categories, artists, every work's details, and (public-domain) images.

    python -m archive.wikiart dictionaries          the category groups -> data/wikiart/dictionaries.json
    python -m archive.wikiart artists               the artist list     -> data/wikiart/artists.json
    python -m archive.wikiart crosswalk             Wikidata items with a WikiArt ID (P6002) -> data/wikiart/wikidata.json
    python -m archive.wikiart api-v2                API v2 (keyless, ~400 requests an hour): every category with its
                                                    id, every artist with categories, periods, series and related
                                                    artists -> data/wikiart/api_v2.json (~25 min, resumes)
    python -m archive.wikiart media                 every medium's works (oil, tempera, engraving, pencil ...)
                                                    -> data/wikiart/media.json (~20 min at 1.5 requests/s)
    python -m archive.wikiart crawl [--limit N] [--artists a,b] [--refresh]
                                                    each artist: details, works list, each work's details
                                                    -> data/wikiart/artists/<url>.json (+ gzip in the bucket)
    python -m archive.wikiart images [--all] [--limit N]
                                                    the works' images -> gs://museum-archive/wikiart/images/
                                                    (only artists who died before PD_DEATH_YEAR unless --all)

Every step resumes: a finished artist file is skipped (--refresh fetches again), a half-done artist resumes
from WORK/wikiart/partial/<url>.jsonl, and images already in the bucket are skipped.

Rights: WikiArt's terms do not restrict automated access and robots.txt allows everything (checked
2026-10-07). We keep facts and public-domain images. WikiArt's own texts (biographies, descriptions) are kept
in the raw archive for comparison only and are not to be published.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import re
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from . import bucket, settings
from .http import Client

BASE = "https://www.wikiart.org/en"
# Category groups (DictionariesJson/<n>); 4-6 and 17+ are empty. Names from the ids artists and works carry.
GROUPS = {1: "movement", 2: "style", 3: "genre", 7: "school", 8: "collection", 9: "auction", 10: "nationality",
          11: "field", 12: "medium", 13: "institution", 14: "artwork_type", 15: "country", 16: "default"}
# Works are public domain in most countries 70 years after the artist's death (life + 70): died before 1956.
PD_DEATH_YEAR = datetime.now(timezone.utc).year - 70
WORKERS = 6

client = Client()


def log(*parts) -> None:
    print(*parts, flush=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json(path: Path, obj) -> None:
    """Atomic: a crash never leaves half a file that a resume would trust."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def year_of(text: str | None) -> int | None:
    """The year of a WikiArt date string ("November 14, 1840", "1978", "c.1500")."""
    m = re.findall(r"(-?\d{3,4})", text or "")
    return int(m[-1]) if m else None


# --- categories and artists ------------------------------------------------------------------------------

def dictionaries() -> None:
    out = {}
    for group, name in GROUPS.items():
        items = client.json(f"{BASE}/App/wiki/DictionariesJson/{group}") or []
        out[str(group)] = {"name": name, "items": items}
        log(f"group {group} ({name}): {len(items)}")
    doc = {"fetchedAt": now_iso(), "groups": out}
    write_json(settings.WIKIART / "dictionaries.json", doc)
    bucket.put_json("wikiart/dictionaries.json", doc)


def artists() -> None:
    items = client.json(f"{BASE}/App/Artist/AlphabetJson?v=new&inPublicDomain=false") or []
    doc = {"fetchedAt": now_iso(), "artists": items}
    write_json(settings.WIKIART / "artists.json", doc)
    bucket.put_json("wikiart/artists.json", doc)
    log(f"{len(items)} artists")


def crosswalk() -> None:
    """Wikidata items with a WikiArt artist ID (P6002): the reliable join to our artists (by QID)."""
    query = """SELECT ?item ?wikiart ?itemLabel WHERE { ?item wdt:P6002 ?wikiart .
               SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } }"""
    r = client.get("https://query.wikidata.org/sparql", params={"query": query, "format": "json"},
                   headers={"Accept": "application/sparql-results+json"})
    r.raise_for_status()
    rows = [{"qid": b["item"]["value"].rsplit("/", 1)[-1], "wikiart": b["wikiart"]["value"],
             "label": b.get("itemLabel", {}).get("value")} for b in r.json()["results"]["bindings"]]
    doc = {"fetchedAt": now_iso(), "rows": rows}
    write_json(settings.WIKIART / "wikidata.json", doc)
    bucket.put_json("wikidata/wikiart_ids.json", doc)
    log(f"{len(rows)} Wikidata items with a WikiArt ID")


# --- API v2: proper category ids, and artists with their periods, series and related artists -----------------
# The keyless API allows about 400 requests an hour ("Free API limit exceeded", HTTP 500, then a wait). We stay
# at one request every 10 s (360 an hour) and wait out the limit when it is hit anyway. ~130 requests in all.

V2 = f"{BASE}/api/2"
V2_INTERVAL = 10.0
V2_STATE = settings.WORK / "wikiart" / "v2"


class ServerFault(RuntimeError):
    """WikiArt's server fails on this request itself (not the hourly limit): retrying will not help."""


def v2_get(path: str, params: dict) -> dict:
    import requests
    from urllib.parse import unquote
    session = requests.Session()
    session.headers["User-Agent"] = settings.USER_AGENT
    params = {k: unquote(v) if k == "paginationToken" else v for k, v in params.items()}
    for attempt in range(40):  # up to ~7 hours of waiting for the hourly limit
        time.sleep(V2_INTERVAL)
        try:
            r = session.get(f"{V2}/{path}", params=params, timeout=60)
        except requests.RequestException as exc:
            log(f"{path}: {exc}; retrying")
            continue
        if r.status_code == 200:
            return r.json()
        if "limit exceeded" in r.text.lower():
            log(f"{path}: hourly API limit reached; waiting 10 minutes")
            time.sleep(600)
            continue
        log(f"{path}: HTTP {r.status_code} {r.text[:120]}; retrying")
        if r.status_code == 500 and "un-representable" in r.text and attempt >= 2:
            # a broken record on WikiArt's side (seen 2026-10-08 on page 86 of UpdatedArtists)
            raise ServerFault(r.text[:200])
        time.sleep(min(300, 10 * 2 ** min(attempt, 5)))
    raise RuntimeError(f"{path} kept failing")


def v2_pages(path: str, params: dict, name: str) -> list[dict]:
    """Every page of a paginated v2 list, resumable: pages so far are kept in WORK/wikiart/v2/<name>.jsonl."""
    V2_STATE.mkdir(parents=True, exist_ok=True)
    pages_file = V2_STATE / f"{name}.jsonl"
    rows: list[dict] = []
    token = None
    if pages_file.exists():
        for line in pages_file.read_text(encoding="utf-8").splitlines():
            page = json.loads(line)
            rows += page["data"]
            token = page.get("paginationToken") if page.get("hasMore") else "DONE"
    while token != "DONE":
        try:
            page = v2_get(path, {**params, **({"paginationToken": token} if token else {})})
        except ServerFault as exc:
            log(f"{name}: WikiArt fails on the next page ({exc}); keeping the {len(rows)} rows so far")
            break
        with pages_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(page, ensure_ascii=False) + "\n")
        rows += page.get("data") or []
        token = page.get("paginationToken") if page.get("hasMore") else "DONE"
        log(f"{name}: {len(rows)} so far")
    return rows


def api_v2() -> None:
    dictionaries_v2 = []
    for group in GROUPS:
        dictionaries_v2 += v2_pages("DictionariesByGroup", {"group": group}, f"dictionaries-{group}")
    artists_v2 = v2_pages("UpdatedArtists", {}, "artists")
    doc = {"fetchedAt": now_iso(), "dictionaries": dictionaries_v2, "artists": artists_v2}
    write_json(settings.WIKIART / "api_v2.json", doc)
    bucket.put_json("wikiart/api_v2.json.gz", doc, gz=True)
    for f in V2_STATE.glob("*.jsonl"):
        f.unlink()  # a later run fetches fresh lists
    log(f"API v2: {len(dictionaries_v2)} categories, {len(artists_v2)} artists")


# --- media: what each work is made of ----------------------------------------------------------------------
# The work JSON carries no medium, but WikiArt lists the works of every medium (paintings-by-media/<url>, 60 a
# page). One pass over the ~365 media tags every work (oil, tempera, engraving, pencil ...), keyed by its image
# path, so prints and drawings can be told from paintings. A few thousand requests, at half the crawl's rate.
# WikiArt lists at most 3,600 works per medium (60 pages): the largest media (oil, canvas, paper) are partial.

def media() -> int:
    lister = Client(rates={"www.wikiart.org": 1.5})
    groups = read_json(settings.WIKIART / "dictionaries.json")["groups"]
    media_list = [d for d in groups["12"]["items"] if d.get("url")]
    part = settings.WORK / "wikiart" / "media.jsonl"
    part.parent.mkdir(parents=True, exist_ok=True)
    done = {json.loads(line)["medium"] for line in part.read_text(encoding="utf-8").splitlines()} if part.exists() else set()
    print(f"{len(media_list)} media, {len(done)} done", flush=True)
    for i, m in enumerate(media_list, 1):
        if m["url"] in done:
            continue
        works, page = [], 1
        while True:
            try:
                d = lister.json(f"{BASE}/paintings-by-media/{m['url']}?json=2&page={page}") or {}
            except Exception as exc:  # noqa: BLE001 - WikiArt fails on some media: keep the pages so far
                print(f"  {m['title']}: page {page} failed ({exc}); keeping {len(works)} works", flush=True)
                break
            batch = d.get("Paintings") or []
            works += [{"id": w.get("id"), "image": (w.get("image") or "").split("!")[0], "title": w.get("title")}
                      for w in batch]
            if not batch or len(works) >= (d.get("AllPaintingsCount") or 0) or page > 2000:
                break
            page += 1
        with part.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"medium": m["url"], "title": m["title"], "works": works}, ensure_ascii=False) + "\n")
        print(f"[{i}/{len(media_list)}] {m['title']}: {len(works)} works", flush=True)
    rows = [json.loads(line) for line in part.read_text(encoding="utf-8").splitlines()]
    doc = {"fetchedAt": now_iso(), "media": rows}
    write_json(settings.WIKIART / "media.json", doc)
    bucket.put_json("wikiart/media.json.gz", doc, gz=True)
    print(f"{sum(len(r['works']) for r in rows)} work-medium pairs in {len(rows)} media", flush=True)
    return 0


# --- one artist ---------------------------------------------------------------------------------------------

def artist_file(url: str) -> Path:
    return settings.WIKIART / "artists" / f"{url}.json"


def partial_file(url: str) -> Path:
    return settings.WORK / "wikiart" / "partial" / f"{url}.jsonl"


def crawl_artist(entry: dict, pool: cf.ThreadPoolExecutor) -> dict:
    url = entry["url"]
    details = client.json(f"{BASE}/{url}?json=2")
    works = client.json(f"{BASE}/App/Painting/PaintingsByArtist?artistUrl={url}&json=2") or []
    # resume: work details fetched before a crash or stop
    part = partial_file(url)
    part.parent.mkdir(parents=True, exist_ok=True)
    done: dict[int, dict] = {}
    if part.exists():
        for line in part.read_text(encoding="utf-8").splitlines():
            try:
                d = json.loads(line)
                done[d["contentId"]] = d
            except (ValueError, KeyError):
                pass  # a line cut short by a crash
    todo = [w for w in works if w.get("contentId") not in done]
    lock = threading.Lock()
    missing: list[int] = []

    def fetch(w: dict) -> None:
        d = client.json(f"{BASE}/App/Painting/ImageJson/{w['contentId']}")
        with lock:
            if d is None:
                missing.append(w["contentId"])
                return
            done[w["contentId"]] = d
            with part.open("a", encoding="utf-8") as f:
                f.write(json.dumps(d, ensure_ascii=False) + "\n")

    for future in cf.as_completed([pool.submit(fetch, w) for w in todo]):
        future.result()  # an error that outlived the retries stops this artist (resumable)
    merged = [{**w, "details": done.get(w.get("contentId"))} for w in works]
    doc = {"fetchedAt": now_iso(), "url": url, "listEntry": entry, "artist": details, "works": merged,
           "missingDetails": missing}
    write_json(artist_file(url), doc)
    bucket.put_json(f"wikiart/artists/{url}.json.gz", doc, gz=True)
    part.unlink(missing_ok=True)
    return doc


def crawl(limit: int | None, only: list[str] | None, refresh: bool) -> int:
    listing = read_json(settings.WIKIART / "artists.json")["artists"]
    if only:
        listing = [a for a in listing if a["url"] in set(only)]
    todo = [a for a in listing if refresh or not artist_file(a["url"]).exists()]
    log(f"{len(listing)} artists, {len(listing) - len(todo)} done, {len(todo)} to crawl")
    if limit:
        todo = todo[:limit]
    failed = []
    started = time.monotonic()
    works_total = 0
    with cf.ThreadPoolExecutor(WORKERS) as pool:
        for i, entry in enumerate(todo, 1):
            try:
                doc = crawl_artist(entry, pool)
            except Exception as exc:  # noqa: BLE001 - one artist's failure must not stop the crawl
                failed.append(entry["url"])
                log(f"FAILED {entry['url']}: {exc}")
                continue
            works_total += len(doc["works"])
            rate = works_total / max(1, time.monotonic() - started)
            log(f"[{i}/{len(todo)}] {entry['url']}: {len(doc['works'])} works "
                f"({len(doc['missingDetails'])} without details), {rate:.1f} works/s")
    if failed:
        log(f"{len(failed)} artists failed (run again to resume): {', '.join(failed[:20])}")
    return 1 if failed else 0


# --- images -------------------------------------------------------------------------------------------------

def image_url(work: dict) -> str | None:
    """The largest rendition WikiArt serves: the file without the "!Large.jpg" style suffix."""
    src = (work.get("details") or {}).get("image") or work.get("image")
    return src.split("!")[0] if src else None


def public_domain(doc: dict) -> bool:
    a = doc.get("artist") or {}
    death = year_of(a.get("deathDayAsString"))
    return death is not None and death < PD_DEATH_YEAR


def images(all_artists: bool, limit: int | None) -> int:
    have = bucket.names("wikiart/images/")
    log(f"{len(have)} images already in the bucket")
    jobs = []
    for path in sorted((settings.WIKIART / "artists").glob("*.json")):
        doc = read_json(path)
        if not all_artists and not public_domain(doc):
            continue
        for w in doc["works"]:
            src = image_url(w)
            if not src:
                continue
            slug = (w.get("details") or {}).get("url") or str(w.get("contentId"))
            ext = Path(src.split("?")[0]).suffix.lower() or ".jpg"
            name = f"wikiart/images/{doc['url']}/{slug}{ext}"
            if name not in have:
                jobs.append((name, src))
    if limit:
        jobs = jobs[:limit]
    log(f"{len(jobs)} images to fetch")
    failed = 0

    def fetch(job) -> None:
        name, src = job
        r = client.get(src)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} for {src}")
        bucket.put_bytes(name, r.content, r.headers.get("Content-Type", "image/jpeg"))

    with cf.ThreadPoolExecutor(WORKERS) as pool:
        futures = {pool.submit(fetch, j): j for j in jobs}
        for i, future in enumerate(cf.as_completed(futures), 1):
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001
                failed += 1
                log(f"FAILED {futures[future][1]}: {exc}")
            if i % 500 == 0:
                log(f"{i}/{len(jobs)} images, {failed} failed")
    log(f"done: {len(jobs) - failed} images stored, {failed} failed")
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    settings.ensure_dirs()
    p = argparse.ArgumentParser(prog="python -m archive.wikiart")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("dictionaries")
    sub.add_parser("artists")
    sub.add_parser("crosswalk")
    sub.add_parser("api-v2")
    sub.add_parser("media")
    c = sub.add_parser("crawl")
    c.add_argument("--limit", type=int)
    c.add_argument("--artists", help="comma-separated WikiArt artist urls")
    c.add_argument("--refresh", action="store_true")
    i = sub.add_parser("images")
    i.add_argument("--all", action="store_true", help="also artists who died less than 70 years ago")
    i.add_argument("--limit", type=int)
    a = p.parse_args(argv)
    if a.cmd == "dictionaries":
        dictionaries()
    elif a.cmd == "artists":
        artists()
    elif a.cmd == "crosswalk":
        crosswalk()
    elif a.cmd == "api-v2":
        api_v2()
    elif a.cmd == "media":
        return media()
    elif a.cmd == "crawl":
        return crawl(a.limit, a.artists.split(",") if a.artists else None, a.refresh)
    elif a.cmd == "images":
        return images(a.all, a.limit)
    return 0


if __name__ == "__main__":
    sys.exit(main())
