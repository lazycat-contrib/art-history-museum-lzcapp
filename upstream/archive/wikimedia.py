"""The museum's Wikimedia media: every link in the warehouse (wikipedia.media), and a mirror into the bucket.

On hold since 2026-10-08 (owner): the site keeps loading from Wikimedia and the links are kept in the warehouse;
the mirror waits for a hosting decision. About 2,000 files were copied before the pause.

    python -m archive.wikimedia manifest      every URL the site can request (npx tsx scripts/media-manifest.ts)
                                              -> WORK/wikimedia/media-manifest.tsv
    python -m archive.wikimedia mirror [--kinds portrait,thumb,wall,near,inspect] [--limit N]
                                              copy what is missing to gs://museum-archive/wikimedia/<path>
    python -m archive.wikimedia file-info     size, author and licence of the Commons files WikiArt-only works
                                              move onto (compare.wikiart_to_wikidata) -> data/wikimedia/file_info.parquet
                                              (metadata only; files already known are not asked again)

The object name is the URL's decoded path, so a URL on the mirror is the same URL with another host:
  https://upload.wikimedia.org/wikipedia/commons/thumb/a/ab/F.jpg/960px-F.jpg
  -> gs://museum-archive/wikimedia/wikipedia/commons/thumb/a/ab/F.jpg/960px-F.jpg
Kinds run in the order given (the small, most-viewed files first). "original" (full files the site never
loads, ~340 GB) is only mirrored when named. Files already in the bucket are skipped, so a run resumes.
Only free files are listed: works marked copyrighted have no image in the museum.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import subprocess
import sys
import time
from urllib.parse import unquote, urlsplit

from . import bucket, settings
from .http import Client

MANIFEST = settings.WORK / "wikimedia" / "media-manifest.tsv"
# what the site loads, smallest first; "original" (full files the site never loads as such) only when named
KINDS = ["portrait", "thumb", "wall", "near", "inspect"]
WORKERS = 6  # Wikimedia answers 429 above about this; the per-host rate limit is the cap
PREFIX = "wikimedia/"

client = Client()


def log(*parts) -> None:
    print(*parts, flush=True)


def object_name(url: str) -> str:
    return PREFIX + unquote(urlsplit(url).path.lstrip("/"))


def manifest() -> int:
    npx = "npx.cmd" if sys.platform == "win32" else "npx"
    return subprocess.call([npx, "tsx", "scripts/media-manifest.ts", str(MANIFEST)], cwd=settings.REPO)


def read_manifest() -> list[tuple[str, str]]:
    rows = []
    for line in MANIFEST.read_text(encoding="utf-8").splitlines():
        kind, url, *_ = line.split("\t")
        rows.append((kind, url))
    return rows


def mirror(kinds: list[str], limit: int | None) -> int:
    if not MANIFEST.exists():
        log("no manifest: run `python -m archive.wikimedia manifest` first")
        return 2
    rows = read_manifest()
    have = bucket.names(PREFIX)
    log(f"{len(rows)} URLs in the manifest, {len(have)} files already in the bucket")
    order = {k: i for i, k in enumerate(kinds)}
    todo = sorted(((k, u) for k, u in rows if k in order and object_name(u) not in have), key=lambda r: order[r[0]])
    if limit:
        todo = todo[:limit]
    log(f"{len(todo)} files to copy ({', '.join(kinds)})")
    failed = 0
    stored_bytes = 0
    started = time.monotonic()

    def copy(url: str) -> int:
        r = client.get(url)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")
        bucket.put_bytes(object_name(url), r.content, r.headers.get("Content-Type", "application/octet-stream"))
        return len(r.content)

    with cf.ThreadPoolExecutor(WORKERS) as pool:
        futures = {pool.submit(copy, url): (kind, url) for kind, url in todo}
        for i, future in enumerate(cf.as_completed(futures), 1):
            try:
                stored_bytes += future.result()
            except Exception as exc:  # noqa: BLE001 - one file must not stop the mirror
                failed += 1
                log(f"FAILED {futures[future][1]}: {exc}")
            if i % 1000 == 0 or i == len(todo):
                mins = (time.monotonic() - started) / 60
                log(f"{i}/{len(todo)} files, {stored_bytes / 1e9:.2f} GB, {failed} failed, {mins:.0f} min")
    log(f"done: {len(todo) - failed} copied, {failed} failed")
    return 1 if failed else 0


FILE_INFO = settings.DATA / "wikimedia" / "file_info.parquet"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"


def plain(html: str | None, cap: int = 0) -> str | None:
    """extmetadata HTML -> plain text (hidden spans dropped, tags stripped), as scripts/lib/credits.ts does."""
    import html as htmllib
    import re
    if not html:
        return None
    s = re.sub(r"<(style|script)\b[^>]*>.*?</\1>", " ", str(html), flags=re.S | re.I)
    for _ in range(3):
        s = re.sub(r'<(span|div)[^>]*style="[^"]*display:\s*none[^"]*"[^>]*>(?:(?!<\1).)*?</\1>', " ", s,
                   flags=re.S | re.I)
    s = re.sub(r"<[^>]*>", " ", re.sub(r"<br\s*/?>", " ", s, flags=re.I))
    s = re.sub(r"\s+", " ", htmllib.unescape(s)).strip()
    if cap and len(s) > cap:
        s = s[:cap - 1].rsplit(" ", 1)[0] + "…"
    return s or None


def file_info() -> int:
    import duckdb
    from datetime import datetime, timezone
    con = duckdb.connect(str(settings.WAREHOUSE), read_only=True)
    try:
        files = sorted({r[0] for r in con.execute(
            "SELECT DISTINCT commons_file FROM compare.wikiart_to_wikidata WHERE commons_file IS NOT NULL").fetchall()})
    finally:
        con.close()
    have: dict[str, tuple] = {}
    if FILE_INFO.exists():
        have = {r[0]: r for r in duckdb.sql(f"SELECT * FROM read_parquet('{FILE_INFO.as_posix()}')").fetchall()}
    todo = [f for f in files if f not in have]
    log(f"{len(files)} Commons files, {len(have)} known, {len(todo)} to ask")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = list(have.values())
    for i in range(0, len(todo), 50):
        batch = todo[i:i + 50]
        r = client.get(COMMONS_API, params={
            "action": "query", "format": "json", "formatversion": "2", "prop": "imageinfo",
            "iiprop": "size|extmetadata", "iiextmetadatalanguage": "en",
            "iiextmetadatafilter": "Artist|LicenseShortName|LicenseUrl|NonFree",
            "titles": "|".join(f"File:{f}" for f in batch)})
        r.raise_for_status()
        q = r.json().get("query", {})
        norm = {n["from"]: n["to"] for n in q.get("normalized", [])}
        pages = {pg["title"]: pg for pg in q.get("pages", [])}
        for f in batch:
            page = pages.get(norm.get(f"File:{f}", f"File:{f}"))
            info = (page or {}).get("imageinfo", [{}])[0] if page and not page.get("missing") else None
            if not info:
                continue
            em = info.get("extmetadata", {})
            v = lambda k: (em.get(k) or {}).get("value")  # noqa: E731
            non_free = bool(v("NonFree")) and v("NonFree") != "false"
            rows.append((f, info.get("width"), info.get("height"), info.get("size"), plain(v("Artist"), 120),
                         plain(v("LicenseShortName")) or ("Fair use" if non_free else "Unknown"),
                         (v("LicenseUrl") or "").strip() or None, non_free, now))
        if (i // 50) % 20 == 0:
            log(f"{min(i + 50, len(todo))}/{len(todo)} files")
    FILE_INFO.parent.mkdir(parents=True, exist_ok=True)
    out = duckdb.connect()
    out.execute("CREATE TABLE f (file VARCHAR, width INT, height INT, bytes BIGINT, author VARCHAR, license VARCHAR, "
                "license_url VARCHAR, non_free BOOLEAN, fetched_at TIMESTAMP)")
    out.executemany("INSERT INTO f VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    tmp = FILE_INFO.with_suffix(".tmp.parquet")
    out.execute(f"COPY f TO '{tmp.as_posix()}' (FORMAT parquet)")
    out.close()
    tmp.replace(FILE_INFO)
    log(f"done: {len(rows)} files in {FILE_INFO}")
    return 0


def main(argv: list[str] | None = None) -> int:
    settings.ensure_dirs()
    p = argparse.ArgumentParser(prog="python -m archive.wikimedia")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("manifest")
    sub.add_parser("file-info")
    m = sub.add_parser("mirror")
    m.add_argument("--kinds", default=",".join(KINDS), help="comma-separated, in order; add original for full files")
    m.add_argument("--limit", type=int)
    a = p.parse_args(argv)
    if a.cmd == "manifest":
        return manifest()
    if a.cmd == "file-info":
        return file_info()
    return mirror([k for k in a.kinds.split(",") if k], a.limit)


if __name__ == "__main__":
    sys.exit(main())
