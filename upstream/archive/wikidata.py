"""Facts from Wikidata, metadata only.

    python -m archive.wikidata artists      -> data/wikidata/artist_facts.json (+ the bucket)
    python -m archive.wikidata works        -> data/wikidata/work_facts/part-<time>.parquet (+ the bucket; resumes)
    python -m archive.wikidata creators     -> data/wikidata/creator_works.parquet (+ the bucket)

Artists: life facts for every artist we know (ours and WikiArt's), the material for rooms themed by an artist's
life and era (birth and death places, where they worked, schools, teachers and students, influences, movements,
notable works, citizenship). One row per artist, property and value: {qid, property, value, label, coord}.

Works: what Wikidata knows about each of the museum's works (wikipedia.works.qid): what it is (painting,
drawing, print ...), movement, genre, material, collection, location, what it depicts, inception, inventory
number and WikiArt ID. One row per work, property and value (qid, property, value, label, fetched_at), written
in parts so a stopped run keeps what it has; a work already fetched is not fetched again (--refresh fetches all).

Creators: every Wikidata item whose creator (P170) is a museum artist, with its type, English title, inception,
Commons image (P18) and WikiArt ID (P6002): one row per item and type (creator, qid, type, type_label, title,
inception, image, wikiart_id). The warehouse matches WikiArt-only works against them, so a work WikiArt adds
moves onto Wikidata and Commons when they have it (compare.wikiart_to_wikidata).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from . import bucket, settings
from .http import Client

PROPERTIES = {
    "P19": "birth_place", "P20": "death_place", "P937": "work_location", "P27": "citizenship",
    "P69": "educated_at", "P1066": "student_of", "P802": "student", "P737": "influenced_by",
    "P135": "movement", "P800": "notable_work", "P463": "member_of", "P1830": "owner_of",
    "P3919": "contributed_to", "P608": "exhibition_history",
}
BATCH = 120
OUT = settings.DATA / "wikidata" / "artist_facts.json"

WORK_PROPERTIES = {
    "P31": "instance_of", "P135": "movement", "P136": "genre", "P186": "material", "P195": "collection",
    "P276": "location", "P180": "depicts", "P921": "main_subject", "P571": "inception", "P1071": "created_in",
    "P217": "inventory_number", "P6002": "wikiart_id", "P2048": "height", "P2049": "width",
}
WORK_BATCH = 150
WORK_DIR = settings.DATA / "wikidata" / "work_facts"
WORK_GLOB = WORK_DIR / "*.parquet"

client = Client()


def artist_qids() -> list[str]:
    qids = set()
    museum = json.loads((settings.WIKIPEDIA / "museum.json").read_text(encoding="utf-8"))
    qids |= {a["qid"] for a in museum["artists"] if a.get("qid")}
    crosswalk = settings.WIKIART / "wikidata.json"
    if crosswalk.exists():
        qids |= {r["qid"] for r in json.loads(crosswalk.read_text(encoding="utf-8"))["rows"]}
    return sorted(qids)


def facts(qids: list[str]) -> list[dict]:
    props = " ".join(f"wdt:{p}" for p in PROPERTIES)
    query = f"""SELECT ?item ?p ?v ?vLabel ?coord WHERE {{
      VALUES ?item {{ {" ".join(f"wd:{q}" for q in qids)} }}
      VALUES ?p {{ {props} }}
      ?item ?p ?v .
      OPTIONAL {{ ?v wdt:P625 ?coord }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,fr,de,it,es,el". }} }}"""
    r = client.get("https://query.wikidata.org/sparql", params={"query": query, "format": "json"},
                   headers={"Accept": "application/sparql-results+json"})
    r.raise_for_status()
    rows = []
    for b in r.json()["results"]["bindings"]:
        pid = b["p"]["value"].rsplit("/", 1)[-1]
        value = b["v"]["value"]
        rows.append({"qid": b["item"]["value"].rsplit("/", 1)[-1], "property": PROPERTIES.get(pid, pid),
                     "value": value.rsplit("/", 1)[-1] if "wikidata.org/entity/" in value else value,
                     "label": b.get("vLabel", {}).get("value"), "coord": b.get("coord", {}).get("value")})
    return rows


def sparql(query: str) -> list[dict]:
    r = client.get("https://query.wikidata.org/sparql", params={"query": query, "format": "json"},
                   headers={"Accept": "application/sparql-results+json"})
    r.raise_for_status()
    return r.json()["results"]["bindings"]


def work_facts(qids: list[str]) -> list[tuple]:
    props = " ".join(f"wdt:{p}" for p in WORK_PROPERTIES)
    rows = sparql(f"""SELECT ?item ?p ?v ?vLabel WHERE {{
      VALUES ?item {{ {" ".join(f"wd:{q}" for q in qids)} }}
      VALUES ?p {{ {props} }}
      ?item ?p ?v .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,fr,de,it,es,nl,el". }} }}""")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    out = []
    for b in rows:
        value = b["v"]["value"]
        item = "wikidata.org/entity/" in value
        label = b.get("vLabel", {}).get("value")
        out.append((b["item"]["value"].rsplit("/", 1)[-1], WORK_PROPERTIES[b["p"]["value"].rsplit("/", 1)[-1]],
                    value.rsplit("/", 1)[-1] if item else value, label if item else None, now))
    # a work with no facts at all still gets a row, so it counts as fetched
    seen = {r[0] for r in out}
    out += [(q, "fetched", None, None, now) for q in qids if q not in seen]
    return out


def work_qids() -> list[str]:
    museum = json.loads((settings.WIKIPEDIA / "museum.json").read_text(encoding="utf-8"))
    return sorted({p["qid"] for a in museum["artists"] for p in a["paintings"] if p.get("qid")})


def works(refresh: bool) -> int:
    import duckdb
    qids = work_qids()
    have: set[str] = set()
    if not refresh and any(WORK_DIR.glob("*.parquet")):
        have = {r[0] for r in duckdb.sql(
            f"SELECT DISTINCT qid FROM read_parquet('{WORK_GLOB.as_posix()}')").fetchall()}
    todo = [q for q in qids if q not in have]
    print(f"{len(qids)} works with a Wikidata item, {len(have)} fetched already, {len(todo)} to fetch", flush=True)
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    rows: list[tuple] = []

    def flush() -> None:
        if not rows:
            return
        path = WORK_DIR / f"part-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}.parquet"
        con = duckdb.connect()
        con.execute("CREATE TABLE f (qid VARCHAR, property VARCHAR, value VARCHAR, label VARCHAR, "
                    "fetched_at TIMESTAMP)")
        con.executemany("INSERT INTO f VALUES (?, ?, ?, ?, ?)", rows)
        con.execute(f"COPY f TO '{path.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
        con.close()
        bucket.put_file(f"wikidata/work_facts/{path.name}", path, "application/vnd.apache.parquet")
        rows.clear()

    failed = 0

    def fetch(batch: list[str]) -> list[tuple]:
        """A batch the query service cannot answer (timeout, a bad item) is split in two and tried again."""
        nonlocal failed
        try:
            return work_facts(batch)
        except Exception as e:  # noqa: BLE001 - the rest of the run goes on; the next run retries these
            if len(batch) == 1:
                failed += 1
                print(f"  {batch[0]}: {e}", flush=True)
                return []
            half = len(batch) // 2
            return fetch(batch[:half]) + fetch(batch[half:])

    for i in range(0, len(todo), WORK_BATCH):
        rows += fetch(todo[i:i + WORK_BATCH])
        if len(rows) >= 50_000:
            flush()
        done = min(i + WORK_BATCH, len(todo))
        if done % 3000 < WORK_BATCH or done == len(todo):
            print(f"{done}/{len(todo)} works", flush=True)
    flush()
    print(f"done: {len(todo) - failed} works, {failed} failed (fetched again next run)", flush=True)
    return 0


CREATORS_OUT = settings.DATA / "wikidata" / "creator_works.parquet"
CREATOR_BATCH = 4


def creator_works(creators: list[str]) -> list[tuple]:
    rows = sparql(f"""SELECT ?creator ?item ?type ?typeLabel ?label ?inception ?image ?wikiart WHERE {{
      VALUES ?creator {{ {" ".join(f"wd:{q}" for q in creators)} }}
      ?item wdt:P170 ?creator ; wdt:P31 ?type .
      OPTIONAL {{ ?type rdfs:label ?typeLabel FILTER (lang(?typeLabel) = "en") }}
      OPTIONAL {{ ?item rdfs:label ?label FILTER (lang(?label) = "en") }}
      OPTIONAL {{ ?item wdt:P571 ?inception }}
      OPTIONAL {{ ?item wdt:P18 ?image }}
      OPTIONAL {{ ?item wdt:P6002 ?wikiart }} }}""")
    out = []
    for b in rows:
        def v(k):
            return b.get(k, {}).get("value")
        image = v("image")
        out.append((v("creator").rsplit("/", 1)[-1], v("item").rsplit("/", 1)[-1], v("type").rsplit("/", 1)[-1],
                    v("typeLabel"), v("label"), v("inception"),
                    # Special:FilePath/<name> -> the Commons file name
                    requests_unquote(image.rsplit("/", 1)[-1]) if image else None, v("wikiart")))
    return out


def requests_unquote(name: str) -> str:
    from urllib.parse import unquote
    return unquote(name).replace("_", " ")


def creators() -> int:
    import duckdb
    museum = json.loads((settings.WIKIPEDIA / "museum.json").read_text(encoding="utf-8"))
    qids = sorted({a["qid"] for a in museum["artists"] if a.get("qid")})
    print(f"{len(qids)} museum artists with a Wikidata item", flush=True)
    rows: list[tuple] = []
    failed: list[str] = []

    def fetch(batch: list[str]) -> list[tuple]:
        try:
            return creator_works(batch)
        except Exception as e:  # noqa: BLE001 - a prolific artist alone can time out; the rest goes on
            if len(batch) == 1:
                failed.append(batch[0])
                print(f"  {batch[0]}: {e}", flush=True)
                return []
            half = len(batch) // 2
            return fetch(batch[:half]) + fetch(batch[half:])

    for i in range(0, len(qids), CREATOR_BATCH):
        rows += fetch(qids[i:i + CREATOR_BATCH])
        done = min(i + CREATOR_BATCH, len(qids))
        if done % 100 < CREATOR_BATCH or done == len(qids):
            print(f"{done}/{len(qids)} artists, {len(rows)} rows", flush=True)
    CREATORS_OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = CREATORS_OUT.with_suffix(".tmp.parquet")
    con = duckdb.connect()
    con.execute("CREATE TABLE f (creator VARCHAR, qid VARCHAR, type VARCHAR, type_label VARCHAR, title VARCHAR, "
                "inception VARCHAR, image VARCHAR, wikiart_id VARCHAR)")
    con.executemany("INSERT INTO f VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.execute(f"COPY (SELECT DISTINCT * FROM f) TO '{tmp.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
    con.close()
    tmp.replace(CREATORS_OUT)
    bucket.put_file("wikidata/creator_works.parquet", CREATORS_OUT, "application/vnd.apache.parquet")
    print(f"done: {len(rows)} rows; {len(failed)} artists failed: {failed}", flush=True)
    return 0


def artists() -> int:
    qids = artist_qids()
    print(f"{len(qids)} artists with a Wikidata item", flush=True)
    rows: list[dict] = []
    for i in range(0, len(qids), BATCH):
        rows += facts(qids[i:i + BATCH])
        print(f"{min(i + BATCH, len(qids))}/{len(qids)} artists, {len(rows)} facts", flush=True)
    doc = {"fetchedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "properties": PROPERTIES,
           "rows": rows}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    tmp.replace(OUT)
    bucket.put_json("wikidata/artist_facts.json.gz", doc, gz=True)
    return 0


if __name__ == "__main__":
    args = sys.argv[1:]
    if args == ["artists"]:
        sys.exit(artists())
    if args and args[0] == "works" and set(args[1:]) <= {"--refresh"}:
        sys.exit(works("--refresh" in args))
    if args == ["creators"]:
        sys.exit(creators())
    sys.exit("usage: python -m archive.wikidata artists | works [--refresh] | creators")
