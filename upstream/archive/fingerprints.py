"""Image fingerprints: a 64-bit difference hash (dHash) of a small rendition of each image, to tell when two
sources show the same painting under different titles or dates.

    python -m archive.fingerprints [--limit N]

Fingerprints are computed for the images that need comparing: every work of every museum artist matched on
WikiArt, on both sides (their Wikimedia 250 px thumbnail, and WikiArt's 210 px "!PinterestSmall" rendition), so
each painting can be matched across the sources (compare.work_match) and WikiArt-only works checked; and the
Commons images of the artists' other Wikidata works, so a WikiArt-only work can move onto Wikidata and Commons
(compare.wikiart_to_wikidata). Small renditions only: about 10 KB each, read in memory; nothing is stored but
two numbers per image: the hash, and the image's colourfulness (Hasler & Suesstrunk 2003), which tells a
black-and-white print or pen drawing from a painting when WikiArt lists no medium for the work.
Results go to data/fingerprints/part-<time>.parquet (url, dhash, colorfulness, fetched_at), appended in parts so
a stopped run keeps what it has; a URL already fingerprinted is never fetched again (a WikiArt one fingerprinted
before colourfulness was measured is fetched once more). The warehouse loads them
(catalogue.image_fingerprints) and archive/site.py uses them.

Two images whose hashes differ in at most SAME_IMAGE bits show the same painting: different photographs and
crops of one canvas land within a few bits, different compositions far apart.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import io
import math
import sys
import time
from datetime import datetime, timezone

import duckdb
from PIL import Image

from . import settings
from .http import Client

DIR = settings.DATA / "fingerprints"
GLOB = DIR / "*.parquet"
SAME_IMAGE = 10
# Colourfulness below MONOCHROME is a black-and-white or single-colour image. Measured on WikiArt works with a
# known medium (200 each, 2026-10-08): no oil or tempera painting scored under 15 (5th percentile 23.5), while
# 45% of prints and 42% of drawings scored under 12.
MONOCHROME = 12.0
WORKERS = 8
PART_EVERY = 500

client = Client()

NEEDED = """
WITH artists AS (SELECT DISTINCT artist_slug, wikiart_url FROM compare.artist_match)  -- matched on both sources
SELECT DISTINCT md.url FROM wikipedia.media md JOIN artists USING (artist_slug)
WHERE md.kind = 'thumb' AND md.work_slug IS NOT NULL
UNION
SELECT DISTINCT w.image_url || '!PinterestSmall.jpg' FROM wikiart.works w JOIN artists a ON a.wikiart_url = w.artist_url
WHERE w.image_url IS NOT NULL
UNION
-- the artists' other Wikidata works with a Commons image: what a WikiArt-only work may move onto
SELECT DISTINCT commons_thumb(c.image, 250) FROM wikidata.creator_works c
JOIN wikipedia.artists wa ON wa.qid = c.creator JOIN artists USING (artist_slug)
WHERE c.image IS NOT NULL AND c.qid NOT IN (SELECT qid FROM wikipedia.works WHERE qid IS NOT NULL)
"""


def dhash(data: bytes) -> int:
    """64-bit difference hash: grey 9x8, one bit per horizontal neighbour comparison. Signed, as DuckDB BIGINT."""
    img = Image.open(io.BytesIO(data)).convert("L").resize((9, 8), Image.Resampling.LANCZOS)
    px = list(img.getdata())
    bits = 0
    for row in range(8):
        for col in range(8):
            bits = (bits << 1) | (px[row * 9 + col] > px[row * 9 + col + 1])
    return bits - (1 << 64) if bits >= 1 << 63 else bits


def colorfulness(data: bytes) -> float:
    """Hasler & Suesstrunk's colourfulness on a 64x64 copy: 0 for grey, about 15+ for any painting."""
    img = Image.open(io.BytesIO(data)).convert("RGB").resize((64, 64), Image.Resampling.BILINEAR)
    rg = [r - g for r, g, b in img.getdata()]
    yb = [(r + g) / 2 - b for r, g, b in img.getdata()]
    mean_rg, mean_yb = sum(rg) / len(rg), sum(yb) / len(yb)
    sd_rg = (sum((v - mean_rg) ** 2 for v in rg) / len(rg)) ** 0.5
    sd_yb = (sum((v - mean_yb) ** 2 for v in yb) / len(yb)) ** 0.5
    return round(math.hypot(sd_rg, sd_yb) + 0.3 * math.hypot(mean_rg, mean_yb), 2)


def distance(a: int, b: int) -> int:
    return bin((a ^ b) & 0xFFFFFFFFFFFFFFFF).count("1")


def table(con) -> str:
    """The fingerprints as one row per URL (url, dhash, colorfulness, fetched_at), for a FROM clause. Parts
    written before colourfulness was measured read as NULL there."""
    parts = f"read_parquet('{str(GLOB).replace(chr(92), '/')}', union_by_name = true)"
    columns = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {parts}").fetchall()}
    colour = "max(colorfulness)" if "colorfulness" in columns else "NULL::DOUBLE"
    return (f"(SELECT url, arg_max(dhash, fetched_at) AS dhash, {colour} AS colorfulness, "
            f"max(fetched_at) AS fetched_at FROM {parts} GROUP BY url)")


def known() -> set[str]:
    if not any(DIR.glob("*.parquet")):
        return set()
    con = duckdb.connect()
    return {r[0] for r in con.execute(
        f"SELECT url FROM {table(con)} WHERE colorfulness IS NOT NULL OR url NOT LIKE '%wikiart.org%'").fetchall()}


def write_part(rows: list[tuple]) -> None:
    if not rows:
        return
    DIR.mkdir(parents=True, exist_ok=True)
    path = DIR / f"part-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}.parquet"
    con = duckdb.connect()
    con.execute("CREATE TABLE f (url VARCHAR, dhash BIGINT, colorfulness DOUBLE, fetched_at TIMESTAMP)")
    con.executemany("INSERT INTO f VALUES (?, ?, ?, ?)", rows)
    con.execute(f"COPY f TO '{str(path).replace(chr(92), '/')}' (FORMAT parquet)")


def run(limit: int | None) -> int:
    con = duckdb.connect(str(settings.WAREHOUSE), read_only=True)
    try:
        needed = [r[0] for r in con.execute(NEEDED).fetchall()]
    finally:
        con.close()
    have = known()
    todo = [u for u in needed if u not in have]
    if limit:
        todo = todo[:limit]
    print(f"{len(needed)} images to compare, {len(have)} fingerprinted already, {len(todo)} to fetch", flush=True)
    rows: list[tuple] = []
    failed = 0
    started = time.monotonic()

    def one(url: str) -> tuple:
        r = client.get(url)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")
        return url, dhash(r.content), colorfulness(r.content), datetime.now(timezone.utc).replace(tzinfo=None)

    with cf.ThreadPoolExecutor(WORKERS) as pool:
        futures = [pool.submit(one, u) for u in todo]
        for i, f in enumerate(cf.as_completed(futures), 1):
            try:
                rows.append(f.result())
            except Exception:  # noqa: BLE001 - a missing image is retried next run
                failed += 1
            if len(rows) >= PART_EVERY:
                write_part(rows)
                rows = []
            if i % 1000 == 0 or i == len(todo):
                print(f"{i}/{len(todo)} images, {failed} failed, {(time.monotonic() - started) / 60:.0f} min", flush=True)
    write_part(rows)
    print(f"done: {len(todo) - failed} fingerprints, {failed} failed (fetched again next run)", flush=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m archive.fingerprints")
    p.add_argument("--limit", type=int)
    return run(p.parse_args(argv).limit)


if __name__ == "__main__":
    sys.exit(main())
