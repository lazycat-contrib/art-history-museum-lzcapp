"""Paths, the bucket and the crawl limits shared by every archive command.

Local data lives under data/ (C:\\MyDrive is a Google Drive mirror, so only finished, compact files go there):
- data/wikipedia/   the museum's own snapshot (npm run ingest / enrich / repair-data): museum.json, artists/*.json
- data/wikiart/     the WikiArt crawl: dictionaries.json, artists.json, artists/<url>.json (artist + every work)
- data/taxonomy/    our own movements, schools, genres and themes, and how source categories map to them
- data/museum.duckdb   the warehouse built from all of the above (python -m archive.warehouse build)
Work in progress (partial crawls, the media manifest) goes to WORK, outside the Drive folder.
Raw copies and media go to the bucket gs://museum-archive.
"""
from __future__ import annotations

import os
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data"
WIKIPEDIA = DATA / "wikipedia"
WIKIART = DATA / "wikiart"
TAXONOMY = DATA / "taxonomy"
WAREHOUSE = Path(os.environ.get("MUSEUM_WAREHOUSE") or DATA / "museum.duckdb")
# Not TEMP: Dagster gives every command its own TEMP folder and removes it afterwards.
WORK = Path(os.environ.get("MUSEUM_ARCHIVE_WORK")
            or Path(os.environ.get("LOCALAPPDATA") or Path.home() / ".cache") / "museum-archive")

BUCKET = os.environ.get("MUSEUM_ARCHIVE_BUCKET", "museum-archive")

# Wikimedia's robot policy asks for a descriptive User-Agent with a way to reach the operator.
USER_AGENT = ("ArtFromPixelsMuseum/1.0 (+https://artmuseum.artfrompixels.com; "
              "https://github.com/justdataplease/art-history-museum) python-requests")

# Requests per second per host, across all threads. WikiArt sits behind Cloudflare and is a small volunteer
# project: stay gentle. upload.wikimedia.org serves cached thumbnails from its CDN.
RATE = {"www.wikiart.org": 3.0, "uploads0.wikiart.org": 4.0, "upload.wikimedia.org": 4.0,
        "query.wikidata.org": 0.5}
DEFAULT_RATE = 2.0


def ensure_dirs() -> None:
    for d in (WIKIART / "artists", TAXONOMY, WORK):
        d.mkdir(parents=True, exist_ok=True)
