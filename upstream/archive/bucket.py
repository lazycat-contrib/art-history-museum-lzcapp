"""The archive bucket (gs://museum-archive): raw source responses, media and the warehouse's Parquet copy.

Layout:
  wikiart/dictionaries.json, wikiart/artists.json        the category lists and the artist list, as fetched
  wikiart/artists/<artist-url>.json.gz                    one artist: details, works list, every work's details
  wikiart/images/<artist-url>/<work-url>.<ext>            the works' images (public domain by default)
  wikidata/wikiart_ids.json                               Wikidata items with a WikiArt ID (P6002)
  wikimedia/<path of the upload.wikimedia.org URL>        the exact files the museum requests, decoded path:
                                                          wikimedia/wikipedia/commons/thumb/a/ab/F.jpg/960px-F.jpg
  warehouse/<schema>/<table>.parquet                      the DuckDB warehouse, table by table
Credentials: Application Default Credentials (gcloud auth application-default login), or
GOOGLE_APPLICATION_CREDENTIALS.
"""
from __future__ import annotations

import gzip
import json
import threading
from pathlib import Path

from google.cloud import storage

from . import settings

_local = threading.local()


def bucket() -> storage.Bucket:
    b = getattr(_local, "bucket", None)
    if b is None:
        b = storage.Client().bucket(settings.BUCKET)
        _local.bucket = b
    return b


def put_bytes(name: str, data: bytes, content_type: str, *, gzipped: bool = False) -> None:
    blob = bucket().blob(name)
    if gzipped:
        blob.content_encoding = "gzip"
    blob.upload_from_string(data, content_type=content_type, retry=storage.retry.DEFAULT_RETRY)


def put_json(name: str, obj, *, gz: bool = False) -> None:
    data = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if gz:
        put_bytes(name, gzip.compress(data, 6), "application/json", gzipped=True)
    else:
        put_bytes(name, data, "application/json")


def put_file(name: str, path: Path, content_type: str | None = None) -> None:
    bucket().blob(name).upload_from_filename(str(path), content_type=content_type,
                                             retry=storage.retry.DEFAULT_RETRY)


def names(prefix: str) -> set[str]:
    """Every object name under `prefix` (one listing; used to resume)."""
    return {b.name for b in storage.Client().list_blobs(settings.BUCKET, prefix=prefix, fields="items(name),nextPageToken")}
