"""The museum archive's Dagster code location. Every run is started by hand; there are no schedules.

Jobs:
- wikiart_crawl     categories, artist list, Wikidata IDs, API v2 (categories with ids, artist periods), the crawl (resumes), then the warehouse and lake
- wikiart_images    on hold (v2): the works' images into the bucket. For now the site loads images straight from the sources
- wikimedia_media   the media manifest, then the mirror of the site's Wikimedia files into the bucket
- wikimedia_links   every Wikimedia link the site uses into the warehouse (the mirror, wikimedia_media, is on hold)
- wikidata_facts    life facts per artist and facts per work from Wikidata, then the warehouse
- warehouse_build   no crawling: data/museum.duckdb from what is on disk, the lake, then data/site/museum.json
Any other choice: select assets in the asset graph and Materialize.
"""
import dagster as dg

from .assets import TAXONOMY, WAREHOUSE, WIKIART, WIKIDATA, WIKIMEDIA, WIKIPEDIA_SNAPSHOT


def job(name: str, keys: list[str], description: str):
    selection = dg.AssetSelection.assets(*[dg.AssetKey(k.split("/")) for k in keys])
    # museum/job: one run of each job at a time (dagster.yaml, run queue)
    return dg.define_asset_job(name, selection=selection, description=description, tags={"museum/job": name})


WAREHOUSE_KEYS = ["warehouse/museum_duckdb", "catalogue/fingerprints", "warehouse/matched", "warehouse/lake",
                  "wikimedia/file_info", "site/museum_json", "site/guide"]

defs = dg.Definitions(
    assets=[WIKIPEDIA_SNAPSHOT, TAXONOMY, *WIKIART, *WIKIDATA, *WIKIMEDIA, *WAREHOUSE],
    jobs=[
        job("wikiart_crawl", ["wikiart/dictionaries", "wikiart/artist_list", "wikiart/wikidata_ids", "wikiart/api_v2",
                              "wikiart/artists",
                              *WAREHOUSE_KEYS],
            "WikiArt: categories, artists, Wikidata IDs, every work's details (resumes), then the warehouse."),
        job("wikiart_images", ["wikiart/images"], "On hold (v2): WikiArt images into gs://museum-archive/wikiart/images."),
        job("wikiart_api_v2", ["wikiart/api_v2"], "WikiArt API v2 only (paced under its hourly limit)."),
        job("wikimedia_links", ["wikipedia/media_manifest", *WAREHOUSE_KEYS],
            "Every Wikimedia link the site uses into the warehouse (wikipedia.media); downloads nothing."),
        job("wikimedia_media", ["wikipedia/media_manifest", "wikipedia/media"],
            "On hold: copy the site's Wikimedia files into gs://museum-archive/wikimedia (resumes)."),
        job("wikidata_facts", ["wikidata/artist_facts", "wikidata/work_facts", "wikidata/creator_works", *WAREHOUSE_KEYS],
            "Artist and work facts from Wikidata (metadata only), then the warehouse."),
        job("warehouse_build", WAREHOUSE_KEYS, "data/museum.duckdb from the files on disk, then Parquet to the bucket."),
    ],
)
