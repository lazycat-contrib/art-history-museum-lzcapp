"""The archive's assets. Materializing one runs one command (archive/README.md has the same commands by hand).

wikiart/    dictionaries, artist_list, wikidata_ids, artists (the crawl), images
wikipedia/  snapshot (data/wikipedia: npm run ingest / enrich / repair-data, outside Dagster), media_manifest,
            media (the mirror in gs://museum-archive/wikimedia)
taxonomy/   curated (data/taxonomy/*.csv: edited by hand)
warehouse/  museum_duckdb (data/museum.duckdb), lake (its tables as Parquet in gs://museum-archive/warehouse)

Pools have one slot each (dagster/home/dagster.yaml): never two WikiArt or two Wikimedia commands at once,
whichever job started them, so the sites see one polite crawler.
"""
from datetime import timedelta
from typing import Callable

import dagster as dg
from pydantic import Field
from scraper_framework.dagster.steps import fail, last_line, report

from . import core


class NoConfig(dg.Config):
    pass


class CrawlConfig(dg.Config):
    limit: int = Field(default=0, description="Crawl at most this many artists (0: all that are not done yet).")
    refresh: bool = Field(default=False, description="Fetch artists that are already done again.")
    artists: list[str] = Field(default=[], description="Only these WikiArt artist urls, e.g. claude-monet.")


class ImagesConfig(dg.Config):
    all_artists: bool = Field(default=False, description="Also artists who died less than 70 years ago "
                                                        "(their works are still in copyright: archive only).")
    limit: int = Field(default=0, description="At most this many images (0: all missing).")


class MediaConfig(dg.Config):
    kinds: list[str] = Field(default=["portrait", "thumb", "wall", "near", "inspect"],
                             description="In this order. Add 'original' for the full files the site never loads "
                                         "(~340 GB).")
    limit: int = Field(default=0, description="At most this many files (0: all missing).")


def command_asset(key: str, *, group: str, description: str, argv: Callable[[dg.Config], list[str]],
                  config_cls: type[dg.Config] = NoConfig, deps: tuple[str, ...] = (), pool: str | None = None,
                  timeout: timedelta = core.TIMEOUT["short"], kinds: set[str] | None = None) -> dg.AssetsDefinition:
    asset_key = dg.AssetKey(key.split("/"))

    @dg.asset(key=asset_key, group_name=group, description=description, pool=pool,
              deps=[dg.AssetKey(d.split("/")) for d in deps], kinds=kinds or {"python"})
    def _asset(context: dg.AssetExecutionContext, config: config_cls):  # type: ignore[valid-type]
        args = argv(config)
        try:
            code, lines = core.run_command(args, label=key, timeout=timeout, log=context.log)
        except core.StepError as exc:
            fail(context, str(exc), f"{key}: {exc}")
        if code:
            context.log.error(f"{key} exited {code}; its last lines:\n" + "\n".join(lines[-30:]))
            fail(context, f"{key} exited {code}", f"{key}: failed (exit {code}): {last_line(lines)}")
        report(context, f"{key}: {last_line(lines)}")
        return dg.MaterializeResult(metadata={"last_line": last_line(lines),
                                              "output": dg.MetadataValue.md("```\n" + "\n".join(lines[-40:]) + "\n```")})

    _asset.__name__ = key.replace("/", "__")
    return _asset


def crawl_argv(c: "CrawlConfig") -> list[str]:
    args = ["crawl"]
    if c.limit:
        args += ["--limit", c.limit]
    if c.refresh:
        args.append("--refresh")
    if c.artists:
        args += ["--artists", ",".join(c.artists)]
    return core.archive_argv("wikiart", *args)


def images_argv(c: ImagesConfig) -> list[str]:
    return core.archive_argv("wikiart", "images", *(["--all"] if c.all_artists else []),
                             *(["--limit", c.limit] if c.limit else []))


def media_argv(c: MediaConfig) -> list[str]:
    return core.archive_argv("wikimedia", "mirror", "--kinds", ",".join(c.kinds),
                             *(["--limit", c.limit] if c.limit else []))


# --- sources maintained outside Dagster ------------------------------------------------------------------------

WIKIPEDIA_SNAPSHOT = dg.AssetSpec(
    dg.AssetKey(["wikipedia", "snapshot"]), group_name="wikipedia", kinds={"json"},
    description="data/wikipedia: the museum's snapshot from Wikipedia, Wikidata and Commons, built by "
                "npm run ingest / enrich / repair-data (docs/DEVELOPMENT.md).")
TAXONOMY = dg.AssetSpec(
    dg.AssetKey(["taxonomy", "curated"]), group_name="taxonomy", kinds={"csv"},
    description="data/taxonomy/terms.csv and mappings.csv: our own movements, schools, genres and themes, and "
                "which source categories map to them. Edited by hand; the warehouse reads them.")

# --- WikiArt ------------------------------------------------------------------------------------------------

WIKIART = [
    command_asset("wikiart/dictionaries", group="wikiart", pool="wikiart",
                  description="WikiArt's category groups (movements, styles, genres, schools, nationalities, media, "
                              "collections, ...) -> data/wikiart/dictionaries.json",
                  argv=lambda c: core.archive_argv("wikiart", "dictionaries")),
    command_asset("wikiart/artist_list", group="wikiart", pool="wikiart",
                  description="WikiArt's artist list -> data/wikiart/artists.json",
                  argv=lambda c: core.archive_argv("wikiart", "artists")),
    command_asset("wikiart/wikidata_ids", group="wikiart", pool="wikidata",
                  description="Wikidata items with a WikiArt ID (P6002): the join to our artists by QID "
                              "-> data/wikiart/wikidata.json",
                  argv=lambda c: core.archive_argv("wikiart", "crosswalk")),
    command_asset("wikiart/api_v2", group="wikiart", pool="wikiart_api", timeout=core.TIMEOUT["crawl"],
                  description="WikiArt API v2 (keyless, ~400 requests an hour, paced at 360): every category with "
                              "its id, every artist with categories, periods, series and related artists "
                              "-> data/wikiart/api_v2.json. ~25 minutes; waits out the hourly limit; resumes.",
                  argv=lambda c: core.archive_argv("wikiart", "api-v2")),
    command_asset("wikiart/artists", group="wikiart", pool="wikiart", config_cls=CrawlConfig, argv=crawl_argv,
                  deps=("wikiart/artist_list",), timeout=core.TIMEOUT["crawl"],
                  description="The crawl: each artist's details, works list and every work's details "
                              "-> data/wikiart/artists/<url>.json and gs://museum-archive/wikiart/artists. Resumes."),
    command_asset("wikiart/images", group="wikiart", pool="wikiart", config_cls=ImagesConfig, argv=images_argv,
                  deps=("wikiart/artists",), timeout=core.TIMEOUT["media"], kinds={"python", "gcs"},
                  description="The works' images -> gs://museum-archive/wikiart/images (public domain unless "
                              "all_artists). Resumes."),
]

# --- Wikimedia media ------------------------------------------------------------------------------------------

WIKIMEDIA = [
    command_asset("wikipedia/media_manifest", group="wikipedia", deps=("wikipedia/snapshot",),
                  description="Every upload.wikimedia.org URL the site can request, from the site's own image "
                              "helpers (scripts/media-manifest.ts)",
                  argv=lambda c: core.archive_argv("wikimedia", "manifest")),
    command_asset("wikipedia/media", group="wikipedia", pool="wikimedia", config_cls=MediaConfig, argv=media_argv,
                  deps=("wikipedia/media_manifest",), timeout=core.TIMEOUT["media"], kinds={"python", "gcs"},
                  description="Copy the missing files to gs://museum-archive/wikimedia/<same path>. Resumes."),
]

# --- Wikidata ----------------------------------------------------------------------------------------------------

WIKIDATA = [
    command_asset("wikidata/artist_facts", group="wikidata", pool="wikidata", deps=("wikipedia/snapshot", "wikiart/wikidata_ids"),
                  description="Life facts for every artist (birth and death places, where they worked, teachers, "
                              "students, influences, movements, notable works) -> data/wikidata/artist_facts.json",
                  argv=lambda c: core.archive_argv("wikidata", "artists")),
    command_asset("wikidata/work_facts", group="wikidata", pool="wikidata", deps=("wikipedia/snapshot",),
                  timeout=core.TIMEOUT["crawl"],
                  description="What Wikidata knows about every museum work (type, movement, genre, material, "
                              "collection, location, depicts, inception, WikiArt ID) -> "
                              "data/wikidata/work_facts/*.parquet. Metadata only; resumes.",
                  argv=lambda c: core.archive_argv("wikidata", "works")),
    command_asset("wikidata/creator_works", group="wikidata", pool="wikidata", deps=("wikipedia/snapshot",),
                  description="Every Wikidata item whose creator is a museum artist (type, title, year, Commons "
                              "image, WikiArt ID) -> data/wikidata/creator_works.parquet: what WikiArt-only works "
                              "move onto. Metadata only.",
                  argv=lambda c: core.archive_argv("wikidata", "creators")),
]

# --- the warehouse ----------------------------------------------------------------------------------------------

WAREHOUSE = [
    command_asset("warehouse/museum_duckdb", group="warehouse", pool="warehouse", kinds={"duckdb"},
                  deps=("wikipedia/snapshot", "taxonomy/curated", "wikiart/dictionaries", "wikiart/artist_list",
                        "wikiart/wikidata_ids", "wikiart/api_v2", "wikiart/artists", "wikidata/artist_facts",
                        "wikidata/work_facts", "wikidata/creator_works", "wikipedia/media_manifest"),
                  description="data/museum.duckdb: wikipedia.*, wikiart.*, wikidata.*, taxonomy.*, compare.* "
                              "(missing artists, candidates, category coverage) and catalogue.* (every artist and "
                              "work in our own format, with links to every source)",
                  argv=lambda c: core.archive_argv("warehouse", "build")),
    command_asset("warehouse/lake", group="warehouse", pool="warehouse", kinds={"parquet", "gcs"},
                  deps=("warehouse/matched",),
                  description="Every warehouse table as Parquet in gs://museum-archive/warehouse/<schema>/<table>",
                  argv=lambda c: core.archive_argv("warehouse", "export")),
    command_asset("catalogue/fingerprints", group="warehouse", pool="wikimedia", deps=("warehouse/museum_duckdb",),
                  timeout=core.TIMEOUT["media"],
                  description="64-bit image fingerprints (dHash of a 210-250 px rendition) and colourfulness of the "
                              "works to compare across sources -> data/fingerprints/*.parquet; the images are read in "
                              "memory and nothing else is stored. Resumes.",
                  argv=lambda c: core.archive_argv("fingerprints")),
    command_asset("warehouse/matched", group="warehouse", pool="warehouse", kinds={"duckdb"},
                  deps=("catalogue/fingerprints",),
                  description="The warehouse again, now with every new fingerprint: the painting match across "
                              "sources (compare.work_match) and the placements use them",
                  argv=lambda c: core.archive_argv("warehouse", "build")),
    command_asset("wikimedia/file_info", group="warehouse", pool="wikimedia", deps=("warehouse/matched",),
                  description="Size, author and licence of the Commons files WikiArt-only works move onto -> "
                              "data/wikimedia/file_info.parquet (metadata only)",
                  argv=lambda c: core.archive_argv("wikimedia", "file-info")),
    command_asset("site/museum_json", group="warehouse", deps=("warehouse/matched", "wikimedia/file_info"),
                  kinds={"json"},
                  description="data/site/museum.json: the site's snapshot, the Wikipedia ingest plus the works only "
                              "WikiArt has for public-domain artists (archive/site.py)",
                  argv=lambda c: core.archive_argv("site")),
    command_asset("site/guide", group="warehouse", deps=("site/museum_json",), kinds={"json"},
                  description="The audio guide's scripts: one standard text per artist (their life) and per work, "
                              "with ids and versions -> data/site/guide/<artist>.json (archive/guide.py)",
                  argv=lambda c: core.archive_argv("guide", "build")),
]
