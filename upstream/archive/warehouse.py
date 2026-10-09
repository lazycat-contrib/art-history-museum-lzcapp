"""The warehouse: one DuckDB file (data/museum.duckdb) with every source side by side, our own taxonomy, and the
comparisons between them.

    python -m archive.warehouse build            (re)build data/museum.duckdb from data/wikipedia, data/wikiart
                                                 and data/taxonomy
    python -m archive.warehouse seed-taxonomy    add new candidate terms and mappings to data/taxonomy/*.csv
                                                 (never changes or removes a row that is already there)
    python -m archive.warehouse export           every table as Parquet -> gs://museum-archive/warehouse/
    python -m archive.warehouse to-postgres --dsn postgresql://...   copy every table into Postgres

Schemas (the same names in Postgres, so moving there is a copy, not a redesign):
  wikipedia.*  periods, artists, works, media (every Wikimedia link the site uses)   the museum's own snapshot
  wikiart.*    dictionaries, artists, artist_dictionaries, works, work_dictionaries, work_tags, work_media, wikidata
  taxonomy.*   terms, mappings (data/taxonomy, edited by hand), term_ancestors, term_lineage, artist_terms,
               work_terms, term_summary
  wikidata.*   artist_facts: life facts per artist (python -m archive.wikidata artists); work_facts: what each
               museum work is, its movement, genre, material, collection, subjects (python -m archive.wikidata works);
               creator_works: every item by a museum artist (python -m archive.wikidata creators)
  wikimedia.*  file_info: size, author and licence of Commons files (python -m archive.wikimedia file-info)
  compare.*    artist_match, work_match, missing_artists, our_artists_not_on_wikiart, artist_work_counts,
               category_coverage
  catalogue.*  artists, works: every artist and work of every source, merged where matched, with all links;
               work_sources, artist_sources (one row per source); image_fingerprints
  exhibition.* exhibitions, rooms (data/taxonomy/exhibitions.csv, exhibition_rooms.csv), room_works
               (plain tables: one row per artist and per work)
Tables use plain types (text, integers, doubles, booleans, text lists) that Postgres also has.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import duckdb

from . import settings
from .catalogue import CATALOGUE_SQL
from .exhibitions import EXHIBITION_SQL
from .wikiart import GROUPS

TERMS = settings.TAXONOMY / "terms.csv"
MAPPINGS = settings.TAXONOMY / "mappings.csv"
TERM_COLUMNS = ["term_id", "kind", "name", "parent_id", "start_year", "end_year", "place", "qid", "description"]
MAPPING_COLUMNS = ["source", "source_group", "source_key", "source_name", "term_id"]


def log(*parts) -> None:
    print(*parts, flush=True)


def sql_path(p: Path) -> str:
    return str(p).replace("\\", "/").replace("'", "''")


# --- sources ----------------------------------------------------------------------------------------------------

WIKIPEDIA_SQL = """
CREATE SCHEMA IF NOT EXISTS wikipedia;
-- every source file is parsed straight into typed structs (read_json with columns): no file ever becomes one
-- JSON value, which is what made large files slow and memory-hungry
CREATE OR REPLACE TEMP TABLE wp_doc AS
SELECT periods, artists FROM read_json('{museum}', maximum_object_size = 200000000, columns = {{
  periods: 'STRUCT(slug VARCHAR, name VARCHAR, startYear INTEGER, endYear INTEGER, color VARCHAR,
                   description VARCHAR, wikipediaUrl VARCHAR)[]',
  artists: 'STRUCT(slug VARCHAR, periodSlug VARCHAR, name VARCHAR, qid VARCHAR, birthYear INTEGER,
                   deathYear INTEGER, tagline VARCHAR, wikipediaUrl VARCHAR, portraitUrl VARCHAR,
                   paintings STRUCT(slug VARCHAR, title VARCHAR, "year" INTEGER, imageUrl VARCHAR,
                                    imageWidth INTEGER, imageHeight INTEGER, widthCm DOUBLE, heightCm DOUBLE,
                                    pageviews BIGINT, sitelinks INTEGER, copyrighted BOOLEAN,
                                    wikipediaUrl VARCHAR, qid VARCHAR)[])[]'}});

CREATE OR REPLACE TABLE wikipedia.periods AS
SELECT p.slug AS period_slug, p.name, p.startYear AS start_year, p.endYear AS end_year, p.color, p.description,
       p.wikipediaUrl AS wikipedia_url
FROM wp_doc, unnest(periods) AS t(p);

CREATE OR REPLACE TEMP TABLE wp_artists AS SELECT a FROM wp_doc, unnest(artists) AS t(a);

CREATE OR REPLACE TABLE wikipedia.artists AS
SELECT a.slug AS artist_slug, a.periodSlug AS period_slug, a.name, a.qid, a.birthYear AS birth_year,
       a.deathYear AS death_year, a.tagline, a.wikipediaUrl AS wikipedia_url, wiki_title(a.wikipediaUrl) AS wikipedia_title,
       name_key(a.name) AS name_key, a.portraitUrl AS portrait_url, len(a.paintings)::INT AS work_count
FROM wp_artists;

-- every Wikimedia link the site uses, per work and use (thumb, wall, near, inspect, original, portrait sizes):
-- scripts/media-manifest.ts (python -m archive.wikimedia manifest)
CREATE OR REPLACE TABLE wikipedia.media AS
SELECT artist_slug, nullif(work_slug, '') AS work_slug, kind, url, TRY_CAST(bytes AS BIGINT) AS bytes,
       'wikimedia/' || url_decode(regexp_extract(url, 'https://upload.wikimedia.org/(.*)', 1)) AS mirror_object
FROM read_csv('{wikimedia_media}', delim = '\\t', header = true, all_varchar = true, quote = '');

CREATE OR REPLACE TABLE wikipedia.works AS
SELECT a.slug AS artist_slug, w.slug AS work_slug, w.qid, w.title, w."year" AS year, w.imageUrl AS image_url,
       w.imageWidth AS image_width, w.imageHeight AS image_height, w.widthCm AS width_cm, w.heightCm AS height_cm,
       w.pageviews, w.sitelinks, coalesce(w.copyrighted, false) AS copyrighted, w.wikipediaUrl AS wikipedia_url,
       title_key(w.title) AS title_key
FROM wp_artists, unnest(a.paintings) AS t(w);
"""

WIKIART_SQL = """
CREATE SCHEMA IF NOT EXISTS wikiart;
CREATE OR REPLACE TEMP TABLE group_names AS SELECT * FROM (VALUES {group_values}) AS t(group_id, group_name);

-- categories: the old JSON (numeric ids, 0 for about a third of them) and API v2 (an id for every one), joined on
-- group and url. dictionary_key is the v2 id when there is one, else 'v1-<id>' (or 'v1-<group>-<url>').
CREATE OR REPLACE TEMP TABLE wa_dict_doc AS
SELECT groups FROM read_json('{dictionaries}', columns = {{groups: 'JSON'}});
CREATE OR REPLACE TEMP TABLE v1_dict AS
SELECT DISTINCT nullif((d->>'id')::BIGINT, 0) AS v1_id, g.key::INT AS group_id, d->>'title' AS title, d->>'url' AS url
FROM wa_dict_doc, json_each(groups) AS g, unnest(CAST(g.value->'items' AS JSON[])) AS t(d);
CREATE OR REPLACE TEMP TABLE v2_doc AS
SELECT dictionaries, artists FROM read_json('{api_v2}', maximum_object_size = 64000000, columns = {{
  dictionaries: 'STRUCT(id VARCHAR, title VARCHAR, url VARCHAR, "group" INTEGER)[]',
  artists: 'STRUCT(id VARCHAR, url VARCHAR, originalArtistName VARCHAR, gender VARCHAR, activeYearsStart VARCHAR,
                   activeYearsCompletion VARCHAR, image VARCHAR, biography VARCHAR, dictionaries VARCHAR[],
                   periods JSON[], series JSON[], relatedArtists VARCHAR[])[]'}});
CREATE OR REPLACE TEMP TABLE v2_dict AS
SELECT DISTINCT d.id AS v2_id, d."group" AS group_id, d.title, d.url FROM v2_doc, unnest(dictionaries) AS t(d);
CREATE OR REPLACE TABLE wikiart.dictionaries AS
SELECT coalesce(v2.v2_id, 'v1-' || v1.v1_id::VARCHAR,
                'v1-' || coalesce(v1.group_id, v2.group_id)::VARCHAR || '-' || coalesce(v1.url, v2.url)) AS dictionary_key,
       coalesce(v1.group_id, v2.group_id) AS group_id, gn.group_name,
       coalesce(v2.title, v1.title) AS title, coalesce(v2.url, v1.url) AS url, v1.v1_id, v2.v2_id
FROM v1_dict v1 FULL JOIN v2_dict v2 ON v1.group_id = v2.group_id AND v1.url = v2.url
LEFT JOIN group_names gn ON gn.group_id = coalesce(v1.group_id, v2.group_id)
QUALIFY row_number() OVER (PARTITION BY dictionary_key ORDER BY v1.v1_id NULLS LAST) = 1;

-- every artist from the list; the crawl's details where crawled, API v2 where fetched
CREATE OR REPLACE TEMP TABLE wa_list AS
SELECT a FROM read_json('{artist_list}', maximum_object_size = 64000000, columns = {{
  artists: 'STRUCT(url VARCHAR, artistName VARCHAR, birthDayAsString VARCHAR, deathDayAsString VARCHAR,
                   wikipediaUrl VARCHAR, dictonaries BIGINT[])[]'}}), unnest(artists) AS t(a);
CREATE OR REPLACE TABLE wikiart.artist_list AS
SELECT a.url AS artist_url, a.artistName AS name, year_of(a.birthDayAsString) AS birth_year,
       year_of(a.deathDayAsString) AS death_year, a.wikipediaUrl AS wikipedia_url,
       wiki_title(a.wikipediaUrl) AS wikipedia_title, name_key(a.artistName) AS name_key,
       coalesce(a.dictonaries, []) AS v1_dictionary_ids
FROM wa_list;

CREATE OR REPLACE TEMP TABLE wa_doc AS
SELECT * FROM read_json('{artists_glob}', maximum_object_size = 64000000, columns = {{
  url: 'VARCHAR', fetchedAt: 'VARCHAR', missingDetails: 'BIGINT[]',
  artist: 'STRUCT(contentId BIGINT, artistName VARCHAR, OriginalArtistName VARCHAR, gender VARCHAR,
                  activeYearsStart VARCHAR, activeYearsCompletion VARCHAR, image VARCHAR, biography VARCHAR,
                  dictonaries BIGINT[])',
  works: 'STRUCT(title VARCHAR, contentId BIGINT, completitionYear VARCHAR, yearAsString VARCHAR, width INTEGER,
                 height INTEGER, image VARCHAR,
                 details STRUCT(url VARCHAR, style VARCHAR, genre VARCHAR, material VARCHAR, technique VARCHAR,
                                sizeX VARCHAR, sizeY VARCHAR, diameter VARCHAR, galleryName VARCHAR,
                                location VARCHAR, period VARCHAR, serie VARCHAR, tags VARCHAR, description VARCHAR,
                                auction VARCHAR, lastPrice VARCHAR, dictionaries BIGINT[]))[]'}});

CREATE OR REPLACE TEMP TABLE v2_artist AS SELECT a.id AS v2_id, a.url AS artist_url, a FROM v2_doc, unnest(artists) AS t(a);

CREATE OR REPLACE TABLE wikiart.artists AS
SELECT l.artist_url, d.artist.contentId AS content_id, v2.v2_id,
       coalesce(d.artist.artistName, l.name) AS name,
       coalesce(d.artist.OriginalArtistName, v2.a.originalArtistName) AS original_name,
       coalesce(d.artist.gender, v2.a.gender) AS gender, l.birth_year, l.death_year,
       coalesce(TRY_CAST(d.artist.activeYearsStart AS INT), TRY_CAST(v2.a.activeYearsStart AS INT)) AS active_start,
       coalesce(TRY_CAST(d.artist.activeYearsCompletion AS INT), TRY_CAST(v2.a.activeYearsCompletion AS INT)) AS active_end,
       l.wikipedia_url, l.wikipedia_title, l.name_key,
       coalesce(d.artist.image, v2.a.image) AS portrait_url,
       nullif(trim(coalesce(d.artist.biography, v2.a.biography)), '') AS biography,
       len(d.works)::INT AS work_count, len(d.missingDetails)::INT AS works_without_details,
       TRY_CAST(d.fetchedAt AS TIMESTAMP) AS fetched_at, d.url IS NOT NULL AS crawled
FROM wikiart.artist_list l
LEFT JOIN wa_doc d ON d.url = l.artist_url
LEFT JOIN v2_artist v2 ON v2.artist_url = l.artist_url;

-- the artist's own periods (Picasso: Blue Period, Rose Period, ...) and series; the artists WikiArt relates
CREATE OR REPLACE TABLE wikiart.artist_periods AS
SELECT DISTINCT artist_url, coalesce(p->>'title', p->>'$') AS title, p->>'id' AS period_id
FROM v2_artist, unnest(a.periods) AS t(p) WHERE coalesce(p->>'title', p->>'$') IS NOT NULL;
CREATE OR REPLACE TABLE wikiart.artist_series AS
SELECT DISTINCT artist_url, coalesce(x->>'title', x->>'$') AS title, x->>'id' AS series_id
FROM v2_artist, unnest(a.series) AS t(x) WHERE coalesce(x->>'title', x->>'$') IS NOT NULL;
CREATE OR REPLACE TABLE wikiart.related_artists AS
SELECT DISTINCT v2.artist_url, r.artist_url AS related_url
FROM v2_artist v2, unnest(v2.a.relatedArtists) AS t(rid) JOIN v2_artist r ON r.v2_id = t.rid;

CREATE OR REPLACE TABLE wikiart.artist_dictionaries AS
SELECT DISTINCT l.artist_url, d.dictionary_key
FROM wikiart.artist_list l, unnest(l.v1_dictionary_ids) AS t(id) JOIN wikiart.dictionaries d ON d.v1_id = t.id
UNION
SELECT DISTINCT w.url, d.dictionary_key
FROM wa_doc w, unnest(w.artist.dictonaries) AS t(id) JOIN wikiart.dictionaries d ON d.v1_id = t.id
UNION
SELECT DISTINCT v2.artist_url, d.dictionary_key
FROM v2_artist v2, unnest(v2.a.dictionaries) AS t(id) JOIN wikiart.dictionaries d ON d.v2_id = t.id;

CREATE OR REPLACE TEMP TABLE wa_works AS SELECT url AS artist_url, w, w.details AS d FROM wa_doc, unnest(works) AS t(w);

CREATE OR REPLACE TABLE wikiart.works AS
SELECT artist_url, w.contentId AS content_id, d.url AS work_url, w.title,
       TRY_CAST(w.completitionYear AS INT) AS year, w.yearAsString AS year_text,
       d.style, d.genre, d.material, d.technique,
       TRY_CAST(d.sizeX AS DOUBLE) AS size_x_cm, TRY_CAST(d.sizeY AS DOUBLE) AS size_y_cm,
       TRY_CAST(d.diameter AS DOUBLE) AS diameter_cm, d.galleryName AS gallery_name,
       d.location, d.period, d.serie, d.tags, d.description, d.auction, d.lastPrice AS last_price,
       split_part(w.image, '!', 1) AS image_url, w.width AS image_width, w.height AS image_height,
       d IS NOT NULL AS has_details, title_key(w.title) AS title_key
FROM wa_works;

-- a work's categories: its numeric ids, plus its style(s) and genre by name (which also reaches the categories
-- whose old id is 0)
CREATE OR REPLACE TABLE wikiart.work_dictionaries AS
SELECT DISTINCT w.contentId AS content_id, d2.dictionary_key
FROM wa_works, unnest(d.dictionaries) AS t(x) JOIN wikiart.dictionaries d2 ON d2.v1_id = t.x
UNION
SELECT DISTINCT w.content_id, d2.dictionary_key
FROM wikiart.works w, unnest(string_split(w.style, ', ')) AS t(name)
JOIN wikiart.dictionaries d2 ON d2.group_name = 'style' AND lower(d2.title) = lower(trim(t.name))
UNION
SELECT DISTINCT w.content_id, d2.dictionary_key
FROM wikiart.works w JOIN wikiart.dictionaries d2 ON d2.group_name = 'genre' AND lower(d2.title) = lower(trim(w.genre));

CREATE OR REPLACE TABLE wikiart.work_tags AS
SELECT DISTINCT w.contentId AS content_id, trim(tag) AS tag
FROM wa_works, unnest(string_split(d.tags, ',')) AS t(tag) WHERE trim(tag) <> '';

-- each work's media (python -m archive.wikiart media), keyed by its image path (the host varies: uploads0..8)
CREATE OR REPLACE TABLE wikiart.work_media AS
SELECT DISTINCT w.content_id, m.medium, m.title AS medium_title
FROM read_json('{media}', maximum_object_size = 64000000, columns = {{
       media: 'STRUCT(medium VARCHAR, title VARCHAR, works STRUCT(id VARCHAR, image VARCHAR)[])[]'}}),
     unnest(media) AS t(m), unnest(m.works) AS u(x)
JOIN wikiart.works w ON regexp_extract(w.image_url, 'wikiart[.]org(/.+)$', 1) = regexp_extract(x.image, 'wikiart[.]org(/.+)$', 1);

CREATE OR REPLACE TABLE wikiart.wikidata AS
SELECT DISTINCT r.qid, r.wikiart AS artist_url, r.label
FROM read_json('{wikidata}', maximum_object_size = 64000000,
               columns = {{rows: 'STRUCT(qid VARCHAR, wikiart VARCHAR, label VARCHAR)[]'}}), unnest(rows) AS t(r);
"""

WIKIDATA_SQL = """
CREATE SCHEMA IF NOT EXISTS wikidata;
CREATE OR REPLACE TABLE wikidata.artist_facts AS
SELECT r.qid, r.property, r.value, r.label,
       TRY_CAST(regexp_extract(r.coord, 'Point[(]([-0-9.]+) ', 1) AS DOUBLE) AS lon,
       TRY_CAST(regexp_extract(r.coord, ' ([-0-9.]+)[)]', 1) AS DOUBLE) AS lat
FROM read_json('{wikidata_facts}', maximum_object_size = 64000000, columns = {{
  rows: 'STRUCT(qid VARCHAR, property VARCHAR, value VARCHAR, label VARCHAR, coord VARCHAR)[]'}}), unnest(rows) AS t(r);

-- Commons file details (python -m archive.wikimedia file-info): size, author and licence of the Commons images
-- that WikiArt-only works move onto (compare.wikiart_to_wikidata)
CREATE SCHEMA IF NOT EXISTS wikimedia;
CREATE OR REPLACE TABLE wikimedia.file_info AS SELECT * FROM read_parquet('{file_info}');

-- what Wikidata knows about each museum work (python -m archive.wikidata works): one row per work, property and
-- value; value is a Wikidata id (label: its English name) or a literal (dates, sizes, inventory numbers, WikiArt
-- ids). Unknown values ("somevalue") are left out.
-- every Wikidata item whose creator is a museum artist (python -m archive.wikidata creators): one row per item,
-- with its types, English title, year and Commons image
CREATE OR REPLACE TABLE wikidata.creator_works AS
SELECT creator, qid, list(DISTINCT type ORDER BY type) AS types,
       list(DISTINCT type_label ORDER BY type_label) FILTER (WHERE type_label IS NOT NULL) AS type_labels,
       min(title) AS title, min(TRY_CAST(regexp_extract(inception, '^([0-9]{{3,4}})-', 1) AS INT)) AS year,
       min(image) AS image, min(wikiart_id) AS wikiart_id
FROM read_parquet('{creator_works}') GROUP BY creator, qid;

CREATE OR REPLACE TABLE wikidata.work_facts AS
SELECT qid, property, value, label, max(fetched_at) AS fetched_at
FROM read_parquet('{work_facts}', union_by_name = true)
WHERE property <> 'fetched' AND value NOT LIKE '%/.well-known/genid/%'
GROUP BY ALL;
"""

MACROS = """
SET memory_limit = '16GB';
SET preserve_insertion_order = false;
CREATE OR REPLACE MACRO year_of(s) AS TRY_CAST(regexp_extract(s, '(-?\\d{3,4})\\D*$', 1) AS INT);
CREATE OR REPLACE MACRO wiki_title(u) AS
  CASE WHEN u ILIKE '%wikipedia.org/wiki/%'
       THEN lower(replace(url_decode(regexp_extract(u, '/wiki/([^#?]+)', 1)), '_', ' ')) END;
CREATE OR REPLACE MACRO name_key(s) AS regexp_replace(lower(strip_accents(coalesce(s, ''))), '[^a-z0-9]+', '', 'g');
CREATE OR REPLACE MACRO title_key(s) AS
  regexp_replace(lower(strip_accents(regexp_replace(coalesce(s, ''), '\\(.*?\\)', '', 'g'))), '[^a-z0-9]+', '', 'g');
-- a Commons file name (as in Wikidata's P18) -> its upload.wikimedia.org URLs (the path is the md5 of the name)
CREATE OR REPLACE MACRO commons_name(f) AS replace(f, ' ', '_');
CREATE OR REPLACE MACRO commons_path(f) AS
  substr(md5(commons_name(f)), 1, 1) || '/' || substr(md5(commons_name(f)), 1, 2) || '/' || commons_name(f);
CREATE OR REPLACE MACRO commons_original(f) AS 'https://upload.wikimedia.org/wikipedia/commons/' || commons_path(f);
CREATE OR REPLACE MACRO commons_thumb(f, w) AS
  'https://upload.wikimedia.org/wikipedia/commons/thumb/' || commons_path(f) || '/' ||
  CASE WHEN regexp_matches(lower(f), '[.]tiff?$') THEN 'lossy-page1-' || w || 'px-' || commons_name(f) || '.jpg'
       WHEN regexp_matches(lower(f), '[.]svg$') THEN w || 'px-' || commons_name(f) || '.png'
       ELSE w || 'px-' || commons_name(f) END;
"""

# --- taxonomy -----------------------------------------------------------------------------------------------------

TAXONOMY_SQL = """
CREATE SCHEMA IF NOT EXISTS taxonomy;
-- terms: the backbone, edited by hand. Kinds, top down: era (the European chronology) and tradition (Asian,
-- Islamic, Byzantine, colonial Latin American painting) > period (the museum's timeline rooms) > movement (also
-- umbrella: a broad label over several movements; historical-period: a dynasty or reign used as a style) >
-- school (a local school), group (an artists' society), academy, exhibition. Genres form their own tree.
-- parent_id points one level up; place and qid (Wikidata) where known.
CREATE OR REPLACE TABLE taxonomy.terms AS
SELECT term_id, kind, name, nullif(parent_id, '') AS parent_id, TRY_CAST(start_year AS INT) AS start_year,
       TRY_CAST(end_year AS INT) AS end_year, nullif(place, '') AS place, nullif(qid, '') AS qid,
       nullif(description, '') AS description
FROM read_csv('{terms}', header = true, all_varchar = true);
-- how each source's categories map to terms: WikiArt movements, styles, schools and genres (by dictionary key),
-- our periods, Wikidata items (movements, genres, groups) by QID. A term's own qid maps to it as well.
CREATE OR REPLACE TABLE taxonomy.mappings AS
SELECT source, source_group, source_key, source_name, term_id
FROM read_csv('{mappings}', header = true, all_varchar = true)
UNION
SELECT 'wikidata', CASE WHEN kind = 'genre' THEN 'genre' ELSE 'movement' END, qid, name, term_id
FROM taxonomy.terms WHERE qid IS NOT NULL;

-- every term with each of its ancestors (itself at depth 0), and its lineage: the nearest era or tradition,
-- period and movement above it (or itself)
CREATE OR REPLACE TABLE taxonomy.term_ancestors AS
WITH RECURSIVE up(term_id, ancestor_id, depth) AS (
  SELECT term_id, term_id, 0 FROM taxonomy.terms
  UNION ALL
  SELECT up.term_id, t.parent_id, up.depth + 1
  FROM up JOIN taxonomy.terms t ON t.term_id = up.ancestor_id
  WHERE t.parent_id IS NOT NULL AND up.depth < 12)
SELECT * FROM up;

CREATE OR REPLACE TABLE taxonomy.term_lineage AS
SELECT a.term_id,
       arg_min(a.ancestor_id, a.depth) FILTER (WHERE t.kind IN ('era', 'tradition')) AS era_id,
       arg_min(a.ancestor_id, a.depth) FILTER (WHERE t.kind = 'period') AS period_id,
       arg_min(a.ancestor_id, a.depth) FILTER (WHERE t.kind IN ('movement', 'umbrella', 'historical-period'))
         AS movement_id,
       string_agg(t.name, ' > ' ORDER BY a.depth DESC) AS path
FROM taxonomy.term_ancestors a JOIN taxonomy.terms t ON t.term_id = a.ancestor_id
GROUP BY a.term_id;

-- which terms each artist carries, from every source; via says where it came from
CREATE OR REPLACE TABLE taxonomy.artist_terms AS
SELECT DISTINCT 'wikiart' AS source, ad.artist_url AS artist_key, m.term_id, 'wikiart ' || d.group_name AS via
FROM wikiart.artist_dictionaries ad
JOIN wikiart.dictionaries d USING (dictionary_key)
JOIN taxonomy.mappings m ON m.source = 'wikiart' AND m.source_key = d.dictionary_key
UNION
SELECT DISTINCT 'wikiart', wa.artist_url, m.term_id, 'wikiart work ' || d.group_name
FROM wikiart.works wa
JOIN wikiart.work_dictionaries wd USING (content_id)
JOIN wikiart.dictionaries d USING (dictionary_key)
JOIN taxonomy.mappings m ON m.source = 'wikiart' AND m.source_key = d.dictionary_key
UNION
SELECT DISTINCT 'museum', a.artist_slug, m.term_id, 'museum period'
FROM wikipedia.artists a JOIN taxonomy.mappings m ON m.source = 'museum' AND m.source_key = a.period_slug
UNION
SELECT DISTINCT 'museum', am.artist_slug, xt.term_id, 'matched wikiart artist'
FROM compare.artist_match am
JOIN wikiart.artist_dictionaries ad ON ad.artist_url = am.wikiart_url
JOIN wikiart.dictionaries d USING (dictionary_key)
JOIN taxonomy.mappings m ON m.source = 'wikiart' AND m.source_key = d.dictionary_key
JOIN taxonomy.terms xt ON xt.term_id = m.term_id
UNION
-- Wikidata: the artist's movements (P135) and the groups they were members of (P463)
SELECT DISTINCT 'museum', a.artist_slug, m.term_id, 'wikidata ' || f.property
FROM wikipedia.artists a
JOIN wikidata.artist_facts f ON f.qid = a.qid AND f.property IN ('movement', 'member_of')
JOIN taxonomy.mappings m ON m.source = 'wikidata' AND m.source_group = 'movement' AND m.source_key = f.value
UNION
SELECT DISTINCT 'wikiart', x.artist_url, m.term_id, 'wikidata ' || f.property
FROM wikiart.wikidata x
JOIN wikidata.artist_facts f ON f.qid = x.qid AND f.property IN ('movement', 'member_of')
JOIN taxonomy.mappings m ON m.source = 'wikidata' AND m.source_group = 'movement' AND m.source_key = f.value
WHERE x.artist_url NOT LIKE '%/%';

CREATE OR REPLACE TABLE taxonomy.work_terms AS
SELECT DISTINCT 'wikiart' AS source, wd.content_id::VARCHAR AS work_key, m.term_id, 'wikiart ' || d.group_name AS via
FROM wikiart.work_dictionaries wd
JOIN wikiart.dictionaries d USING (dictionary_key)
JOIN taxonomy.mappings m ON m.source = 'wikiart' AND m.source_key = d.dictionary_key
UNION
SELECT DISTINCT 'museum', w.artist_slug || '/' || w.work_slug, m.term_id, 'museum period'
FROM wikipedia.works w JOIN wikipedia.artists a USING (artist_slug)
JOIN taxonomy.mappings m ON m.source = 'museum' AND m.source_key = a.period_slug
UNION
-- Wikidata: the work's movement (P135) and genre (P136)
SELECT DISTINCT 'museum', w.artist_slug || '/' || w.work_slug, m.term_id, 'wikidata ' || f.property
FROM wikipedia.works w
JOIN wikidata.work_facts f ON f.qid = w.qid AND f.property IN ('movement', 'genre')
JOIN taxonomy.mappings m ON m.source = 'wikidata' AND m.source_group = f.property AND m.source_key = f.value;

CREATE OR REPLACE TABLE taxonomy.term_summary AS
WITH artists AS (
  SELECT xt.term_id,
         count(DISTINCT xt.artist_key) FILTER (WHERE xt.source = 'museum') AS museum_artists,
         count(DISTINCT xt.artist_key) FILTER (WHERE xt.source = 'wikiart') AS wikiart_artists,
         count(DISTINCT xt.artist_key) FILTER (WHERE xt.source = 'wikiart' AND am.wikiart_url IS NULL)
           AS wikiart_artists_missing
  FROM taxonomy.artist_terms xt
  LEFT JOIN compare.artist_match am ON xt.source = 'wikiart' AND am.wikiart_url = xt.artist_key
  GROUP BY xt.term_id),
works AS (
  SELECT term_id, count(*) FILTER (WHERE source = 'museum') AS museum_works,
         count(*) FILTER (WHERE source = 'wikiart') AS wikiart_works
  FROM taxonomy.work_terms GROUP BY term_id)
SELECT t.term_id, t.kind, t.name, t.parent_id, l.path,
       coalesce(a.museum_artists, 0) AS museum_artists, coalesce(a.wikiart_artists, 0) AS wikiart_artists,
       coalesce(a.wikiart_artists_missing, 0) AS wikiart_artists_missing,
       coalesce(w.museum_works, 0) AS museum_works, coalesce(w.wikiart_works, 0) AS wikiart_works
FROM taxonomy.terms t LEFT JOIN taxonomy.term_lineage l USING (term_id)
LEFT JOIN artists a USING (term_id) LEFT JOIN works w USING (term_id)
ORDER BY t.kind, t.name;
"""

# --- comparisons --------------------------------------------------------------------------------------------------

COMPARE_SQL = """
CREATE SCHEMA IF NOT EXISTS compare;

-- our artists matched to WikiArt: by Wikidata's WikiArt ID, else the Wikipedia article, else name + birth year
CREATE OR REPLACE TABLE compare.artist_match AS
WITH wa AS (SELECT artist_url, name, birth_year, wikipedia_title, name_key FROM wikiart.artist_list),
by_qid AS (SELECT a.artist_slug, x.artist_url AS wikiart_url, 'wikidata' AS method, 1 AS rank
           FROM wikipedia.artists a JOIN wikiart.wikidata x ON x.qid = a.qid JOIN wa ON wa.artist_url = x.artist_url),
by_wiki AS (SELECT a.artist_slug, wa.artist_url, 'wikipedia', 2
            FROM wikipedia.artists a JOIN wa ON wa.wikipedia_title = a.wikipedia_title),
by_name AS (SELECT a.artist_slug, wa.artist_url, 'name+birth', 3
            FROM wikipedia.artists a JOIN wa ON wa.name_key = a.name_key AND wa.birth_year = a.birth_year),
ranked AS (SELECT *, row_number() OVER (PARTITION BY artist_slug ORDER BY rank) AS n
           FROM (SELECT * FROM by_qid UNION ALL SELECT * FROM by_wiki UNION ALL SELECT * FROM by_name))
SELECT artist_slug, wikiart_url, method FROM ranked WHERE n = 1;

CREATE OR REPLACE TEMP TABLE wa_groups AS
SELECT ad.artist_url, d.group_name, list(DISTINCT d.title ORDER BY d.title) AS titles
FROM wikiart.artist_dictionaries ad JOIN wikiart.dictionaries d USING (dictionary_key)
GROUP BY ALL;

CREATE OR REPLACE TABLE compare.missing_artists AS
SELECT l.artist_url, l.name, l.birth_year, l.death_year, l.wikipedia_url, x.qid,
       l.death_year IS NOT NULL AND l.death_year < year(current_date) - 70 AS public_domain,
       (SELECT titles FROM wa_groups g WHERE g.artist_url = l.artist_url AND g.group_name = 'movement') AS movements,
       (SELECT titles FROM wa_groups g WHERE g.artist_url = l.artist_url AND g.group_name = 'nationality') AS nationalities,
       a.work_count AS wikiart_works, coalesce(a.crawled, false) AS crawled,
       'https://www.wikiart.org/en/' || l.artist_url AS wikiart_page
FROM wikiart.artist_list l
LEFT JOIN compare.artist_match m ON m.wikiart_url = l.artist_url
LEFT JOIN wikiart.artists a ON a.artist_url = l.artist_url
LEFT JOIN wikiart.wikidata x ON x.artist_url = l.artist_url
WHERE m.wikiart_url IS NULL
ORDER BY public_domain DESC, wikiart_works DESC NULLS LAST, l.name;

CREATE OR REPLACE TABLE compare.our_artists_not_on_wikiart AS
SELECT a.artist_slug, a.name, a.period_slug, a.qid, a.work_count
FROM wikipedia.artists a LEFT JOIN compare.artist_match m USING (artist_slug)
WHERE m.artist_slug IS NULL ORDER BY a.work_count DESC;

CREATE OR REPLACE TABLE compare.artist_work_counts AS
SELECT a.artist_slug, a.name, a.period_slug, a.work_count AS museum_works, w.work_count AS wikiart_works,
       w.work_count - a.work_count AS difference, m.wikiart_url, m.method,
       (SELECT count(*) FROM wikiart.works ww JOIN wikipedia.works mw
          ON mw.artist_slug = a.artist_slug AND mw.title_key = ww.title_key AND mw.title_key <> ''
        WHERE ww.artist_url = m.wikiart_url) AS same_title
FROM wikipedia.artists a JOIN compare.artist_match m USING (artist_slug)
LEFT JOIN wikiart.artists w ON w.artist_url = m.wikiart_url
ORDER BY difference DESC NULLS LAST;

-- every WikiArt category: how many of its artists we have, and how many we miss
CREATE OR REPLACE TABLE compare.category_coverage AS
WITH artist_cat AS (
  SELECT artist_url, dictionary_key FROM wikiart.artist_dictionaries
  UNION
  SELECT DISTINCT w.artist_url, wd.dictionary_key FROM wikiart.works w JOIN wikiart.work_dictionaries wd USING (content_id))
SELECT d.group_name, d.title, d.dictionary_key,
       count(DISTINCT ac.artist_url) AS wikiart_artists,
       count(DISTINCT ac.artist_url) FILTER (WHERE m.wikiart_url IS NOT NULL) AS museum_has,
       count(DISTINCT ac.artist_url) FILTER (WHERE m.wikiart_url IS NULL) AS museum_missing,
       (SELECT count(*) FROM wikiart.work_dictionaries wd WHERE wd.dictionary_key = d.dictionary_key) AS wikiart_works,
       list_slice(list(DISTINCT l.name ORDER BY l.name) FILTER (WHERE m.wikiart_url IS NULL), 1, 15) AS missing_examples
FROM wikiart.dictionaries d
JOIN artist_cat ac USING (dictionary_key)
JOIN wikiart.artist_list l ON l.artist_url = ac.artist_url
LEFT JOIN compare.artist_match m ON m.wikiart_url = ac.artist_url
GROUP BY ALL
ORDER BY d.group_name, wikiart_artists DESC;
"""


def api_v2_file() -> Path:
    """data/wikiart/api_v2.json, or an empty stand-in until `python -m archive.wikiart api-v2` has run."""
    path = settings.WIKIART / "api_v2.json"
    if path.exists():
        return path
    empty = settings.WORK / "api_v2.empty.json"
    empty.parent.mkdir(parents=True, exist_ok=True)
    empty.write_text('{"dictionaries": [], "artists": []}', encoding="utf-8")
    return empty


def load_fingerprints(con) -> None:
    """catalogue.image_fingerprints: url -> 64-bit dHash and colourfulness (python -m archive.fingerprints), for
    telling the same painting apart across sources and a print or drawing from a painting."""
    from .fingerprints import GLOB, table
    con.execute("CREATE SCHEMA IF NOT EXISTS catalogue")
    if any(GLOB.parent.glob("*.parquet")):
        con.execute(f"CREATE OR REPLACE TABLE catalogue.image_fingerprints AS SELECT * FROM {table(con)}")
    else:
        con.execute("CREATE OR REPLACE TABLE catalogue.image_fingerprints (url VARCHAR, dhash BIGINT, colorfulness DOUBLE, "
                    "fetched_at TIMESTAMP)")


FEATURED_TS = settings.REPO / "src" / "components" / "timeline" / "featured-artists.ts"


def load_featured(con) -> None:
    """wikipedia.featured: the editorial Featured selection (src/components/timeline/featured-artists.ts, the most
    influential painters per section), so our tables keep featured and not-featured artists apart."""
    import re
    text = FEATURED_TS.read_text(encoding="utf-8")
    rows = [(slug, period, i + 1)
            for period, body in re.findall(r'"([a-z0-9-]+)":\s*\[([^\]]*)\]', text)
            for i, slug in enumerate(re.findall(r'"([a-z0-9-]+)"', body))]
    con.execute("CREATE SCHEMA IF NOT EXISTS wikipedia")
    con.execute("CREATE OR REPLACE TABLE wikipedia.featured (artist_slug VARCHAR, period_slug VARCHAR, featured_rank INTEGER)")
    con.executemany("INSERT INTO wikipedia.featured VALUES (?, ?, ?)", rows)


def stand_in(path: Path, empty_text: str) -> Path:
    """`path`, or an empty stand-in until the step that writes it has run."""
    if path.exists():
        return path
    empty = settings.WORK / f"{path.stem}.empty{path.suffix}"
    empty.parent.mkdir(parents=True, exist_ok=True)
    empty.write_text(empty_text.replace("{{", "{").replace("}}", "}"), encoding="utf-8")
    log(f"{path} does not exist yet: its tables are empty")
    return empty


def empty_parquet(path: Path, columns: str) -> Path:
    """path when it exists, else an empty Parquet stand-in with these columns (before the first fetch)."""
    if path.exists():
        return path
    empty = settings.WORK / f"{path.stem}.empty.parquet"
    empty.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"COPY (SELECT {columns} WHERE false) TO '{sql_path(empty)}' (FORMAT parquet)")
    con.close()
    return empty


def work_facts_file() -> Path:
    """data/wikidata/work_facts/*.parquet, or an empty stand-in before the first `python -m archive.wikidata works`."""
    from .wikidata import WORK_DIR, WORK_GLOB
    if any(WORK_DIR.glob("*.parquet")):
        return WORK_GLOB
    empty = settings.WORK / "work_facts.empty.parquet"
    empty.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"COPY (SELECT NULL::VARCHAR AS qid, 'fetched' AS property, NULL::VARCHAR AS value, "
                f"NULL::VARCHAR AS label, NULL::TIMESTAMP AS fetched_at) TO '{sql_path(empty)}' (FORMAT parquet)")
    con.close()
    return empty


def media_file() -> Path:
    """The media manifest (python -m archive.wikimedia manifest), or an empty stand-in."""
    path = settings.WORK / "wikimedia" / "media-manifest.tsv"
    if path.exists():
        with path.open(encoding="utf-8") as f:
            if f.read(11) == "artist_slug":
                return path
    empty = settings.WORK / "media.empty.tsv"
    empty.parent.mkdir(parents=True, exist_ok=True)
    empty.write_text("artist_slug\twork_slug\tkind\turl\tbytes\n", encoding="utf-8")
    log("no media manifest yet: wikipedia.media is empty (python -m archive.wikimedia manifest)")
    return empty


def ensure_taxonomy_files() -> None:
    settings.TAXONOMY.mkdir(parents=True, exist_ok=True)
    for path, cols in ((TERMS, TERM_COLUMNS), (MAPPINGS, MAPPING_COLUMNS)):
        if not path.exists():
            with path.open("w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(cols)


def build() -> int:
    ensure_taxonomy_files()
    museum = settings.WIKIPEDIA / "museum.json"
    wa = settings.WIKIART
    needed = [museum, wa / "dictionaries.json", wa / "artists.json", wa / "wikidata.json"]
    missing = [str(p) for p in needed if not p.exists()]
    if missing:
        log("REFUSED: missing inputs (run the WikiArt steps first):\n  " + "\n  ".join(missing))
        return 2
    tmp = settings.WAREHOUSE.with_suffix(".building.duckdb")
    tmp.unlink(missing_ok=True)
    con = duckdb.connect(str(tmp))
    try:
        con.execute(MACROS)
        load_featured(con)
        fmt = {"museum": sql_path(museum), "dictionaries": sql_path(wa / "dictionaries.json"),
               "artists_glob": sql_path(wa / "artists" / "*.json"), "wikidata": sql_path(wa / "wikidata.json"),
               "artist_list": sql_path(wa / "artists.json"), "terms": sql_path(TERMS), "mappings": sql_path(MAPPINGS),
               "exhibitions": sql_path(settings.TAXONOMY / "exhibitions.csv"),
               "exhibition_rooms": sql_path(settings.TAXONOMY / "exhibition_rooms.csv"),
               "api_v2": sql_path(api_v2_file()), "media": sql_path(stand_in(wa / "media.json", '{{"media": []}}')),
               "wikimedia_media": sql_path(media_file()),
               "work_facts": sql_path(work_facts_file()),
               "creator_works": sql_path(empty_parquet(settings.DATA / "wikidata" / "creator_works.parquet",
                   "NULL::VARCHAR AS creator, NULL::VARCHAR AS qid, NULL::VARCHAR AS type, NULL::VARCHAR AS type_label, "
                   "NULL::VARCHAR AS title, NULL::VARCHAR AS inception, NULL::VARCHAR AS image, NULL::VARCHAR AS wikiart_id")),
               "file_info": sql_path(empty_parquet(settings.DATA / "wikimedia" / "file_info.parquet",
                   "NULL::VARCHAR AS file, NULL::INT AS width, NULL::INT AS height, NULL::BIGINT AS bytes, "
                   "NULL::VARCHAR AS author, NULL::VARCHAR AS license, NULL::VARCHAR AS license_url, "
                   "NULL::BOOLEAN AS non_free, NULL::TIMESTAMP AS fetched_at")),
               "wikidata_facts": sql_path(stand_in(settings.DATA / "wikidata" / "artist_facts.json", '{{"rows": []}}')),
               "group_values": ", ".join(f"({g}, '{n}')" for g, n in GROUPS.items())}
        for name, script in (("wikipedia", WIKIPEDIA_SQL), ("wikiart", WIKIART_SQL), ("compare", COMPARE_SQL),
                             ("wikidata", WIKIDATA_SQL), ("taxonomy", TAXONOMY_SQL),
                             ("catalogue", CATALOGUE_SQL), ("exhibition", EXHIBITION_SQL)):
            if name == "catalogue":
                load_fingerprints(con)  # the work match compares images
            if name == "wikiart" and not any((wa / "artists").glob("*.json")):
                log("no crawled WikiArt artists yet: wikiart.artists and works are empty")
            con.execute(script.format(**fmt))
            log(f"built {name}.*")
        for schema, table, rows in tables(con):
            log(f"  {schema}.{table}: {rows:,} rows")
    finally:
        con.close()
    tmp.replace(settings.WAREHOUSE)  # readers never see half a build
    log(f"warehouse: {settings.WAREHOUSE}")
    return 0


def tables(con) -> list[tuple[str, str, int]]:
    rows = con.execute("""SELECT schema_name, table_name, estimated_size FROM duckdb_tables()
                          WHERE NOT temporary ORDER BY schema_name, table_name""").fetchall()
    return [(s, t, con.execute(f'SELECT count(*) FROM "{s}"."{t}"').fetchone()[0]) for s, t, _ in rows]


# --- taxonomy seeding -------------------------------------------------------------------------------------------

def slug(text: str) -> str:
    import re
    import unicodedata
    s = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def read_rows(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def seed_taxonomy(min_artists: int) -> int:
    """Candidate terms from our periods and WikiArt's movements, styles, schools and genres; added only when new.

    Movements and styles with the same name become one movement term (WikiArt files "Impressionism" in both);
    a museum period is mapped to the movement of the same name when there is one. Rows already in the files are
    left exactly as they are, so hand edits (renames, merges, parents, new themes) survive every reseed."""
    ensure_taxonomy_files()
    if not settings.WAREHOUSE.exists():
        log("REFUSED: build the warehouse first (python -m archive.warehouse build)")
        return 2
    con = duckdb.connect(str(settings.WAREHOUSE), read_only=True)
    terms = read_rows(TERMS)
    mappings = read_rows(MAPPINGS)
    have_terms = {t["term_id"] for t in terms}
    have_maps = {(m["source"], m["source_key"]) for m in mappings}
    new_terms: list[dict] = []
    new_maps: list[dict] = []

    def add_term(term_id: str, kind: str, name: str, start=None, end=None, description="") -> str:
        if term_id not in have_terms:
            have_terms.add(term_id)
            new_terms.append({"term_id": term_id, "kind": kind, "name": name, "parent_id": "",
                              "start_year": start or "", "end_year": end or "", "place": "", "qid": "",
                              "description": description})
        return term_id

    def add_map(source, group, key, name, term_id) -> None:
        if (source, str(key)) not in have_maps:
            have_maps.add((source, str(key)))
            new_maps.append({"source": source, "source_group": group, "source_key": key, "source_name": name,
                             "term_id": term_id})

    # WikiArt categories with at least min_artists artists (artist level or through their works)
    rows = con.execute("""SELECT group_name, dictionary_key, title, wikiart_artists FROM compare.category_coverage
                          WHERE group_name IN ('movement', 'style', 'school', 'genre') AND wikiart_artists >= ?
                          ORDER BY group_name, wikiart_artists DESC""", [min_artists]).fetchall()
    kind_of = {"movement": "movement", "style": "movement", "school": "school", "genre": "genre"}
    by_name: dict[tuple[str, str], str] = {}
    for group, dict_id, title, _ in rows:
        kind = kind_of[group]
        term_id = by_name.setdefault((kind, slug(title)), f"{kind}:{slug(title)}")
        add_term(term_id, kind, title.strip())
        add_map("wikiart", group, dict_id, title, term_id)
    # our timeline periods: a period term each, mapped to itself
    for period_slug, name, start, end in con.execute(
            "SELECT period_slug, name, start_year, end_year FROM wikipedia.periods ORDER BY start_year").fetchall():
        term_id = add_term(f"period:{period_slug}", "period", name, start, end)
        add_map("museum", "period", period_slug, name, term_id)
    con.close()
    for path, cols, old, new in ((TERMS, TERM_COLUMNS, terms, new_terms), (MAPPINGS, MAPPING_COLUMNS, mappings, new_maps)):
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(old + new)
    log(f"taxonomy: {len(new_terms)} new terms ({len(terms) + len(new_terms)} in all), "
        f"{len(new_maps)} new mappings ({len(mappings) + len(new_maps)} in all)")
    return 0


# --- export ---------------------------------------------------------------------------------------------------------

def export() -> int:
    from . import bucket
    out = settings.WORK / "warehouse"
    con = duckdb.connect(str(settings.WAREHOUSE), read_only=True)
    try:
        for schema, table, rows in tables(con):
            path = out / schema / f"{table}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            con.execute(f"COPY \"{schema}\".\"{table}\" TO '{sql_path(path)}' (FORMAT parquet, COMPRESSION zstd)")
            bucket.put_file(f"warehouse/{schema}/{table}.parquet", path, "application/vnd.apache.parquet")
            log(f"{schema}.{table}: {rows:,} rows -> gs://{settings.BUCKET}/warehouse/{schema}/{table}.parquet")
    finally:
        con.close()
    return 0


def to_postgres(dsn: str) -> int:
    """Copy every table into Postgres (same schema and table names) with DuckDB's postgres extension."""
    con = duckdb.connect(str(settings.WAREHOUSE), read_only=True)
    try:
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute("ATTACH ? AS pg (TYPE postgres)", [dsn])
        for schema, table, rows in tables(con):
            con.execute(f'CREATE SCHEMA IF NOT EXISTS pg."{schema}"')
            con.execute(f'CREATE OR REPLACE TABLE pg."{schema}"."{table}" AS SELECT * FROM "{schema}"."{table}"')
            log(f"{schema}.{table}: {rows:,} rows copied")
    finally:
        con.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    settings.ensure_dirs()
    p = argparse.ArgumentParser(prog="python -m archive.warehouse")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("build")
    s = sub.add_parser("seed-taxonomy")
    s.add_argument("--min-artists", type=int, default=3)
    sub.add_parser("export")
    pg = sub.add_parser("to-postgres")
    pg.add_argument("--dsn", required=True)
    a = p.parse_args(argv)
    if a.cmd == "build":
        return build()
    if a.cmd == "seed-taxonomy":
        return seed_taxonomy(a.min_artists)
    if a.cmd == "export":
        return export()
    return to_postgres(a.dsn)


if __name__ == "__main__":
    sys.exit(main())
