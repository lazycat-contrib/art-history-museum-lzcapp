"""The catalogue: one row per artist and one per work, across sources, with every link we have. Plain tables in
the warehouse (DuckDB now, Postgres later via `python -m archive.warehouse to-postgres`).

Built by `python -m archive.warehouse build` after compare, taxonomy and wikidata:
  compare.work_match    our work <-> WikiArt work, one to one: Wikidata's WikiArt ID, image fingerprints, title
  catalogue.artists     every artist of every source. artist_id: our slug when the museum has them, else the
                        WikiArt url. Identity (name, years, gender, Wikidata id, in_museum, featured: the editorial
                        selection of the most influential painters, public_domain); our
                        period and work count; WikiArt movements, schools, nationalities; our taxonomy terms;
                        life (birth and death place, work locations, citizenship, educated_at, teachers, students,
                        influenced_by, Wikidata movements, notable works, member_of, WikiArt's named periods and
                        series): the material for rooms themed by an artist's life and era; links (Wikipedia,
                        Wikidata, WikiArt pages; portraits from Wikimedia and WikiArt).
  catalogue.works       every work of every source, merged where matched: identity (work_id = artist_id/work,
                        title, year, Wikidata id, in_museum, copyrighted); size (cm, pixels, bytes); Wikimedia links
                        as the site uses them (thumb, wall, near, inspect, original) and the Commons page; WikiArt
                        (page, image, style, genre, material, technique, collection, location, period, series,
                        tags); Wikipedia, Wikidata, pageviews; our taxonomy terms.
  catalogue.artist_placement, work_placement   each artist and work in our backbone: main term, movement,
                        period, era (and the work's genre), from WikiArt and Wikidata together (see below).
  catalogue.works also carries each field resolved across the sources with the source it came from (title,
  year, size, type, material, collection, location, image, movement, period, era, genre: <field>_source). The
  base is Wikipedia / Wikidata / Commons; WikiArt fills what they lack and gives each painting's style:
    identity, image, title, year, size, collection, material   Wikipedia / Wikidata / Commons, else WikiArt
    movement (style)                                          WikiArt's style, else Wikidata, else the artist's
    genre                                                     WikiArt, else Wikidata
  A WikiArt-only work moves onto Wikidata and Commons when compare.wikiart_to_wikidata finds it there.
Place coordinates for maps are in wikidata.artist_facts (lat, lon).
"""
from __future__ import annotations

CATALOGUE_SQL = """
CREATE SCHEMA IF NOT EXISTS catalogue;

-- our work <-> WikiArt work, one to one. Every pair of works by the same matched artist is scored, and each work
-- keeps the WikiArt work that is also its own best (mutual best). Methods, strongest first:
--   wikidata     the work's Wikidata item names the WikiArt work (P6002)
--   image        fingerprints (catalogue.image_fingerprints) within 4 bits, unless the years disagree by more than
--                3; within 10 bits, with a like title (Jaro-Winkler >= 0.6) or the same year (+-1). Titles often
--                differ only by language ("Roche pourrie" / "Crumbling Rocks"): the image decides.
--   image+title  within 20 bits and nearly the same title (>= 0.85): one painting photographed differently
--   title+year / title / title+year~   the same normalized title, years within 2, where a fingerprint is missing
--                on either side. With both fingerprints and the images far apart, the same title is a second
--                version, not a match.
-- Thresholds measured on 1.7M same-artist pairs of 247 artists (2026-10-08).
CREATE OR REPLACE TEMP TABLE match_ours AS
SELECT w.artist_slug, w.work_slug, w.title_key, w.year, w.qid, f.dhash
FROM wikipedia.works w
LEFT JOIN wikipedia.media md ON md.artist_slug = w.artist_slug AND md.work_slug = w.work_slug AND md.kind = 'thumb'
LEFT JOIN catalogue.image_fingerprints f ON f.url = md.url
QUALIFY row_number() OVER (PARTITION BY w.artist_slug, w.work_slug ORDER BY f.dhash IS NULL) = 1;

CREATE OR REPLACE TEMP TABLE match_theirs AS
SELECT ww.artist_url, ww.work_url, ww.content_id, ww.title_key, ww.year, f.dhash
FROM wikiart.works ww LEFT JOIN catalogue.image_fingerprints f ON f.url = ww.image_url || '!PinterestSmall.jpg';

CREATE OR REPLACE TEMP TABLE wikiart_ids_of_works AS
SELECT DISTINCT qid, artist_url AS wikiart_id FROM wikiart.wikidata WHERE artist_url LIKE '%/%'
UNION SELECT DISTINCT qid, value FROM wikidata.work_facts WHERE property = 'wikiart_id' AND value LIKE '%/%';

CREATE OR REPLACE TABLE compare.work_match AS
WITH pairs AS (
  SELECT o.artist_slug, o.work_slug, t.content_id, t.artist_url AS wikiart_artist_url,
         bit_count(xor(o.dhash, t.dhash)) AS distance,
         jaro_winkler_similarity(o.title_key, t.title_key) AS title_similarity,
         o.title_key = t.title_key AND o.title_key <> '' AS same_title,
         abs(o.year - t.year) AS year_gap,
         x.qid IS NOT NULL AS by_wikidata
  FROM match_ours o
  JOIN compare.artist_match m USING (artist_slug)
  JOIN match_theirs t ON t.artist_url = m.wikiart_url
  LEFT JOIN wikiart_ids_of_works x ON x.qid = o.qid AND x.wikiart_id = t.artist_url || '/' || t.work_url
  WHERE x.qid IS NOT NULL OR bit_count(xor(o.dhash, t.dhash)) <= 20
     OR (o.title_key = t.title_key AND o.title_key <> '')),
scored AS (
  SELECT *, CASE
      WHEN by_wikidata THEN 'wikidata'
      WHEN distance <= 4 AND coalesce(year_gap, 0) <= 3 THEN 'image'
      WHEN distance <= 10 AND (title_similarity >= 0.6 OR year_gap <= 1) THEN 'image'
      WHEN distance <= 20 AND title_similarity >= 0.85 THEN 'image+title'
      WHEN distance IS NULL AND same_title AND coalesce(year_gap, 0) <= 2 THEN
        CASE WHEN year_gap = 0 THEN 'title+year' WHEN year_gap IS NULL THEN 'title' ELSE 'title+year~' END
    END AS method
  FROM pairs),
ranked AS (
  SELECT *, CASE method WHEN 'wikidata' THEN 0 WHEN 'image' THEN 1 + distance WHEN 'image+title' THEN 20 + distance
                        ELSE 50 + coalesce(year_gap, 1) END - title_similarity / 10 AS cost
  FROM scored WHERE method IS NOT NULL)
SELECT artist_slug, work_slug, content_id, wikiart_artist_url, method, distance,
       round(title_similarity, 3) AS title_similarity
FROM ranked
QUALIFY row_number() OVER (PARTITION BY artist_slug, work_slug ORDER BY cost, content_id) = 1
    AND row_number() OVER (PARTITION BY content_id ORDER BY cost, work_slug) = 1;

-- a WikiArt-only work (no match among our works) <-> the same painting on Wikidata, among the artist's other
-- Wikidata works (wikidata.creator_works, by a museum artist, not hung in the museum). Same methods and thresholds
-- as compare.work_match: the WikiArt ID on the Wikidata item, the Commons image's fingerprint, then the title.
-- A matched work moves onto the base: its Wikidata item, its Commons image (with licence), Wikidata's type
-- ("print", "drawing" ... tell what WikiArt does not).
CREATE OR REPLACE TEMP TABLE wikidata_candidates AS
SELECT a.artist_slug, c.qid, c.title, title_key(c.title) AS title_key, c.year, c.image, c.wikiart_id, c.type_labels,
       f.dhash
FROM wikidata.creator_works c JOIN wikipedia.artists a ON a.qid = c.creator
LEFT JOIN catalogue.image_fingerprints f ON f.url = commons_thumb(c.image, 250)
WHERE c.qid NOT IN (SELECT qid FROM wikipedia.works WHERE qid IS NOT NULL);

CREATE OR REPLACE TABLE compare.wikiart_to_wikidata AS
WITH pairs AS (
  SELECT m.artist_slug, t.content_id, c.qid, c.title, c.year, c.image, c.type_labels,
         bit_count(xor(c.dhash, t.dhash)) AS distance,
         jaro_winkler_similarity(c.title_key, t.title_key) AS title_similarity,
         c.title_key = t.title_key AND c.title_key <> '' AS same_title,
         abs(c.year - t.year) AS year_gap,
         c.wikiart_id = t.artist_url || '/' || t.work_url AS by_wikidata
  FROM match_theirs t
  JOIN compare.artist_match m ON m.wikiart_url = t.artist_url
  JOIN wikidata_candidates c ON c.artist_slug = m.artist_slug
  WHERE t.content_id NOT IN (SELECT content_id FROM compare.work_match)
    AND (c.wikiart_id = t.artist_url || '/' || t.work_url OR bit_count(xor(c.dhash, t.dhash)) <= 20
         OR (c.title_key = t.title_key AND c.title_key <> ''))),
scored AS (
  SELECT *, CASE
      WHEN by_wikidata THEN 'wikidata'
      WHEN distance <= 4 AND coalesce(year_gap, 0) <= 3 THEN 'image'
      WHEN distance <= 10 AND (title_similarity >= 0.6 OR year_gap <= 1) THEN 'image'
      WHEN distance <= 20 AND title_similarity >= 0.85 THEN 'image+title'
      WHEN distance IS NULL AND same_title AND coalesce(year_gap, 0) <= 2 THEN
        CASE WHEN year_gap = 0 THEN 'title+year' WHEN year_gap IS NULL THEN 'title' ELSE 'title+year~' END
    END AS method
  FROM pairs),
ranked AS (
  SELECT *, CASE method WHEN 'wikidata' THEN 0 WHEN 'image' THEN 1 + distance WHEN 'image+title' THEN 20 + distance
                        ELSE 50 + coalesce(year_gap, 1) END - title_similarity / 10 AS cost
  FROM scored WHERE method IS NOT NULL)
SELECT artist_slug, content_id, qid, title, year, image AS commons_file, type_labels, method, distance,
       round(title_similarity, 3) AS title_similarity
FROM ranked
QUALIFY row_number() OVER (PARTITION BY content_id ORDER BY cost, qid) = 1
    AND row_number() OVER (PARTITION BY qid ORDER BY cost, content_id) = 1;

CREATE OR REPLACE TEMP TABLE media_by_work AS
SELECT artist_slug, work_slug,
       max(url) FILTER (WHERE kind = 'thumb') AS wikimedia_thumb,
       max(url) FILTER (WHERE kind = 'wall') AS wikimedia_wall,
       max(url) FILTER (WHERE kind = 'near') AS wikimedia_near,
       max(url) FILTER (WHERE kind = 'inspect') AS wikimedia_inspect,
       max(url) FILTER (WHERE kind = 'original') AS wikimedia_original,
       max(bytes) FILTER (WHERE kind = 'original') AS original_bytes
FROM wikipedia.media WHERE work_slug IS NOT NULL GROUP BY ALL;

CREATE OR REPLACE TEMP TABLE portrait_by_artist AS
SELECT artist_slug, max(url) FILTER (WHERE kind = 'portrait_200_2x') AS wikimedia_portrait
FROM wikipedia.media WHERE work_slug IS NULL GROUP BY ALL;

-- artist ids: our slug for museum artists; the WikiArt url for the others (prefixed when it would clash)
CREATE OR REPLACE TEMP TABLE wikiart_ids AS
SELECT l.artist_url,
       coalesce(m.artist_slug, CASE WHEN l.artist_url IN (SELECT artist_slug FROM wikipedia.artists)
                                    THEN 'wikiart-' || l.artist_url ELSE l.artist_url END) AS artist_id
FROM wikiart.artist_list l LEFT JOIN compare.artist_match m ON m.wikiart_url = l.artist_url;

CREATE OR REPLACE TEMP TABLE artist_groups AS
SELECT ad.artist_url, d.group_name, list(DISTINCT d.title ORDER BY d.title) AS titles
FROM wikiart.artist_dictionaries ad JOIN wikiart.dictionaries d USING (dictionary_key) GROUP BY ALL;

CREATE OR REPLACE TEMP TABLE life AS
SELECT qid,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'birth_place') AS birth_place,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'death_place') AS death_place,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'work_location') AS work_locations,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'citizenship') AS citizenship,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'educated_at') AS educated_at,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'student_of') AS teachers,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'student') AS students,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'influenced_by') AS influenced_by,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'movement') AS wikidata_movements,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'notable_work') AS notable_works,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'member_of') AS member_of
FROM wikidata.artist_facts WHERE label IS NOT NULL AND NOT regexp_matches(label, '^Q[0-9]+$') GROUP BY qid;

CREATE OR REPLACE TEMP TABLE artist_terms_by_key AS
SELECT source, artist_key, list(DISTINCT term_id ORDER BY term_id) AS terms FROM taxonomy.artist_terms GROUP BY ALL;
CREATE OR REPLACE TEMP TABLE work_terms_by_key AS
SELECT source, work_key, list(DISTINCT term_id ORDER BY term_id) AS terms FROM taxonomy.work_terms GROUP BY ALL;
CREATE OR REPLACE TEMP TABLE tags_by_work AS
SELECT content_id, list(tag ORDER BY tag) AS tags FROM wikiart.work_tags GROUP BY ALL;

CREATE OR REPLACE TABLE catalogue.artists AS
WITH museum AS (
  SELECT a.artist_slug AS artist_id, a.name, a.birth_year, a.death_year, a.qid, a.period_slug, a.tagline,
         a.wikipedia_url, m.wikiart_url, a.work_count AS museum_works, p.wikimedia_portrait
  FROM wikipedia.artists a LEFT JOIN compare.artist_match m USING (artist_slug)
  LEFT JOIN portrait_by_artist p USING (artist_slug)),
wikiart_only AS (
  SELECT i.artist_id, l.name, l.birth_year, l.death_year, x.qid, NULL AS period_slug, NULL AS tagline,
         l.wikipedia_url, l.artist_url AS wikiart_url, 0 AS museum_works, NULL AS wikimedia_portrait
  FROM wikiart.artist_list l JOIN wikiart_ids i USING (artist_url)
  LEFT JOIN compare.artist_match m ON m.wikiart_url = l.artist_url
  LEFT JOIN (SELECT artist_url, min(qid) AS qid FROM wikiart.wikidata GROUP BY ALL) x ON x.artist_url = l.artist_url
  WHERE m.wikiart_url IS NULL)
SELECT u.*, u.museum_works > 0 OR u.period_slug IS NOT NULL AS in_museum,
       u.artist_id IN (SELECT artist_slug FROM wikipedia.featured) AS featured,
       CASE WHEN u.wikiart_url IS NOT NULL THEN 'https://www.wikiart.org/en/' || u.wikiart_url END AS wikiart_page,
       CASE WHEN u.qid IS NOT NULL THEN 'https://www.wikidata.org/wiki/' || u.qid END AS wikidata_page,
       wa.portrait_url AS wikiart_portrait, wa.work_count AS wikiart_works, wa.gender,
       (SELECT titles FROM artist_groups g WHERE g.artist_url = u.wikiart_url AND g.group_name = 'movement') AS movements,
       (SELECT titles FROM artist_groups g WHERE g.artist_url = u.wikiart_url AND g.group_name = 'school') AS schools,
       (SELECT titles FROM artist_groups g WHERE g.artist_url = u.wikiart_url AND g.group_name = 'nationality') AS nationalities,
       (SELECT list(DISTINCT title ORDER BY title) FROM wikiart.artist_periods p WHERE p.artist_url = u.wikiart_url) AS life_periods,
       list_sort(list_distinct(list_concat(coalesce(tm.terms, []), coalesce(tw.terms, [])))) AS terms,
       (SELECT list(DISTINCT title ORDER BY title) FROM wikiart.artist_series s WHERE s.artist_url = u.wikiart_url) AS series,
       u.death_year IS NOT NULL AND u.death_year < year(current_date) - 70 AS public_domain,
       life.* EXCLUDE (qid)
FROM (SELECT * FROM museum UNION ALL SELECT * FROM wikiart_only) u
LEFT JOIN wikiart.artists wa ON wa.artist_url = u.wikiart_url
LEFT JOIN life ON life.qid = u.qid
LEFT JOIN artist_terms_by_key tm ON tm.source = 'museum' AND tm.artist_key = u.artist_id
LEFT JOIN artist_terms_by_key tw ON tw.source = 'wikiart' AND tw.artist_key = u.wikiart_url;

CREATE OR REPLACE TABLE catalogue.works AS
WITH museum AS (
  SELECT w.artist_slug AS artist_id, w.artist_slug || '/' || w.work_slug AS work_id, w.work_slug, w.title, w.year,
         w.qid, w.width_cm, w.height_cm, w.pageviews, w.copyrighted, w.wikipedia_url, w.image_width, w.image_height,
         md.wikimedia_thumb, md.wikimedia_wall, md.wikimedia_near, md.wikimedia_inspect, md.wikimedia_original,
         md.original_bytes, wm.content_id, true AS in_museum
  FROM wikipedia.works w
  LEFT JOIN media_by_work md USING (artist_slug, work_slug)
  LEFT JOIN compare.work_match wm USING (artist_slug, work_slug)),
wikiart_only AS (
  SELECT i.artist_id, i.artist_id || '/' || coalesce(ww.work_url, ww.content_id::VARCHAR) AS work_id,
         coalesce(ww.work_url, ww.content_id::VARCHAR) AS work_slug, ww.title, ww.year, NULL AS qid,
         ww.size_x_cm AS width_cm, ww.size_y_cm AS height_cm, NULL::BIGINT AS pageviews, NULL::BOOLEAN AS copyrighted,
         NULL AS wikipedia_url, ww.image_width, ww.image_height, NULL AS wikimedia_thumb, NULL AS wikimedia_wall,
         NULL AS wikimedia_near, NULL AS wikimedia_inspect, NULL AS wikimedia_original, NULL::BIGINT AS original_bytes,
         ww.content_id, false AS in_museum
  FROM wikiart.works ww JOIN wikiart_ids i ON i.artist_url = ww.artist_url
  WHERE ww.content_id NOT IN (SELECT content_id FROM compare.work_match))
SELECT u.*,
       CASE WHEN u.wikimedia_original IS NOT NULL THEN
         'https://' || CASE WHEN u.wikimedia_original LIKE '%/wikipedia/commons/%' THEN 'commons.wikimedia.org'
                            ELSE regexp_extract(u.wikimedia_original, '/wikipedia/([^/]+)/', 1) || '.wikipedia.org' END
         || '/wiki/File:' || CASE WHEN u.wikimedia_original LIKE '%/thumb/%'
                                  THEN regexp_extract(u.wikimedia_original, '/thumb/[0-9a-f]/[0-9a-f]{{2}}/([^/]+)/', 1)
                                  ELSE regexp_extract(u.wikimedia_original, '/([^/]+)$', 1) END END AS commons_page,
       CASE WHEN u.qid IS NOT NULL THEN 'https://www.wikidata.org/wiki/' || u.qid END AS wikidata_page,
       ww.artist_url AS wikiart_artist_url, ww.work_url AS wikiart_work_url,
       CASE WHEN ww.work_url IS NOT NULL THEN 'https://www.wikiart.org/en/' || ww.artist_url || '/' || ww.work_url END AS wikiart_page,
       ww.image_url AS wikiart_image, CASE WHEN ww.image_url IS NOT NULL THEN ww.image_url || '!Large.jpg' END AS wikiart_image_large,
       ww.style AS wikiart_style, ww.genre AS wikiart_genre, ww.material AS wikiart_material, ww.technique AS wikiart_technique,
       ww.gallery_name AS wikiart_gallery, ww.location AS wikiart_location, ww.period AS wikiart_period, ww.serie AS wikiart_series,
       tg.tags AS wikiart_tags,
       list_sort(list_distinct(list_concat(coalesce(tm.terms, []), coalesce(tw.terms, [])))) AS terms
FROM (SELECT * FROM museum UNION ALL SELECT * FROM wikiart_only) u
LEFT JOIN wikiart.works ww ON ww.content_id = u.content_id
LEFT JOIN tags_by_work tg ON tg.content_id = u.content_id
LEFT JOIN work_terms_by_key tm ON tm.source = 'museum' AND tm.work_key = u.work_id
LEFT JOIN work_terms_by_key tw ON tw.source = 'wikiart' AND tw.work_key = u.content_id::VARCHAR;
"""

CATALOGUE_SQL += """
-- where each artist sits in the backbone (taxonomy.terms), from every source. Evidence, by weight:
--   wikiart works    the share of the artist's WikiArt works in each style (x2: one vote per painting)
--   wikiart artist   WikiArt's movements and schools for the artist
--   wikidata         Wikidata's movements (P135) and memberships (P463)
-- A school, group, academy or exhibition counts (x0.5) for the movement or period above it. Umbrella labels
-- (Modernism, Contemporary ...) count x0.3 and dynasty labels x0.5, so a specific movement wins when there is one.
-- main_term is the best-scored term and gives movement_id. period_id and era_id come from its lineage when the
-- artist worked in that period's years (within 15 years: a painter born in 1981 who paints in a Surrealist manner
-- keeps movement Surrealism but hangs with the contemporaries). A museum artist keeps the museum's own period (the
-- timeline is curated). Otherwise the period whose years best cover the artist's working life (from age 20)
-- decides, inside the main term's era when it has one (placed_by 'years' when nothing else placed them).
-- terms: every term with its score, best first.
CREATE OR REPLACE TEMP TABLE placement_evidence AS
WITH work_styles AS (
  SELECT a.artist_id, xt.term_id, count(*) AS n
  FROM catalogue.artists a JOIN wikiart.works w ON w.artist_url = a.wikiart_url
  JOIN taxonomy.work_terms xt ON xt.source = 'wikiart' AND xt.work_key = w.content_id::VARCHAR
  JOIN taxonomy.terms t USING (term_id) WHERE t.kind <> 'genre'
  GROUP BY ALL),
raw AS (
  SELECT artist_id, term_id, 2.0 * n / sum(n) OVER (PARTITION BY artist_id) AS weight, 'wikiart works' AS via
  FROM work_styles
  UNION ALL
  SELECT a.artist_id, xt.term_id, 1.0 / count(*) OVER (PARTITION BY a.artist_id), 'wikiart artist'
  FROM catalogue.artists a JOIN taxonomy.artist_terms xt ON xt.source = 'wikiart' AND xt.artist_key = a.wikiart_url
  JOIN taxonomy.terms t USING (term_id)
  WHERE xt.via IN ('wikiart movement', 'wikiart school') AND t.kind <> 'genre'
  UNION ALL
  SELECT a.artist_id, m.term_id, 1.0 / count(*) OVER (PARTITION BY a.artist_id), 'wikidata'
  FROM catalogue.artists a JOIN wikidata.artist_facts f ON f.qid = a.qid AND f.property IN ('movement', 'member_of')
  JOIN taxonomy.mappings m ON m.source = 'wikidata' AND m.source_group = 'movement' AND m.source_key = f.value)
SELECT r.artist_id, r.via,
       CASE WHEN t.kind IN ('school', 'group', 'academy', 'exhibition') THEN coalesce(l.movement_id, l.period_id, l.era_id)
            ELSE r.term_id END AS term_id,
       r.weight * CASE WHEN t.kind IN ('school', 'group', 'academy', 'exhibition') THEN 0.5 ELSE 1 END AS weight
FROM raw r JOIN taxonomy.terms t USING (term_id) JOIN taxonomy.term_lineage l USING (term_id);

CREATE OR REPLACE TEMP TABLE placement_scores AS
SELECT e.artist_id, e.term_id,
       sum(e.weight) * CASE t.kind WHEN 'umbrella' THEN 0.3 WHEN 'historical-period' THEN 0.5 ELSE 1 END AS score,
       arg_max(e.via, e.weight) AS via
FROM placement_evidence e JOIN taxonomy.terms t USING (term_id)
WHERE e.term_id IS NOT NULL AND t.kind IN ('movement', 'umbrella', 'historical-period', 'period', 'era', 'tradition')
GROUP BY e.artist_id, e.term_id, t.kind;

CREATE OR REPLACE TEMP TABLE period_by_years AS
SELECT a.artist_id, p.term_id, p.parent_id AS era_id,
       least(coalesce(a.death_year, a.birth_year + 60), p.end_year) - greatest(a.birth_year + 20, p.start_year) AS overlap
FROM catalogue.artists a JOIN taxonomy.terms p
  ON p.kind = 'period' AND p.start_year <= coalesce(a.death_year, a.birth_year + 60) AND p.end_year >= a.birth_year + 20
WHERE a.birth_year IS NOT NULL;

CREATE OR REPLACE TABLE catalogue.artist_placement AS
WITH best AS (
  SELECT artist_id, arg_max(term_id, score) AS main_term, arg_max(via, score) AS via, max(score) / sum(score) AS share,
         list(term_id ORDER BY score DESC) AS terms
  FROM placement_scores GROUP BY artist_id),
museum AS (SELECT artist_slug AS artist_id, 'period:' || period_slug AS period_id FROM wikipedia.artists),
-- the main term's period, when the artist's working life comes within 15 years of it
styled AS (
  SELECT b.artist_id, bl.period_id
  FROM best b JOIN taxonomy.term_lineage bl ON bl.term_id = b.main_term
  JOIN taxonomy.terms p ON p.term_id = bl.period_id JOIN catalogue.artists a USING (artist_id)
  WHERE a.birth_year IS NULL OR (a.birth_year + 20 <= p.end_year + 15
                                 AND coalesce(a.death_year, a.birth_year + 60) >= p.start_year - 15)),
-- by years: the European chronology, without the national schools (Dutch Golden Age, Hudson River School,
-- Pre-Raphaelites, Group of Seven ...): years alone cannot say who belongs to those. Inside the main term's era
-- when the artist's life falls in it; inside its tradition always (Asian, Islamic ... periods by their own years).
years AS (
  SELECT y.artist_id, arg_max(y.term_id, y.overlap) AS period_id
  FROM period_by_years y
  LEFT JOIN best b USING (artist_id) LEFT JOIN taxonomy.term_lineage bl ON bl.term_id = b.main_term
  JOIN taxonomy.terms p ON p.term_id = y.term_id
  WHERE CASE WHEN bl.era_id LIKE 'tradition:%' THEN y.era_id = bl.era_id
             ELSE y.era_id LIKE 'era:%' AND coalesce(p.place, 'Italy') IN ('Italy', 'Northern Europe') END
  GROUP BY y.artist_id),
era_by_years AS (
  SELECT a.artist_id, arg_max(e.term_id,
           least(coalesce(a.death_year, a.birth_year + 60), e.end_year) - greatest(a.birth_year + 20, e.start_year)) AS era_id
  FROM catalogue.artists a JOIN taxonomy.terms e
    ON e.kind = 'era' AND e.start_year <= coalesce(a.death_year, a.birth_year + 60) AND e.end_year >= a.birth_year + 20
  WHERE a.birth_year IS NOT NULL GROUP BY a.artist_id)
SELECT a.artist_id, b.main_term, ml.movement_id,
       coalesce(mu.period_id, st.period_id, y.period_id) AS period_id,
       -- a main term above any period (Antiquity, Byzantine art ...) names the era itself; WikiArt's collective
       -- "artists" (Ancient Greek Pottery, Orthodox Icons) carry placeholder birth years
       coalesce(pl.era_id, CASE WHEN ml.period_id IS NULL OR a.birth_year IS NULL THEN ml.era_id END, ey.era_id) AS era_id,
       CASE WHEN mu.period_id IS NOT NULL THEN 'museum' WHEN b.main_term IS NOT NULL THEN b.via
            WHEN y.period_id IS NOT NULL OR ey.era_id IS NOT NULL THEN 'years' END AS placed_by,
       round(b.share, 3) AS confidence, b.terms
FROM catalogue.artists a
LEFT JOIN best b USING (artist_id)
LEFT JOIN taxonomy.term_lineage ml ON ml.term_id = b.main_term
LEFT JOIN museum mu USING (artist_id)
LEFT JOIN styled st USING (artist_id)
LEFT JOIN years y USING (artist_id)
LEFT JOIN era_by_years ey USING (artist_id)
LEFT JOIN taxonomy.term_lineage pl ON pl.term_id = coalesce(mu.period_id, st.period_id, y.period_id);

-- where each work sits: its own WikiArt style when it has one (the most specific), else Wikidata's movement for
-- it, else its artist's main term; its genre from WikiArt, else Wikidata (the most specific).
CREATE OR REPLACE TABLE catalogue.work_placement AS
WITH depth AS (SELECT term_id, max(depth) AS depth FROM taxonomy.term_ancestors GROUP BY term_id),
work_term AS (
  SELECT w.work_id, xt.term_id, t.kind, d.depth, 'wikiart' AS via
  FROM catalogue.works w
  JOIN taxonomy.work_terms xt ON xt.source = 'wikiart' AND xt.work_key = w.content_id::VARCHAR
  JOIN taxonomy.terms t USING (term_id) JOIN depth d USING (term_id)
  UNION ALL
  SELECT w.work_id, xt.term_id, t.kind, d.depth, 'wikidata'
  FROM catalogue.works w
  JOIN taxonomy.work_terms xt ON xt.source = 'museum' AND xt.work_key = w.work_id AND xt.via LIKE 'wikidata %'
  JOIN taxonomy.terms t USING (term_id) JOIN depth d USING (term_id)),
style AS (
  SELECT work_id, arg_max(term_id, (via = 'wikiart')::INT * 100 + depth) AS term_id,
         arg_max(via, (via = 'wikiart')::INT * 100 + depth) AS via
  FROM work_term WHERE kind IN ('movement', 'umbrella', 'historical-period', 'period') GROUP BY work_id),
genre AS (
  SELECT work_id, arg_max(term_id, (via = 'wikiart')::INT * 100 + depth) AS genre_id,
         arg_max(via, (via = 'wikiart')::INT * 100 + depth) AS via
  FROM work_term WHERE kind = 'genre' GROUP BY work_id)
SELECT w.work_id, w.artist_id, coalesce(s.term_id, ap.main_term) AS main_term,
       coalesce(sl.movement_id, ap.movement_id) AS movement_id,
       coalesce(sl.period_id, ap.period_id) AS period_id,
       coalesce(sl.era_id, ap.era_id) AS era_id,
       g.genre_id,
       CASE WHEN s.term_id IS NOT NULL THEN s.via WHEN ap.main_term IS NOT NULL OR ap.period_id IS NOT NULL THEN 'artist' END
         AS placed_by,
       g.via AS genre_via
FROM catalogue.works w
LEFT JOIN style s USING (work_id) LEFT JOIN taxonomy.term_lineage sl ON sl.term_id = s.term_id
LEFT JOIN genre g USING (work_id)
LEFT JOIN catalogue.artist_placement ap USING (artist_id);

-- every field resolved across the sources, with where it came from (see the top of this file)
CREATE OR REPLACE TEMP TABLE facts_by_qid AS
SELECT qid,
       min(TRY_CAST(regexp_extract(value, '^([0-9]{{3,4}})-', 1) AS INT)) FILTER (WHERE property = 'inception') AS year,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'instance_of' AND label IS NOT NULL) AS types,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'material' AND label IS NOT NULL) AS materials,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'collection' AND label IS NOT NULL) AS collections,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property = 'location' AND label IS NOT NULL) AS locations,
       list(DISTINCT label ORDER BY label) FILTER (WHERE property IN ('depicts', 'main_subject') AND label IS NOT NULL)
         AS depicts,
       min(value) FILTER (WHERE property = 'inventory_number') AS inventory_number
FROM wikidata.work_facts GROUP BY qid;

CREATE OR REPLACE TEMP TABLE media_list AS
SELECT content_id, list(medium ORDER BY medium) AS media FROM wikiart.work_media GROUP BY content_id;

CREATE OR REPLACE TEMP TABLE works_resolved AS
SELECT w.* REPLACE (coalesce(w.qid, u.qid) AS qid,
                    coalesce(CASE WHEN w.in_museum THEN w.year END, f.year, u.year, w.year) AS year,
                    coalesce(w.wikidata_page, 'https://www.wikidata.org/wiki/' || u.qid) AS wikidata_page,
                    coalesce(w.commons_page, CASE WHEN u.commons_file IS NOT NULL THEN
                      'https://commons.wikimedia.org/wiki/File:' || commons_name(u.commons_file) END) AS commons_page),
       CASE WHEN w.in_museum THEN 'wikipedia' WHEN u.title IS NOT NULL THEN 'wikidata' ELSE 'wikiart' END AS title_source,
       u.title AS wikidata_title,
       CASE WHEN w.in_museum AND w.year IS NOT NULL THEN 'wikipedia' WHEN coalesce(f.year, u.year) IS NOT NULL THEN 'wikidata'
            WHEN w.year IS NOT NULL THEN 'wikiart' END AS year_source,
       CASE WHEN w.width_cm IS NOT NULL THEN CASE WHEN w.in_museum THEN 'wikipedia' ELSE 'wikiart' END END AS size_source,
       coalesce(f.types, u.type_labels) AS types,
       CASE WHEN coalesce(f.types, u.type_labels) IS NOT NULL THEN 'wikidata' END AS types_source,
       coalesce(f.materials, CASE WHEN ww.material IS NOT NULL OR ww.technique IS NOT NULL
                                  THEN list_filter([ww.material, ww.technique], x -> x IS NOT NULL) END, ml.media) AS materials,
       CASE WHEN f.materials IS NOT NULL THEN 'wikidata'
            WHEN ww.material IS NOT NULL OR ww.technique IS NOT NULL OR ml.media IS NOT NULL THEN 'wikiart' END AS materials_source,
       coalesce(f.collections, CASE WHEN ww.gallery_name IS NOT NULL THEN [ww.gallery_name] END) AS collections,
       CASE WHEN f.collections IS NOT NULL THEN 'wikidata' WHEN ww.gallery_name IS NOT NULL THEN 'wikiart' END AS collections_source,
       coalesce(f.locations, CASE WHEN ww.location IS NOT NULL THEN [ww.location] END) AS locations,
       CASE WHEN f.locations IS NOT NULL THEN 'wikidata' WHEN ww.location IS NOT NULL THEN 'wikiart' END AS locations_source,
       f.depicts, f.inventory_number,
       u.commons_file AS commons_file_moved_to,
       coalesce(w.wikimedia_original, CASE WHEN u.commons_file IS NOT NULL THEN commons_original(u.commons_file) END,
                w.wikiart_image) AS image_url,
       CASE WHEN w.wikimedia_original IS NOT NULL OR u.commons_file IS NOT NULL THEN 'commons'
            WHEN w.wikiart_image IS NOT NULL THEN 'wikiart' END AS image_source,
       p.main_term, p.movement_id, p.period_id, p.era_id, p.genre_id,
       p.placed_by AS movement_source, p.genre_via AS genre_source,
       u.method AS moved_by
FROM catalogue.works w
LEFT JOIN compare.wikiart_to_wikidata u ON u.content_id = w.content_id AND NOT w.in_museum
LEFT JOIN facts_by_qid f ON f.qid = coalesce(w.qid, u.qid)
LEFT JOIN wikiart.works ww ON ww.content_id = w.content_id
LEFT JOIN media_list ml ON ml.content_id = w.content_id
LEFT JOIN catalogue.work_placement p USING (work_id);

CREATE OR REPLACE TABLE catalogue.works AS SELECT * FROM works_resolved;

-- artists to add next, every nationality: public domain, not in the museum, an English Wikipedia article, with the
-- museum period they would hang in (catalogue.artist_placement).
CREATE OR REPLACE TABLE compare.candidates AS
SELECT a.artist_id, a.name, a.birth_year, a.death_year, a.nationalities, a.movements, a.schools,
       a.wikiart_works, a.wikipedia_url, a.qid, replace(p.period_id, 'period:', '') AS suggested_period,
       p.main_term, p.era_id, p.placed_by, p.confidence
FROM catalogue.artists a LEFT JOIN catalogue.artist_placement p USING (artist_id)
WHERE NOT a.in_museum AND a.public_domain AND a.wikipedia_url IS NOT NULL
ORDER BY a.wikiart_works DESC NULLS LAST, a.name;

-- one row per work (or artist) per source: the links, licence and the source's own id. A new source (a museum's
-- IIIF collection, Europeana ...) adds rows here, not columns.
CREATE OR REPLACE TABLE catalogue.work_sources AS
SELECT w.work_id, 'wikimedia' AS source, w.commons_page AS source_id, w.commons_page AS page_url,
       w.image_url, coalesce(w.wikimedia_thumb, commons_thumb(w.commons_file_moved_to, 250)) AS image_small,
       coalesce(w.wikimedia_wall, commons_thumb(w.commons_file_moved_to, 960)) AS image_medium,
       w.image_url AS image_full, CASE WHEN w.copyrighted THEN 'in copyright' ELSE fi.license END AS licence
FROM catalogue.works w LEFT JOIN wikimedia.file_info fi ON fi.file = w.commons_file_moved_to
WHERE w.image_source = 'commons'
UNION ALL
SELECT work_id, 'wikipedia', wikipedia_url, wikipedia_url, NULL, NULL, NULL, NULL, 'CC BY-SA 4.0 (text)'
FROM catalogue.works WHERE wikipedia_url IS NOT NULL
UNION ALL
SELECT work_id, 'wikidata', qid, wikidata_page, NULL, NULL, NULL, NULL, 'CC0'
FROM catalogue.works WHERE qid IS NOT NULL
UNION ALL
SELECT w.work_id, 'wikiart', w.content_id::VARCHAR, w.wikiart_page, w.wikiart_image,
       w.wikiart_image || '!PinterestLarge.jpg', w.wikiart_image || '!Large.jpg', w.wikiart_image,
       CASE WHEN a.public_domain THEN 'public domain (artist died more than 70 years ago)' ELSE 'in copyright' END
FROM catalogue.works w JOIN catalogue.artists a USING (artist_id) WHERE w.content_id IS NOT NULL;

CREATE OR REPLACE TABLE catalogue.artist_sources AS
SELECT artist_id, 'wikipedia' AS source, wikipedia_url AS page_url FROM catalogue.artists WHERE wikipedia_url IS NOT NULL
UNION ALL SELECT artist_id, 'wikidata', wikidata_page FROM catalogue.artists WHERE qid IS NOT NULL
UNION ALL SELECT artist_id, 'wikiart', wikiart_page FROM catalogue.artists WHERE wikiart_url IS NOT NULL;

"""
