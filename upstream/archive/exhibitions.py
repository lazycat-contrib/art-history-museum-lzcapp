"""Exhibitions: rooms hung the way museums hang them, across artists, schools and eras, built from our taxonomy.

Defined by hand in data/taxonomy (tracked):
  exhibitions.csv        exhibition_id, title, kind (school, theme, period, artist-life ...), intro
  exhibition_rooms.csv   exhibition_id, room, title, wall_text, then the room's selector, every part optional:
                           terms        space-separated taxonomy term_ids; a work or its artist carries one
                           artists      space-separated artist_ids
                           nationality  |-separated: WikiArt nationalities or Wikidata citizenship ("Greece|Greeks")
                           year_from, year_to   the work's year
                           title_words  a regular expression on the work's title ("sea|harbou?r|ships?")
                         max_works (default 30) and rank_by: featured (featured artists first, then the most
                         viewed works, then by date; the default) or year (by date).

Built by `python -m archive.warehouse build` (after the catalogue):
  exhibition.exhibitions, exhibition.rooms   the CSVs, with the selector parts as lists
  exhibition.room_works                      the works each room hangs, in order (museum works with an image)
"""

EXHIBITION_SQL = """
CREATE SCHEMA IF NOT EXISTS exhibition;
CREATE OR REPLACE TABLE exhibition.exhibitions AS
SELECT exhibition_id, title, kind, intro FROM read_csv('{exhibitions}', header = true, all_varchar = true);

CREATE OR REPLACE TABLE exhibition.rooms AS
SELECT exhibition_id, room::INT AS room, title, wall_text,
       nullif(string_split(trim(coalesce(terms, '')), ' '), ['']) AS terms,
       nullif(string_split(trim(coalesce(artists, '')), ' '), ['']) AS artists,
       nullif(string_split(trim(coalesce(nationality, '')), '|'), ['']) AS nationality,
       TRY_CAST(year_from AS INT) AS year_from, TRY_CAST(year_to AS INT) AS year_to,
       nullif(trim(coalesce(title_words, '')), '') AS title_words,
       coalesce(TRY_CAST(max_works AS INT), 30) AS max_works, coalesce(nullif(rank_by, ''), 'featured') AS rank_by
FROM read_csv('{exhibition_rooms}', header = true, all_varchar = true);

CREATE OR REPLACE TABLE exhibition.room_works AS
WITH hung AS (
  SELECT w.work_id, w.artist_id, w.title, w.year, w.pageviews, w.terms AS work_terms, a.terms AS artist_terms,
         a.featured, list_concat(coalesce(a.nationalities, []), coalesce(a.citizenship, [])) AS nations
  FROM catalogue.works w JOIN catalogue.artists a USING (artist_id)
  WHERE w.in_museum AND w.wikimedia_original IS NOT NULL AND NOT coalesce(w.copyrighted, false)),
picked AS (
  SELECT r.exhibition_id, r.room, h.work_id, h.artist_id, h.title, h.year, h.featured, h.pageviews, r.max_works,
         row_number() OVER (PARTITION BY r.exhibition_id, r.room ORDER BY
           CASE WHEN r.rank_by = 'year' THEN 0 ELSE CASE WHEN h.featured THEN 0 ELSE 1 END END,
           CASE WHEN r.rank_by = 'year' THEN 0 ELSE -coalesce(h.pageviews, 0) END,
           h.year NULLS LAST, h.work_id) AS position
  FROM exhibition.rooms r JOIN hung h
    ON (r.terms IS NULL OR list_has_any(coalesce(h.work_terms, []), r.terms) OR list_has_any(coalesce(h.artist_terms, []), r.terms))
   AND (r.artists IS NULL OR list_contains(r.artists, h.artist_id))
   AND (r.nationality IS NULL OR list_has_any(h.nations, r.nationality))
   AND (r.year_from IS NULL OR h.year >= r.year_from)
   AND (r.year_to IS NULL OR h.year <= r.year_to)
   AND (r.title_words IS NULL OR regexp_matches(lower(h.title), '\\b(' || r.title_words || ')\\b')))
SELECT exhibition_id, room, position, work_id, artist_id, title, year
FROM picked WHERE position <= max_works ORDER BY exhibition_id, room, position;
"""
