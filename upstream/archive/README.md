# Data archive

Source crawls and one DuckDB warehouse (`data/museum.duckdb`) that holds every artist and every work in our own
format, with links to every source (Wikipedia, Wikimedia Commons, Wikidata, WikiArt; museums later). Wikipedia,
Wikidata and Commons are the base (texts, licences, sizes, the sharpest images); WikiArt adds coverage (more
works and artists) and categories. Our own taxonomy (`data/taxonomy/`) is the structure rooms are built from: eras, periods, movements, schools
and genres, with WikiArt's and Wikidata's categories mapped onto it. Every painting is matched across the sources
(`compare.work_match`), and every artist and painting is placed in that structure (`catalogue.artist_placement`,
`catalogue.work_placement`). Metadata only: images load straight from the sources, nothing is downloaded to keep.

The site reads `data/site/museum.json` (`python -m archive.site`): the Wikipedia ingest plus the works only
WikiArt has, for public-domain artists.

## What is where

| Place | What |
|---|---|
| `data/wikipedia/` | The museum's own snapshot (`npm run ingest`, `enrich`, `repair-data`): `museum.json`, `artists/*.json`. Was `data/cache/`. |
| `data/wikiart/` | The WikiArt crawl (git-ignored): `dictionaries.json` (13 category groups), `artists.json` (5,751 artists), `wikidata.json` (Wikidata items with a WikiArt ID), `api_v2.json` (category IDs, artist periods, series, related artists), `artists/<url>.json` (one artist with every work and its details). |
| `data/wikidata/` | (git-ignored) `artist_facts.json`: life facts per artist; `work_facts/*.parquet`: per museum work, what it is, movement, genre, material, collection, location, depicts, inception, WikiArt ID. |
| `data/wikidata/creator_works.parquet` | (git-ignored) every Wikidata item by a museum artist: type, title, year, Commons image, WikiArt ID. |
| `data/wikimedia/file_info.parquet` | (git-ignored) size, author and licence of the Commons files WikiArt-only works move onto. |
| `data/fingerprints/` | (git-ignored) one 64-bit image fingerprint and a colourfulness number per image compared; no images. |
| `data/taxonomy/` | **Our own categories** (tracked): `terms.csv` and `mappings.csv`. Edited by hand; see below. |
| `data/site/` | (git-ignored, deployed) `museum.json`: the site's snapshot, `python -m archive.site`; `rooms.json`: what custom rooms select from (every hung work's era, period, movement, genre; artists' nationalities and schools); `guide/<artist>.json`: the audio guide's scripts, `python -m archive.guide build`. |
| `data/guide/recording/` | (git-ignored) recording sheets for a narrator, `python -m archive.guide sheet`. |
| `data/museum.duckdb` | The warehouse (git-ignored), rebuilt from the three folders above. |
| `gs://museum-archive/` | Raw copies of every WikiArt response, and the warehouse as Parquet. No images for now (v2): the site loads them straight from Wikimedia and WikiArt; the image copies (`wikiart_images`, `wikimedia_media`) are on hold. The layout is in `archive/bucket.py`. |
| `%LOCALAPPDATA%\museum-archive\` | Work in progress outside Google Drive: partial crawls, the media manifest. |

## Run it

Double-click `run_dagster.cmd` (first time: it installs `dagster/.venv` and `archive/.venv`), then open
<http://127.0.0.1:3080> → **Jobs** → **Launch run**:

| Job | What it does | Time |
|---|---|---|
| `wikiart_crawl` | Categories, artist list, Wikidata IDs, API v2, then every artist's works with details, then the warehouse | about a day; resumes |
| `wikiart_api_v2` | API v2 only (about 130 requests, paced under the keyless limit of about 400 an hour) | about 25 min |
| `wikiart_images` | On hold (v2). WikiArt images into the bucket: artists who died more than 70 years ago (`all_artists: true` for all) | hours; resumes |
| `wikimedia_media` | Every Wikimedia file the site requests (portraits, thumbnails, wall, close-up and inspect textures, about 365k files) into the bucket at the same path | about half a day; resumes |
| `wikidata_facts` | Life facts per artist and facts per work from Wikidata (metadata only), then the warehouse | about 5 h the first time; resumes |
| `wikimedia_links` | Every Wikimedia link the site uses into `wikipedia.media`; downloads nothing | about 2 min |
| `warehouse_build` | Rebuild `data/museum.duckdb`, fingerprint new images, rebuild with them (the painting match), Parquet to the bucket, then `data/site/museum.json` | minutes, or hours when many images are new |

The Wikimedia mirror (`wikimedia_media`) is on hold: the site keeps loading from Wikimedia and the links are
kept in the warehouse until hosting is decided.

One crawler per site at a time (Dagster pools), one run per job at a time. Stopped runs are launched again;
they continue where they stopped. `run_dagster.cmd status | stop` as in scrape-cars.

By hand, with `archive/.venv/Scripts/python`:

```
python -m archive.wikiart dictionaries | artists | crosswalk | api-v2
python -m archive.wikiart crawl [--limit N] [--artists claude-monet,qi-baishi] [--refresh]
python -m archive.wikiart images [--all] [--limit N]
python -m archive.wikimedia manifest
python -m archive.wikimedia mirror [--kinds portrait,thumb,wall,near,inspect] [--limit N]   # add original for full files (~340 GB)
python -m archive.wikidata artists | works [--refresh] | creators
python -m archive.wikimedia file-info                       # size, author, licence of Commons files works move onto
python -m archive.warehouse build | seed-taxonomy | export
python -m archive.fingerprints                              # image fingerprints for the painting match
python -m archive.site                                      # data/site/museum.json and rooms.json for the site
python -m archive.guide build                               # data/site/guide: the audio guide's scripts
python -m archive.guide sheet [--artists a,b]               # recording sheets for a narrator
python -m archive.warehouse to-postgres --dsn postgresql://...   # every table, same schema and names
```

## The warehouse

| Schema | Tables |
|---|---|
| `wikipedia` | `periods`, `artists`, `works` |
| `wikiart` | `dictionaries` (every category of the 13 groups, one `dictionary_key` across both ID systems), `artist_list`, `artists`, `artist_dictionaries`, `artist_periods`, `artist_series`, `related_artists`, `works`, `work_dictionaries`, `work_tags`, `wikidata` |
| `taxonomy` | `terms`, `mappings` (from the CSVs), `term_ancestors` and `term_lineage` (each term's era, period and movement), `artist_terms`, `work_terms` (WikiArt and Wikidata), `term_summary` |
| `wikidata` | `artist_facts`: one row per artist, property and value, with coordinates for places; `work_facts`: the same per museum work |
| `compare` | `artist_match` (our artist ↔ WikiArt: by Wikidata ID, else Wikipedia article, else name and birth year), `work_match` (one to one: Wikidata's WikiArt ID, else image fingerprints, else title and year; see below), `missing_artists`, `candidates` (public-domain artists to add next, every nationality, with a suggested period), `our_artists_not_on_wikiart`, `artist_work_counts`, `category_coverage` |
| `catalogue` | **Our own format.** `artists` and `works`: every artist and work of every source, merged where matched (identity, `featured`, sizes, categories, life facts, taxonomy terms, links). `work_sources` and `artist_sources`: one row per source with its page, image links (small, medium, full), licence and the source's own ID. A new source adds rows, not columns. `artist_placement` and `work_placement`: each artist and work in our structure (main term, movement, period, era; the work's genre) and what placed it. |

WikiArt's groups: movement, style, genre, school, collection, auction, nationality, field, medium, institution,
artwork type, country. Every source file is parsed by DuckDB straight into typed columns (`read_json` with
`columns`), so a full build takes seconds; the build is capped at 16 GB of memory. Tables use plain types that Postgres also has, so moving to Postgres is
`to-postgres`, not a redesign. DuckDB opens the file directly: `duckdb data/museum.duckdb`, or from Python.

Examples:

```sql
-- public-domain painters WikiArt has and we don't, biggest catalogues first
SELECT name, birth_year, death_year, movements, nationalities, wikiart_works
FROM compare.missing_artists WHERE public_domain LIMIT 50;

-- movements and schools where we miss the most artists
SELECT group_name, title, wikiart_artists, museum_has, museum_missing, missing_examples
FROM compare.category_coverage WHERE group_name IN ('movement', 'school') ORDER BY museum_missing DESC LIMIT 30;

-- one of our terms across both sources
SELECT * FROM taxonomy.term_summary WHERE term_id = 'movement:impressionism';

-- who to add next, by nationality, with a suggested period
SELECT name, birth_year, death_year, wikiart_works, suggested_period FROM compare.candidates
WHERE list_contains(nationalities, 'Portuguese');

-- Greek painters of the nineteenth century, by era and nationality
SELECT a.name, p.period_id, p.main_term FROM catalogue.artists a JOIN catalogue.artist_placement p USING (artist_id)
WHERE p.era_id = 'era:nineteenth-century' AND list_contains(a.nationalities, 'Greeks');

-- every Venetian-school painting with both sources
SELECT w.title, w.wikipedia_url, w.wikiart_page FROM catalogue.works w JOIN taxonomy.work_terms t ON
  t.work_key IN (w.work_id, w.content_id::VARCHAR) WHERE t.term_id = 'school:venetian-school' AND w.content_id IS NOT NULL;

-- an artist's life, for a room themed on it
SELECT name, birth_place, work_locations, teachers, influenced_by, life_periods FROM catalogue.artists
WHERE artist_id = 'pablo-picasso';

-- every source of one work
SELECT * FROM catalogue.work_sources WHERE work_id = 'claude-monet/impression-sunrise';
```

## Our own taxonomy (`data/taxonomy/`)

The point is to combine categories freely (a room from a theme, a school and a century), the way museums hang
collections. We keep our own terms in one tree and say how each source's categories map to them.

`terms.csv`: `term_id, kind, name, parent_id, start_year, end_year, place, qid, description`. Kinds, top down:

| Kind | What | Examples |
|---|---|---|
| `era` | The European chronology | Medieval, Renaissance, Baroque, Eighteenth century, Nineteenth century, Modern, Post-war and contemporary |
| `tradition` | Branches outside it, over many centuries | East Asian, South Asian, Islamic and Persian, Byzantine and post-Byzantine, Colonial Latin American |
| `period` | The museum's timeline rooms (35), each under an era or tradition | Dutch Golden Age (Baroque), Cretan & Ionian Schools (Byzantine) |
| `movement` | Movements and styles, under a period, an era or a larger movement | Luminism under Hudson River School, Synthetic Cubism under Cubism |
| `umbrella` | A broad label over several movements; counts less when placing an artist | Modernism, Avant-garde, Abstract art |
| `historical-period` | A dynasty or reign used as a style | Edo, Ming, Safavid |
| `school`, `group`, `academy`, `exhibition` | A local school, an artists' society, a teaching institution, an exhibition, each under the movement or period it belongs to, with a place | Venetian School, Peredvizhniki, Degenerate Art (Munich, 1937) |
| `genre` | Its own tree, the academic hierarchy | History painting > Religious, Mythological, Allegory, Battle; Landscape > Cityscape > Veduta |

- `term_id`: `<kind>:<slug>`, stable (WikiArt's terms keep the id they were seeded with, whatever their kind now);
  rename the `name`, never the id once rooms use it.
- `parent_id`: one level up; `taxonomy.term_lineage` gives every term its era, period and movement.
- `qid`: the Wikidata item, checked against its label. A term's qid maps Wikidata's uses of that item to it.

`mappings.csv`: `source, source_group, source_key, source_name, term_id`
- `wikiart, movement | style | school | genre, <dictionary_key>`: WikiArt's categories. WikiArt files artists
  under movements and each painting under a style; both map here.
- `wikidata, movement, <QID>`: Wikidata movements (and groups) used on artists' and works' pages, when they are
  not a term's own qid (e.g. `French Realism` to `movement:realism`).
- `museum, period, <period slug>`: the timeline.

Placing artists (`catalogue.artist_placement`): the share of the artist's WikiArt paintings in each style counts
double, WikiArt's and Wikidata's movements once, schools and groups half (for the movement above them); umbrella
labels count less. The best term gives the movement, period and era; a museum artist keeps the museum's period;
with no evidence, the years decide. Paintings (`catalogue.work_placement`) take their own WikiArt style, else
Wikidata's movement, else their artist's; their genre from WikiArt, else Wikidata.

`python -m archive.warehouse seed-taxonomy` adds candidate terms and mappings for new categories (at least 3
artists) and **never changes or removes an existing row**, so hand edits survive. Run it after a crawl, place the
new terms in the tree by hand, then `warehouse_build`.

## Exhibitions: rooms the way museums hang them

Rooms that mix artists, schools and eras are defined in two tracked CSVs next to the taxonomy, and resolved by
the warehouse (`archive/exhibitions.py`):

- `data/taxonomy/exhibitions.csv`: `exhibition_id, title, kind, intro`.
- `data/taxonomy/exhibition_rooms.csv`: one row per room: `title, wall_text`, then a selector, every part
  optional: `terms` (taxonomy term_ids), `artists`, `nationality` (WikiArt nationality or Wikidata citizenship,
  `Greece|Greeks`), `year_from`, `year_to`, `title_words` (a regular expression), and `max_works`, `rank_by`
  (`featured`: featured artists first, then the most viewed works; or `year`).
- `exhibition.room_works` lists the works each room hangs, in order.

First two: "Greek Painting, from Crete to Gaïtis" and "The Sea". The galleries do not render exhibitions yet:
that is the next step (a suite built from a room list, labels naming each work's artist, room design per room).

## The base, and what WikiArt adds

Wikipedia, Wikidata and Commons are the base: open identifiers every museum links to (Wikidata QIDs), images
with a licence and author on every file, and per-work facts (collection, material, size, date). WikiArt fills
gaps (artists and works the base lacks) and classifies (a style and genre on every painting). `catalogue.works`
resolves every field and says where it came from (`<field>_source`):

| Field | First | Then |
|---|---|---|
| Identity | Wikidata QID | WikiArt id, for works Wikidata lacks |
| Image | Commons | WikiArt (public-domain artists only) |
| Title, year, size, type, material, collection, location | Wikipedia / Wikidata | WikiArt |
| Movement (style) | WikiArt's style for the painting | Wikidata's movement, then the artist's |
| Genre | WikiArt | Wikidata |
| Text | Wikipedia only | WikiArt's texts are never published |

A WikiArt-only work is provisional. `compare.wikiart_to_wikidata` looks for it among the artist's other
Wikidata works (`wikidata.creator_works`): Wikidata's WikiArt ID, then the Commons image's fingerprint, then the
title, as below. Found, it moves onto the base: its Wikidata item, the Commons image with its author and licence
(`wikimedia.file_info`), and Wikidata's type, which tells a print or a drawing from a painting where WikiArt does
not. `archive/site.py` hangs it that way.

## One painting, both sources

The same painting often has another title (often in another language) or date on WikiArt than on Wikipedia.
`python -m archive.fingerprints` stores a 64-bit fingerprint (dHash) of a small rendition of every work of
every artist found on both sources (`data/fingerprints/*.parquet`, `catalogue.image_fingerprints`); the images
are read in memory and dropped. `compare.work_match` then pairs each of our works with at most one WikiArt work,
the best for both:

1. `wikidata`: the work's Wikidata item names the WikiArt work (P6002).
2. `image`: fingerprints within 4 bits (unless the years disagree by more than 3), or within 10 bits with a like
   title or the same year. "Roche pourrie" and "Crumbling Rocks" are one Courbet.
3. `image+title`: within 20 bits and nearly the same title (one painting, photographed differently).
4. `title+year`, `title`, `title+year~`: the same title, years within 2, only where a fingerprint is missing. Same
   title but far-apart images is a second version, not a match.

Measured on 1.7 million same-artist pairs (2026-10-08): of the matches Wikidata states, 114 of 115 lie within 20
bits. `catalogue.works` merges each match into one row with the links of both. `archive/site.py` adds a WikiArt
work only when it matches none of ours, by this table and by image against all of the artist's works.

Prints and drawings: most WikiArt works list no medium, so `archive/site.py` also leaves out works tagged as
sketches or drawings and works with a black-and-white image (the fingerprint step's colourfulness below 12; no
oil or tempera painting measured scores under 15), except Chinese and Japanese ink painting.

## Custom rooms and the audio guide

The site's custom rooms (`/rooms` to choose, `/room?...` to walk, `src/lib/rooms.ts`) select from
`data/site/rooms.json`: every hung work with its era, period, movement and genre (`catalogue.work_placement`) and
the museums holding it (`catalogue.works.collections`: Wikidata's collection and WikiArt's "Name, City, Country"
as one museum, linked by a stable slug, `site.museum_name` / `museum_slug`), and every artist's nationalities and
schools. The selection is the URL, so a room's link is the room. With a museum, works picked by hand and a floor
plan of one's own, a room recreates a museum (`src/lib/museum-rooms.ts`: the National Gallery of Greece, the
Rijksmuseum, the Louvre's paintings). `python -m archive.site rooms` rebuilds only `rooms.json`.

The audio guide reads standard scripts, not text composed in the browser: `python -m archive.guide build` writes
one per artist (their life: dates, where they were born, studied and worked, the Wikipedia summary) and one per
work (first what you see in it: the Wikipedia text's describing sentences, else what Wikidata says it depicts;
then title, artist, year, medium and size, the rest of the Wikipedia text, two facts, the collection), each with a
stable id (`artist:<slug>`, `work:<artist>/<work>`) and a version. The browser's voice reads them today; a
narrator can record them from `python -m archive.guide sheet` (a recording named by the id, in
`public/audio/guide/`, is played instead while its version is unchanged); a live AI voice can read the same
scripts later. Text from Wikipedia is credited (CC BY-SA 4.0) under the subtitles.

## Rights

WikiArt's terms do not restrict automated access, and robots.txt allows everything (checked 2026-10-07). Its
keyless API v2 is limited to about 400 requests an hour, so the crawl uses its plain JSON pages and calls API v2
only for category IDs and artist data. We keep facts and public-domain images; WikiArt's own texts (biographies,
descriptions) stay in the archive for comparison and are not published. Works by artists who died less than 70
years ago are archived only with `all_artists`, and never shown.
