"""The site's snapshot: the Wikipedia ingest plus the works only WikiArt has, for public-domain artists.

    python -m archive.site      data/wikipedia/museum.json + the warehouse -> data/site/museum.json

Each museum artist who is in the public domain and matched on WikiArt (compare.artist_match) gets the WikiArt
works that match none of their Wikipedia works (compare.work_match): paintings only (no sketches, designs,
illustrations, sculpture, photos ..., by genre; no prints or drawings, by medium: wikiart.work_media), no details.
Most WikiArt works list no medium, so when none is listed a print or drawing is also told by WikiArt's image tags
("Sketch", "Figure drawing" ...) and by a black-and-white image (fingerprints.MONOCHROME), except in the ink-painting
traditions. A WikiArt work keeps the same shape as every other work. When the same painting is on Wikidata with a
Commons image (compare.wikiart_to_wikidata), it hangs on the base: Wikidata's title, year and item, the Commons
image with its author and licence (wikimedia.file_info); and Wikidata's type decides when it is a print or a
drawing. Otherwise its image is WikiArt's full file (src/lib/img.ts picks WikiArt's smaller renditions from far
away), credited "Public domain" with the WikiArt page as its source. Works by artists still in copyright are
never added.

The site (src/lib/data.ts) and `npm run load-db` read data/site/museum.json when it exists, else the ingest's.
Run it after every ingest / repair-data, once the warehouse is rebuilt.
"""
from __future__ import annotations

import difflib
import json
import re
import sys
import unicodedata
from collections import Counter
from datetime import datetime, timezone

import duckdb

from . import settings
from .fingerprints import GLOB as FINGERPRINTS, MONOCHROME, SAME_IMAGE, distance, table as fingerprint_table

OUT = settings.DATA / "site" / "museum.json"
# What custom rooms select from (src/lib/rooms.ts, server only): every hung work with its era, period, movement
# and genre (catalogue.work_placement), every artist's nationalities and terms, and the taxonomy terms in use.
ROOMS_OUT = settings.DATA / "site" / "rooms.json"
NOT_PAINTING = {"sketch and study", "design", "illustration", "sculpture", "installation", "photo", "performance",
                "poster", "ornament", "utensil", "architecture", "graffiti", "caricature", "jewelry", "manga",
                "digital", "advertisement", "video", "tapestry", "mosaic", "calligraphy", "tessellation"}
# WikiArt media (wikiart.work_media). A work is left out when every medium it lists is a print, drawing, photo or
# sculpture medium and none is a paint; supports (paper, canvas, wood, board) and ink say nothing either way.
PAINT = re.compile(r"^(oil|acryl|oleo|huile|pintura|tempera|fresco|gouache|goache|watercolo|pastel|oil-pastel|"
                   r"wax-pastel|encaustic|casein|enamel|gilt|gold|lacquer|mural|paint|pigment|natural-pigments|"
                   r"grisaille|magna|polymer-paint|emulsion|wash|lavis|distemper)")
NOT_PAINT = re.compile(r"^(aquatint|drypoint|engraving|etching|intaglio|lino|lithograph|mezzotint|screenprint|"
                       r"silkscreen|woodcut|wood-engraving|color-woodblock|cliche-verre|photogravure|photolitho|"
                       r"collotype|print|pencil|graphite|chalk|charcoal|conte|crayon|colored-pencil|silverpoint|"
                       r"metalpoint|leadpoint|sanguine|pen$|felt-tip|stump|drawing|photograph|gelatin|photogram|"
                       r"photomontage|bronze|marble|plaster|terracotta|ceramics|porcelain|stained-glass|tapestry|"
                       r"embroidery|digital|video|sculpture)")
PRINTS_ALLOWED = {"ukiyo-e"}  # the ukiyo-e masters' woodblock prints hang, as in the ingest
# WikiArt image tags that mark a drawing, print or design: on WikiArt works with a known medium, "Sketch" tags
# 1,414 prints and drawings and 13 paintings, "Figure drawing" 466 and 4 (2026-10-08). Used only when the work
# lists no paint medium.
GRAPHIC_TAGS = {"sketch", "figure drawing", "monochrome", "designs-and-sketches", "fashion illustration",
                "monochrome photography", "architectural drawings", "technical drawing", "handwriting"}
# Wikidata types (instance of) that are not paintings; a work keeps its place when any of its types is a painting.
NOT_PAINTING_TYPE = re.compile(r"print|engraving|etching|drypoint|aquatint|mezzotint|woodcut|lithograph|drawing|"
                               r"sketch|study|photograph|sculpture|statue|relief|tapestry|book|manuscript|"
                               r"illustration|poster|stamp|coin|medal", re.I)
PAINTING_TYPE = re.compile(r"painting|fresco|icon|altarpiece|triptych|diptych|polyptych|mural|miniature|panel", re.I)


def wikidata_says_not_painting(types: list[str] | None) -> bool:
    types = types or []
    return bool(types) and not any(PAINTING_TYPE.search(t) for t in types) and any(
        NOT_PAINTING_TYPE.search(t) for t in types)


# Ink painting is monochrome painting: no colour test there.
INK_PAINTING = {"chinese-painting", "japanese-painting"}


def not_a_painting(media: list[str], period: str) -> bool:
    if not media or any(PAINT.match(m) for m in media):
        return False
    if period in PRINTS_ALLOWED and any(m.startswith(("woodcut", "color-woodblock")) for m in media):
        return False
    return any(NOT_PAINT.match(m) for m in media)


DETAIL = re.compile(r"\bdetails?\b|\(fragment\)|\bfragment of\b", re.I)
PD_MARK = "https://creativecommons.org/publicdomain/mark/1.0/"
# A WikiArt title this close to one of the artist's works is taken as the same painting. The two sites often
# title and date the same canvas differently ("St. Peter" 1613 / "St. Peter" 1610), and title alone cannot tell
# a duplicate from a second version, so a close title is left out: a missing version is better than the same
# painting hung twice. (An image comparison would keep the true second versions.)
SAME_TITLE = 0.8
# Fingerprints within SAME_IMAGE bits are the same image. Within SAME_IMAGE_TITLED bits *and* a close title they
# are the same painting photographed differently (El Greco's single "St Luke Painting the Virgin" lands at 16
# bits); further apart, a close title is a second version (measured on the first 32 artists, 2026-10-08:
# same-title pairs cluster at 0-20 bits and again at 24-40).
SAME_IMAGE_TITLED = 20


def title_key(title: str | None) -> str:
    t = re.sub(r"\(.*?\)", " ", (title or "").lower())
    t = re.sub(r"\bst\.?\s", "saint ", t)
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    return " ".join(w for w in t.split() if w not in {"the", "a", "an", "of", "de", "la", "le"})


def same_title(key: str, others: list[str]) -> bool:
    for other in others:
        if key == other:
            return True
        m = difflib.SequenceMatcher(None, key, other)
        if m.real_quick_ratio() >= SAME_TITLE and m.quick_ratio() >= SAME_TITLE and m.ratio() >= SAME_TITLE:
            return True
        # one title contains the other ("Summer Offering" / "Summer Offering (Young Girl with Roses)")
        if min(len(key), len(other)) >= 12 and (key in other or other in key):
            return True
    return False

QUERY = """
SELECT a.artist_slug, a.name AS artist_name, w.content_id, w.work_url, w.title, w.year, w.genre, w.image_url,
       w.image_width, w.image_height, w.size_x_cm, w.size_y_cm,
       'https://www.wikiart.org/en/' || w.artist_url || '/' || w.work_url AS page, a.period_slug,
       (SELECT list(medium) FROM wikiart.work_media wm WHERE wm.content_id = w.content_id) AS media,
       (SELECT list(tag) FROM wikiart.work_tags wt WHERE wt.content_id = w.content_id) AS tags,
       u.qid AS wd_qid, u.title AS wd_title, u.year AS wd_year, u.type_labels AS wd_types,
       CASE WHEN u.commons_file IS NOT NULL THEN commons_original(u.commons_file) END AS commons_url,
       CASE WHEN u.commons_file IS NOT NULL THEN
         'https://commons.wikimedia.org/wiki/File:' || commons_name(u.commons_file) END AS commons_page,
       fi.width AS fi_width, fi.height AS fi_height, fi.bytes AS fi_bytes, fi.author AS fi_author,
       fi.license AS fi_license, fi.license_url AS fi_license_url, fi.non_free AS fi_non_free
FROM wikipedia.artists a
JOIN compare.artist_match m USING (artist_slug)
JOIN catalogue.artists ca ON ca.artist_id = a.artist_slug
JOIN wikiart.works w ON w.artist_url = m.wikiart_url
LEFT JOIN compare.wikiart_to_wikidata u ON u.content_id = w.content_id
LEFT JOIN read_parquet('{file_info}') fi ON fi.file = u.commons_file
WHERE ca.public_domain AND w.image_url IS NOT NULL AND w.work_url IS NOT NULL
  AND w.content_id NOT IN (SELECT content_id FROM compare.work_match)
ORDER BY a.artist_slug, w.year NULLS LAST, w.title
"""


def graphic(row: dict, colour: float | None) -> bool:
    """A print or drawing that WikiArt files without a medium: told by its tags or a black-and-white image."""
    if any(PAINT.match(m) for m in row["media"] or []):
        return False
    if {t.lower() for t in row["tags"] or []} & GRAPHIC_TAGS:
        return True
    return colour is not None and colour < MONOCHROME and row["period_slug"] not in INK_PAINTING


def painting(row: dict, birth: int | None, death: int | None, colour: float | None = None) -> dict | None:
    genres = {g.strip().lower() for g in (row["genre"] or "").split(",") if g.strip()}
    if (genres & NOT_PAINTING or DETAIL.search(row["title"] or "") or wikidata_says_not_painting(row["wd_types"])
            or not_a_painting(row["media"] or [], row["period_slug"]) or graphic(row, colour)):
        return None
    if row["commons_url"] and row["fi_width"] and not row["fi_non_free"]:
        return on_the_base(row, birth, death)
    year = row["year"]
    if year is not None and ((birth and year < birth) or (death and year > death)):
        year = None  # a date outside the artist's life is a cataloguing error, not a fact
    width, height = row["size_x_cm"], row["size_y_cm"]
    return {
        "slug": f"wikiart-{row['work_url']}",
        "title": row["title"],
        "year": year,
        "imageUrl": row["image_url"],
        "imageWidth": row["image_width"],
        "imageHeight": row["image_height"],
        "story": "",
        "facts": [],
        "wikipediaUrl": None,
        "sitelinks": 0,
        "widthCm": width if width and width > 0 else None,
        "heightCm": height if height and height > 0 else None,
        "pageviews": None,
        "qid": None,
        "imageBytes": None,
        "imageCredit": {"author": row["artist_name"], "license": "Public domain", "licenseUrl": PD_MARK,
                        "page": row["page"]},
        "source": "wikiart",
    }


def on_the_base(row: dict, birth: int | None, death: int | None) -> dict:
    """A WikiArt-only work found on Wikidata with a Commons image: hung with the base's facts and image."""
    year = row["wd_year"] if row["wd_year"] is not None else row["year"]
    if year is not None and ((birth and year < birth) or (death and year > death)):
        year = None
    width, height = row["size_x_cm"], row["size_y_cm"]
    return {
        "slug": f"wikiart-{row['work_url']}",
        "title": row["wd_title"] or row["title"],
        "year": year,
        "imageUrl": row["commons_url"],
        "imageWidth": row["fi_width"],
        "imageHeight": row["fi_height"],
        "story": "",
        "facts": [],
        "wikipediaUrl": None,
        "sitelinks": 0,
        "widthCm": width if width and width > 0 else None,
        "heightCm": height if height and height > 0 else None,
        "pageviews": None,
        "qid": row["wd_qid"],
        "imageBytes": row["fi_bytes"],
        "imageCredit": {"author": row["fi_author"] or row["artist_name"], "license": row["fi_license"],
                        "licenseUrl": row["fi_license_url"], "page": row["commons_page"]},
        "source": "wikidata",
    }


def fingerprints(con) -> tuple[dict[str, int], dict[str, float], dict[tuple[str, str], str]]:
    """url -> dhash and url -> colourfulness (archive/fingerprints.py), and (artist, work) -> the Wikimedia
    thumbnail that was hashed."""
    if not any(FINGERPRINTS.parent.glob("*.parquet")):
        return {}, {}, {}
    rows = con.execute(f"SELECT url, dhash, colorfulness FROM {fingerprint_table(con)}").fetchall()
    hashes = {u: h for u, h, _ in rows}
    colours = {u: c for u, _, c in rows if c is not None}
    thumbs = {(a, w): u for a, w, u in con.execute(
        "SELECT artist_slug, work_slug, url FROM wikipedia.media WHERE kind = 'thumb' AND work_slug IS NOT NULL").fetchall()}
    return hashes, colours, thumbs


def work_key(artist_slug: str, painting_slug: str) -> str:
    """The catalogue's work_id for a hung work (WikiArt additions are wikiart-<work url>)."""
    if painting_slug.startswith("wikiart-"):
        return f"{artist_slug}/{painting_slug[len('wikiart-'):]}"
    return f"{artist_slug}/{painting_slug}"


# One name per museum: Wikidata's collection labels and WikiArt's "Name, City, Country" for the same place.
MUSEUM_ALIASES = {
    "National Gallery of Athens": "National Gallery of Greece",
    "National Art Gallery (Alexandros Soutzos Museum)": "National Gallery of Greece",
    "National Gallery – Alexandros Soutsos Museum": "National Gallery of Greece",
    "Department of Paintings of the Louvre": "Louvre",
    "Louvre Museum": "Louvre",
    "Musée du Louvre": "Louvre",
    "Hermitage": "Hermitage Museum",
    "State Hermitage Museum": "Hermitage Museum",
    "Tretyakov Gallery": "State Tretyakov Gallery",
    "Prado Museum": "Museo del Prado",
    "Uffizi Gallery": "Uffizi",
    "Galleria degli Uffizi": "Uffizi",
    "Van Gogh Museum, Amsterdam": "Van Gogh Museum",
    "Kröller-Müller Museum": "Kröller-Müller Museum",
    "Metropolitan Museum of Art, New York City, NY, US": "Metropolitan Museum of Art",
}


def museum_name(label: str) -> str | None:
    """The museum a collection label names (None for private collections)."""
    label = (label or "").strip()
    if not label or label.lower().startswith("private") or label.lower() in {"unknown", "lost"}:
        return None
    parts = [p.strip() for p in label.split(",")]
    if len(parts) >= 3:
        label = parts[0]  # WikiArt: "Name, City, Country" (or "Name, upper floor, gallery IIa")
    return MUSEUM_ALIASES.get(label, label)


def museum_slug(name: str) -> str:
    """A museum's id in room links (?m=national-gallery-of-greece): stable across rebuilds."""
    plain = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", plain).strip("-")[:80] or "museum"


def rooms_index(museum: dict) -> dict:
    con = duckdb.connect(str(settings.WAREHOUSE), read_only=True)
    try:
        placement = {r[0]: r[1:] for r in con.execute(
            "SELECT work_id, era_id, period_id, movement_id, genre_id FROM catalogue.work_placement").fetchall()}
        held = {r[0]: r[1] for r in con.execute(
            "SELECT work_id, collections FROM catalogue.works WHERE in_museum AND len(collections) > 0").fetchall()}
        artist_rows = con.execute("""
            SELECT a.artist_id, coalesce(a.nationalities, []), coalesce(a.citizenship, []),
                   list_distinct(list_concat(coalesce(p.terms, []),
                     coalesce((SELECT list(DISTINCT term_id) FROM taxonomy.artist_terms t JOIN taxonomy.terms x
                               USING (term_id) WHERE x.kind IN ('school', 'group', 'academy', 'exhibition')
                               AND ((t.source = 'museum' AND t.artist_key = a.artist_id)
                                    OR (t.source = 'wikiart' AND t.artist_key = a.wikiart_url))), [])))
            FROM catalogue.artists a LEFT JOIN catalogue.artist_placement p USING (artist_id)
            WHERE a.in_museum""").fetchall()
        terms = con.execute("""SELECT t.term_id, t.kind, t.name, t.parent_id, t.start_year, t.end_year, t.place
                               FROM taxonomy.terms t""").fetchall()
        ancestors = {}
        for term_id, anc in con.execute("""SELECT term_id, list(ancestor_id ORDER BY depth) FROM taxonomy.term_ancestors
                                           GROUP BY term_id""").fetchall():
            ancestors[term_id] = anc
    finally:
        con.close()
    artists = {a: {"nationalities": sorted(set(n) | set()), "citizenship": sorted(set(c)), "terms": sorted(t)}
               for a, n, c, t in artist_rows}
    works = []
    used: Counter = Counter()
    # the museums holding the works (Wikidata P195, WikiArt's gallery), numbered by first appearance
    museum_ids: dict[str, int] = {}
    museum_works: Counter = Counter()

    def holders(key: str) -> list[int]:
        out: list[int] = []
        for label in held.get(key) or []:
            name = museum_name(label)
            if name is None:
                continue
            i = museum_ids.setdefault(name, len(museum_ids))
            if i not in out:
                out.append(i)
        return out

    for artist in museum["artists"]:
        for pnt in artist["paintings"]:
            if not pnt.get("imageUrl"):
                continue  # rooms hang images only
            key = work_key(artist["slug"], pnt["slug"])
            era, period, movement, genre = placement.get(key, (None, None, None, None))
            period = period or f"period:{artist['periodSlug']}"
            where = holders(key)
            museum_works.update(where)
            works.append([artist["slug"], pnt["slug"], era, period, movement, genre, pnt.get("year"),
                          pnt.get("pageviews") or 0, where or None])
            for t in {era, period, movement, genre} - {None}:
                for a in ancestors.get(t, [t]):
                    used[a] += 1
    for a in artists.values():
        for t in a["terms"]:
            for anc in ancestors.get(t, [t]):
                used[anc] += 0  # listed even when no work carries it directly (schools are artist-level)
    return {
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "terms": [{"id": t, "kind": k, "name": n, "parent": pa, "start": s_, "end": e, "place": pl,
                   "ancestors": ancestors.get(t, [t]), "works": used.get(t, 0)}
                  for t, k, n, pa, s_, e, pl in terms if t in used],
        "artists": artists,
        # every museum holding a work, in the order the works refer to them (a work lists each museum holding it
        # by its position here); `id` is what room links use
        "museums": [{"id": museum_slug(n), "name": n, "works": museum_works[i]} for n, i in museum_ids.items()],
        # [artist, painting, era, period, movement, genre, year, pageviews, museums]
        "works": works,
    }


def build() -> int:
    museum = json.loads((settings.WIKIPEDIA / "museum.json").read_text(encoding="utf-8"))
    con = duckdb.connect(str(settings.WAREHOUSE), read_only=True)
    try:
        from .wikimedia import FILE_INFO
        file_info = FILE_INFO.as_posix() if FILE_INFO.exists() else None
        query = QUERY if file_info else QUERY.replace("LEFT JOIN read_parquet('{file_info}') fi ON fi.file = u.commons_file",
            "LEFT JOIN (SELECT NULL::VARCHAR AS file, NULL::INT AS width, NULL::INT AS height, NULL::BIGINT AS bytes, "
            "NULL::VARCHAR AS author, NULL::VARCHAR AS license, NULL::VARCHAR AS license_url, NULL::BOOLEAN AS non_free) "
            "fi ON fi.file = u.commons_file")
        cur = con.execute(query.replace("{file_info}", file_info or ""))
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        hashes, colours, thumbs = fingerprints(con)
    finally:
        con.close()
    by_artist: dict[str, list[dict]] = {}
    for r in rows:
        by_artist.setdefault(r["artist_slug"], []).append(r)
    added: Counter = Counter()
    skipped = 0
    similar = 0
    same_image = 0
    kept_versions = 0
    for artist in museum["artists"]:
        extra = by_artist.get(artist["slug"])
        if not extra:
            continue
        slugs = {p["slug"] for p in artist["paintings"]}
        images = {p.get("imageUrl") for p in artist["paintings"]}
        titles = [k for k in (title_key(p.get("title")) for p in artist["paintings"]) if k]
        # the artist's images as fingerprints; the comparison is trusted only when nearly all are known
        with_image = [p for p in artist["paintings"] if p.get("imageUrl") and not p.get("copyrighted")]
        ours = [(title_key(p.get("title")), hashes[u]) for p, u in
                ((p, thumbs.get((artist["slug"], p["slug"]))) for p in with_image) if u in hashes]
        compare_images = bool(with_image) and len(ours) >= 0.9 * len(with_image)
        for r in extra:
            small = r["image_url"] + "!PinterestSmall.jpg"
            p = painting(r, artist.get("birthYear"), artist.get("deathYear"), colours.get(small))
            if p is None or p["slug"] in slugs or p["imageUrl"] in images:
                skipped += 1
                continue
            key = title_key(p["title"])
            h = hashes.get(small)
            if compare_images and h is not None:
                # the image decides: the same painting under any title is left out, a second version is kept
                if any(distance(h, o) <= SAME_IMAGE or (distance(h, o) <= SAME_IMAGE_TITLED and key and t
                                                         and same_title(key, [t])) for t, o in ours):
                    same_image += 1
                    continue
                if key and same_title(key, titles):
                    kept_versions += 1
                ours.append((key, h))
            elif key and same_title(key, titles):
                similar += 1
                continue
            titles.append(key)
            slugs.add(p["slug"])
            artist["paintings"].append(p)
            added[artist["slug"]] += 1
    museum["generatedAt"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    on_base = sum(1 for a in museum["artists"] for p in a["paintings"] if p.get("source") == "wikidata")
    museum["sources"] = {"wikipedia": "data/wikipedia/museum.json", "wikiart": sum(added.values()),
                         "wikiart_on_commons": on_base}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT.with_suffix(".tmp")
    tmp.write_text(json.dumps(museum, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(OUT)
    write_rooms(museum)
    total = sum(len(a["paintings"]) for a in museum["artists"])
    print(f"{OUT}: {len(museum['artists'])} artists, {total} works; {sum(added.values())} from WikiArt "
          f"for {len(added)} artists. Left out: {skipped} not paintings, details or exact duplicates; {same_image} the "
          f"same painting as a work already there (by image); {similar} a close title where no fingerprints decide. Kept "
          f"{kept_versions} close-titled works whose images differ (second versions). {on_base} of the WikiArt "
          f"works hang with their Wikidata item and Commons image.")
    for slug, n in added.most_common(15):
        print(f"  {slug}: +{n}")
    return 0


def write_rooms(museum: dict) -> None:
    rooms = rooms_index(museum)
    tmp = ROOMS_OUT.with_suffix(".tmp")
    tmp.write_text(json.dumps(rooms, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(ROOMS_OUT)
    print(f"{ROOMS_OUT}: {len(rooms['works'])} works, {len(rooms['terms'])} terms, {len(rooms['artists'])} artists, "
          f"{len(rooms['museums'])} museums")


if __name__ == "__main__":
    # `python -m archive.site rooms`: only the custom rooms' index, from the snapshot already built
    if sys.argv[1:] == ["rooms"]:
        write_rooms(json.loads(OUT.read_text(encoding="utf-8")))
        sys.exit(0)
    sys.exit(build())
