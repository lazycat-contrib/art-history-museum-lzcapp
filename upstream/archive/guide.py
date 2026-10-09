"""The audio guide's scripts: one standard text per artist (their life) and per work, the same whoever reads it.

    python -m archive.guide build                  -> data/site/guide/<artist>.json (every museum artist)
    python -m archive.guide sheet [--artists a,b]  -> data/guide/recording/<artist>.md (for a narrator)

The text is fixed data, not composed in the browser: today the browser's voice reads it (src/components/museum/
AudioGuide.tsx), a narrator can record it, and a live AI voice can read it later, all from the same script.
Every script has a stable id (artist:<slug>, work:<artist>/<work>) and a version: a recording names its file by
the id (public/audio/guide/<id with / and : as _>.mp3) and is used while its script's version is unchanged.

Templates (each line a sentence; a line is left out when its facts are missing):
  artist   <Name>, <born>–<died>.  <tagline>.  Born in <place>[; studied with <teacher>].  Worked in <places>.
           <the first three sentences of the Wikipedia summary>
  work     <what you see: the Wikipedia text's sentences that describe the picture ("Here you see a young woman
           reading a letter ..."), else what Wikidata says it depicts>  <Title>, by <Artist>, <year>.  <Material>,
           <h> by <w> centimetres.  <the rest of the Wikipedia text, up to four sentences>  <up to two facts>
           Today in <collection>[, <place>].
Sources: the Wikipedia summaries (CC BY-SA 4.0, credited wherever a script is shown or played) and Wikidata
facts (CC0) from the warehouse (catalogue.artists, catalogue.works). WikiArt's own texts are never used.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

import duckdb

from . import settings

OUT = settings.DATA / "site" / "guide"
SHEETS = settings.DATA / "guide" / "recording"
AUDIO = settings.REPO / "public" / "audio" / "guide"
WORDS_PER_SECOND = 2.5
BIO_SENTENCES = 3
STORY_SENTENCES = 4
TEMPLATE = 3  # bump when the templates change: every script gets a new version
SEEN_SENTENCES = 2

# A sentence that describes what is in the picture (rather than its history).
DESCRIBES = re.compile(
    r"\b(depict(s|ed|ing)?|shows?|showing|portray(s|ed|ing)?|represents?|in the (fore|back|middle ?)ground|"
    r"on the (left|right)|at the (centre|center|top|bottom)|to the (left|right)|figures?|seated|standing|kneeling|"
    r"sitting|reclining|wearing|dressed in|holding|looks? (out|at|toward)|gazes?|lit by|the light|the scene|"
    r"the composition|in the cent(re|er)|surrounded by|landscape with|still life of)\b", re.I)
STRONG = re.compile(r"\b(depict(s|ed|ing)?|shows?|showing|portray(s|ed|ing)?)\b", re.I)
NEGATED = re.compile(r"\b(not|never|no longer|misnomer|nor)\b", re.I)
ABSTRACT = {"beauty", "love", "death", "vanity", "faith", "hope", "charity", "melancholy", "allegory", "truth",
            "time", "youth", "old age", "wealth", "power", "virtue", "vice", "sin", "fear", "joy", "grief", "peace",
            "war", "religion", "nature", "life", "eternity", "transience", "female beauty", "nudity", "human",
            # faiths, myths and genres: what a picture is about, not something in it
            "christianity", "catholicism", "christian art", "christian symbolism", "judaism", "islam", "buddhism",
            "hinduism", "mythology", "greek mythology", "roman mythology", "classical mythology", "religious art",
            "history painting", "genre art", "genre painting", "still life", "portrait", "self-portrait", "landscape",
            "landscape art", "marine art", "cityscape", "seascape", "nude", "allegorical painting", "vanitas"}
# "The painting depicts X" -> "Here you see X"
SEE_LEAD = re.compile(
    r"^(?:(?:the|this)(?: [\w-]+){0,2}? (?:painting|picture|work|canvas|panel|portrait|scene|composition|image|"
    r"triptych|altarpiece)(?: itself)?|it) (?:depicts|shows|portrays|represents)\s+", re.I)


def sentences(text: str | None, limit: int) -> list[str]:
    if not text:
        return []
    found = re.findall(r"[^.!?]+[.!?]+[\"”’)\]]*(?=\s|$)|[^.!?]+$", re.sub(r"\s+", " ", text).strip())
    out = []
    for x in found:
        x = x.strip()
        if x.count('"') % 2 == 1 and x.startswith('"'):
            x = x[1:].strip()  # a quotation's closing mark, split off the sentence before
        if len(x) > 1:
            out.append(x)
    return out[:limit]


def years(a: dict) -> str:
    b, d = a.get("birthYear"), a.get("deathYear")
    if b is None:
        return ""
    return f"{b}–{d}" if d is not None else f"born {b}"


def listing(xs: list[str], n: int = 3) -> str:
    xs = [x for x in xs if x and not re.fullmatch(r"Q\d+", x)][:n]
    return xs[0] if len(xs) == 1 else ", ".join(xs[:-1]) + " and " + xs[-1] if xs else ""


def medium(materials: list[str] | None) -> str:
    """Wikidata materials -> "Oil on canvas" (a paint on a support), else the first two."""
    m = [x.lower() for x in (materials or []) if x and not re.fullmatch(r"Q\d+", x)]
    if not m:
        return ""
    paints = [x for x in m if re.search(r"paint|tempera|fresco|watercolou?r|gouache|pastel|ink|encaustic", x)]
    supports = [x for x in m if re.search(r"canvas|panel|wood|oak|poplar|paper|copper|board|cardboard|silk|plaster|wall", x)]
    if paints and supports:
        paint = paints[0].replace(" paint", "")
        return f"{paint.capitalize()} on {supports[0]}"
    return " and ".join(m[:2]).capitalize()


def script_id(kind: str, key: str) -> str:
    return f"{kind}:{key}"


def version(lines: list[str]) -> str:
    return hashlib.sha1(f"{TEMPLATE}|{'|'.join(lines)}".encode("utf-8")).hexdigest()[:10]


def audio_for(sid: str) -> str | None:
    name = re.sub(r"[/:]", "_", sid) + ".mp3"
    return f"/audio/guide/{name}" if (AUDIO / name).exists() else None


def finish(sid: str, lines: list[str], sources: list[dict]) -> dict:
    # a quotation's closing mark split off the sentence before; repeated lines
    lines = [x[1:].strip() if x.startswith('"') and x.count('"') % 2 == 1 else x for x in lines]
    seen: set[str] = set()
    lines = [x for x in lines if x and not (x.lower() in seen or seen.add(x.lower()))]
    words = sum(len(s.split()) for s in lines)
    return {"id": sid, "version": version(lines), "lines": lines, "seconds": round(words / WORDS_PER_SECOND),
            "sources": sources, "audio": audio_for(sid)}


def artist_script(a: dict, life: dict) -> dict:
    lines = [f"{a['name']}{', ' + years(a) if years(a) else ''}."]
    bio = sentences(a.get("bio"), BIO_SENTENCES)
    tagline = re.sub(r"\s*\([^)]*\d{3,4}[^)]*\)", "", a.get("tagline") or "").strip().rstrip(".")
    # the summary's first sentence usually says what the tagline says: keep the tagline only when it does not
    if tagline and not (bio and a["name"].split()[0] in bio[0]):
        lines.append(f"{tagline}.")
    born = listing(life.get("birth_place") or [], 1)
    teachers = listing(life.get("teachers") or [], 2)
    if born:
        lines.append(f"Born in {born}{f'; studied with {teachers}' if teachers else ''}.")
    worked = listing([p for p in life.get("work_locations") or [] if p != born], 3)
    if worked:
        lines.append(f"Worked in {worked}.")
    lines += bio
    sources = [{"name": "Wikipedia", "url": a.get("wikipediaUrl"), "license": "CC BY-SA 4.0"}]
    if life.get("qid"):
        sources.append({"name": "Wikidata", "url": f"https://www.wikidata.org/wiki/{life['qid']}", "license": "CC0"})
    return finish(script_id("artist", a["slug"]), lines, sources)


def what_you_see(story: list[str], depicts: list[str] | None, artist: str = "") -> tuple[list[str], list[str]]:
    """The opening: what is in the picture. The story's describing sentences ("The painting depicts ..." read as
    "Here you see ..."), else Wikidata's depicts; and the story without them."""
    scored = [(2 if STRONG.search(x) else 1, i, x) for i, x in enumerate(story)
              if DESCRIBES.search(x) and not NEGATED.search(x)]
    seen = [x for _, i, x in sorted(sorted(scored, key=lambda t: (-t[0], t[1]))[:SEEN_SENTENCES], key=lambda t: t[1])]
    rest = [x for x in story if x not in seen]
    out = []
    for x in seen:
        lead = SEE_LEAD.match(x)
        if lead:
            body = re.sub(r"^(his|her) ", "the artist's ", x[lead.end():])
            out.append(f"Here you see {body}")
        else:
            out.append(x)
    if not out:
        # Wikidata's depicts: things ("the earring") and people or subjects by name ("Saint Jerome"); abstract
        # notions ("beauty", "vanity") are not something to look for
        items = [d for d in depicts or []
                 if d and len(d) <= 40 and not re.fullmatch(r"Q\d+", d) and d.lower() not in ABSTRACT]
        # a self-portrait depicts its painter
        things = ["the painter" if d == artist else f"the {d}" if d[0].islower() else d for d in items][:4]
        if things:
            out.append(f"In the picture, look for {listing(things, 4)}.")
    return out, rest


def work_script(a: dict, p: dict, facts: dict) -> dict:
    title = (p.get("title") or "Untitled").strip().rstrip(".")
    story = sentences(p.get("story"), 12)
    seen, rest = what_you_see(story, facts.get("depicts"), a["name"])
    # what you see first, then what it is
    lines = seen + [f"{title}, by {a['name']}{', ' + str(p['year']) if p.get('year') is not None else ''}."]
    m = medium(facts.get("materials"))
    w, h = p.get("widthCm"), p.get("heightCm")
    size = f"{round(h)} by {round(w)} centimetres" if w and h else ""
    if m or size:
        lines.append(", ".join(x for x in (m, size) if x) + ".")
    lines += rest[:STORY_SENTENCES]
    lines += [f.strip().rstrip(".") + "." for f in (p.get("facts") or [])[:2] if f.strip()]
    collection = listing(facts.get("collections") or [], 1)
    place = listing(facts.get("locations") or [], 1)
    if collection and collection.lower() not in " ".join(lines).lower():  # the story may already say where it is
        where = collection if collection.lower().startswith(("the ", "a ", "private")) else f"the {collection}"
        lines.append(f"Today in {where}{f', {place}' if place and place not in collection else ''}.")
    sources = []
    if p.get("wikipediaUrl"):
        sources.append({"name": "Wikipedia", "url": p["wikipediaUrl"], "license": "CC BY-SA 4.0"})
    if p.get("qid"):
        sources.append({"name": "Wikidata", "url": f"https://www.wikidata.org/wiki/{p['qid']}", "license": "CC0"})
    return finish(script_id("work", f"{a['slug']}/{p['slug']}"), lines, sources)


def work_id(artist_slug: str, painting_slug: str) -> str:
    if painting_slug.startswith("wikiart-"):
        return f"{artist_slug}/{painting_slug[len('wikiart-'):]}"
    return f"{artist_slug}/{painting_slug}"


def build() -> int:
    site = settings.DATA / "site" / "museum.json"
    museum = json.loads((site if site.exists() else settings.WIKIPEDIA / "museum.json").read_text(encoding="utf-8"))
    con = duckdb.connect(str(settings.WAREHOUSE), read_only=True)
    try:
        lives = {r[0]: dict(zip(("qid", "birth_place", "work_locations", "teachers"), r[1:])) for r in con.execute(
            "SELECT artist_id, qid, birth_place, work_locations, teachers FROM catalogue.artists WHERE in_museum"
        ).fetchall()}
        facts = {r[0]: {"materials": r[1], "collections": r[2], "locations": r[3], "depicts": r[4]} for r in con.execute(
            "SELECT work_id, materials, collections, locations, depicts FROM catalogue.works WHERE artist_id IN "
            "(SELECT artist_id FROM catalogue.artists WHERE in_museum)").fetchall()}
    finally:
        con.close()
    OUT.mkdir(parents=True, exist_ok=True)
    n_works = seconds = 0
    for a in museum["artists"]:
        doc = {"artist": artist_script(a, lives.get(a["slug"], {})), "works": {}}
        for p in a["paintings"]:
            s = work_script(a, p, facts.get(work_id(a["slug"], p["slug"]), {}))
            doc["works"][p["slug"]] = s
            seconds += s["seconds"]
        n_works += len(doc["works"])
        (OUT / f"{a['slug']}.json").write_text(json.dumps(doc, ensure_ascii=False, separators=(",", ":")),
                                              encoding="utf-8")
    print(f"{OUT}: {len(museum['artists'])} artists, {n_works} works, about {seconds / 3600:.0f} hours of narration")
    return 0


def sheet(artists: list[str] | None) -> int:
    """A recording sheet per artist: each script's id, version and text, in the order a visitor meets them."""
    SHEETS.mkdir(parents=True, exist_ok=True)
    files = sorted(OUT.glob("*.json")) if not artists else [OUT / f"{a}.json" for a in artists]
    for f in files:
        doc = json.loads(f.read_text(encoding="utf-8"))
        out = [f"# Audio guide: {doc['artist']['lines'][0].rstrip('.')}", "",
               "Record each script as one file named by its id (`/` and `:` become `_`), e.g. "
               "`work_claude-monet_impression-sunrise.mp3`, into `public/audio/guide/`, then run "
               "`python -m archive.guide build`. A recording is used while the script's version is unchanged.", ""]
        for s in [doc["artist"], *doc["works"].values()]:
            out += [f"## `{s['id']}` (version {s['version']}, about {s['seconds']} s)", "", *s["lines"], ""]
        (SHEETS / f"{f.stem}.md").write_text("\n".join(out), encoding="utf-8")
    print(f"{len(files)} recording sheets in {SHEETS}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m archive.guide")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("build")
    s = sub.add_parser("sheet")
    s.add_argument("--artists", default="")
    a = p.parse_args(argv)
    if a.cmd == "build":
        return build()
    return sheet([x for x in a.artists.split(",") if x] or None)


if __name__ == "__main__":
    sys.exit(main())
