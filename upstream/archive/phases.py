"""The phases of an artist's life their gallery's rooms follow: Picasso's Blue and Rose periods,
Dalí's, Magritte's, Gauguin's ... from WikiArt's per-painting periods.

    python -m archive.phases      the warehouse + data/site/museum.json -> data/site/phases.json

WikiArt files many works of an artist under a period of their life ("Blue Period", "Brittany Period"). For each
museum artist matched on WikiArt (compare.artist_match) whose works carry such periods, every period with at least
MIN_WORKS works is a phase, spanning the middle 80% of its works' years (a stray late reworking does not stretch
it) and ordered by its median year. Each of the artist's works in the site's snapshot then takes the period of its
WikiArt match (catalogue.works, compare.work_match), else the phase its year falls in (the nearest median when
phases overlap, the nearest span when it falls in none); an undated, unmatched work takes none. Artists with fewer
than two phases holding works are left out: their galleries keep the even split by year.

The site (src/lib/phases.ts) splits a gallery's rooms at its phases and names each room after its phase. Only the
period names (facts) and years are kept, no WikiArt text. Run after `python -m archive.site`.
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from statistics import median

import duckdb

from . import settings

SITE = settings.DATA / "site" / "museum.json"
OUT = settings.DATA / "site" / "phases.json"
MIN_WORKS = 3
# a phase that would hang fewer of the artist's works joins the phase nearest it in time
MIN_HUNG = 2
# years in the name ("Rome Period (1552-1553)", "Early Career NY: 1948–1958"): the room shows its own
YEARS_IN_NAME = re.compile(r"\s*(\(\s*\d{4}\s*[-–]\s*\d{4}\s*\)|:\s*\d{4}\s*[-–]\s*\d{4})\s*$")


def quantile(xs: list[int], q: float) -> int:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, round(q * (len(xs) - 1))))]


def main() -> None:
    site = json.loads(SITE.read_text(encoding="utf-8"))
    ours = {a["slug"]: a["paintings"] for a in site["artists"]}
    con = duckdb.connect(str(settings.WAREHOUSE), read_only=True)
    periods = con.sql("""
        select a.artist_slug, trim(w.period), w.year
        from compare.artist_match a join wikiart.works w on w.artist_url = a.wikiart_url
        where w.period is not null and trim(w.period) <> '' and w.year is not null
    """).fetchall()
    # our works' own WikiArt periods: by the catalogue's link, else by the work match
    direct = con.sql("""
        select c.artist_id, c.work_slug, trim(w.period)
        from catalogue.works c
        join compare.artist_match a on a.artist_slug = c.artist_id
        join wikiart.works w on w.artist_url = a.wikiart_url and w.work_url = c.wikiart_work_url
        where w.period is not null and trim(w.period) <> ''
        union all
        select m.artist_slug, m.work_slug, trim(w.period)
        from compare.work_match m join wikiart.works w on w.content_id = m.content_id
        where w.period is not null and trim(w.period) <> ''
    """).fetchall()
    con.close()

    years: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    for artist, period, year in periods:
        if artist in ours:
            years[artist][period].append(int(year))
    known: dict[tuple[str, str], str] = {}
    for artist, work, period in direct:
        known.setdefault((artist, work), period)

    out: dict[str, dict] = {}
    for artist, by in sorted(years.items()):
        spans = sorted(
            (
                {"name": name, "from": quantile(ys, 0.1), "to": quantile(ys, 0.9), "mid": median(ys)}
                for name, ys in by.items()
                if len(ys) >= MIN_WORKS
            ),
            key=lambda s: (s["mid"], s["from"]),
        )
        def assign(spans: list[dict]) -> dict[str, int]:
            index = {s["name"]: i for i, s in enumerate(spans)}
            works: dict[str, int] = {}
            for p in ours[artist]:
                i = index.get(known.get((artist, p["slug"]), ""))
                y = p.get("year")
                if i is None and isinstance(y, int):
                    inside = [k for k, s in enumerate(spans) if s["from"] <= y <= s["to"]]
                    pool = inside or range(len(spans))
                    i = min(pool, key=lambda k: (0 if inside else min(abs(y - spans[k]["from"]), abs(y - spans[k]["to"])),
                                                 abs(y - spans[k]["mid"])))
                if i is not None:
                    works[p["slug"]] = i
            return works

        # drop the phase that hangs the fewest (under MIN_HUNG) until each left hangs enough; its works go by year
        while len(spans) >= 2:
            works = assign(spans)
            hung = [sum(1 for i in works.values() if i == k) for k in range(len(spans))]
            k = min(range(len(spans)), key=lambda k: hung[k])
            if hung[k] >= MIN_HUNG:
                break
            spans = spans[:k] + spans[k + 1:]
        if len(spans) < 2:
            continue
        works = assign(spans)
        # only the phases that hold works, renumbered
        used = sorted(set(works.values()))
        if len(used) < 2:
            continue
        renum = {old: new for new, old in enumerate(used)}
        out[artist] = {
            "phases": [{"name": YEARS_IN_NAME.sub("", spans[i]["name"]), "from": spans[i]["from"], "to": spans[i]["to"]}
                       for i in used],
            "works": {slug: renum[i] for slug, i in works.items()},
        }

    OUT.write_text(
        json.dumps(
            {"generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"), "source": "WikiArt artist periods",
             "artists": out},
            ensure_ascii=False, separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    print(f"{len(out)} artists with phases -> {OUT}")
    for artist, d in out.items():
        counts = defaultdict(int)
        for i in d["works"].values():
            counts[i] += 1
        print(f"  {artist}: " + ", ".join(f"{p['name']} {p['from']}-{p['to']} ({counts[i]})" for i, p in enumerate(d["phases"]))
              + f"; {len(ours[artist]) - len(d['works'])} without")


if __name__ == "__main__":
    main()
