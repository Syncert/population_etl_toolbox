"""Web page addresses for geographies, by the web's own address rule.

The rule is the web's: ``placeSlug``, ``stateSegment``, ``countySegment`` and
``citySegment`` in ``apps/web/lib/placeChapters.ts``. The place pages resolve
an address by matching it against the slugs of the catalog's own rows, so a
path built here by any other rule would be a link to a 404. The two
implementations are held together by one fixture both test suites assert,
``tests/fixtures/shared/place_segments.json``; a change to either rule fails
the other suite until both agree.

A segment is a name's slug unless a sibling slugs the same way, in which case
it falls back to the FIPS code, which is never ambiguous: a state's two
digits, a county's five, a city's seven. A city also falls back when its slug
is a county's segment in the same state, because the two share
``/us/<state>/<segment>`` and the county keeps the plain slug.
"""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

_COMBINING_MARKS = re.compile("[̀-ͯ]")
_NOT_SLUG = re.compile(r"[^a-z0-9]+")


def place_slug(name: object) -> str:
    """The URL segment for a name: lower case, letters and digits, hyphens."""
    text = unicodedata.normalize("NFKD", str(name or ""))
    text = _COMBINING_MARKS.sub("", text).lower().replace("&", " and ")
    return _NOT_SLUG.sub("-", text).strip("-")


@dataclass(frozen=True)
class SegmentRow:
    """One geography as the address rule reads it."""

    geo_id: str
    geo_level: str
    state_fips: str | None
    county_fips: str | None
    place_fips: str | None
    segment_name: str | None

    @classmethod
    def from_mapping(cls, row: Mapping[str, object]) -> "SegmentRow":
        def text(key: str) -> str | None:
            value = row.get(key)
            return None if value is None else str(value)

        return cls(
            geo_id=str(row["geo_id"]),
            geo_level=str(row["geo_level"]).upper(),
            state_fips=text("state_fips"),
            county_fips=text("county_fips"),
            place_fips=text("place_fips"),
            segment_name=text("segment_name"),
        )

    @property
    def name(self) -> str:
        return self.segment_name or self.geo_id


class PagePaths:
    """Page paths for states, counties and cities, from their siblings.

    Built from every state and from the counties and places of the states a
    caller needs paths in. A geography outside those rows, or of a grain with
    no page (metro, tract, ZCTA), has no path.
    """

    def __init__(self, rows: Iterable[SegmentRow | Mapping[str, object]]) -> None:
        parsed = [
            row if isinstance(row, SegmentRow) else SegmentRow.from_mapping(row)
            for row in rows
        ]
        self._paths: dict[str, str] = {}
        states = [row for row in parsed if row.geo_level == "STATE"]
        state_slugs = Counter(place_slug(row.name) for row in states)
        state_segments: dict[str, str] = {}
        for state in states:
            slug = place_slug(state.name)
            segment = (
                slug if slug and state_slugs[slug] == 1 else (state.state_fips or "")
            )
            if state.state_fips is not None:
                state_segments[state.state_fips] = segment
            self._paths[state.geo_id] = f"/us/{segment}"

        by_state: dict[str, list[SegmentRow]] = {}
        for row in parsed:
            if row.geo_level in {"COUNTY", "PLACE"} and row.state_fips:
                by_state.setdefault(row.state_fips, []).append(row)

        for state_fips, members in by_state.items():
            state_segment = state_segments.get(state_fips)
            if not state_segment:
                continue
            counties = [row for row in members if row.geo_level == "COUNTY"]
            places = [row for row in members if row.geo_level == "PLACE"]
            county_slugs = Counter(place_slug(row.name) for row in counties)
            county_segments: set[str] = set()
            for county in counties:
                slug = place_slug(county.name)
                segment = (
                    slug
                    if slug and county_slugs[slug] == 1
                    else f"{county.state_fips or ''}{county.county_fips or ''}"
                )
                county_segments.add(segment)
                self._paths[county.geo_id] = f"/us/{state_segment}/{segment}"
            place_slugs = Counter(place_slug(row.name) for row in places)
            for place in places:
                slug = place_slug(place.name)
                fallback = f"{place.state_fips or ''}{place.place_fips or ''}"
                taken = place_slugs[slug] > 1 or slug in county_segments
                segment = fallback if not slug or taken else slug
                self._paths[place.geo_id] = f"/us/{state_segment}/{segment}"

    def path(self, geo_id: str | None) -> str | None:
        if geo_id is None:
            return None
        return self._paths.get(geo_id)
