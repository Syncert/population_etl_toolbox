"""Find a place by name or by location (api-place-search).

Search reads ``gold_glossary.geo_search``, a projection of every current
state, county, metro, place and ZCTA with the words its name is matched by and
the newest ACS total population it is ranked by. Locate reads
``gold_glossary.geo_boundary`` and answers the geographies whose current
boundary covers a point. Both are catalog reads: the routes sit under
``/catalog/`` so they spend the catalog rate-limit budget and are answered
from the shared response cache.
"""

from __future__ import annotations

import math
import re
import unicodedata
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from sqlalchemy.orm import Session

from apps.api.schemas import (
    GeographyLocateResponse,
    GeographySearchItem,
    GeographySearchResponse,
    GeographySearchVia,
    LocatedGeography,
)
from apps.api.services.contracts import require_relation
from apps.api.services.place_paths import PagePaths
from data_ingestion_toolbox.sql.catalog_queries import (
    GEO_BOUNDARY_RELATION,
    GEO_LOCATE_METRO_QUERY,
    GEO_LOCATE_QUERY,
    GEO_SEARCH_COUNTY_METROS_QUERY,
    GEO_SEARCH_GRAINS,
    GEO_SEARCH_QUERY,
    GEO_SEARCH_RELATION,
    GEO_SEARCH_ROWS_QUERY,
    GEO_SEARCH_SEGMENTS_QUERY,
    GEOGRAPHY_RELATION,
    RELATIONSHIP_RELATION,
)

#: The shortest query, in normalised characters, a search answers. One letter
#: matches thousands of names, and ranking them by population answers a
#: question nobody asked.
MIN_QUERY_CHARACTERS = 2
#: The longest a query may be, before normalising: longer than any place name.
MAX_QUERY_CHARACTERS = 100
MAX_LIMIT = 25
DEFAULT_LIMIT = 10
#: How many of the best city matches have their county and metro offered
#: beside them. Three: enough that "Springfield" offers the counties of the
#: largest Springfields without burying every other match under counties.
CONTAINING_FOR_TOP_CITIES = 3
#: Direct matches fetched beyond the limit, so a page stays full after the
#: county and metro of a city that also matched directly are de-duplicated.
_FETCH_MARGIN = 10

_MATCH_WORDS = {0: "exact", 1: "prefix", 2: "word_prefix", 3: "fuzzy"}
_COMBINING_MARKS = re.compile("[̀-ͯ]")
_NOT_A_WORD = re.compile(r"[^a-z0-9]+")


class PlaceSearchRefusal(ValueError):
    """A search or locate request that is not one this resource answers."""


def normalise_query(value: str) -> str:
    """The query in the projection's word shape, without the leading space.

    Lower case, accents removed, ``&`` spelled "and", every run of anything
    else one space -- what ``gold_glossary.search_words`` does to a name. Only
    ``[a-z0-9 ]`` survives, so the ``LIKE`` operands built from it cannot
    carry a wildcard.
    """
    text = unicodedata.normalize("NFKD", str(value))
    text = _COMBINING_MARKS.sub("", text).lower().replace("&", " and ")
    return _NOT_A_WORD.sub(" ", text).strip()


def grain_refusal(grains: Sequence[str]) -> Optional[str]:
    unknown = [
        grain for grain in grains if grain.strip().upper() not in GEO_SEARCH_GRAINS
    ]
    if not unknown:
        return None
    return f"grain must be one of: {', '.join(GEO_SEARCH_GRAINS)}"


def _segments(db: Session, state_fips: set[str]) -> PagePaths:
    rows = (
        db.execute(GEO_SEARCH_SEGMENTS_QUERY, {"state_fips": sorted(state_fips)})
        .mappings()
        .all()
    )
    return PagePaths(rows)


def search_places(
    db: Session,
    q: str,
    grains: Sequence[str] | None = None,
    state_fips: Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
) -> GeographySearchResponse:
    words = normalise_query(q)
    if len(words.replace(" ", "")) < MIN_QUERY_CHARACTERS:
        raise PlaceSearchRefusal(
            f"q must hold at least {MIN_QUERY_CHARACTERS} letters or digits"
        )
    wanted = [grain.strip().upper() for grain in grains or [] if grain.strip()]
    refusal = grain_refusal(wanted)
    if refusal is not None:
        raise PlaceSearchRefusal(refusal)
    grain_filter = sorted(set(wanted)) or None
    state_filter = (state_fips or "").strip() or None

    require_relation(db, GEO_SEARCH_RELATION)
    require_relation(db, RELATIONSHIP_RELATION)
    spaced = f" {words}"
    direct = (
        db.execute(
            GEO_SEARCH_QUERY,
            {
                "words": spaced,
                "prefix": f"{spaced}%",
                "word_prefix": f"%{spaced}%",
                "grains": grain_filter,
                "state_fips": state_filter,
                "fetch": limit + _FETCH_MARGIN,
            },
        )
        .mappings()
        .all()
    )

    containing = _containing_rows(db, direct, grain_filter, state_filter)
    ordered = _interleave(direct, containing, limit)

    states = {
        str(row["state_fips"])
        for row, _ in ordered
        if row.get("state_fips") and row["geo_level"] in {"COUNTY", "PLACE"}
    }
    paths = _segments(db, states)
    items = [_search_item(row, via, paths) for row, via in ordered]
    return GeographySearchResponse(query=words, limit=limit, items=items)


def _containing_rows(
    db: Session,
    direct: Sequence[Mapping[str, Any]],
    grains: Optional[list[str]],
    state_fips: Optional[str],
) -> dict[str, list[Mapping[str, Any]]]:
    """The county and metro of each of the best city matches, by city."""
    cities = [
        row
        for row in direct
        if row["geo_level"] == "PLACE" and int(row["match_tier"]) < 3
    ][:CONTAINING_FOR_TOP_CITIES]
    if not cities:
        return {}
    allowed = set(grains) if grains else set(GEO_SEARCH_GRAINS)
    county_of = {
        str(city["geo_id"]): str(city["county_geo_id"])
        for city in cities
        if city.get("county_geo_id")
    }
    metro_of: dict[str, str] = {}
    if county_of and "METRO" in allowed:
        links = (
            db.execute(
                GEO_SEARCH_COUNTY_METROS_QUERY,
                {"county_geo_ids": sorted(set(county_of.values()))},
            )
            .mappings()
            .all()
        )
        for link in links:
            metro_of.setdefault(str(link["county_geo_id"]), str(link["metro_geo_id"]))
    wanted_ids: list[str] = []
    for city in cities:
        county = county_of.get(str(city["geo_id"]))
        if county and "COUNTY" in allowed:
            wanted_ids.append(county)
        if county and county in metro_of:
            wanted_ids.append(metro_of[county])
    if not wanted_ids:
        return {}
    rows = (
        db.execute(GEO_SEARCH_ROWS_QUERY, {"geo_ids": sorted(set(wanted_ids))})
        .mappings()
        .all()
    )
    by_id = {
        str(row["geo_id"]): row
        for row in rows
        if row["geo_level"] in allowed
        and (state_fips is None or row.get("state_fips") == state_fips)
    }
    result: dict[str, list[Mapping[str, Any]]] = {}
    for city in cities:
        county = county_of.get(str(city["geo_id"]))
        found = [
            by_id[geo_id]
            for geo_id in (county, metro_of.get(county or ""))
            if geo_id and geo_id in by_id
        ]
        if found:
            result[str(city["geo_id"])] = found
    return result


def _interleave(
    direct: Sequence[Mapping[str, Any]],
    containing: Mapping[str, list[Mapping[str, Any]]],
    limit: int,
) -> list[tuple[Mapping[str, Any], Optional[Mapping[str, Any]]]]:
    """Direct matches in order, each top city followed by its county and metro.

    A geography appears once, at its first position. One that also matched
    directly keeps its own match word; one offered only for the city it
    contains is a ``contains_match`` naming that city.
    """
    direct_ids = {str(row["geo_id"]): row for row in direct}
    ordered: list[tuple[Mapping[str, Any], Optional[Mapping[str, Any]]]] = []
    seen: set[str] = set()
    for row in direct:
        geo_id = str(row["geo_id"])
        if geo_id not in seen:
            seen.add(geo_id)
            ordered.append((row, None))
        for extra in containing.get(geo_id, []):
            extra_id = str(extra["geo_id"])
            if extra_id in seen:
                continue
            seen.add(extra_id)
            if extra_id in direct_ids:
                ordered.append((direct_ids[extra_id], None))
            else:
                ordered.append((extra, row))
    return ordered[:limit]


def _number(value: Any) -> Optional[float]:
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _search_item(
    row: Mapping[str, Any], via: Optional[Mapping[str, Any]], paths: PagePaths
) -> GeographySearchItem:
    tier = row.get("match_tier")
    match = "contains_match" if via is not None else _MATCH_WORDS[int(tier)]
    year = row.get("population_year")
    return GeographySearchItem(
        geo_id=str(row["geo_id"]),
        geo_level=str(row["geo_level"]),
        geo_name=str(row["geo_name"]),
        short_name=str(row["short_name"]),
        match=match,
        matched_via=(
            GeographySearchVia(geo_id=str(via["geo_id"]), geo_name=str(via["geo_name"]))
            if via is not None
            else None
        ),
        state_fips=row.get("state_fips"),
        state_name=row.get("state_name"),
        county_geo_id=row.get("county_geo_id"),
        county_name=row.get("county_name"),
        population=_number(row.get("population")),
        population_year=None if year is None else int(year),
        population_metric_code=row.get("population_metric_code"),
        page_path=paths.path(str(row["geo_id"])),
    )


# ---------------------------------------------------------------------------
# Locate
# ---------------------------------------------------------------------------


def coordinate_refusal(lat: float, lon: float) -> Optional[str]:
    if not (math.isfinite(lat) and -90 <= lat <= 90):
        return "lat must be a finite latitude from -90 to 90"
    if not (math.isfinite(lon) and -180 <= lon <= 180):
        return "lon must be a finite longitude from -180 to 180"
    return None


def locate_place(
    db: Session, lat: float, lon: float
) -> Optional[GeographyLocateResponse]:
    """The geographies containing a point, or ``None`` when no state does."""
    refusal = coordinate_refusal(lat, lon)
    if refusal is not None:
        raise PlaceSearchRefusal(refusal)
    require_relation(db, GEO_BOUNDARY_RELATION)
    require_relation(db, RELATIONSHIP_RELATION)
    require_relation(db, GEOGRAPHY_RELATION)
    require_relation(db, GEO_SEARCH_RELATION)
    rows = db.execute(GEO_LOCATE_QUERY, {"lat": lat, "lon": lon}).mappings().all()
    by_grain = {str(row["geo_level"]).upper(): row for row in rows}
    state = by_grain.get("STATE")
    if state is None:
        return None
    county = by_grain.get("COUNTY")
    metro = None
    if county is not None:
        metro = (
            db.execute(GEO_LOCATE_METRO_QUERY, {"county_geo_id": str(county["geo_id"])})
            .mappings()
            .first()
        )
    paths = _segments(
        db, {str(state["state_fips"])} if state.get("state_fips") else set()
    )

    def located(row: Optional[Mapping[str, Any]]) -> Optional[LocatedGeography]:
        if row is None:
            return None
        vintage = row.get("boundary_vintage")
        return LocatedGeography(
            geo_id=str(row["geo_id"]),
            geo_level=str(row["geo_level"]).upper(),
            geo_name=row.get("geo_name"),
            boundary_vintage=None if vintage is None else int(vintage),
            page_path=paths.path(str(row["geo_id"])),
        )

    place = located(by_grain.get("PLACE"))
    county_answer = located(county)
    state_answer = located(state)
    assert state_answer is not None
    landing = place or county_answer or state_answer
    return GeographyLocateResponse(
        landing=landing,
        state=state_answer,
        county=county_answer,
        place=place,
        tract=located(by_grain.get("TRACT")),
        zcta=located(by_grain.get("ZCTA")),
        metro=located(metro),
    )
