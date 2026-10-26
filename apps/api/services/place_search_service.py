"""Server-side place search and point lookup over the geography catalog (A-1).

Search reads names from ``gold_glossary.dim_geo_latest`` (current
geographies only) and ranks matches by kind -- exact, prefix, a later word
starting with the query, then sounds-like on the first word (``dmetaphone``
from ``fuzzystrmatch``) -- and within a kind by served population. For each
matched city the county containing its internal point is offered too, so
"Austin" finds Travis County, which no name match would.

The point lookup answers which served geographies contain a latitude and
longitude, by boundary (``ST_Contains`` on the reference geometry); a metro is
found through the county it contains, because metros are served without
boundaries of their own.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.schemas.search import (
    LocatedPlace,
    LocateResponse,
    PlaceMatch,
    PlaceSearchResponse,
)
from apps.api.services.contracts import require_relation

GEOGRAPHY = "gold_glossary.dim_geo_latest"
SEARCH_LEVELS = ("STATE", "COUNTY", "METRO", "PLACE", "ZCTA")
POPULATION_METRIC = "CENSUS_ACS:acs5:B01003_001"
#: Levels a point lookup answers, smallest first.
LOCATE_LEVELS = ("TRACT", "ZCTA", "PLACE", "COUNTY", "STATE")


def escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


SEARCH_SQL = text(
    f"""
    WITH named AS (
        SELECT g.geo_id, g.geo_level, g.state_fips, g.state_name,
               g.latitude, g.longitude,
               COALESCE(
                   CASE g.geo_level
                       WHEN 'PLACE' THEN g.place_name
                       WHEN 'COUNTY' THEN g.county_name
                       WHEN 'STATE' THEN g.state_name
                   END,
                   g.area_name, g.place_name, g.county_name, g.geo_id
               ) AS name
        FROM {GEOGRAPHY} g
        WHERE g.geography_state = 'current'
          AND g.geo_level = ANY(:levels)
    ), matched AS (
        SELECT named.*,
               CASE
                   WHEN lower(name) = :q THEN 0
                   WHEN lower(name) LIKE :prefix ESCAPE '\\' THEN 1
                   WHEN lower(name) LIKE :word ESCAPE '\\'
                     OR lower(name) LIKE :hyphen ESCAPE '\\' THEN 2
                   WHEN length(:q) >= 4
                    AND dmetaphone(split_part(name, ' ', 1)) = dmetaphone(:q) THEN 3
               END AS match_rank
        FROM named
    ), ranked AS (
        SELECT matched.*,
               COALESCE(
                   (SELECT round(m.estimate_value)::BIGINT
                      FROM gold_census.mv_acs_latest m
                     WHERE m.metric_code = :population_metric
                       AND m.geo_id = matched.geo_id
                     ORDER BY m.vintage_year DESC
                     LIMIT 1),
                   -- A metro's own figures load later (W-7); until then it
                   -- ranks by the sum of its counties' served populations.
                   (SELECT sum(county_population)::BIGINT
                      FROM gold_glossary.geo_relationship r
                      CROSS JOIN LATERAL (
                          SELECT round(m.estimate_value) AS county_population
                          FROM gold_census.mv_acs_latest m
                          WHERE m.metric_code = :population_metric
                            AND m.geo_id = r.related_geo_id
                          ORDER BY m.vintage_year DESC
                          LIMIT 1
                      ) county
                     WHERE matched.geo_level = 'METRO'
                       AND r.geo_id = matched.geo_id
                       AND r.relationship_type = 'contains')
               ) AS population
        FROM matched
        WHERE match_rank IS NOT NULL
        ORDER BY match_rank
        LIMIT 500
    )
    SELECT ranked.geo_id, ranked.geo_level, ranked.name, ranked.state_fips,
           ranked.state_name, ranked.population, ranked.match_rank,
           county.geo_id AS county_geo_id, county.county_name
    FROM ranked
    LEFT JOIN LATERAL (
        SELECT c.geo_id, c.county_name
        FROM {GEOGRAPHY} c
        WHERE ranked.geo_level IN ('PLACE', 'TRACT', 'ZCTA')
          AND ranked.latitude IS NOT NULL
          AND c.geo_level = 'COUNTY'
          AND c.geography_state = 'current'
          AND ST_Contains(
                c.geo_geom,
                ST_SetSRID(ST_MakePoint(ranked.longitude, ranked.latitude), 4326)
              )
        LIMIT 1
    ) county ON TRUE
    ORDER BY ranked.match_rank, ranked.population DESC NULLS LAST, ranked.name
    LIMIT :limit
    """
)

METRO_SQL = text(
    f"""
    SELECT DISTINCT ON (r.related_geo_id) r.related_geo_id AS county_geo_id,
           r.geo_id AS metro_geo_id, m.area_name AS metro_name
    FROM gold_glossary.geo_relationship r
    JOIN {GEOGRAPHY} m ON m.geo_id = r.geo_id AND m.geo_level = 'METRO'
    WHERE r.relationship_type = 'contains'
      AND r.related_geo_id = ANY(:counties)
    ORDER BY r.related_geo_id, r.geo_id
    """
)

COUNTY_SQL = text(
    f"""
    SELECT g.geo_id, g.county_name, g.state_fips, g.state_name,
           (SELECT round(m.estimate_value)::BIGINT
              FROM gold_census.mv_acs_latest m
             WHERE m.metric_code = :population_metric AND m.geo_id = g.geo_id
             ORDER BY m.vintage_year DESC LIMIT 1) AS population
    FROM {GEOGRAPHY} g
    WHERE g.geo_id = ANY(:ids) AND g.geography_state = 'current'
    """
)

LOCATE_SQL = text(
    f"""
    SELECT g.geo_id, g.geo_level, g.state_fips,
           COALESCE(
               CASE g.geo_level
                   WHEN 'PLACE' THEN g.place_name
                   WHEN 'COUNTY' THEN g.county_name
                   WHEN 'STATE' THEN g.state_name
               END,
               g.area_name, g.geo_id
           ) AS name
    FROM {GEOGRAPHY} g
    WHERE g.geography_state = 'current'
      AND g.geo_level = ANY(:levels)
      AND ST_Contains(g.geo_geom, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326))
    """
)

MATCH_KINDS = {0: "exact", 1: "prefix", 2: "word", 3: "sounds_like"}


def normalize_query(query: str) -> str:
    return " ".join(query.lower().split())


def _metros(db: Session, county_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not county_ids:
        return {}
    rows = db.execute(METRO_SQL, {"counties": county_ids}).mappings().all()
    return {row["county_geo_id"]: dict(row) for row in rows}


def search_places(
    db: Session, query: str, levels: list[str] | None, limit: int
) -> PlaceSearchResponse:
    require_relation(db, GEOGRAPHY)
    q = normalize_query(query)
    wanted = [level for level in (levels or SEARCH_LEVELS) if level in SEARCH_LEVELS]
    pattern = escape_like(q)
    rows = (
        db.execute(
            SEARCH_SQL,
            {
                "q": q,
                "prefix": f"{pattern}%",
                "word": f"% {pattern}%",
                "hyphen": f"%-{pattern}%",
                "levels": wanted,
                "population_metric": POPULATION_METRIC,
                "limit": limit,
            },
        )
        .mappings()
        .all()
    )
    items: list[PlaceMatch] = [
        PlaceMatch(
            geo_id=row["geo_id"],
            geo_level=row["geo_level"],
            name=row["name"],
            state_fips=row["state_fips"],
            state_name=row["state_name"],
            county_geo_id=row["county_geo_id"],
            county_name=row["county_name"],
            population=row["population"],
            match=MATCH_KINDS[int(row["match_rank"])],
        )
        for row in rows
    ]
    # A city matched by its name brings its county, placed right after it
    # (once); a sounds-like match brings nothing, so a guess is not compounded.
    seen = {item.geo_id for item in items}
    containing = [
        item
        for item in items
        if item.geo_level == "PLACE"
        and item.match != "sounds_like"
        and item.county_geo_id
        and item.county_geo_id not in seen
    ]
    if "COUNTY" in wanted and containing:
        counties = {
            row["geo_id"]: row
            for row in db.execute(
                COUNTY_SQL,
                {
                    "ids": sorted({item.county_geo_id for item in containing}),
                    "population_metric": POPULATION_METRIC,
                },
            ).mappings()
        }
        for item in containing:
            county = counties.get(item.county_geo_id)
            if county is None or county["geo_id"] in seen:
                continue
            seen.add(county["geo_id"])
            items.insert(
                items.index(item) + 1,
                PlaceMatch(
                    geo_id=county["geo_id"],
                    geo_level="COUNTY",
                    name=county["county_name"],
                    state_fips=county["state_fips"],
                    state_name=county["state_name"],
                    county_geo_id=county["geo_id"],
                    county_name=county["county_name"],
                    population=county["population"],
                    match="contains_a_match",
                    contains_geo_id=item.geo_id,
                ),
            )

    def county_of(item: PlaceMatch) -> str | None:
        return item.geo_id if item.geo_level == "COUNTY" else item.county_geo_id

    metros = _metros(
        db,
        sorted(
            {
                county
                for item in items
                if item.geo_level in ("PLACE", "COUNTY") and (county := county_of(item))
            }
        ),
    )
    for item in items:
        metro = metros.get(county_of(item) or "")
        if metro:
            item.metro_geo_id = metro["metro_geo_id"]
            item.metro_name = metro["metro_name"]
    return PlaceSearchResponse(query=q, levels=wanted, items=items[:limit])


def locate_point(db: Session, latitude: float, longitude: float) -> LocateResponse:
    require_relation(db, GEOGRAPHY)
    rows = (
        db.execute(
            LOCATE_SQL,
            {"lat": latitude, "lon": longitude, "levels": list(LOCATE_LEVELS)},
        )
        .mappings()
        .all()
    )
    order = {level: position for position, level in enumerate(LOCATE_LEVELS)}
    found = sorted(rows, key=lambda row: order.get(row["geo_level"], len(order)))
    items = [
        LocatedPlace(
            geo_id=row["geo_id"],
            geo_level=row["geo_level"],
            name=row["name"],
            state_fips=row["state_fips"],
        )
        for row in found
    ]
    counties = [item.geo_id for item in items if item.geo_level == "COUNTY"]
    for county_geo_id, metro in _metros(db, counties).items():
        items.insert(
            len([item for item in items if item.geo_level != "STATE"]),
            LocatedPlace(
                geo_id=metro["metro_geo_id"],
                geo_level="METRO",
                name=metro["metro_name"],
            ),
        )
    return LocateResponse(latitude=latitude, longitude=longitude, items=items)
