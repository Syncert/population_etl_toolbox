"""SQL query builders for catalog discovery (sources, metrics, geographies).

Every relation named here is a documented ``gold_glossary`` contract created by
the bootstrap manifest (``sql/gold_contract/002_gold_glossary_schema.sql``).
API-003 retired the four-way relation probing that used to live in the catalog
service: the manifest creates ``gold_glossary.dim_metric``,
``gold_glossary.dim_geography``, and ``gold_glossary.dim_source_system``
unconditionally, so the ``gold.*`` and ``*_legacy`` fallbacks could never be
selected on a manifest-built warehouse, and silently answering discovery from
whichever relation happened to exist is exactly the behaviour the API plan
forbids. An absent contract now fails explicitly before any query runs
(``apps.api.services.contracts``).

Each list builder returns a ``(list_query, count_query, params)`` triple of
``sqlalchemy.text`` objects and a params dict. Relation names are reviewed
module constants -- never request text -- which is what makes the string
interpolation below safe; ``CATALOG_RELATIONS`` is the derived allowlist the
unit suite pins.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Optional

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

#: The documented glossary contracts catalog discovery reads. Nothing else in
#: this module may name a relation.
SOURCE_RELATION = "gold_glossary.dim_source_system"
METRIC_RELATION = "gold_glossary.dim_metric"
GEOGRAPHY_RELATION = "gold_glossary.dim_geography"
#: The served geography relationship projection (nearby-and-related-places).
RELATIONSHIP_RELATION = "gold_glossary.geo_relationship"

#: The place-search projection: one row per current state, county, metro,
#: place and ZCTA with its match words and ranking population (api-place-search).
GEO_SEARCH_RELATION = "gold_glossary.geo_search"
#: Current boundaries, for locating a point (api-place-search).
GEO_BOUNDARY_RELATION = "gold_glossary.geo_boundary"

#: Every relation a catalog query may read, for the allowlist assertions.
CATALOG_RELATIONS: frozenset[str] = frozenset(
    {
        SOURCE_RELATION,
        METRIC_RELATION,
        GEOGRAPHY_RELATION,
        RELATIONSHIP_RELATION,
        GEO_SEARCH_RELATION,
        GEO_BOUNDARY_RELATION,
    }
)

#: The escape character the ``q`` searches declare, rather than relying on the
#: server's default.
_LIKE_ESCAPE = "\\"

#: Spelled into the SQL as a one-character literal. ``standard_conforming_strings``
#: has been on by default since PostgreSQL 9.1, so this is one backslash.
LIKE_ESCAPE_CLAUSE = "ESCAPE '\\'"


def like_contains(value: str) -> str:
    """``value`` as a bound ``LIKE`` operand that matches it literally.

    ``q`` is a search box, not a pattern language: the route is named ``q``,
    the guide calls it "Metric search", and nothing in the contract offers
    wildcard syntax. Without escaping, ``LIKE``'s two reserved characters made
    it one anyway -- and every metric code this warehouse publishes carries an
    underscore (``CENSUS_ACS:acs5:B01003_001``, ``BLS:LAU:UNEMP_RATE``), so
    the most ordinary search there is ran as a pattern, while a bare ``%``
    returned the whole catalog under a filter the caller believed narrowed it.

    The value was always bound, so this was never an injection. It was a
    wrong answer.
    """
    escaped = (
        str(value)
        .replace(_LIKE_ESCAPE, _LIKE_ESCAPE * 2)
        .replace("%", f"{_LIKE_ESCAPE}%")
        .replace("_", f"{_LIKE_ESCAPE}_")
    )
    return f"%{escaped}%"


#: Explicit projections. ``SELECT *`` would silently widen the API's read
#: surface whenever the warehouse adds a column; these lists are the reviewed
#: statement of exactly what discovery serves.
_METRIC_COLUMNS = (
    "metric_code, metric_display_name, source_code, source_object_type, "
    "source_object_key, units, measure_kind, valid_geo_grains, "
    "valid_time_grains, aggregation_characteristic, physical_lineage, "
    "publisher_contract_version, source_watermark, source_run_id, "
    "publication_time, harvested_at, freshness_state"
)
_GEOGRAPHY_COLUMNS = (
    "geo_id, geo_level, geo_name, state_fips, county_fips, place_fips, "
    "state_name, county_name, place_name, geo_latitude, geo_longitude, "
    "geography_state, retired_at, is_active, area_name"
)

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

SOURCES_QUERY: TextClause = text(
    f"""
    SELECT
        source_code,
        source_name,
        source_type,
        reference_url
    FROM {SOURCE_RELATION}
    ORDER BY source_code
    """
)

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def _build_metric_where(
    source_code: Optional[str],
    active_only: Optional[bool],
    q: Optional[str],
    params: dict,
) -> str:
    clauses: list[str] = []
    if source_code:
        clauses.append("UPPER(source_code) = UPPER(:source_code)")
        params["source_code"] = source_code
    if active_only:
        clauses.append("is_active = TRUE")
    if q:
        clauses.append(
            f"(UPPER(metric_code) LIKE UPPER(:q) {LIKE_ESCAPE_CLAUSE}"
            f" OR UPPER(metric_display_name) LIKE UPPER(:q) {LIKE_ESCAPE_CLAUSE})"
        )
        params["q"] = like_contains(q)
    return " AND ".join(clauses) if clauses else "TRUE"


def build_metrics_queries(
    source_code: Optional[str],
    active_only: Optional[bool],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    params: dict = {"limit": limit, "offset": offset}
    where = _build_metric_where(source_code, active_only, q, params)
    list_q = text(
        f"SELECT {_METRIC_COLUMNS} FROM {METRIC_RELATION} WHERE {where} "
        "ORDER BY metric_code LIMIT :limit OFFSET :offset"
    )
    count_q = text(f"SELECT COUNT(*) FROM {METRIC_RELATION} WHERE {where}")
    return list_q, count_q, params


def build_metric_detail_query(metric_code: str) -> tuple[TextClause, dict]:
    """One metric's full published capability row, by exact metric code."""
    detail_q = text(
        f"SELECT {_METRIC_COLUMNS} FROM {METRIC_RELATION} "
        "WHERE metric_code = :metric_code"
    )
    return detail_q, {"metric_code": metric_code}


def build_metric_details_query(metric_codes: Sequence[str]) -> tuple[TextClause, dict]:
    """The same rows for several codes, in one statement (API-147).

    Same columns and same predicate as the single form -- `= ANY(:codes)`
    rather than `= :metric_code` -- so a caller reading a composition gets
    exactly what it would have got code by code. A composition may name eight
    measures (`/comparison/matrix`) or fifty blocks (an evidence packet), and
    resolving them one at a time was one round trip each.

    The order is `metric_code`, which is the relation's own unique key, so the
    answer is stable; callers index it by code rather than by position.
    """
    details_q = text(
        f"SELECT {_METRIC_COLUMNS} FROM {METRIC_RELATION} "
        "WHERE metric_code = ANY(:metric_codes) ORDER BY metric_code"
    )
    return details_q, {"metric_codes": list(metric_codes)}


# ---------------------------------------------------------------------------
# Geographies
# ---------------------------------------------------------------------------


def _build_geo_where(
    geo_level: Optional[str],
    state_fips: Optional[str],
    active_only: Optional[bool],
    q: Optional[str],
    params: dict,
    county_fips: Optional[str] = None,
) -> str:
    clauses: list[str] = []
    if active_only:
        clauses.append("is_active = TRUE")
    if geo_level:
        clauses.append("UPPER(geo_level) = UPPER(:geo_level)")
        params["geo_level"] = geo_level
    if state_fips:
        clauses.append("state_fips = :state_fips")
        params["state_fips"] = state_fips
    # A tract carries its county's code, so `state_fips` with `county_fips`
    # lists one county's tracts (sub-county-geography).
    if county_fips:
        clauses.append("county_fips = :county_fips")
        params["county_fips"] = county_fips
    if q:
        clauses.append(
            f"(UPPER(geo_id) LIKE UPPER(:q) {LIKE_ESCAPE_CLAUSE}"
            f" OR UPPER(geo_name) LIKE UPPER(:q) {LIKE_ESCAPE_CLAUSE}"
            f" OR UPPER(state_name) LIKE UPPER(:q) {LIKE_ESCAPE_CLAUSE}"
            f" OR UPPER(county_name) LIKE UPPER(:q) {LIKE_ESCAPE_CLAUSE}"
            f" OR UPPER(place_name) LIKE UPPER(:q) {LIKE_ESCAPE_CLAUSE})"
        )
        params["q"] = like_contains(q)
    return " AND ".join(clauses) if clauses else "TRUE"


def build_geographies_queries(
    geo_level: Optional[str],
    state_fips: Optional[str],
    active_only: Optional[bool],
    q: Optional[str],
    limit: int,
    offset: int,
    county_fips: Optional[str] = None,
) -> tuple[TextClause, TextClause, dict]:
    """The geography catalog page, retired geographies included by default.

    A geography the reference stops listing is retired rather than deleted
    (DB-038), and its observations are still served, so hiding it by default
    would leave rows a client resolving through the catalog could not name.
    ``active_only`` is the caller's choice, exactly as it is for metrics.
    """
    params: dict = {"limit": limit, "offset": offset}
    where = _build_geo_where(geo_level, state_fips, active_only, q, params, county_fips)
    list_q = text(
        f"SELECT {_GEOGRAPHY_COLUMNS} FROM {GEOGRAPHY_RELATION} WHERE {where} "
        "ORDER BY geo_id LIMIT :limit OFFSET :offset"
    )
    count_q = text(f"SELECT COUNT(*) FROM {GEOGRAPHY_RELATION} WHERE {where}")
    return list_q, count_q, params


# ---------------------------------------------------------------------------
# Geography relationships
# ---------------------------------------------------------------------------

#: Every relationship of one geography, from its own point of view.
#:
#: ``contains`` reads the hierarchy downward (a state's counties; a state's
#: places are left to its counties' ``intersects`` rows, because a place can
#: cross a county line), ``part_of`` reads it upward, including the
#: grandparent (a county's nation through its state, with that row's own
#: vintage and evidence), ``intersects`` pairs a county with the places its
#: boundary overlaps in either direction, and ``adjacent`` names the counties
#: sharing a boundary. Every row keeps the vintage and evidence the bridge
#: recorded; nothing is matched by name.
#:
#: A place is in no metro by code: OMB delineates metros from whole counties,
#: and a place can cross a county line, so the reference records no
#: place-to-metro edge. The last branch reaches a place's metro through the
#: counties it intersects, as ``intersects`` rather than ``part_of`` because
#: nothing makes the place nest in it: ``overlap_weight`` is the share of the
#: place inside that metro's counties (the sum of its county overlaps) and the
#: vintage and evidence are the delineation's that put those counties in it.
GEOGRAPHY_RELATED_QUERY: TextClause = text(
    f"""
    WITH related AS (
        SELECT relationship.related_geo_id AS other_geo_id,
               relationship.relationship_type AS relationship,
               relationship.geography_vintage,
               relationship.overlap_area_m2,
               relationship.overlap_weight,
               relationship.evidence_source
        FROM {RELATIONSHIP_RELATION} AS relationship
        WHERE relationship.geo_id = :geo_id
        UNION ALL
        SELECT relationship.geo_id,
               CASE relationship.relationship_type
                   WHEN 'contains' THEN 'part_of'
                   ELSE relationship.relationship_type
               END,
               relationship.geography_vintage,
               relationship.overlap_area_m2,
               relationship.overlap_weight,
               relationship.evidence_source
        FROM {RELATIONSHIP_RELATION} AS relationship
        WHERE relationship.related_geo_id = :geo_id
          AND relationship.relationship_type IN ('contains', 'intersects')
        UNION ALL
        SELECT ancestor.geo_id,
               'part_of',
               ancestor.geography_vintage,
               ancestor.overlap_area_m2,
               ancestor.overlap_weight,
               ancestor.evidence_source
        FROM {RELATIONSHIP_RELATION} AS parent_link
        JOIN {RELATIONSHIP_RELATION} AS ancestor
             ON ancestor.related_geo_id = parent_link.geo_id
            AND ancestor.relationship_type = 'contains'
        WHERE parent_link.related_geo_id = :geo_id
          AND parent_link.relationship_type = 'contains'
        UNION ALL
        SELECT metro_link.geo_id,
               'intersects',
               MAX(metro_link.geography_vintage),
               SUM(place_county.overlap_area_m2),
               SUM(place_county.overlap_weight),
               MIN(metro_link.evidence_source)
        FROM {RELATIONSHIP_RELATION} AS place_county
        JOIN {RELATIONSHIP_RELATION} AS metro_link
             ON metro_link.related_geo_id = place_county.geo_id
            AND metro_link.relationship_type = 'contains'
        JOIN {GEOGRAPHY_RELATION} AS county
             ON county.geo_id = place_county.geo_id
            AND county.geo_level = 'COUNTY'
        JOIN {GEOGRAPHY_RELATION} AS metro
             ON metro.geo_id = metro_link.geo_id
            AND metro.geo_level = 'METRO'
        WHERE place_county.related_geo_id = :geo_id
          AND place_county.relationship_type = 'intersects'
        GROUP BY metro_link.geo_id
    )
    SELECT DISTINCT ON (related.relationship, geography.geo_id)
           related.relationship,
           geography.geo_id,
           geography.geo_level,
           geography.geo_name,
           geography.state_fips,
           related.geography_vintage,
           related.overlap_area_m2,
           related.overlap_weight,
           related.evidence_source
    FROM related
    JOIN {GEOGRAPHY_RELATION} AS geography
         ON geography.geo_id = related.other_geo_id
    WHERE NOT (related.relationship = 'contains' AND geography.geo_level = 'PLACE')
    ORDER BY related.relationship, geography.geo_id, related.geography_vintage DESC
    """
)

#: The catalog's names for a page of geographies, by id. An observation row
#: carries its geography's identity only; a city row has no state- or
#: county-level attribution to fall back on, so this is the one name it has.
GEOGRAPHY_NAMES_QUERY: TextClause = text(
    f"SELECT geo_id, geo_name FROM {GEOGRAPHY_RELATION} WHERE geo_id = ANY(:geo_ids)"
)

#: Two or more geographies' identity, by id, for ``/comparison/places``: the
#: level decides which comparison rules apply and the name labels each side.
GEOGRAPHY_IDENTITY_QUERY: TextClause = text(
    f"SELECT geo_id, geo_level, geo_name, state_fips FROM {GEOGRAPHY_RELATION} "
    "WHERE geo_id = ANY(:geo_ids)"
)

#: Whether the catalog serves a geography at all, for the route's 404.
GEOGRAPHY_EXISTS_QUERY: TextClause = text(
    f"SELECT geo_id, geo_level FROM {GEOGRAPHY_RELATION} WHERE geo_id = :geo_id"
)


# ---------------------------------------------------------------------------
# Publication freshness
# ---------------------------------------------------------------------------

#: Per-source publication and freshness state, rolled up from the glossary's
#: harvested metric catalog. ``freshness_state`` is the warehouse's published
#: data-quality signal (``current`` / ``stale`` / ``retired``); the API reports
#: it rather than recomputing quality from internals it must not read.
#: ``geo_grains`` is the union of the grains the source's non-retired metrics
#: publish, so a reader-facing page can say what a source covers without
#: paging through every metric it publishes.
SOURCE_FRESHNESS_QUERY: TextClause = text(
    f"""
    SELECT
        source_code,
        COUNT(*)::int AS metric_count,
        COUNT(*) FILTER (WHERE freshness_state = 'current')::int AS current_count,
        COUNT(*) FILTER (WHERE freshness_state = 'stale')::int AS stale_count,
        COUNT(*) FILTER (WHERE freshness_state = 'retired')::int AS retired_count,
        MAX(publication_time) AS latest_publication_time,
        MAX(harvested_at) AS latest_harvested_at,
        COALESCE(
            (
                SELECT ARRAY_AGG(DISTINCT grain ORDER BY grain)
                FROM {METRIC_RELATION} AS covered
                CROSS JOIN LATERAL UNNEST(covered.valid_geo_grains) AS grain
                WHERE covered.source_code = metric.source_code
                  AND covered.freshness_state IS DISTINCT FROM 'retired'
            ),
            ARRAY[]::text[]
        ) AS geo_grains
    FROM {METRIC_RELATION} AS metric
    GROUP BY source_code
    ORDER BY source_code
    """
)


# ---------------------------------------------------------------------------
# Finding a place by name or by location (api-place-search)
# ---------------------------------------------------------------------------

#: What a search result carries, from the projection.
_GEO_SEARCH_COLUMNS = (
    "search.geo_id, search.geo_level, search.geo_name, search.short_name, "
    "search.state_fips, search.state_name, search.county_geo_id, "
    "search.county_name, search.population, search.population_year, "
    "search.population_metric_code"
)

#: The grains a search may be narrowed to, and the order a tie in population
#: falls back to: the page a resident most likely means first.
GEO_SEARCH_GRAINS: tuple[str, ...] = ("STATE", "COUNTY", "METRO", "PLACE", "ZCTA")

#: Names matched to a query, best first.
#:
#: ``:words`` is the normalised query with the projection's leading space
#: (``" austin"``), ``:prefix`` that followed by ``%`` and ``:word_prefix``
#: that preceded by ``%`` too; the normaliser leaves only ``[a-z0-9 ]`` in
#: them, so neither ``LIKE`` operand can carry a wildcard the caller typed.
#: Tiers: 0 the short or full name is the query, 1 it starts with it, 2 a
#: later word does, 3 a trigram near-miss (``%``, pg_trgm's default 0.3
#: threshold). Within a tier: closeness for a near-miss, then population,
#: larger first and unranked last, then grain, then ``geo_id`` so the order
#: is total.
GEO_SEARCH_QUERY: TextClause = text(
    f"""
    SELECT matched.geo_id, matched.geo_level, matched.geo_name,
           matched.short_name, matched.state_fips, matched.state_name,
           matched.county_geo_id, matched.county_name, matched.population,
           matched.population_year, matched.population_metric_code,
           matched.match_tier
    FROM (
        SELECT {_GEO_SEARCH_COLUMNS},
               CASE
                   WHEN search.short_words = :words OR search.name_words = :words THEN 0
                   WHEN search.name_words LIKE :prefix OR search.short_words LIKE :prefix THEN 1
                   WHEN search.name_words LIKE :word_prefix THEN 2
                   ELSE 3
               END AS match_tier,
               similarity(search.short_words, :words) AS closeness
        FROM {GEO_SEARCH_RELATION} AS search
        WHERE (
                search.name_words LIKE :word_prefix
             OR search.short_words LIKE :prefix
             OR search.short_words % :words
              )
          AND (CAST(:grains AS TEXT[]) IS NULL
               OR search.geo_level = ANY(CAST(:grains AS TEXT[])))
          AND (CAST(:state_fips AS TEXT) IS NULL
               OR search.state_fips = CAST(:state_fips AS TEXT))
    ) AS matched
    ORDER BY matched.match_tier,
             CASE WHEN matched.match_tier = 3 THEN matched.closeness ELSE 0 END DESC,
             matched.population DESC NULLS LAST,
             CASE matched.geo_level
                 WHEN 'PLACE' THEN 0
                 WHEN 'COUNTY' THEN 1
                 WHEN 'METRO' THEN 2
                 WHEN 'STATE' THEN 3
                 ELSE 4
             END,
             matched.geo_id
    LIMIT :fetch
    """
)

#: Search rows by id: the county and metro offered beside a matched city.
GEO_SEARCH_ROWS_QUERY: TextClause = text(
    f"""
    SELECT {_GEO_SEARCH_COLUMNS}
    FROM {GEO_SEARCH_RELATION} AS search
    WHERE search.geo_id = ANY(:geo_ids)
    ORDER BY search.geo_id
    """
)

#: The metro each county is delineated into, by the relationship the
#: reference recorded (a county is in at most one CBSA per delineation).
GEO_SEARCH_COUNTY_METROS_QUERY: TextClause = text(
    f"""
    SELECT link.related_geo_id AS county_geo_id, link.geo_id AS metro_geo_id
    FROM {RELATIONSHIP_RELATION} AS link
    JOIN {GEO_SEARCH_RELATION} AS metro
         ON metro.geo_id = link.geo_id
        AND metro.geo_level = 'METRO'
    WHERE link.relationship_type = 'contains'
      AND link.related_geo_id = ANY(:county_geo_ids)
    ORDER BY link.related_geo_id, link.geography_vintage DESC, link.geo_id
    """
)

#: The names the web's address rule slugs -- the published ``geo_name``, which
#: is the state, county or place name the web's helpers read -- for every
#: state and for the counties and places of the states asked for. A county or city segment falls back to its
#: FIPS code when a sibling slugs the same way, so a path needs its siblings.
GEO_SEARCH_SEGMENTS_QUERY: TextClause = text(
    f"""
    SELECT search.geo_id, search.geo_level, search.state_fips,
           search.county_fips, search.place_fips,
           search.geo_name AS segment_name
    FROM {GEO_SEARCH_RELATION} AS search
    WHERE search.geo_level = 'STATE'
       OR (search.geo_level IN ('COUNTY', 'PLACE')
           AND search.state_fips = ANY(:state_fips))
    ORDER BY search.geo_id
    """
)

#: The grains a point is located in by boundary. A metro is not: OMB
#: delineates it from whole counties, so it follows from the county.
GEO_LOCATE_GRAINS: tuple[str, ...] = ("STATE", "COUNTY", "PLACE", "TRACT", "ZCTA")

#: The current geographies whose boundary covers a point, one per grain. Where
#: two boundaries of one grain cover it (a tract the newer decade split, both
#: still current), the newer boundary vintage wins, then ``geo_id``.
GEO_LOCATE_QUERY: TextClause = text(
    f"""
    SELECT DISTINCT ON (boundary.geo_level)
           boundary.geo_id, boundary.geo_level, boundary.geo_name,
           boundary.state_fips, boundary.county_fips, boundary.place_fips,
           boundary.boundary_vintage
    FROM {GEO_BOUNDARY_RELATION} AS boundary
    WHERE boundary.geo_level IN ('STATE', 'COUNTY', 'PLACE', 'TRACT', 'ZCTA')
      AND ST_Covers(
              boundary.geo_geom,
              ST_SetSRID(
                  ST_MakePoint(
                      CAST(:lon AS DOUBLE PRECISION),
                      CAST(:lat AS DOUBLE PRECISION)
                  ),
                  4326
              )
          )
    ORDER BY boundary.geo_level, boundary.boundary_vintage DESC NULLS LAST,
             boundary.geo_id
    """
)

#: A county's metro, newest delineation first.
GEO_LOCATE_METRO_QUERY: TextClause = text(
    f"""
    SELECT metro.geo_id, metro.geo_level, metro.geo_name,
           link.geography_vintage AS boundary_vintage
    FROM {RELATIONSHIP_RELATION} AS link
    JOIN {GEOGRAPHY_RELATION} AS metro
         ON metro.geo_id = link.geo_id
        AND metro.geo_level = 'METRO'
        AND metro.geography_state = 'current'
    WHERE link.relationship_type = 'contains'
      AND link.related_geo_id = :county_geo_id
    ORDER BY link.geography_vintage DESC, metro.geo_id
    LIMIT 1
    """
)

#: The boundary vintages each versioned tile layer holds
#: (tiles-places-and-metros), newest first per grain.
TILE_LAYER_VINTAGE_RELATION = "gold.tile_layer_vintage"
TILE_LAYER_VINTAGES_QUERY: TextClause = text(
    f"""
    SELECT held.geo_level, held.boundary_vintage, held.feature_count,
           held.is_default
    FROM {TILE_LAYER_VINTAGE_RELATION} AS held
    ORDER BY held.geo_level, held.boundary_vintage DESC
    """
)
