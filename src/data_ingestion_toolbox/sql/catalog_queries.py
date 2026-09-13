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

from typing import Optional

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

#: The documented glossary contracts catalog discovery reads. Nothing else in
#: this module may name a relation.
SOURCE_RELATION = "gold_glossary.dim_source_system"
METRIC_RELATION = "gold_glossary.dim_metric"
GEOGRAPHY_RELATION = "gold_glossary.dim_geography"

#: Every relation a catalog query may read, for the allowlist assertions.
CATALOG_RELATIONS: frozenset[str] = frozenset(
    {SOURCE_RELATION, METRIC_RELATION, GEOGRAPHY_RELATION}
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
    "state_name, county_name, place_name, geo_latitude, geo_longitude"
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


# ---------------------------------------------------------------------------
# Geographies
# ---------------------------------------------------------------------------


def _build_geo_where(
    geo_level: Optional[str],
    state_fips: Optional[str],
    q: Optional[str],
    params: dict,
) -> str:
    clauses: list[str] = []
    if geo_level:
        clauses.append("UPPER(geo_level) = UPPER(:geo_level)")
        params["geo_level"] = geo_level
    if state_fips:
        clauses.append("state_fips = :state_fips")
        params["state_fips"] = state_fips
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
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    params: dict = {"limit": limit, "offset": offset}
    where = _build_geo_where(geo_level, state_fips, q, params)
    list_q = text(
        f"SELECT {_GEOGRAPHY_COLUMNS} FROM {GEOGRAPHY_RELATION} WHERE {where} "
        "ORDER BY geo_id LIMIT :limit OFFSET :offset"
    )
    count_q = text(f"SELECT COUNT(*) FROM {GEOGRAPHY_RELATION} WHERE {where}")
    return list_q, count_q, params


# ---------------------------------------------------------------------------
# Publication freshness
# ---------------------------------------------------------------------------

#: Per-source publication and freshness state, rolled up from the glossary's
#: harvested metric catalog. ``freshness_state`` is the warehouse's published
#: data-quality signal (``current`` / ``stale`` / ``retired``); the API reports
#: it rather than recomputing quality from internals it must not read.
SOURCE_FRESHNESS_QUERY: TextClause = text(
    f"""
    SELECT
        source_code,
        COUNT(*)::int AS metric_count,
        COUNT(*) FILTER (WHERE freshness_state = 'current')::int AS current_count,
        COUNT(*) FILTER (WHERE freshness_state = 'stale')::int AS stale_count,
        COUNT(*) FILTER (WHERE freshness_state = 'retired')::int AS retired_count,
        MAX(publication_time) AS latest_publication_time,
        MAX(harvested_at) AS latest_harvested_at
    FROM {METRIC_RELATION}
    GROUP BY source_code
    ORDER BY source_code
    """
)
