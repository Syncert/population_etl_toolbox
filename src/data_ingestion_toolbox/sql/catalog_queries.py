"""SQL query builders for catalog endpoints (sources, metrics, geographies).

Each builder returns a ``(list_query, count_query, params)`` triple of
``sqlalchemy.text`` objects and a params dict.  Source queries are plain
``TextClause`` constants.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

# ---------------------------------------------------------------------------
# Source queries
# ---------------------------------------------------------------------------

SOURCES_QUERY: TextClause = text(
    """
    SELECT
        source_code,
        source_name,
        source_type,
        reference_url
    FROM gold.dim_source_system
    ORDER BY source_code
    """
)

SOURCES_QUERY_GLOSSARY: TextClause = text(
    """
    SELECT
        source_code,
        source_name,
        source_type,
        reference_url
    FROM gold_glossary.dim_source_system
    ORDER BY source_code
    """
)


# ---------------------------------------------------------------------------
# Helper: build WHERE clause for metric catalog queries
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
            "(UPPER(metric_code) LIKE UPPER(:q) OR UPPER(metric_display_name) LIKE UPPER(:q))"
        )
        params["q"] = f"%{q}%"
    return " AND ".join(clauses) if clauses else "TRUE"


# ---------------------------------------------------------------------------
# Metric catalog queries – standard gold schema
# ---------------------------------------------------------------------------


def build_metrics_queries(
    source_code: Optional[str],
    active_only: Optional[bool],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    params: dict = {"limit": limit, "offset": offset}
    where = _build_metric_where(source_code, active_only, q, params)
    view = "gold.dim_metric"
    list_q = text(
        f"SELECT * FROM {view} WHERE {where} ORDER BY metric_code LIMIT :limit OFFSET :offset"
    )
    count_q = text(f"SELECT COUNT(*) FROM {view} WHERE {where}")
    return list_q, count_q, params


def build_metrics_queries_legacy(
    source_code: Optional[str],
    active_only: Optional[bool],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    params: dict = {"limit": limit, "offset": offset}
    where = _build_metric_where(source_code, active_only, q, params)
    view = "gold.dim_metric_catalog"
    list_q = text(
        f"SELECT * FROM {view} WHERE {where} ORDER BY metric_code LIMIT :limit OFFSET :offset"
    )
    count_q = text(f"SELECT COUNT(*) FROM {view} WHERE {where}")
    return list_q, count_q, params


# ---------------------------------------------------------------------------
# Metric catalog queries – gold_glossary schema
# ---------------------------------------------------------------------------


def build_metrics_queries_glossary(
    source_code: Optional[str],
    active_only: Optional[bool],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    params: dict = {"limit": limit, "offset": offset}
    where = _build_metric_where(source_code, active_only, q, params)
    view = "gold_glossary.dim_metric"
    list_q = text(
        f"SELECT * FROM {view} WHERE {where} ORDER BY metric_code LIMIT :limit OFFSET :offset"
    )
    count_q = text(f"SELECT COUNT(*) FROM {view} WHERE {where}")
    return list_q, count_q, params


def build_metrics_queries_glossary_legacy(
    source_code: Optional[str],
    active_only: Optional[bool],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    params: dict = {"limit": limit, "offset": offset}
    where = _build_metric_where(source_code, active_only, q, params)
    view = "gold_glossary.dim_metric_catalog"
    list_q = text(
        f"SELECT * FROM {view} WHERE {where} ORDER BY metric_code LIMIT :limit OFFSET :offset"
    )
    count_q = text(f"SELECT COUNT(*) FROM {view} WHERE {where}")
    return list_q, count_q, params


# ---------------------------------------------------------------------------
# Helper: build WHERE clause for geography queries
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
            "(UPPER(geo_id) LIKE UPPER(:q) OR UPPER(geo_name) LIKE UPPER(:q)"
            " OR UPPER(state_name) LIKE UPPER(:q) OR UPPER(county_name) LIKE UPPER(:q)"
            " OR UPPER(place_name) LIKE UPPER(:q))"
        )
        params["q"] = f"%{q}%"
    return " AND ".join(clauses) if clauses else "TRUE"


# ---------------------------------------------------------------------------
# Geography catalog queries – standard gold schema
# ---------------------------------------------------------------------------


def build_geographies_queries(
    geo_level: Optional[str],
    state_fips: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    params: dict = {"limit": limit, "offset": offset}
    where = _build_geo_where(geo_level, state_fips, q, params)
    view = "gold.dim_geography"
    list_q = text(
        f"SELECT * FROM {view} WHERE {where} ORDER BY geo_id LIMIT :limit OFFSET :offset"
    )
    count_q = text(f"SELECT COUNT(*) FROM {view} WHERE {where}")
    return list_q, count_q, params


def build_geographies_queries_legacy(
    geo_level: Optional[str],
    state_fips: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    params: dict = {"limit": limit, "offset": offset}
    where = _build_geo_where(geo_level, state_fips, q, params)
    view = "gold.dim_geo_latest"
    list_q = text(
        f"SELECT * FROM {view} WHERE {where} ORDER BY geo_id LIMIT :limit OFFSET :offset"
    )
    count_q = text(f"SELECT COUNT(*) FROM {view} WHERE {where}")
    return list_q, count_q, params


# ---------------------------------------------------------------------------
# Geography catalog queries – gold_glossary schema
# ---------------------------------------------------------------------------


def build_geographies_queries_glossary(
    geo_level: Optional[str],
    state_fips: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    params: dict = {"limit": limit, "offset": offset}
    where = _build_geo_where(geo_level, state_fips, q, params)
    view = "gold_glossary.dim_geography"
    list_q = text(
        f"SELECT * FROM {view} WHERE {where} ORDER BY geo_id LIMIT :limit OFFSET :offset"
    )
    count_q = text(f"SELECT COUNT(*) FROM {view} WHERE {where}")
    return list_q, count_q, params


def build_geographies_queries_glossary_legacy(
    geo_level: Optional[str],
    state_fips: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    params: dict = {"limit": limit, "offset": offset}
    where = _build_geo_where(geo_level, state_fips, q, params)
    view = "gold_glossary.dim_geo_latest"
    list_q = text(
        f"SELECT * FROM {view} WHERE {where} ORDER BY geo_id LIMIT :limit OFFSET :offset"
    )
    count_q = text(f"SELECT COUNT(*) FROM {view} WHERE {where}")
    return list_q, count_q, params
