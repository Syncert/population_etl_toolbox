from typing import Optional

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

# Sources queries — gold_glossary is preferred; gold is the backward-compat fallback.
SOURCES_QUERY_GLOSSARY = text(
    """
    SELECT source_code, source_name, source_type, reference_url
    FROM gold_glossary.dim_source_system
    ORDER BY source_code ASC
    """
)

SOURCES_QUERY = text(
    """
    SELECT source_code, source_name, source_type, reference_url
    FROM gold.dim_source_system
    ORDER BY source_code ASC
    """
)


def build_metrics_query(
    source_code: Optional[str],
    active_only: Optional[bool],
    dashboard_suitability: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, dict]:
    list_query, _count_query, params = build_metrics_queries(
        source_code=source_code,
        active_only=active_only,
        dashboard_suitability=dashboard_suitability,
        q=q,
        limit=limit,
        offset=offset,
    )
    return list_query, params


def _build_metrics_queries_from_table(
    table: str,
    source_code: Optional[str],
    active_only: Optional[bool],
    dashboard_suitability: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    where_clauses = []
    params: dict = {"limit": limit, "offset": offset}

    if source_code:
        where_clauses.append("source_code = :source_code")
        params["source_code"] = source_code

    if active_only is not None:
        where_clauses.append("is_active = :active_only")
        params["active_only"] = active_only

    if dashboard_suitability:
        where_clauses.append("dashboard_suitability = :dashboard_suitability")
        params["dashboard_suitability"] = dashboard_suitability

    if q:
        where_clauses.append(
            "(metric_code ILIKE :q OR metric_display_name ILIKE :q OR COALESCE(business_definition, '') ILIKE :q)"
        )
        params["q"] = f"%{q}%"

    from_sql = f"\n        FROM {table}\n    "

    if where_clauses:
        from_sql += " WHERE " + " AND ".join(where_clauses)

    list_sql = """
        SELECT
            metric_code,
            metric_display_name,
            source_code,
            source_object_type,
            business_definition,
            caveats,
            valid_geo_grains,
            valid_time_grains,
            dashboard_suitability,
            comparability_group,
            do_not_compare_with,
            recommended_aggregation,
            owner_team,
            is_active,
            updated_at
    """

    list_sql += from_sql
    list_sql += " ORDER BY metric_code ASC LIMIT :limit OFFSET :offset"

    count_sql = "SELECT COUNT(*) " + from_sql
    return text(list_sql), text(count_sql), params


def build_metrics_queries(
    source_code: Optional[str],
    active_only: Optional[bool],
    dashboard_suitability: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    return _build_metrics_queries_from_table(
        "gold.dim_metric",
        source_code=source_code,
        active_only=active_only,
        dashboard_suitability=dashboard_suitability,
        q=q,
        limit=limit,
        offset=offset,
    )


def build_metrics_queries_glossary(
    source_code: Optional[str],
    active_only: Optional[bool],
    dashboard_suitability: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    """Build metrics queries targeting gold_glossary.dim_metric."""
    return _build_metrics_queries_from_table(
        "gold_glossary.dim_metric",
        source_code=source_code,
        active_only=active_only,
        dashboard_suitability=dashboard_suitability,
        q=q,
        limit=limit,
        offset=offset,
    )


def build_metrics_queries_legacy(
    source_code: Optional[str],
    active_only: Optional[bool],
    dashboard_suitability: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    return _build_metrics_queries_from_table(
        "gold.dim_metric_catalog",
        source_code=source_code,
        active_only=active_only,
        dashboard_suitability=dashboard_suitability,
        q=q,
        limit=limit,
        offset=offset,
    )


def build_geographies_query(
    geo_level: Optional[str],
    state_fips: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, dict]:
    list_query, _count_query, params = build_geographies_queries(
        geo_level=geo_level,
        state_fips=state_fips,
        q=q,
        limit=limit,
        offset=offset,
    )
    return list_query, params


def _build_geographies_queries_from_table(
    table: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    where_clauses = []
    params: dict = {"limit": limit, "offset": offset}

    if geo_level:
        where_clauses.append("geo_level = :geo_level")
        params["geo_level"] = geo_level

    if state_fips:
        where_clauses.append("state_fips = :state_fips")
        params["state_fips"] = state_fips

    if q:
        where_clauses.append(
            "(geo_id ILIKE :q OR COALESCE(state_name, '') ILIKE :q OR COALESCE(county_name, '') ILIKE :q)"
        )
        params["q"] = f"%{q}%"

    from_sql = f"\n        FROM {table}\n    "

    if where_clauses:
        from_sql += " WHERE " + " AND ".join(where_clauses)

    list_sql = """
        SELECT
            geo_id,
            geo_level,
            state_fips,
            county_fips,
            state_name,
            county_name,
            latitude,
            longitude,
            refreshed_at
    """

    list_sql += from_sql
    list_sql += " ORDER BY geo_id ASC LIMIT :limit OFFSET :offset"

    count_sql = "SELECT COUNT(*) " + from_sql
    return text(list_sql), text(count_sql), params


def build_geographies_queries(
    geo_level: Optional[str],
    state_fips: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    return _build_geographies_queries_from_table(
        "gold.dim_geography",
        geo_level=geo_level,
        state_fips=state_fips,
        q=q,
        limit=limit,
        offset=offset,
    )


def build_geographies_queries_glossary(
    geo_level: Optional[str],
    state_fips: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    """Build geography queries targeting gold_glossary.dim_geography."""
    return _build_geographies_queries_from_table(
        "gold_glossary.dim_geography",
        geo_level=geo_level,
        state_fips=state_fips,
        q=q,
        limit=limit,
        offset=offset,
    )


def build_geographies_queries_legacy(
    geo_level: Optional[str],
    state_fips: Optional[str],
    q: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    return _build_geographies_queries_from_table(
        "gold.dim_geo_latest",
        geo_level=geo_level,
        state_fips=state_fips,
        q=q,
        limit=limit,
        offset=offset,
    )
