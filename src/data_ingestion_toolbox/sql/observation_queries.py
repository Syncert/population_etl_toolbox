from datetime import date
from typing import Optional

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause


_OBSERVATION_SELECT = """
    source_code,
    observation_date,
    duration_start,
    duration_end,
    time_sk,
    as_of_date,
    updated_at,
    geo_id,
    geo_level,
    state_fips,
    county_fips,
    state_name,
    county_name,
    geo_latitude,
    geo_longitude,
    metric_code,
    metric_display_name,
    dashboard_suitability,
    value,
    value_type,
    units,
    seasonal_adjustment_status
"""


def build_latest_mv_queries(
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    where_clauses = ["metric_code = :metric_code"]
    params: dict = {
        "metric_code": metric_code,
        "limit": limit,
        "offset": offset,
    }

    if geo_level:
        where_clauses.append("geo_level = :geo_level")
        params["geo_level"] = geo_level

    if state_fips:
        where_clauses.append("state_fips = :state_fips")
        params["state_fips"] = state_fips

    where_sql = " AND ".join(where_clauses)

    list_sql = f"""
        SELECT
            {_OBSERVATION_SELECT}
        FROM gold.v_metric_latest_by_geo
        WHERE {where_sql}
        ORDER BY geo_id ASC
        LIMIT :limit OFFSET :offset
    """

    count_sql = f"""
        SELECT COUNT(*)
        FROM gold.v_metric_latest_by_geo
        WHERE {where_sql}
    """

    return text(list_sql), text(count_sql), params


def build_latest_mv_queries_legacy(
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    where_clauses = ["metric_code = :metric_code"]
    params: dict = {
        "metric_code": metric_code,
        "limit": limit,
        "offset": offset,
    }

    if geo_level:
        where_clauses.append("geo_level = :geo_level")
        params["geo_level"] = geo_level

    if state_fips:
        where_clauses.append("state_fips = :state_fips")
        params["state_fips"] = state_fips

    where_sql = " AND ".join(where_clauses)

    list_sql = f"""
        SELECT
            {_OBSERVATION_SELECT}
        FROM gold.mv_latest_dashboard
        WHERE {where_sql}
        ORDER BY geo_id ASC
        LIMIT :limit OFFSET :offset
    """

    count_sql = f"""
        SELECT COUNT(*)
        FROM gold.mv_latest_dashboard
        WHERE {where_sql}
    """

    return text(list_sql), text(count_sql), params


def build_latest_rpt_fallback_queries(
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    where_clauses = ["metric_code = :metric_code"]
    params: dict = {
        "metric_code": metric_code,
        "limit": limit,
        "offset": offset,
    }

    if geo_level:
        where_clauses.append("geo_level = :geo_level")
        params["geo_level"] = geo_level

    if state_fips:
        where_clauses.append("state_fips = :state_fips")
        params["state_fips"] = state_fips

    where_sql = " AND ".join(where_clauses)

    base_sql = f"""
        WITH ranked AS (
            SELECT
                {_OBSERVATION_SELECT},
                ROW_NUMBER() OVER (
                    PARTITION BY geo_id, metric_code
                    ORDER BY observation_date DESC, updated_at DESC
                ) AS rn
                FROM gold.v_metric_timeseries_by_geo
            WHERE {where_sql}
        )
    """

    list_sql = base_sql + """
        SELECT
            source_code,
            observation_date,
            duration_start,
            duration_end,
            time_sk,
            as_of_date,
            updated_at,
            geo_id,
            geo_level,
            state_fips,
            county_fips,
            state_name,
            county_name,
            geo_latitude,
            geo_longitude,
            metric_code,
            metric_display_name,
            dashboard_suitability,
            value,
            value_type,
            units,
            seasonal_adjustment_status
        FROM ranked
        WHERE rn = 1
        ORDER BY geo_id ASC
        LIMIT :limit OFFSET :offset
    """

    count_sql = base_sql + """
        SELECT COUNT(*)
        FROM ranked
        WHERE rn = 1
    """

    return text(list_sql), text(count_sql), params


def build_latest_rpt_fallback_queries_legacy(
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    where_clauses = ["metric_code = :metric_code"]
    params: dict = {
        "metric_code": metric_code,
        "limit": limit,
        "offset": offset,
    }

    if geo_level:
        where_clauses.append("geo_level = :geo_level")
        params["geo_level"] = geo_level

    if state_fips:
        where_clauses.append("state_fips = :state_fips")
        params["state_fips"] = state_fips

    where_sql = " AND ".join(where_clauses)

    base_sql = f"""
        WITH ranked AS (
            SELECT
                {_OBSERVATION_SELECT},
                ROW_NUMBER() OVER (
                    PARTITION BY geo_id, metric_code
                    ORDER BY observation_date DESC, updated_at DESC
                ) AS rn
                FROM gold.rpt_observation_dashboard
            WHERE {where_sql}
        )
    """

    list_sql = base_sql + """
        SELECT
            source_code,
            observation_date,
            duration_start,
            duration_end,
            time_sk,
            as_of_date,
            updated_at,
            geo_id,
            geo_level,
            state_fips,
            county_fips,
            state_name,
            county_name,
            geo_latitude,
            geo_longitude,
            metric_code,
            metric_display_name,
            dashboard_suitability,
            value,
            value_type,
            units,
            seasonal_adjustment_status
        FROM ranked
        WHERE rn = 1
        ORDER BY geo_id ASC
        LIMIT :limit OFFSET :offset
    """

    count_sql = base_sql + """
        SELECT COUNT(*)
        FROM ranked
        WHERE rn = 1
    """

    return text(list_sql), text(count_sql), params


def build_timeseries_query(
    metric_code: str,
    geo_id: str,
    start_date: Optional[date],
    end_date: Optional[date],
    limit: int,
) -> tuple[TextClause, dict]:
    list_query, _count_query, params = build_timeseries_queries(
        metric_code=metric_code,
        geo_id=geo_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    return list_query, params


def build_timeseries_queries(
    metric_code: str,
    geo_id: str,
    start_date: Optional[date],
    end_date: Optional[date],
    limit: int,
) -> tuple[TextClause, TextClause, dict]:
    where_clauses = ["metric_code = :metric_code", "geo_id = :geo_id"]
    params: dict = {
        "metric_code": metric_code,
        "geo_id": geo_id,
        "limit": limit,
    }

    if start_date:
        where_clauses.append("observation_date >= :start_date")
        params["start_date"] = start_date

    if end_date:
        where_clauses.append("observation_date <= :end_date")
        params["end_date"] = end_date

    from_sql = """
        FROM gold.v_metric_timeseries_by_geo
        WHERE
    """

    from_sql += " AND ".join(where_clauses)

    list_sql = """
        SELECT
            source_code,
            observation_date,
            duration_start,
            duration_end,
            time_sk,
            as_of_date,
            updated_at,
            geo_id,
            geo_level,
            state_fips,
            county_fips,
            state_name,
            county_name,
            geo_latitude,
            geo_longitude,
            metric_code,
            metric_display_name,
            dashboard_suitability,
            value,
            value_type,
            units,
            seasonal_adjustment_status
    """

    list_sql += from_sql
    list_sql += " ORDER BY observation_date ASC LIMIT :limit"

    count_sql = "SELECT COUNT(*) " + from_sql
    return text(list_sql), text(count_sql), params


def build_timeseries_queries_legacy(
    metric_code: str,
    geo_id: str,
    start_date: Optional[date],
    end_date: Optional[date],
    limit: int,
) -> tuple[TextClause, TextClause, dict]:
    where_clauses = ["metric_code = :metric_code", "geo_id = :geo_id"]
    params: dict = {
        "metric_code": metric_code,
        "geo_id": geo_id,
        "limit": limit,
    }

    if start_date:
        where_clauses.append("observation_date >= :start_date")
        params["start_date"] = start_date

    if end_date:
        where_clauses.append("observation_date <= :end_date")
        params["end_date"] = end_date

    from_sql = """
        FROM gold.rpt_observation_dashboard
        WHERE
    """

    from_sql += " AND ".join(where_clauses)

    list_sql = """
        SELECT
            source_code,
            observation_date,
            duration_start,
            duration_end,
            time_sk,
            as_of_date,
            updated_at,
            geo_id,
            geo_level,
            state_fips,
            county_fips,
            state_name,
            county_name,
            geo_latitude,
            geo_longitude,
            metric_code,
            metric_display_name,
            dashboard_suitability,
            value,
            value_type,
            units,
            seasonal_adjustment_status
    """

    list_sql += from_sql
    list_sql += " ORDER BY observation_date ASC LIMIT :limit"

    count_sql = "SELECT COUNT(*) " + from_sql
    return text(list_sql), text(count_sql), params
