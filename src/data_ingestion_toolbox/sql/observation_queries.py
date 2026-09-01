"""SQL query builders for the cross-source observation contract views.

Each builder returns a ``(list_query, count_query, params)`` triple of
``sqlalchemy.text`` objects and a params dict that can be passed directly to
``Session.execute``. Every user value is bound; the only interpolated text is
the module's own reviewed relation names and select lists.

Per-source reads do not live here. They are built from the serving contracts in
``apps.api.registry``, which declares each source's relations and the fields it
actually publishes.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

# ---------------------------------------------------------------------------
# The observation contract's select list, shared by every builder below.
# ---------------------------------------------------------------------------

_OBSERVATION_SELECT = """
    source_code,
    source_code AS source,
    observation_date,
    period,
    duration_start,
    duration_end,
    time_sk,
    as_of_date,
    as_of_date AS release_date,
    updated_at,
    geo_id,
    geo_level,
    COALESCE(county_name, state_name, geo_id) AS geo_name,
    state_fips,
    county_fips,
    state_name,
    county_name,
    geo_latitude,
    geo_longitude,
    metric_code,
    metric_display_name,
    value::TEXT AS value,
    value_type,
    units,
    units AS unit,
    seasonal_adjustment_status,
    dataset_code,
    dataset_code AS dataset,
    vintage_year,
    vintage_year::TEXT AS vintage,
    margin_of_error::TEXT AS margin_of_error,
    margin_of_error_pct::TEXT AS margin_of_error_pct
"""

# Legacy select (pre-MVP views) – narrower column set, NULLed extensions


#: Output column names shared by both select lists above, in order. A ranked
#: fallback must project them explicitly: ``SELECT * EXCEPT(rn)`` is DuckDB and
#: BigQuery syntax that PostgreSQL rejects outright, so every fallback built
#: that way raised a ProgrammingError the moment the primary view returned no
#: rows -- turning "this metric has no observations" into a 503.
_OBSERVATION_COLUMNS: tuple[str, ...] = (
    "source_code",
    "source",
    "observation_date",
    "period",
    "duration_start",
    "duration_end",
    "time_sk",
    "as_of_date",
    "release_date",
    "updated_at",
    "geo_id",
    "geo_level",
    "geo_name",
    "state_fips",
    "county_fips",
    "state_name",
    "county_name",
    "geo_latitude",
    "geo_longitude",
    "metric_code",
    "metric_display_name",
    "value",
    "value_type",
    "units",
    "unit",
    "seasonal_adjustment_status",
    "dataset_code",
    "dataset",
    "vintage_year",
    "vintage",
    "margin_of_error",
    "margin_of_error_pct",
)

_RANKED_PROJECTION = ", ".join(_OBSERVATION_COLUMNS)


def _build_where_latest(
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    params: dict,
) -> str:
    clauses = ["metric_code = :metric_code"]
    params["metric_code"] = metric_code
    if geo_level:
        clauses.append("UPPER(geo_level) = UPPER(:geo_level)")
        params["geo_level"] = geo_level
    if state_fips:
        clauses.append("state_fips = :state_fips")
        params["state_fips"] = state_fips
    return " AND ".join(clauses)


def _build_where_timeseries(
    metric_code: str,
    geo_id: str,
    start_date: Optional[date],
    end_date: Optional[date],
    params: dict,
) -> str:
    clauses = ["metric_code = :metric_code", "geo_id = :geo_id"]
    params["metric_code"] = metric_code
    params["geo_id"] = geo_id
    if start_date:
        clauses.append("observation_date >= :start_date")
        params["start_date"] = start_date
    if end_date:
        clauses.append("observation_date <= :end_date")
        params["end_date"] = end_date
    return " AND ".join(clauses)


# ---------------------------------------------------------------------------
# Latest observations – cross-source contract view
# ---------------------------------------------------------------------------


def build_latest_mv_queries(
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    params: dict = {"limit": limit, "offset": offset}
    where = _build_where_latest(metric_code, geo_level, state_fips, params)
    view = "gold.v_metric_latest_by_geo"
    list_q = text(
        f"SELECT {_OBSERVATION_SELECT} FROM {view} WHERE {where} ORDER BY geo_id LIMIT :limit OFFSET :offset"
    )
    count_q = text(f"SELECT COUNT(*) FROM {view} WHERE {where}")
    return list_q, count_q, params


# ---------------------------------------------------------------------------
# Latest observations RPT fallback – cross-source gold schema
# ---------------------------------------------------------------------------


def build_latest_rpt_fallback_queries(
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> tuple[TextClause, TextClause, dict]:
    params: dict = {"limit": limit, "offset": offset}
    where = _build_where_latest(metric_code, geo_level, state_fips, params)
    view = "gold.v_metric_timeseries_by_geo"
    cte = f"WITH ranked AS (SELECT {_OBSERVATION_SELECT}, ROW_NUMBER() OVER (PARTITION BY geo_id ORDER BY observation_date DESC) AS rn FROM {view} WHERE {where})"
    list_q = text(
        f"{cte} SELECT {_RANKED_PROJECTION} FROM ranked WHERE rn = 1 "
        f"ORDER BY geo_id LIMIT :limit OFFSET :offset"
    )
    count_q = text(f"{cte} SELECT COUNT(*) FROM ranked WHERE rn = 1")
    return list_q, count_q, params


# ---------------------------------------------------------------------------
# Timeseries observations – cross-source contract view
# ---------------------------------------------------------------------------


def build_timeseries_queries(
    metric_code: str,
    geo_id: str,
    start_date: Optional[date],
    end_date: Optional[date],
    limit: int,
) -> tuple[TextClause, TextClause, dict]:
    params: dict = {"limit": limit}
    where = _build_where_timeseries(metric_code, geo_id, start_date, end_date, params)
    view = "gold.v_metric_timeseries_by_geo"
    list_q = text(
        f"SELECT {_OBSERVATION_SELECT} FROM {view} WHERE {where} ORDER BY observation_date ASC LIMIT :limit"
    )
    count_q = text(f"SELECT COUNT(*) FROM {view} WHERE {where}")
    return list_q, count_q, params
