from datetime import date
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from data_ingestion_toolbox.models import ObservationDashboard, ObservationListResponse
from data_ingestion_toolbox.sql.observation_queries import (
    SOURCE_SCHEMA_MAP,
    build_latest_mv_queries,
    build_latest_mv_queries_for_schema,
    build_latest_mv_queries_legacy,
    build_latest_rpt_fallback_queries,
    build_latest_rpt_fallback_queries_for_schema,
    build_latest_rpt_fallback_queries_legacy,
    build_timeseries_queries,
    build_timeseries_queries_for_schema,
    build_timeseries_queries_legacy,
)


SOURCE_LATEST_TABLE_MAP: dict[str, str] = {
    "bls": "gold_bls.mv_bls_latest",
    "census": "gold_census.mv_acs_latest",
    "fred": "gold_fred.mv_fred_latest",
    "pep": "gold_pep.mv_pep_latest",
}

SOURCE_TIMESERIES_TABLE_MAP: dict[str, str] = {
    "bls": "gold_bls.rpt_bls_observations",
    "census": "gold_census.rpt_acs_observations",
    "fred": "gold_fred.rpt_fred_observations",
    "pep": "gold_pep.rpt_pep_observations",
}


def _relation_exists(db: Session, relation_name: str) -> bool:
    if not hasattr(db, "bind"):
        return True

    exists_query = text("SELECT to_regclass(:relation_name) IS NOT NULL")
    try:
        exists = db.execute(exists_query, {"relation_name": relation_name}).scalar()
    except SQLAlchemyError:
        return False
    if exists is None:
        return True
    return bool(exists)


def _relation_has_columns(
    db: Session, relation_name: str, column_names: list[str]
) -> bool:
    if not hasattr(db, "bind"):
        return True

    schema_name, _, table_name = relation_name.partition(".")
    if not schema_name or not table_name:
        return False

    columns_query = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema_name
          AND table_name = :table_name
          AND column_name = ANY(:column_names)
        """
    )
    rows = (
        db.execute(
            columns_query,
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "column_names": column_names,
            },
        )
        .scalars()
        .all()
    )
    return set(rows) == set(column_names)


def _relation_is_mvp_observation_contract(db: Session, relation_name: str) -> bool:
    return _relation_exists(db, relation_name) and _relation_has_columns(
        db,
        relation_name,
        [
            "dataset_code",
            "vintage_year",
            "margin_of_error",
            "margin_of_error_pct",
        ],
    )


def _source_select_sql(source: str) -> str:
    normalized = source.lower()
    seasonal_expr = (
        "seasonal_adjustment_status"
        if normalized in {"bls", "fred"}
        else "NULL::TEXT AS seasonal_adjustment_status"
    )
    has_census_vintage = normalized in {"census", "pep"}
    dataset_expr = (
        "dataset_code" if has_census_vintage else "NULL::TEXT AS dataset_code"
    )
    vintage_year_expr = (
        "vintage_year" if has_census_vintage else "NULL::INT AS vintage_year"
    )
    vintage_expr = (
        "vintage_year::TEXT AS vintage"
        if has_census_vintage
        else "NULL::TEXT AS vintage"
    )
    moe_expr = (
        "margin_of_error::TEXT AS margin_of_error"
        if has_census_vintage
        else "NULL::TEXT AS margin_of_error"
    )
    moe_pct_expr = (
        "margin_of_error_pct::TEXT AS margin_of_error_pct"
        if has_census_vintage
        else "NULL::TEXT AS margin_of_error_pct"
    )

    geo_name_expr = (
        "COALESCE(place_name, county_name, state_name, geo_id)"
        if normalized == "pep"
        else "COALESCE(county_name, state_name, geo_id)"
    )
    return f"""
        source_code,
        source_code AS source,
        observation_date,
        observation_date::TEXT AS period,
        duration_start,
        duration_end,
        time_sk,
        as_of_date,
        as_of_date AS release_date,
        updated_at,
        geo_id,
        geo_level,
        {geo_name_expr} AS geo_name,
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
        {seasonal_expr},
        {dataset_expr},
        {dataset_expr.split(" AS ")[0]} AS dataset,
        {vintage_year_expr},
        {vintage_expr},
        {moe_expr},
        {moe_pct_expr}
    """


def _list_latest_observations_from_source_table(
    db: Session,
    source: str,
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> ObservationListResponse:
    table_name = SOURCE_LATEST_TABLE_MAP[source.lower()]
    where_clauses = ["metric_code = :metric_code"]
    params: dict = {
        "metric_code": metric_code,
        "limit": limit,
        "offset": offset,
    }

    if geo_level:
        where_clauses.append("UPPER(geo_level) = UPPER(:geo_level)")
        params["geo_level"] = geo_level

    if state_fips:
        where_clauses.append("state_fips = :state_fips")
        params["state_fips"] = state_fips

    where_sql = " AND ".join(where_clauses)
    select_sql = _source_select_sql(source)

    list_query = text(
        f"""
        SELECT
            {select_sql}
        FROM {table_name}
        WHERE {where_sql}
        ORDER BY geo_id ASC
        LIMIT :limit OFFSET :offset
        """
    )
    count_query = text(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE {where_sql}
        """
    )

    total = int(db.execute(count_query, params).scalar() or 0)
    rows = db.execute(list_query, params).mappings().all()
    items = [ObservationDashboard.model_validate(row) for row in rows]
    return ObservationListResponse(total=total, limit=limit, offset=offset, items=items)


def _list_timeseries_observations_from_source_table(
    db: Session,
    source: str,
    metric_code: str,
    geo_id: str,
    start_date: Optional[date],
    end_date: Optional[date],
    limit: int,
) -> ObservationListResponse:
    table_name = SOURCE_TIMESERIES_TABLE_MAP[source.lower()]
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

    where_sql = " AND ".join(where_clauses)
    select_sql = _source_select_sql(source)

    list_query = text(
        f"""
        SELECT
            {select_sql}
        FROM {table_name}
        WHERE {where_sql}
        ORDER BY observation_date ASC
        LIMIT :limit
        """
    )
    count_query = text(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE {where_sql}
        """
    )

    total = int(db.execute(count_query, params).scalar() or 0)
    rows = db.execute(list_query, params).mappings().all()
    items = [ObservationDashboard.model_validate(row) for row in rows]
    return ObservationListResponse(total=total, limit=limit, offset=0, items=items)


def list_latest_observations(
    db: Session,
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> ObservationListResponse:
    latest_builder = build_latest_mv_queries
    if not _relation_is_mvp_observation_contract(db, "gold.v_metric_latest_by_geo"):
        latest_builder = build_latest_mv_queries_legacy

    mv_list_query, mv_count_query, mv_params = latest_builder(
        metric_code=metric_code,
        geo_level=geo_level,
        state_fips=state_fips,
        limit=limit,
        offset=offset,
    )
    mv_total = int(db.execute(mv_count_query, mv_params).scalar() or 0)
    rows = db.execute(mv_list_query, mv_params).mappings().all()
    total = mv_total

    if mv_total == 0:
        latest_fallback_builder = build_latest_rpt_fallback_queries
        if not _relation_is_mvp_observation_contract(
            db, "gold.v_metric_timeseries_by_geo"
        ):
            latest_fallback_builder = build_latest_rpt_fallback_queries_legacy

        rpt_list_query, rpt_count_query, rpt_params = latest_fallback_builder(
            metric_code=metric_code,
            geo_level=geo_level,
            state_fips=state_fips,
            limit=limit,
            offset=offset,
        )
        total = int(db.execute(rpt_count_query, rpt_params).scalar() or 0)
        rows = db.execute(rpt_list_query, rpt_params).mappings().all()

    items = [ObservationDashboard.model_validate(row) for row in rows]
    return ObservationListResponse(total=total, limit=limit, offset=offset, items=items)


def list_timeseries_observations(
    db: Session,
    metric_code: str,
    geo_id: str,
    start_date: Optional[date],
    end_date: Optional[date],
    limit: int,
) -> ObservationListResponse:
    timeseries_builder = build_timeseries_queries
    if not _relation_is_mvp_observation_contract(db, "gold.v_metric_timeseries_by_geo"):
        timeseries_builder = build_timeseries_queries_legacy

    list_query, count_query, params = timeseries_builder(
        metric_code=metric_code,
        geo_id=geo_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    total = int(db.execute(count_query, params).scalar() or 0)
    rows = db.execute(list_query, params).mappings().all()
    items = [ObservationDashboard.model_validate(row) for row in rows]
    return ObservationListResponse(total=total, limit=limit, offset=0, items=items)


def list_latest_observations_for_source(
    db: Session,
    source: str,
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> ObservationListResponse:
    """Return latest observations from a source-specific gold schema.

    ``source`` must be one of "bls", "census", "fred", or "pep".
    Falls back to the cross-source ``gold`` schema when the per-source schema
    does not yet exist.
    """
    normalized_source = source.lower()
    schema = SOURCE_SCHEMA_MAP.get(normalized_source, "gold")

    latest_table = SOURCE_LATEST_TABLE_MAP.get(normalized_source)
    if latest_table and _relation_exists(db, latest_table):
        return _list_latest_observations_from_source_table(
            db,
            source=normalized_source,
            metric_code=metric_code,
            geo_level=geo_level,
            state_fips=state_fips,
            limit=limit,
            offset=offset,
        )

    mv_view = f"{schema}.v_metric_latest_by_geo"
    if not _relation_is_mvp_observation_contract(db, mv_view):
        # Source schema not yet deployed – fall back to cross-source gold schema.
        return list_latest_observations(
            db,
            metric_code=metric_code,
            geo_level=geo_level,
            state_fips=state_fips,
            limit=limit,
            offset=offset,
        )

    mv_list_query, mv_count_query, mv_params = build_latest_mv_queries_for_schema(
        schema=schema,
        metric_code=metric_code,
        geo_level=geo_level,
        state_fips=state_fips,
        limit=limit,
        offset=offset,
    )
    mv_total = int(db.execute(mv_count_query, mv_params).scalar() or 0)
    rows = db.execute(mv_list_query, mv_params).mappings().all()
    total = mv_total

    if mv_total == 0:
        rpt_list_query, rpt_count_query, rpt_params = (
            build_latest_rpt_fallback_queries_for_schema(
                schema=schema,
                metric_code=metric_code,
                geo_level=geo_level,
                state_fips=state_fips,
                limit=limit,
                offset=offset,
            )
        )
        total = int(db.execute(rpt_count_query, rpt_params).scalar() or 0)
        rows = db.execute(rpt_list_query, rpt_params).mappings().all()

    items = [ObservationDashboard.model_validate(row) for row in rows]
    return ObservationListResponse(total=total, limit=limit, offset=offset, items=items)


def list_timeseries_observations_for_source(
    db: Session,
    source: str,
    metric_code: str,
    geo_id: str,
    start_date: Optional[date],
    end_date: Optional[date],
    limit: int,
) -> ObservationListResponse:
    """Return time-series observations from a source-specific gold schema.

    Falls back to the cross-source ``gold`` schema when the per-source schema
    does not yet exist.
    """
    normalized_source = source.lower()
    schema = SOURCE_SCHEMA_MAP.get(normalized_source, "gold")

    timeseries_table = SOURCE_TIMESERIES_TABLE_MAP.get(normalized_source)
    if timeseries_table and _relation_exists(db, timeseries_table):
        return _list_timeseries_observations_from_source_table(
            db,
            source=normalized_source,
            metric_code=metric_code,
            geo_id=geo_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

    ts_view = f"{schema}.v_metric_timeseries_by_geo"
    if not _relation_is_mvp_observation_contract(db, ts_view):
        return list_timeseries_observations(
            db,
            metric_code=metric_code,
            geo_id=geo_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

    list_query, count_query, params = build_timeseries_queries_for_schema(
        schema=schema,
        metric_code=metric_code,
        geo_id=geo_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    total = int(db.execute(count_query, params).scalar() or 0)
    rows = db.execute(list_query, params).mappings().all()
    items = [ObservationDashboard.model_validate(row) for row in rows]
    return ObservationListResponse(total=total, limit=limit, offset=0, items=items)
