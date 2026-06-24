from datetime import date
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from data_ingestion_toolbox.models import ObservationDashboard, ObservationListResponse
from data_ingestion_toolbox.sql.observation_queries import (
    build_latest_mv_queries,
    build_latest_mv_queries_legacy,
    build_latest_rpt_fallback_queries,
    build_latest_rpt_fallback_queries_legacy,
    build_timeseries_queries,
    build_timeseries_queries_legacy,
)


def _relation_exists(db: Session, relation_name: str) -> bool:
    if not hasattr(db, "bind"):
        return True

    exists_query = text("SELECT to_regclass(:relation_name) IS NOT NULL")
    exists = db.execute(exists_query, {"relation_name": relation_name}).scalar()
    if exists is None:
        return True
    return bool(exists)


def _relation_has_columns(db: Session, relation_name: str, column_names: list[str]) -> bool:
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
    rows = db.execute(
        columns_query,
        {
            "schema_name": schema_name,
            "table_name": table_name,
            "column_names": column_names,
        },
    ).scalars().all()
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
        if not _relation_is_mvp_observation_contract(db, "gold.v_metric_timeseries_by_geo"):
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
