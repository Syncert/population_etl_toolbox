from datetime import date
from typing import Optional

from sqlalchemy.orm import Session

from data_ingestion_toolbox.models import ObservationDashboard, ObservationListResponse
from data_ingestion_toolbox.sql.observation_queries import (
    build_latest_mv_queries,
    build_latest_rpt_fallback_queries,
    build_timeseries_queries,
)


def list_latest_observations(
    db: Session,
    metric_code: str,
    geo_level: Optional[str],
    state_fips: Optional[str],
    limit: int,
    offset: int,
) -> ObservationListResponse:
    mv_list_query, mv_count_query, mv_params = build_latest_mv_queries(
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
        rpt_list_query, rpt_count_query, rpt_params = build_latest_rpt_fallback_queries(
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
    list_query, count_query, params = build_timeseries_queries(
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
