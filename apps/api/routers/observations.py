from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.services.observations_service import list_latest_observations, list_timeseries_observations
from data_ingestion_toolbox.models import ObservationListResponse

router = APIRouter(prefix="/api/observations", tags=["observations"])


def _resolve_metric_code(metric_code: Optional[str], metric_id: Optional[str]) -> str:
    if metric_code:
        return metric_code
    if metric_id:
        return metric_id
    raise HTTPException(status_code=422, detail="metric_code or metric_id is required")


@router.get("/latest", response_model=ObservationListResponse)
def get_latest_observations(
    metric_code: Optional[str] = None,
    metric_id: Optional[str] = None,
    geo_level: Optional[str] = None,
    state_fips: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db_session_dep),
) -> ObservationListResponse:
    resolved_metric_code = _resolve_metric_code(metric_code=metric_code, metric_id=metric_id)

    try:
        return list_latest_observations(
            db,
            metric_code=resolved_metric_code,
            geo_level=geo_level,
            state_fips=state_fips,
            limit=limit,
            offset=offset,
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/timeseries", response_model=ObservationListResponse)
def get_timeseries_observations(
    geo_id: str,
    metric_code: Optional[str] = None,
    metric_id: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = Query(1000, ge=1, le=5000),
    db: Session = Depends(get_db_session_dep),
) -> ObservationListResponse:
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=422, detail="start_date must be less than or equal to end_date")

    resolved_metric_code = _resolve_metric_code(metric_code=metric_code, metric_id=metric_id)

    try:
        return list_timeseries_observations(
            db,
            metric_code=resolved_metric_code,
            geo_id=geo_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
