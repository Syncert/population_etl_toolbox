"""Census Population Estimates source-explorer endpoints."""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.metric_aliases import resolve_metric_code
from apps.api.services.observations_service import (
    list_latest_observations_for_source,
    list_timeseries_observations_for_source,
)
from data_ingestion_toolbox.models import ObservationListResponse

router = APIRouter(prefix="/api/pep", tags=["census-pep"])


def _metric(metric_code: Optional[str], metric_id: Optional[str]) -> str:
    return resolve_metric_code(
        metric_code=metric_code,
        metric_id=metric_id,
        detail="metric_code or metric_id is required",
    )


@router.get("/observations/latest", response_model=ObservationListResponse)
def get_pep_latest_observations(
    metric_code: Optional[str] = Query(None, max_length=200),
    metric_id: Optional[str] = Query(None, max_length=200),
    geo_level: Optional[str] = Query(None, max_length=50),
    state_fips: Optional[str] = Query(None, max_length=2),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db_session_dep),
) -> ObservationListResponse:
    """Return newest-complete-vintage PEP observations."""
    try:
        return list_latest_observations_for_source(
            db,
            "pep",
            _metric(metric_code, metric_id),
            geo_level,
            state_fips,
            limit,
            offset,
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/observations/timeseries", response_model=ObservationListResponse)
def get_pep_timeseries_observations(
    geo_id: str = Query(..., max_length=200),
    metric_code: Optional[str] = Query(None, max_length=200),
    metric_id: Optional[str] = Query(None, max_length=200),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = Query(1000, ge=1, le=5000),
    db: Session = Depends(get_db_session_dep),
) -> ObservationListResponse:
    """Return as-released PEP observations with visible vintage context."""
    if start_date and end_date and start_date > end_date:
        raise HTTPException(422, "start_date must be less than or equal to end_date")
    try:
        return list_timeseries_observations_for_source(
            db,
            "pep",
            _metric(metric_code, metric_id),
            geo_id,
            start_date,
            end_date,
            limit,
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
