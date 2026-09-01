from datetime import date
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.services.neutral_observations_service import (
    NeutralQueryError,
    list_metric_releases,
    list_neutral_observations,
)
from apps.api.services.observations_service import (
    list_latest_observations,
    list_timeseries_observations,
)
from apps.api.schemas import (
    MetricReleaseListResponse,
    NeutralObservationListResponse,
    ObservationListResponse,
)

router = APIRouter(prefix="/observations", tags=["observations"])

METRIC_NOT_FOUND_DETAIL = "metric_code not found"
REVERSED_YEAR_DETAIL = "year_from must be less than or equal to year_to"


@router.get(
    "",
    response_model=NeutralObservationListResponse,
    name="get_neutral_observations",
    summary="Observations for any completed source's metric",
)
def get_neutral_observations(
    metric_code: str = Query(..., min_length=1, max_length=200),
    scope: Literal["latest", "as_released"] = Query("latest"),
    release: Optional[str] = Query(None, min_length=1, max_length=100),
    geo_id: Optional[str] = Query(None, max_length=200),
    geo_level: Optional[str] = Query(None, max_length=50),
    state_fips: Optional[str] = Query(None, max_length=2),
    county_fips: Optional[str] = Query(None, max_length=3),
    stratum_id: Optional[str] = Query(None, max_length=200),
    adjustment_status: Optional[str] = Query(None, max_length=50),
    domain_desc: Optional[str] = Query(None, max_length=200),
    domaincat_desc: Optional[str] = Query(None, max_length=200),
    subject_type: Optional[str] = Query(None, max_length=50),
    subject_code: Optional[str] = Query(None, max_length=50),
    year_from: Optional[int] = Query(None, ge=1700, le=2200),
    year_to: Optional[int] = Query(None, ge=1700, le=2200),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0, le=100000),
    db: Session = Depends(get_db_session_dep),
) -> NeutralObservationListResponse:
    """Provider-neutral observations, dispatched through the reviewed registry.

    The metric resolves to its owning source via the published glossary and is
    answered from that source's own serving relations, preserving its release,
    suppression, uncertainty, and dimensional semantics. Filters beyond the
    universal parameters are per-source; ``/catalog/capabilities`` declares
    which apply, and an unsupported filter is rejected with an explanation.
    """
    if year_from is not None and year_to is not None and year_from > year_to:
        raise HTTPException(status_code=422, detail=REVERSED_YEAR_DETAIL)

    try:
        response = list_neutral_observations(
            db,
            metric_code=metric_code,
            scope=scope,
            release=release,
            filters={
                "geo_id": geo_id,
                "geo_level": geo_level,
                "state_fips": state_fips,
                "county_fips": county_fips,
                "stratum_id": stratum_id,
                "adjustment_status": adjustment_status,
                "domain_desc": domain_desc,
                "domaincat_desc": domaincat_desc,
                "subject_type": subject_type,
                "subject_code": subject_code,
                "year_from": year_from,
                "year_to": year_to,
            },
            limit=limit,
            offset=offset,
        )
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
    if response is None:
        raise HTTPException(status_code=404, detail=METRIC_NOT_FOUND_DETAIL)
    return response


@router.get(
    "/releases",
    response_model=MetricReleaseListResponse,
    name="get_metric_releases",
    summary="Published releases holding a metric's observations",
)
def get_metric_releases(
    metric_code: str = Query(..., min_length=1, max_length=200),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0, le=100000),
    db: Session = Depends(get_db_session_dep),
) -> MetricReleaseListResponse:
    """Release identities a client can pin with ``scope=as_released``."""
    try:
        response = list_metric_releases(
            db, metric_code=metric_code, limit=limit, offset=offset
        )
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
    if response is None:
        raise HTTPException(status_code=404, detail=METRIC_NOT_FOUND_DETAIL)
    return response


@router.get("/latest", response_model=ObservationListResponse)
def get_latest_observations(
    metric_code: str = Query(..., min_length=1, max_length=200),
    geo_level: Optional[str] = Query(None, max_length=50),
    state_fips: Optional[str] = Query(None, max_length=2),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0, le=100000),
    db: Session = Depends(get_db_session_dep),
) -> ObservationListResponse:
    try:
        return list_latest_observations(
            db,
            metric_code=metric_code,
            geo_level=geo_level,
            state_fips=state_fips,
            limit=limit,
            offset=offset,
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/timeseries", response_model=ObservationListResponse)
def get_timeseries_observations(
    geo_id: str = Query(..., max_length=200),
    metric_code: str = Query(..., min_length=1, max_length=200),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = Query(1000, ge=1, le=5000),
    db: Session = Depends(get_db_session_dep),
) -> ObservationListResponse:
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=422, detail="start_date must be less than or equal to end_date"
        )

    try:
        return list_timeseries_observations(
            db,
            metric_code=metric_code,
            geo_id=geo_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
