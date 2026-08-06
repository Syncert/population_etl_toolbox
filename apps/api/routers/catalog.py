from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.services.catalog_service import (
    list_geographies,
    list_metrics,
    list_sources,
)
from data_ingestion_toolbox.models import (
    GeographyListResponse,
    MetricListResponse,
    SourceSystem,
)

router = APIRouter(prefix="/api/catalog", tags=["catalog"])


@router.get("/sources", response_model=list[SourceSystem])
def get_sources(db: Session = Depends(get_db_session_dep)) -> list[SourceSystem]:
    try:
        return list_sources(db)
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/metrics", response_model=MetricListResponse)
def get_metrics(
    source_code: Optional[str] = None,
    active_only: Optional[bool] = None,
    dashboard_suitability: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db_session_dep),
) -> MetricListResponse:
    try:
        return list_metrics(
            db,
            source_code=source_code,
            active_only=active_only,
            dashboard_suitability=dashboard_suitability,
            q=q,
            limit=limit,
            offset=offset,
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/geographies", response_model=GeographyListResponse)
def get_geographies(
    geo_level: Optional[str] = None,
    state_fips: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db_session_dep),
) -> GeographyListResponse:
    try:
        return list_geographies(
            db,
            geo_level=geo_level,
            state_fips=state_fips,
            q=q,
            limit=limit,
            offset=offset,
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
