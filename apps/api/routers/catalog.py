from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.failures import NOT_FOUND
from apps.api.services.catalog_service import (
    get_geography_related,
    get_metric_capability,
    list_geographies,
    list_metrics,
    list_source_capabilities,
    list_source_freshness,
    list_sources,
)
from apps.api.schemas.search import LocateResponse, PlaceSearchResponse
from apps.api.services.place_search_service import (
    SEARCH_LEVELS,
    locate_point,
    search_places,
)
from apps.api.schemas import (
    CapabilityListResponse,
    FreshnessListResponse,
    GeographyListResponse,
    GeographyRelatedResponse,
    MetricCapability,
    MetricListResponse,
    SourceSystem,
)

router = APIRouter(prefix="/catalog", tags=["catalog"])


@router.get("/sources", response_model=list[SourceSystem])
def get_sources(db: Session = Depends(get_db_session_dep)) -> list[SourceSystem]:
    try:
        return list_sources(db)
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/metrics", response_model=MetricListResponse)
def get_metrics(
    source_code: Optional[str] = Query(None, max_length=50),
    active_only: Optional[bool] = None,
    q: Optional[str] = Query(None, max_length=200),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0, le=100000),
    db: Session = Depends(get_db_session_dep),
) -> MetricListResponse:
    try:
        return list_metrics(
            db,
            source_code=source_code,
            active_only=active_only,
            q=q,
            limit=limit,
            offset=offset,
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/geographies", response_model=GeographyListResponse)
def get_geographies(
    geo_level: Optional[str] = Query(None, max_length=50),
    state_fips: Optional[str] = Query(None, max_length=2),
    county_fips: Optional[str] = Query(None, max_length=3),
    active_only: Optional[bool] = None,
    q: Optional[str] = Query(None, max_length=200),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0, le=100000),
    db: Session = Depends(get_db_session_dep),
) -> GeographyListResponse:
    try:
        return list_geographies(
            db,
            geo_level=geo_level,
            state_fips=state_fips,
            county_fips=county_fips,
            active_only=active_only,
            q=q,
            limit=limit,
            offset=offset,
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/capabilities", response_model=CapabilityListResponse)
def get_capabilities(request: Request) -> CapabilityListResponse:
    """Machine-readable capability metadata for every completed source.

    Served from the reviewed discovery registry and the application's own
    OpenAPI contract -- no database read -- so a client learns which routes and
    filters reach each source without hard-coding a source enumeration.
    """
    return list_source_capabilities(request.app.openapi().get("paths") or {})


@router.get("/freshness", response_model=FreshnessListResponse)
def get_freshness(db: Session = Depends(get_db_session_dep)) -> FreshnessListResponse:
    """Per-source publication and freshness state from the harvested glossary."""
    try:
        return list_source_freshness(db)
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get(
    "/metrics/{metric_code}",
    response_model=MetricCapability,
    responses=NOT_FOUND,
)
def get_metric(
    request: Request,
    metric_code: str = Path(..., min_length=1, max_length=200),
    db: Session = Depends(get_db_session_dep),
) -> MetricCapability:
    """One metric's published semantics plus the routes that can serve it."""
    try:
        capability = get_metric_capability(
            db, metric_code, request.app.openapi().get("paths") or {}
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
    if capability is None:
        raise HTTPException(status_code=404, detail="metric_code not found")
    return capability


@router.get("/geographies/search", response_model=PlaceSearchResponse)
def get_geography_search(
    q: str = Query(..., min_length=2, max_length=100),
    level: Optional[list[str]] = Query(None, max_length=5),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db_session_dep),
) -> PlaceSearchResponse:
    """Find a state, county, metro, city or ZIP code area by name.

    Ranked by how the name matches (exact, prefix, a later word, sounds-like)
    and then by served population; a matched city's county is offered beside
    it. ``level`` narrows the grains (STATE, COUNTY, METRO, PLACE, ZCTA).
    """
    levels = [value.upper() for value in level] if level else None
    if levels and not set(levels) <= set(SEARCH_LEVELS):
        raise HTTPException(
            status_code=422,
            detail=f"level must be among {', '.join(SEARCH_LEVELS)}",
        )
    try:
        return search_places(db, q, levels, limit)
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/geographies/locate", response_model=LocateResponse)
def get_geography_locate(
    latitude: float = Query(..., ge=-90, le=90),
    longitude: float = Query(..., ge=-180, le=180),
    db: Session = Depends(get_db_session_dep),
) -> LocateResponse:
    """The served geographies containing a point: tract, ZIP code area, city,
    county, metro and state, smallest first. Nothing is guessed for a point
    outside every boundary."""
    try:
        return locate_point(db, latitude, longitude)
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get(
    "/geographies/{geo_id}/related",
    response_model=GeographyRelatedResponse,
    responses=NOT_FOUND,
)
def get_geography_relationships(
    geo_id: str = Path(..., min_length=1, max_length=100),
    db: Session = Depends(get_db_session_dep),
) -> GeographyRelatedResponse:
    """The geographies one served geography contains, is part of, intersects,
    and borders, each with the vintage and evidence the reference recorded."""
    try:
        related = get_geography_related(db, geo_id)
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
    if related is None:
        raise HTTPException(status_code=404, detail="geo_id not found")
    return related
