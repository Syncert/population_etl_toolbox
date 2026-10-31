from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.failures import NOT_FOUND
from apps.api.query_lists import code_list
from apps.api.schemas.derived import DerivedRatioCatalogResponse
from apps.api.services.derived_ratio_service import list_pairings
from apps.api.services.catalog_service import (
    get_geography_related,
    get_metric_capability,
    list_geographies,
    list_metrics,
    list_source_capabilities,
    list_source_freshness,
    list_sources,
)
from apps.api.services.tile_layers import list_tile_layers
from apps.api.services.place_search import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    MAX_QUERY_CHARACTERS,
    PlaceSearchRefusal,
    locate_place,
    search_places,
)
from apps.api.schemas import (
    CapabilityListResponse,
    FreshnessListResponse,
    GeographyListResponse,
    GeographyLocateResponse,
    GeographySearchResponse,
    GeographyRelatedResponse,
    MetricCapability,
    MetricListResponse,
    SourceSystem,
    TileLayerCatalogResponse,
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
    metric_codes: Optional[str] = Query(
        None,
        max_length=8000,
        description="Comma-separated metric codes: only these rows, in one read. "
        "At most 200 codes; a code the catalog does not hold is simply absent.",
    ),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0, le=100000),
    db: Session = Depends(get_db_session_dep),
) -> MetricListResponse:
    codes = code_list(metric_codes, "metric_codes", 200)
    try:
        return list_metrics(
            db,
            source_code=source_code,
            active_only=active_only,
            q=q,
            limit=limit,
            offset=offset,
            metric_codes=codes,
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


@router.get("/geographies/search", response_model=GeographySearchResponse)
def search_geographies(
    q: str = Query(..., min_length=1, max_length=MAX_QUERY_CHARACTERS),
    grain: Optional[list[str]] = Query(None, max_length=10),
    state_fips: Optional[str] = Query(None, max_length=2),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    db: Session = Depends(get_db_session_dep),
) -> GeographySearchResponse:
    """Places a typed name may mean, best match first, ranked by population.

    Matches a state, county, metro, city or town, or ZCTA by its name without
    the Census description, by name prefix, by a later word's prefix, or by a
    close spelling. The county and metro of each of the three best city
    matches follow it, so "Austin" offers Austin city, Travis County and the
    Austin metro. ``grain`` narrows the grains (repeat it, or separate words
    with commas).
    """
    grains = [word for value in grain or [] for word in value.split(",")]
    try:
        return search_places(db, q, grains=grains, state_fips=state_fips, limit=limit)
    except PlaceSearchRefusal as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get(
    "/geographies/locate",
    response_model=GeographyLocateResponse,
    responses=NOT_FOUND,
)
def locate_geographies(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    db: Session = Depends(get_db_session_dep),
) -> GeographyLocateResponse:
    """The city or town, county, tract, ZCTA, state and metro a point lies in.

    ``landing`` is the city or town, or the county when the point is in none,
    so "use my location" opens the page a resident calls home. The response
    does not echo the coordinates. A point no state boundary covers (outside
    the United States, or offshore) is ``404 {"detail": "no geography contains
    this point"}``.
    """
    try:
        located = locate_place(db, lat, lon)
    except PlaceSearchRefusal as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
    if located is None:
        raise HTTPException(status_code=404, detail="no geography contains this point")
    return located


@router.get("/tile-layers", response_model=TileLayerCatalogResponse)
def get_tile_layers(
    db: Session = Depends(get_db_session_dep),
) -> TileLayerCatalogResponse:
    """Every vector tile layer a map can draw, and the boundary vintages it holds.

    ``counties`` (states and counties) and ``tracts`` draw current
    boundaries. ``places`` (cities and towns) and ``metros`` are versioned:
    each listed vintage has its own ``tile_template`` (``?vintage=YYYY``), the
    default is the newest, and a vintage not listed draws nothing rather than
    another vintage's boundaries. Tiles carry no values; ``uncoloured_rule``
    is the rule a map joining values on ``join_key`` follows.
    """
    try:
        return list_tile_layers(db)
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


@router.get("/derived-ratios", response_model=DerivedRatioCatalogResponse)
def get_derived_ratios(
    numerator: Optional[str] = Query(
        None,
        max_length=200,
        description=(
            "A metric code: only the pairings that divide this metric. A place "
            "page card asks this to learn which per-capita or share views its "
            "count offers."
        ),
    ),
    served_only: bool = Query(
        False,
        description="Only the pairings /observations/derived will answer.",
    ),
) -> DerivedRatioCatalogResponse:
    """The reviewed per-capita and share pairings (ADR-0009).

    Each names its numerator, its denominator (or the provider's own rate),
    the universe the result describes, its status and whether it is served.
    Read from the versioned semantic registry; no database read.
    """
    return list_pairings(numerator=numerator or None, served_only=served_only)


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
