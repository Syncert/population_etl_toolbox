"""USDA NASS Quick Stats crop explorer endpoints.

The Quick Stats grain is multidimensional, so these endpoints filter on the
provider's own classification rather than on a single opaque metric code, and
every response carries the unit, source program, domain, release, coefficient
of variation, and suppression state alongside the value.
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.services.usda_nass_service import (
    NassObservationFilters,
    NassQueryError,
    NassSeriesFilters,
    list_measures,
    list_observations,
    list_series,
    source_notes,
)
from apps.api.schemas import (
    NassMeasureListResponse,
    NassObservationListResponse,
    NassSeriesListResponse,
    NassSourceNotesResponse,
)

router = APIRouter(prefix="/usda-nass", tags=["usda-nass"])


@router.get("/observations", response_model=NassObservationListResponse)
def get_usda_nass_observations(
    product_id: Optional[str] = Query(None, max_length=100),
    source_desc: Optional[str] = Query(None, max_length=20),
    commodity_desc: Optional[str] = Query(None, max_length=100),
    class_desc: Optional[str] = Query(None, max_length=100),
    statisticcat_desc: Optional[str] = Query(None, max_length=100),
    short_desc: Optional[str] = Query(None, max_length=300),
    unit_desc: Optional[str] = Query(None, max_length=100),
    freq_desc: Optional[str] = Query(None, max_length=50),
    domain_desc: Optional[str] = Query(None, max_length=100),
    domaincat_desc: Optional[str] = Query(None, max_length=300),
    agg_level_desc: Optional[str] = Query(None, max_length=50),
    geo_id: Optional[str] = Query(None, max_length=100),
    state_fips: Optional[str] = Query(None, max_length=2),
    release_watermark: Optional[str] = Query(None, max_length=64),
    value_status: Optional[str] = Query(None, max_length=40),
    year_start: Optional[int] = Query(None, ge=1800, le=2200),
    year_end: Optional[int] = Query(None, ge=1800, le=2200),
    latest: bool = Query(False),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0, le=100000),
    db: Session = Depends(get_db_session_dep),
) -> NassObservationListResponse:
    """Return crop observations with their full source classification."""
    try:
        filters = NassObservationFilters(
            product_id=product_id,
            source_desc=source_desc,
            commodity_desc=commodity_desc,
            class_desc=class_desc,
            statisticcat_desc=statisticcat_desc,
            short_desc=short_desc,
            unit_desc=unit_desc,
            freq_desc=freq_desc,
            domain_desc=domain_desc,
            domaincat_desc=domaincat_desc,
            agg_level_desc=agg_level_desc,
            geo_id=geo_id,
            state_fips=state_fips,
            release_watermark=release_watermark,
            value_status=value_status,
            year_start=year_start,
            year_end=year_end,
            latest_release_only=latest,
            limit=limit,
            offset=offset,
        )
    except NassQueryError as exc:
        raise HTTPException(422, str(exc)) from exc
    try:
        return list_observations(db, filters)
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/series", response_model=NassSeriesListResponse)
def get_usda_nass_series(
    product_id: Optional[str] = Query(None, max_length=100),
    source_desc: Optional[str] = Query(None, max_length=20),
    commodity_desc: Optional[str] = Query(None, max_length=100),
    statisticcat_desc: Optional[str] = Query(None, max_length=100),
    unit_desc: Optional[str] = Query(None, max_length=100),
    domain_desc: Optional[str] = Query(None, max_length=100),
    agg_level_desc: Optional[str] = Query(None, max_length=50),
    geo_id: Optional[str] = Query(None, max_length=100),
    freq_desc: Optional[str] = Query(None, max_length=50),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0, le=100000),
    db: Session = Depends(get_db_session_dep),
) -> NassSeriesListResponse:
    """Return stable series identities for the registered crop products."""
    try:
        filters = NassSeriesFilters(
            product_id=product_id,
            source_desc=source_desc,
            commodity_desc=commodity_desc,
            statisticcat_desc=statisticcat_desc,
            unit_desc=unit_desc,
            domain_desc=domain_desc,
            agg_level_desc=agg_level_desc,
            geo_id=geo_id,
            freq_desc=freq_desc,
            limit=limit,
            offset=offset,
        )
    except NassQueryError as exc:
        raise HTTPException(422, str(exc)) from exc
    try:
        return list_series(db, filters)
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/measures", response_model=NassMeasureListResponse)
def get_usda_nass_measures(
    db: Session = Depends(get_db_session_dep),
) -> NassMeasureListResponse:
    """Return the source-backed measure export with exact units."""
    try:
        return list_measures(db)
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/source-notes", response_model=NassSourceNotesResponse)
def get_usda_nass_source_notes() -> NassSourceNotesResponse:
    """Return the source notes a consumer must read before aggregating."""
    return source_notes()
