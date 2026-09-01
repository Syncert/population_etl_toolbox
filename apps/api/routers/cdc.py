"""CDC illness and chronic-disease source-explorer endpoints.

The route exposes the registered CDC products separately. `dataset=cdi` returns
provider-published national and state indicators; `dataset=places_county`
returns model-based small-area county estimates. The API never merges, rolls
up, or reinterprets them, and it never fills a suppressed or missing value.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.services.cdc_service import list_cdc_observations
from data_ingestion_toolbox.cdc.registry import enabled_assets
from apps.api.schemas import CdcObservationListResponse
from data_ingestion_toolbox.sql.cdc_queries import (
    ADJUSTMENT_STATUSES,
    GEOGRAPHY_TYPES,
)

router = APIRouter(prefix="/cdc", tags=["cdc"])


def _registered_datasets() -> tuple[str, ...]:
    """Read the registered dataset identities at request time."""
    return tuple(asset.asset_id for asset in enabled_assets())


def _validated_choice(
    value: Optional[str], allowed: tuple[str, ...], field: str
) -> Optional[str]:
    if value is None:
        return None
    if value not in allowed:
        raise HTTPException(422, f"{field} must be one of: {', '.join(allowed)}")
    return value


@router.get("/observations", response_model=CdcObservationListResponse)
def get_cdc_observations(
    dataset: Optional[str] = Query(None, max_length=50),
    measure_id: Optional[str] = Query(None, max_length=200),
    value_type_id: Optional[str] = Query(None, max_length=200),
    geo_id: Optional[str] = Query(None, max_length=200),
    geo_type: Optional[str] = Query(None, max_length=50),
    year_from: Optional[int] = Query(None, ge=1900, le=2200),
    year_to: Optional[int] = Query(None, ge=1900, le=2200),
    stratum_id: Optional[str] = Query(None, max_length=64),
    adjustment: Optional[str] = Query(None, max_length=50),
    release: Optional[str] = Query(None, max_length=64),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db_session_dep),
) -> CdcObservationListResponse:
    """Return published CDC observations for the latest or a named release."""
    dataset = _validated_choice(dataset, _registered_datasets(), "dataset")
    geo_type = _validated_choice(geo_type, GEOGRAPHY_TYPES, "geo_type")
    adjustment = _validated_choice(adjustment, ADJUSTMENT_STATUSES, "adjustment")
    if year_from is not None and year_to is not None and year_from > year_to:
        raise HTTPException(422, "year_from must be less than or equal to year_to")

    try:
        return list_cdc_observations(
            db,
            dataset=dataset,
            measure_id=measure_id,
            value_type_id=value_type_id,
            geo_id=geo_id,
            geo_type=geo_type,
            year_from=year_from,
            year_to=year_to,
            stratum_id=stratum_id,
            adjustment_status=adjustment,
            release=release,
            limit=limit,
            offset=offset,
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
