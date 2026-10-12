"""Derived county crime roll-up, built on the published warehouse contract.

The resource serves `gold_fbi.county_rollup` (ETL-053): a declared-derived
sum of agency-reported absolute totals per county, offense measure, and
month. It never serves a provider-published county figure, never computes
a rate, and refuses an unmapped county explicitly.
"""

from __future__ import annotations

import re
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.failures import NOT_FOUND
from apps.api.schemas.crime_rollup import CountyRollupListResponse
from apps.api.services.crime_rollup_service import (
    CountyNotMappedError,
    list_county_rollup,
)
from apps.api.services.neutral_observations_service import NeutralQueryError
from data_ingestion_toolbox.fbi_ucr.registry import ALL_PRODUCTS

router = APIRouter(prefix="/crime", tags=["crime"])


def _registered_products() -> tuple[str, ...]:
    """Read the registered FBI product identities at request time."""
    return tuple(product.product_id for product in ALL_PRODUCTS)


def _validated_state_fips(value: Optional[str]) -> Optional[str]:
    """Two census digits, or absent. An empty value is absent (API-117)."""
    if value is None:
        return None
    word = value.strip()
    if not word:
        return None
    if re.fullmatch(r"[0-9]{2}", word) is None:
        raise HTTPException(422, "state_fips must be two digits")
    return word


@router.get(
    "/county-rollup",
    response_model=CountyRollupListResponse,
    responses=NOT_FOUND,
    name="get_crime_county_rollup",
    summary="Derived county roll-up of agency-reported crime totals",
)
def get_crime_county_rollup(
    product_id: Optional[str] = Query(None, max_length=100),
    measure_id: Optional[str] = Query(None, max_length=200),
    geo_id: Optional[str] = Query(None, max_length=200),
    state_fips: Optional[str] = Query(None, max_length=2),
    year_from: Optional[int] = Query(None, ge=1900, le=2200),
    year_to: Optional[int] = Query(None, ge=1900, le=2200),
    release: Optional[str] = Query(None, max_length=64),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0, le=100000),
    db: Session = Depends(get_db_session_dep),
) -> CountyRollupListResponse:
    """Return the derived roll-up for the latest or a named release."""
    state_fips = _validated_state_fips(state_fips)
    if product_id and product_id not in _registered_products():
        raise HTTPException(
            422,
            "product_id must be one of: " + ", ".join(_registered_products()),
        )
    if year_from is not None and year_to is not None and year_from > year_to:
        raise HTTPException(422, "year_from must be less than or equal to year_to")

    try:
        return list_county_rollup(
            db,
            product_id=product_id,
            measure_id=measure_id,
            geo_id=geo_id,
            state_fips=state_fips,
            year_from=year_from,
            year_to=year_to,
            release=release,
            limit=limit,
            offset=offset,
        )
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except CountyNotMappedError as exc:
        raise HTTPException(status_code=404, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
