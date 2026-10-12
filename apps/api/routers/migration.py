"""County-to-county migration flows, as IRS SOI publishes them.

The resource serves ``gold_irs_migration.flow_latest`` (ADR-0008): for one
county, direction and pair of filing years, the county-to-county flows
ranked by a measure, the file's own totals, and SOI's categories. It is
provider-published data, not a derivation, and computes no net figure.
"""

from __future__ import annotations

from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.failures import NOT_FOUND
from apps.api.schemas.migration import MigrationFlowsResponse
from apps.api.services.migration_service import (
    MigrationNotPublishedError,
    list_migration_flows,
)
from apps.api.services.neutral_observations_service import NeutralQueryError

router = APIRouter(tags=["migration"])


@router.get(
    "/migration-flows",
    response_model=MigrationFlowsResponse,
    responses=NOT_FOUND,
    name="get_migration_flows",
    summary="Where a county's movers came from or went, as IRS SOI publishes it",
)
def get_migration_flows(
    geo_id: str = Query(..., max_length=200),
    direction: Literal["inflow", "outflow"] = Query(...),
    year_pair: Optional[str] = Query(None, max_length=9),
    measure: Literal["returns", "individuals", "agi"] = Query("returns"),
    limit: int = Query(25, ge=1, le=500),
    db: Session = Depends(get_db_session_dep),
) -> MigrationFlowsResponse:
    """Top origins (inflow) or destinations (outflow) for one county."""
    year_pair = year_pair.strip() or None if year_pair is not None else None
    try:
        return list_migration_flows(
            db,
            geo_id=geo_id,
            direction=direction,
            year_pair=year_pair,
            measure=measure,
            limit=limit,
        )
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except MigrationNotPublishedError as exc:
        raise HTTPException(status_code=404, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
