"""Derived readings about one place, built on the published observations."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.failures import NOT_FOUND
from apps.api.schemas.distinctive import DistinctiveResponse
from apps.api.services.distinctive_service import UnknownGeography, distinctive_measures
from apps.api.schemas.tract_history import TractHistoryResponse
from apps.api.services.tract_history_service import (
    TractHistoryRequestError,
    UnknownTract,
    tract_history,
)

router = APIRouter(prefix="/place", tags=["place"])


@router.get(
    "/distinctive",
    response_model=DistinctiveResponse,
    responses=NOT_FOUND,
    name="get_place_distinctive",
    summary="Where one place stands among its siblings, one measure at a time",
)
def get_place_distinctive(
    geo_id: str = Query(..., min_length=1, max_length=100),
    db: Session = Depends(get_db_session_dep),
) -> DistinctiveResponse:
    """Within-parent percentile ranks, derived and labelled as such.

    Each reviewed measure is ranked on its own: this geography's newest
    published value against its siblings' for the same period. Measures are
    never combined.
    """
    try:
        return distinctive_measures(db, geo_id)
    except UnknownGeography as exc:
        raise HTTPException(status_code=404, detail="geo_id not found") from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get(
    "/tract-history",
    response_model=TractHistoryResponse,
    responses=NOT_FOUND,
    name="get_place_tract_history",
    summary="One Census tract's history on its 2020 boundary, derived where redrawn",
)
def get_place_tract_history(
    geo_id: str = Query(..., min_length=1, max_length=100),
    metric_codes: str = Query(
        ...,
        min_length=1,
        max_length=1200,
        description="Comma-separated ACS 5-year metric codes, at most 24.",
    ),
    db: Session = Depends(get_db_session_dep),
) -> TractHistoryResponse:
    """Whether the tract's code named the same area before the 2020 redraw,
    from the Bureau's relationship files, and each measure's history on the
    tract's 2020 boundary: published values from 2020 on; before 2020 the
    published values of an unchanged tract, or the DERIVED comparable counts
    of a redrawn one (each point marked ``derived`` with its weight basis and
    source tracts). Medians and rates are never apportioned."""
    try:
        return tract_history(db, geo_id, metric_codes)
    except UnknownTract as exc:
        raise HTTPException(status_code=404, detail="geo_id not found") from exc
    except TractHistoryRequestError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
