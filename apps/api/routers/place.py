"""Derived readings about one place, built on the published observations."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.failures import NOT_FOUND
from apps.api.schemas.distinctive import DistinctiveResponse
from apps.api.services.distinctive_service import UnknownGeography, distinctive_measures

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
