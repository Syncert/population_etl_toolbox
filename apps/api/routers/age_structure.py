"""A place's age and sex structure (ADR-0013): pyramid, history, cohorts."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.failures import NOT_FOUND
from apps.api.schemas.age_structure import (
    AgeCohortsResponse,
    AgeStructureHistoryResponse,
    AgeStructureResponse,
)
from apps.api.services.age_structure_service import (
    AgeStructureRequestError,
    UnknownAgeGeography,
    age_cohorts,
    age_structure,
    age_structure_history,
)

router = APIRouter(prefix="/age-structure", tags=["age-structure"])

_DATASET = Query("acs5", max_length=10, description="`acs5` (default) or `acs1`.")


def _answer(call):
    try:
        return call()
    except UnknownAgeGeography as exc:
        raise HTTPException(
            status_code=404, detail=f"{exc.parameter} not found"
        ) from exc
    except AgeStructureRequestError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get(
    "",
    response_model=AgeStructureResponse,
    name="get_age_structure",
    summary="A place's age and sex structure beside every level it sits in",
    responses=NOT_FOUND,
)
def get_age_structure(
    geo_id: str = Query(..., min_length=1, max_length=60),
    dataset: str = _DATASET,
    release: Optional[str] = Query(
        None, max_length=10, description="`latest` (default) or a vintage year."
    ),
    compare_geo_ids: Optional[str] = Query(
        None,
        max_length=200,
        description="Comma-separated places to draw beside it, at most 3.",
    ),
    db: Session = Depends(get_db_session_dep),
) -> AgeStructureResponse:
    """Eighteen five-year age groups per sex as shares of the place's total,
    with Bureau margins, the provider's median ages and the dependency and
    sex ratios -- for the place and, in one answer, each level it sits in
    (a city's county, state and nation; a county's state and nation), next
    level up first, so a comparison toggle needs no further request. Every
    place is read at the place's own release; a level without it is
    ``not_published``, never another release."""
    compare = [
        code.strip() for code in (compare_geo_ids or "").split(",") if code.strip()
    ]
    return _answer(lambda: age_structure(db, geo_id, dataset, release, compare))


@router.get(
    "/history",
    response_model=AgeStructureHistoryResponse,
    name="get_age_structure_history",
    summary="A place's age structure across releases",
    responses=NOT_FOUND,
)
def get_age_structure_history(
    geo_id: str = Query(..., min_length=1, max_length=60),
    dataset: str = _DATASET,
    periods: str = Query(
        "non_overlapping",
        max_length=20,
        description="`non_overlapping` (default: releases sharing no survey year) or `all`.",
    ),
    include_bands: bool = Query(False),
    db: Session = Depends(get_db_session_dep),
) -> AgeStructureHistoryResponse:
    """Each release's broad age groups, ratios and medians, newest first, read
    as released."""
    return _answer(
        lambda: age_structure_history(db, geo_id, dataset, periods, include_bands)
    )


@router.get(
    "/cohorts",
    response_model=AgeCohortsResponse,
    name="get_age_structure_cohorts",
    summary="Each age group five years later, one group older",
    responses=NOT_FOUND,
)
def get_age_structure_cohorts(
    geo_id: str = Query(..., min_length=1, max_length=60),
    from_release: str = Query(..., min_length=4, max_length=4),
    to_release: str = Query(..., min_length=4, max_length=4),
    dataset: str = _DATASET,
    db: Session = Depends(get_db_session_dep),
) -> AgeCohortsResponse:
    """Each five-year group in the earlier release beside the next group up in
    the later one. Refused (``comparable: false``) when the periods overlap,
    do not end five years apart, or a tract was redrawn between them."""
    return _answer(lambda: age_cohorts(db, geo_id, from_release, to_release, dataset))
