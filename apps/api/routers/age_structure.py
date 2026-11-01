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
    AgeMapResponse,
    AgeRatesResponse,
    AgeStructureHistoryResponse,
    AgeStructureResponse,
)
from apps.api.services.age_structure_service import (
    AgeStructureRequestError,
    UnknownAgeGeography,
    age_cohorts,
    age_structure,
    age_structure_history,
    age_structure_map,
    age_structure_rates,
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


@router.get(
    "/map",
    response_model=AgeMapResponse,
    name="get_age_structure_map",
    summary="One age indicator for every place of a grain",
)
def get_age_structure_map(
    indicator: str = Query(
        ...,
        max_length=40,
        description=(
            "`under_18`, `age_18_64`, `age_65_plus`, `age_85_plus` (shares of "
            "everyone), `child_dependency_ratio`, `old_age_dependency_ratio`, "
            "`total_dependency_ratio`, `sex_ratio`, or `median_age` "
            "(`_men`, `_women`)."
        ),
    ),
    geo_level: str = Query(..., max_length=20),
    state_fips: Optional[str] = Query(None, max_length=2),
    dataset: str = _DATASET,
    release: Optional[str] = Query(
        None, max_length=10, description="`latest` (default) or a vintage year."
    ),
    db: Session = Depends(get_db_session_dep),
) -> AgeMapResponse:
    """The indicator for every state, metro or county, or every city or tract
    in one state, at one release (the newest the grain has by default), from
    the derived summary table, computed with the same arithmetic as the place
    page. A place whose value is withheld is listed with its reason, never as
    zero."""
    return _answer(
        lambda: age_structure_map(
            db, indicator, geo_level, state_fips, dataset, release
        )
    )


@router.get(
    "/rates",
    response_model=AgeRatesResponse,
    name="get_age_structure_rates",
    summary="One by-age table's rate per age group, beside every level it sits in",
    responses=NOT_FOUND,
)
def get_age_structure_rates(
    geo_id: str = Query(..., min_length=1, max_length=60),
    rate: str = Query(
        ...,
        max_length=40,
        description=(
            "`poverty_by_age` (B17001), `uninsured_by_age` (B27010), "
            "`disability_by_age` (C18108), `moved_by_age` (B07001) or "
            "`veterans_by_age` (B21001)."
        ),
    ),
    dataset: str = _DATASET,
    release: Optional[str] = Query(
        None, max_length=10, description="`latest` (default) or a vintage year."
    ),
    db: Session = Depends(get_db_session_dep),
) -> AgeRatesResponse:
    """Each age group's rate as a percent of that age group's own universe
    (people whose poverty status is determined, the civilian
    noninstitutionalized population, and so on), with its Bureau margin, for
    everyone and for each sex where the table publishes one -- for the place
    and each level it sits in, read at the place's release. A level without
    that release is ``not_published``; a withheld line leaves its group null
    with the reason, never zero."""
    return _answer(lambda: age_structure_rates(db, geo_id, rate, dataset, release))
