from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.failures import NOT_FOUND
from apps.api.services.comparison_service import (
    UnknownAnalysisMetric,
    list_metric_comparison,
    metric_correlation,
    preflight_metric_comparison,
)
from apps.api.services.neutral_observations_service import NeutralQueryError
from apps.api.services.comparison_matrix_service import metric_matrix
from apps.api.schemas import (
    ComparisonCorrelationResponse,
    ComparisonMatrixResponse,
    ComparisonPreflightResponse,
    ComparisonResponse,
)

router = APIRouter(tags=["comparison"])


@router.get(
    "/comparison/preflight",
    response_model=ComparisonPreflightResponse,
    name="get_comparison_preflight",
    summary="Whether two metrics can be compared, and why",
    responses=NOT_FOUND,
)
def get_comparison_preflight(
    metric_code_a: str = Query(..., min_length=1, max_length=200),
    metric_code_b: str = Query(..., min_length=1, max_length=200),
    db: Session = Depends(get_db_session_dep),
) -> ComparisonPreflightResponse:
    """Evaluate the declared compatibility rules for a metric pair.

    Always answers 200 for known metrics — an incompatible pair is a verdict
    to explain, not an error. The comparison route enforces exactly this
    decision.
    """
    try:
        return preflight_metric_comparison(
            db, metric_code_a=metric_code_a, metric_code_b=metric_code_b
        )
    except UnknownAnalysisMetric as exc:
        raise HTTPException(
            status_code=404, detail=f"{exc.parameter} not found"
        ) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get("/comparison", response_model=ComparisonResponse, responses=NOT_FOUND)
def get_metric_comparison(
    metric_code_a: str = Query(..., min_length=1, max_length=200),
    metric_code_b: str = Query(..., min_length=1, max_length=200),
    geo_level: Optional[str] = Query(None, max_length=50),
    state_fips: Optional[str] = Query(None, max_length=2),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0, le=100000),
    db: Session = Depends(get_db_session_dep),
) -> ComparisonResponse:
    """Aligned comparison of two compatible metrics, latest value per geography.

    An incompatible pair is rejected with the failed rules;
    ``/comparison/preflight`` explains the full evaluation.
    """
    try:
        return list_metric_comparison(
            db,
            metric_code_a=metric_code_a,
            metric_code_b=metric_code_b,
            geo_level=geo_level,
            state_fips=state_fips,
            limit=limit,
            offset=offset,
        )
    except UnknownAnalysisMetric as exc:
        raise HTTPException(
            status_code=404, detail=f"{exc.parameter} not found"
        ) from exc
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get(
    "/comparison/correlation",
    response_model=ComparisonCorrelationResponse,
    name="get_comparison_correlation",
    summary="API-derived correlation between two comparable metrics",
    responses=NOT_FOUND,
)
def get_comparison_correlation(
    metric_code_a: str = Query(..., min_length=1, max_length=200),
    metric_code_b: str = Query(..., min_length=1, max_length=200),
    geo_level: Optional[str] = Query(None, max_length=50),
    state_fips: Optional[str] = Query(None, max_length=2),
    year: Optional[int] = Query(None, ge=1000, le=9999),
    db: Session = Depends(get_db_session_dep),
) -> ComparisonCorrelationResponse:
    """Pearson and Spearman over exactly the pairs ``/comparison`` would page.

    Takes the parameters ``/comparison`` takes, minus the paging: a statistic
    over a page would describe a hundred geographies and be read as
    describing the country. ``year`` is the one addition — it reduces each
    side within that calendar year instead of to its newest published period,
    so a same-year answer is available on request, at the cost of the
    coverage the answer then reports.

    An incomparable pair is refused with its failed rules, exactly as
    ``/comparison`` refuses it; every coefficient is API-derived, and a
    coefficient the pairs cannot support is ``null`` with its reason in
    ``caveats`` rather than a ``0`` that reads as a measured absence of
    association.
    """
    try:
        return metric_correlation(
            db,
            metric_code_a=metric_code_a,
            metric_code_b=metric_code_b,
            geo_level=geo_level,
            state_fips=state_fips,
            year=year,
        )
    except UnknownAnalysisMetric as exc:
        raise HTTPException(
            status_code=404, detail=f"{exc.parameter} not found"
        ) from exc
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get(
    "/comparison/matrix",
    response_model=ComparisonMatrixResponse,
    name="get_comparison_matrix",
    summary="Two to eight measures aligned on geography, with pairwise verdicts",
    responses=NOT_FOUND,
)
def get_comparison_matrix(
    metric_codes: str = Query(..., min_length=1, max_length=1700),
    geo_level: Optional[str] = Query(None, max_length=50),
    state_fips: Optional[str] = Query(None, max_length=2),
    year: Optional[int] = Query(None, ge=1000, le=9999),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0, le=100000),
    db: Session = Depends(get_db_session_dep),
) -> ComparisonMatrixResponse:
    """Two to eight comma-separated measures, aligned on geography.

    Answers three things a pair cannot: a wide row holding every measure for
    one geography, a compatibility verdict per unordered pair, and that pair's
    API-derived coefficients where the verdict allows them.

    A pair the policy declines is a **cell** — ``comparable: false`` with its
    failed rules — and the request still answers ``200``. A measure whose
    source the analysis routes decline, an unknown code, and a request in
    which every pair is declined refuse the whole request instead: those are
    refusals about a measure, not about a combination.

    ``items`` pages the union of the geographies the measures published, in
    ``(geo_level, geo_id)`` order; the statistics are measured over the whole
    join, never over the page.
    """
    try:
        return metric_matrix(
            db,
            metric_codes=metric_codes,
            geo_level=geo_level,
            state_fips=state_fips,
            year=year,
            limit=limit,
            offset=offset,
        )
    except UnknownAnalysisMetric as exc:
        raise HTTPException(
            status_code=404, detail=f"{exc.parameter} not found"
        ) from exc
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
