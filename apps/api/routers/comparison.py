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
from apps.api.query_lists import code_list
from apps.api.services.cross_level_service import (
    BATCH_MOST,
    UnknownGeography,
    compare_places,
    compare_places_batch,
)
from apps.api.services.derived_ratio_service import UnknownRatio
from apps.api.services.neutral_observations_service import (
    NeutralQueryError,
    validated_period_start,
)
from apps.api.services.comparison_matrix_service import metric_matrix
from apps.api.schemas import (
    ComparisonCorrelationResponse,
    ComparisonMatrixResponse,
    ComparisonPreflightResponse,
    ComparisonResponse,
)
from apps.api.schemas.cross_level import (
    PlacesComparisonBatchResponse,
    PlacesComparisonResponse,
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


@router.get(
    "/comparison/places",
    response_model=PlacesComparisonResponse,
    name="get_places_comparison",
    summary="One measure in two places, at any geographic level",
    responses=NOT_FOUND,
)
def get_places_comparison(
    geo_id_a: str = Query(..., min_length=1, max_length=60),
    geo_id_b: str = Query(..., min_length=1, max_length=60),
    metric_code: Optional[str] = Query(None, max_length=200),
    ratio_id: Optional[str] = Query(
        None,
        max_length=200,
        description="A per-capita or share pairing from /catalog/derived-ratios, "
        "in place of metric_code.",
    ),
    metric_code_b: Optional[str] = Query(
        None,
        max_length=200,
        description="A different code for side b; refused unless it is the same "
        "measure from the same program.",
    ),
    period_start: Optional[str] = Query(None, max_length=10),
    db: Session = Depends(get_db_session_dep),
) -> PlacesComparisonResponse:
    """Two places, possibly at different levels, on one measure (ADR-0010).

    A city beside its county, a county beside its state, a metro beside the
    nation. A median, mean, rate, share or index compares across levels; a
    count does not, and is refused with ``counts_differ_in_size`` and the
    served per-capita or share views that would answer instead (ADR-0009).
    A refusal is a verdict to explain and answers ``200`` with
    ``comparable: false``.

    Each side shows the newest period both places publish, or each its own
    newest with the note ``periods_differ``; a side with no value is "not
    published", never zero. The answer says how the places relate (one
    inside the other, overlapping, or neither), whether the two 90 percent
    intervals overlap, and computes ``difference`` only for two independent
    places in the same period. It never ranks or names a winner.
    """
    # An empty optional value is the same request as omitting it (API-124).
    try:
        return compare_places(
            db,
            geo_id_a=geo_id_a,
            geo_id_b=geo_id_b,
            metric_code=metric_code or None,
            ratio_id=ratio_id or None,
            metric_code_b=metric_code_b or None,
            period_start=validated_period_start(period_start or None),
        )
    except UnknownAnalysisMetric as exc:
        raise HTTPException(
            status_code=404, detail=f"{exc.parameter} not found"
        ) from exc
    except UnknownGeography as exc:
        raise HTTPException(
            status_code=404, detail=f"{exc.parameter} not found"
        ) from exc
    except UnknownRatio as exc:
        raise HTTPException(status_code=404, detail="ratio_id not found") from exc
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc


@router.get(
    "/comparison/places/batch",
    response_model=PlacesComparisonBatchResponse,
    name="get_places_comparison_batch",
    summary="Several measures in the same two places, in one request",
    responses=NOT_FOUND,
)
def get_places_comparison_batch(
    geo_id_a: str = Query(..., min_length=1, max_length=60),
    geo_id_b: str = Query(..., min_length=1, max_length=60),
    metric_codes: Optional[str] = Query(
        None,
        max_length=8000,
        description="Comma-separated metric codes, each answered as "
        "/comparison/places?metric_code= would answer it.",
    ),
    ratio_ids: Optional[str] = Query(
        None,
        max_length=8000,
        description="Comma-separated pairings from /catalog/derived-ratios, each "
        "answered as /comparison/places?ratio_id= would answer it.",
    ),
    follow_alternatives: bool = Query(
        True,
        description="Answer a refused total's first offered per-capita or share "
        "view as the item's `instead`.",
    ),
    db: Session = Depends(get_db_session_dep),
) -> PlacesComparisonBatchResponse:
    """The compare page's chapter in one request (ADR-0010).

    Every item is the answer ``/comparison/places`` gives for that measure
    alone, in the order asked, metric codes first. The two places, how they
    relate and the rows are read once for the batch rather than once per
    measure. An item the route would refuse carries that status and detail;
    an unknown place is a ``404`` for the whole batch.
    """
    codes = code_list(metric_codes, "metric_codes", BATCH_MOST) or []
    ratios = code_list(ratio_ids, "ratio_ids", BATCH_MOST) or []
    try:
        return compare_places_batch(
            db,
            geo_id_a=geo_id_a,
            geo_id_b=geo_id_b,
            metric_codes=codes,
            ratio_ids=ratios,
            follow_alternatives=follow_alternatives,
        )
    except UnknownGeography as exc:
        raise HTTPException(
            status_code=404, detail=f"{exc.parameter} not found"
        ) from exc
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
