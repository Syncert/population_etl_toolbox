from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.metric_aliases import resolve_metric_code
from apps.api.services.comparison_service import (
    UnknownAnalysisMetric,
    list_metric_comparison,
    preflight_metric_comparison,
)
from apps.api.services.neutral_observations_service import NeutralQueryError
from apps.api.schemas import ComparisonPreflightResponse, ComparisonResponse

router = APIRouter(tags=["comparison"])


@router.get(
    "/comparison/preflight",
    response_model=ComparisonPreflightResponse,
    name="get_comparison_preflight",
    summary="Whether two metrics can be compared, and why",
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


@router.get("/comparison", response_model=ComparisonResponse)
def get_metric_comparison(
    metric_code_a: Optional[str] = Query(None, max_length=200),
    metric_id_a: Optional[str] = Query(None, max_length=200),
    metric_code_b: Optional[str] = Query(None, max_length=200),
    metric_id_b: Optional[str] = Query(None, max_length=200),
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
    resolved_metric_code_a = resolve_metric_code(
        metric_code_a,
        metric_id_a,
        detail="metric_code_a or metric_id_a is required",
    )
    resolved_metric_code_b = resolve_metric_code(
        metric_code_b,
        metric_id_b,
        detail="metric_code_b or metric_id_b is required",
    )

    try:
        return list_metric_comparison(
            db,
            metric_code_a=resolved_metric_code_a,
            metric_code_b=resolved_metric_code_b,
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
