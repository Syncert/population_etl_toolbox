from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.metric_aliases import resolve_metric_code
from apps.api.services.comparison_service import list_metric_comparison
from apps.api.schemas import ComparisonResponse

router = APIRouter(tags=["comparison"])


@router.get("/comparison", response_model=ComparisonResponse)
def get_metric_comparison(
    metric_code_a: Optional[str] = Query(None, max_length=200),
    metric_id_a: Optional[str] = Query(None, max_length=200),
    metric_code_b: Optional[str] = Query(None, max_length=200),
    metric_id_b: Optional[str] = Query(None, max_length=200),
    geo_level: Optional[str] = Query(None, max_length=50),
    state_fips: Optional[str] = Query(None, max_length=2),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db_session_dep),
) -> ComparisonResponse:
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
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
