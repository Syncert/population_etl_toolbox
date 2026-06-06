from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.services.comparison_service import list_metric_comparison
from data_ingestion_toolbox.models import ComparisonResponse

router = APIRouter(prefix="/api", tags=["comparison"])


def _resolve_metric_code_a(metric_code_a: Optional[str], metric_id_a: Optional[str]) -> str:
    if metric_code_a:
        return metric_code_a
    if metric_id_a:
        return metric_id_a
    raise HTTPException(status_code=422, detail="metric_code_a or metric_id_a is required")


def _resolve_metric_code_b(metric_code_b: Optional[str], metric_id_b: Optional[str]) -> str:
    if metric_code_b:
        return metric_code_b
    if metric_id_b:
        return metric_id_b
    raise HTTPException(status_code=422, detail="metric_code_b or metric_id_b is required")


@router.get("/comparison", response_model=ComparisonResponse)
def get_metric_comparison(
    metric_code_a: Optional[str] = None,
    metric_id_a: Optional[str] = None,
    metric_code_b: Optional[str] = None,
    metric_id_b: Optional[str] = None,
    geo_level: Optional[str] = None,
    state_fips: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db_session_dep),
) -> ComparisonResponse:
    resolved_metric_code_a = _resolve_metric_code_a(metric_code_a=metric_code_a, metric_id_a=metric_id_a)
    resolved_metric_code_b = _resolve_metric_code_b(metric_code_b=metric_code_b, metric_id_b=metric_id_b)

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
