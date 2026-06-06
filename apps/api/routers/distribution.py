from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.services.distribution_service import list_distribution_bins
from data_ingestion_toolbox.models import DistributionBinsResponse

router = APIRouter(prefix="/api/distribution", tags=["distribution"])


def _resolve_metric_code(metric_code: Optional[str], metric_id: Optional[str]) -> str:
    if metric_code:
        return metric_code
    if metric_id:
        return metric_id
    raise HTTPException(status_code=422, detail="metric_code or metric_id is required")


@router.get("/bins", response_model=DistributionBinsResponse)
def get_distribution_bins(
    metric_code: Optional[str] = None,
    metric_id: Optional[str] = None,
    geo_level: Optional[str] = None,
    state_fips: Optional[str] = None,
    bin_count: int = Query(7, ge=1, le=20),
    db: Session = Depends(get_db_session_dep),
) -> DistributionBinsResponse:
    resolved_metric_code = _resolve_metric_code(metric_code=metric_code, metric_id=metric_id)

    try:
        return list_distribution_bins(
            db,
            metric_code=resolved_metric_code,
            geo_level=geo_level,
            state_fips=state_fips,
            bin_count=bin_count,
        )
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
