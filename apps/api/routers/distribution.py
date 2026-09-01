from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.metric_aliases import resolve_metric_code
from apps.api.services.comparison_service import UnknownAnalysisMetric
from apps.api.services.distribution_service import list_distribution_bins
from apps.api.services.neutral_observations_service import NeutralQueryError
from apps.api.schemas import DistributionBinsResponse

router = APIRouter(prefix="/distribution", tags=["distribution"])


@router.get("/bins", response_model=DistributionBinsResponse)
def get_distribution_bins(
    metric_code: Optional[str] = Query(None, max_length=200),
    metric_id: Optional[str] = Query(None, max_length=200),
    geo_level: Optional[str] = Query(None, max_length=50),
    state_fips: Optional[str] = Query(None, max_length=2),
    bin_count: int = Query(7, ge=1, le=20),
    db: Session = Depends(get_db_session_dep),
) -> DistributionBinsResponse:
    """API-derived equal-width bins over one metric's latest values.

    Dispatched to the metric's owning source; a stratified source is declined
    with its declared restriction rather than silently collapsed.
    """
    resolved_metric_code = resolve_metric_code(
        metric_code=metric_code, metric_id=metric_id
    )

    try:
        return list_distribution_bins(
            db,
            metric_code=resolved_metric_code,
            geo_level=geo_level,
            state_fips=state_fips,
            bin_count=bin_count,
        )
    except UnknownAnalysisMetric as exc:
        raise HTTPException(
            status_code=404, detail=f"{exc.parameter} not found"
        ) from exc
    except NeutralQueryError as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
