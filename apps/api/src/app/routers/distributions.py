from fastapi import APIRouter, Query

from app.services.comparison_service import get_distribution

router = APIRouter(prefix="/api/distribution", tags=["distribution"])


@router.get("/bins")
def distribution_bins(
    metric_id: str = Query(...),
    geo_level: str = Query(...),
    method: str = Query(default="quantile"),
    limit: int = Query(default=100, ge=1, le=5000),
) -> dict:
    return {
        "metric_id": metric_id,
        "geo_level": geo_level,
        "method": method,
        "records": get_distribution(metric_id=metric_id, geo_level=geo_level, method=method, limit=limit),
    }
