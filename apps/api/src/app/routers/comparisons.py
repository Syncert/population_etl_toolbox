from fastapi import APIRouter, Query

from app.schemas.comparison import ComparisonRecord, ComparisonResponse
from app.services.comparison_service import get_comparison

router = APIRouter(prefix="/api", tags=["comparison"])


@router.get("/comparison", response_model=ComparisonResponse)
def comparison(
    metric_a: str = Query(...),
    metric_b: str = Query(...),
    geo_level: str = Query(...),
    period: str = Query(default="latest"),
    limit: int = Query(default=5000, ge=1, le=50000),
) -> ComparisonResponse:
    rows = [ComparisonRecord(**row) for row in get_comparison(metric_a=metric_a, metric_b=metric_b, geo_level=geo_level, period=period, limit=limit)]
    return ComparisonResponse(metric_a=metric_a, metric_b=metric_b, geo_level=geo_level, period=period, records=rows)
