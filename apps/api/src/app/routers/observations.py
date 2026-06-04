from fastapi import APIRouter, Query

from app.schemas.observation import LatestObservationCollectionResponse, ObservationResponse
from app.services.observation_service import get_latest_observations, get_timeseries

router = APIRouter(prefix="/api/observations", tags=["observations"])


@router.get("/latest", response_model=LatestObservationCollectionResponse)
def latest(
    metric_id: str = Query(...),
    geo_level: str = Query(...),
    period: str = Query(default="latest"),
    limit: int = Query(default=5000, ge=1, le=50000),
) -> LatestObservationCollectionResponse:
    rows = [ObservationResponse(**row) for row in get_latest_observations(metric_id=metric_id, geo_level=geo_level, period=period, limit=limit)]
    return LatestObservationCollectionResponse(metric_id=metric_id, geo_level=geo_level, period=period, count=len(rows), observations=rows)


@router.get("/timeseries", response_model=list[ObservationResponse])
def timeseries(
    metric_id: str = Query(...),
    geo_id: str = Query(...),
    limit: int = Query(default=5000, ge=1, le=50000),
) -> list[ObservationResponse]:
    return [ObservationResponse(**row) for row in get_timeseries(metric_id=metric_id, geo_id=geo_id, limit=limit)]
