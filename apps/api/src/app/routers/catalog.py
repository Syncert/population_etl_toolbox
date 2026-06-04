from fastapi import APIRouter, Query

from app.schemas.catalog import GeographyResponse, MetricResponse, SourceResponse
from app.services import catalog_service

router = APIRouter(prefix="/api/catalog", tags=["catalog"])


@router.get("/sources", response_model=list[SourceResponse])
def sources() -> list[SourceResponse]:
    return [SourceResponse(**row) for row in catalog_service.get_sources()]


@router.get("/metrics", response_model=list[MetricResponse])
def metrics(limit: int = Query(default=500, ge=1, le=10000)) -> list[MetricResponse]:
    return [MetricResponse(**row) for row in catalog_service.get_metrics(limit=limit)]


@router.get("/geographies", response_model=list[GeographyResponse])
def geographies(
    geo_level: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=10000),
) -> list[GeographyResponse]:
    return [GeographyResponse(**row) for row in catalog_service.get_geographies(limit=limit, geo_level=geo_level)]
