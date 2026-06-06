from fastapi import APIRouter

from data_ingestion_toolbox.models import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/api/health", response_model=HealthResponse)
@router.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    return HealthResponse(status="ok", service="data-ingestion-toolbox-api")
