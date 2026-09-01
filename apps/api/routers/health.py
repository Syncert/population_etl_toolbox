from fastapi import APIRouter

from apps.api.schemas import HealthResponse

#: Mounted under every API prefix, so ``/api/v1/health`` and the legacy
#: ``/api/health`` both answer. The bare ``/health`` probe is registered
#: separately by the application factory: it is deployment infrastructure rather
#: than a versioned resource. See ``apps/api/versioning.py``.
router = APIRouter(tags=["health"])

probe_router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    return HealthResponse(status="ok", service="data-ingestion-toolbox-api")


@probe_router.get("/health", response_model=HealthResponse)
def health_probe() -> HealthResponse:
    """Answer the container and load-balancer probe on the unprefixed path."""
    return HealthResponse(status="ok", service="data-ingestion-toolbox-api")
