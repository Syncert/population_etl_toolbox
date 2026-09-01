from fastapi import APIRouter, Depends, Response
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.schemas import HealthResponse, ReadinessResponse
from data_ingestion_toolbox.config import get_settings

#: Mounted under the versioned prefix, so ``/api/v1/health`` answers as an
#: ordinary versioned resource. The bare ``/health`` and ``/health/ready``
#: probes are registered separately by the application factory: they are
#: deployment infrastructure rather than versioned resources, and they sit
#: outside the version policy. See ``apps/api/versioning.py``.
router = APIRouter(tags=["health"])

probe_router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    return HealthResponse(status="ok", service="data-ingestion-toolbox-api")


@probe_router.get("/health", response_model=HealthResponse)
def health_probe() -> HealthResponse:
    """Answer the container and load-balancer probe on the unprefixed path."""
    return HealthResponse(status="ok", service="data-ingestion-toolbox-api")


@probe_router.get("/health/ready", response_model=ReadinessResponse)
def readiness_probe(
    response: Response,
    db: Session = Depends(get_db_session_dep),
) -> ReadinessResponse:
    """Readiness: the process can serve, not merely that it is running.

    The database is required — an unready answer is a 503 so orchestration
    stops routing traffic here. Redis is reported but never gates readiness:
    the cache is an optimization the API is proven to survive without, and
    failing readiness on it would turn a cache outage into an API outage.
    """
    try:
        db.execute(text("SELECT 1"))
        database_state = "ok"
    except SQLAlchemyError:
        database_state = "unavailable"
    cache_state = "configured" if get_settings().redis_url else "disabled"
    ready = database_state == "ok"
    if not ready:
        response.status_code = 503
    return ReadinessResponse(
        status="ready" if ready else "unready",
        database=database_state,
        cache=cache_state,
    )
