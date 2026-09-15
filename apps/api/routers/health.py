from typing import Union

from fastapi import APIRouter, Depends, Response
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.registry import OBSERVATION_DISPATCH
from apps.api.schemas import ContentHealthResponse, HealthResponse, ReadinessResponse
from apps.api.schemas.errors import ErrorDetail
from apps.api.services.content_health import grade_content, read_source_content
from data_ingestion_toolbox.config import get_settings

#: Mounted under the versioned prefix, so ``/api/v1/health`` answers as an
#: ordinary versioned resource. The bare ``/health`` and ``/health/ready``
#: probes are registered separately by the application factory: they are
#: deployment infrastructure rather than versioned resources, and they sit
#: outside the version policy. See ``apps/api/versioning.py``.
router = APIRouter(tags=["health"])

probe_router = APIRouter(tags=["health"])

#: The content report, deliberately a router of its own rather than another
#: route on ``router`` above. ``apps/api/ratelimit.py`` derives its exempt
#: paths from *every* route the health routers serve -- the web application
#: calls health on each page load, and a tight budget made the health check
#: the thing that failed first (API-101). This resource reads the warehouse,
#: so inheriting that exemption would publish an unauthenticated, unmetered
#: grouped scan. It is a warehouse read, and it is metered and declared as
#: one.
content_router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    return HealthResponse(status="ok", service="data-ingestion-toolbox-api")


@probe_router.get("/health", response_model=HealthResponse)
def health_probe() -> HealthResponse:
    """Answer the container and load-balancer probe on the unprefixed path."""
    return HealthResponse(status="ok", service="data-ingestion-toolbox-api")


@probe_router.get(
    "/health/ready",
    response_model=ReadinessResponse,
    # The only route whose 503 has two bodies, and both are declared: an
    # unready answer is this resource's own report (`status: "unready"` with
    # the database and cache states), while a session the dependency cannot
    # open at all is the sanitized `detail` sentence every other route's 503
    # carries. A client that typed one shape would mis-read the other
    # (API-121).
    responses={
        503: {
            "model": Union[ReadinessResponse, ErrorDetail],
            "description": (
                "Not ready to serve. The readiness report when the database "
                "check failed, or the sanitized refusal when no session could "
                "be opened."
            ),
        }
    },
)
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


@content_router.get("/health/content", response_model=ContentHealthResponse)
def content_health(
    db: Session = Depends(get_db_session_dep),
) -> ContentHealthResponse:
    """What this deployment can actually serve, per source.

    Readiness answers whether the process can reach its database. This
    answers the question a blank dashboard asks: of the sources this API
    declares observation routes for, which ones publish a measure a client
    could ask for right now.

    Always ``200`` while the warehouse is reachable, ``empty`` and
    ``degraded`` included. The states are the report, not a refusal: a caller
    cannot read a body the resource declined to send, and content is not a
    reason to take the process out of the load balancer -- see the module
    docstring in ``apps/api/services/content_health.py``. An unreachable
    warehouse is the one case this cannot report on, and it answers the same
    sanitized 503 every other warehouse read does.
    """
    try:
        rows = read_source_content(db)
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
    return ContentHealthResponse(**grade_content(OBSERVATION_DISPATCH, rows))
