import hashlib
import json
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI

from apps.api.database import dispose_engine
from apps.api.dependencies import serving_contract_unavailable
from apps.api.freshness import PublicationEpochProvider
from apps.api.middleware import RedisResponseCacheMiddleware, SecurityHeadersMiddleware
from apps.api.ratelimit import RateLimitMiddleware
from apps.api.routers import (
    catalog,
    cdc,
    comparison,
    distribution,
    health,
    observations,
    usda_nass,
)
from apps.api.routers.source_observations import SOURCE_ROUTERS
from apps.api.services.observations_service import ServingContractUnavailable
from apps.api.telemetry import RequestTelemetryMiddleware
from apps.api.versioning import (
    API_ROOT,
    VERSIONED_ROOT,
    LegacyDeprecationMiddleware,
)
from data_ingestion_toolbox.config import Settings, get_settings

#: Every public resource, in the order it appears in the generated documentation.
#: Routers declare version-relative prefixes so the same router object can serve
#: both the versioned surface and the legacy alias without a second definition
#: that could drift from it.
#:
#: The model-status router is gone (API-006): it probed three relations no
#: manifest asset creates and reported whichever happened to exist -- naming
#: them in the response body. Modelling surfaces are a plan non-goal; when one
#: is designed, it arrives as a declared contract, not a probe.
PUBLIC_ROUTERS: tuple[APIRouter, ...] = (
    health.router,
    catalog.router,
    observations.router,
    distribution.router,
    comparison.router,
    # Per-source gold schema routers. The observation pairs are generated from
    # the serving registry; CDC and USDA NASS keep hand-written routers because
    # their source-explorer contracts are not the shared observation shape.
    *SOURCE_ROUTERS,
    cdc.router,
    usda_nass.router,
)


def contract_fingerprint(application: FastAPI) -> str:
    """A short digest of the served contract, for cache-key versioning.

    Any change to the public surface -- an operation, a bound, a schema field
    -- rotates every cache key, so a body cached under the previous contract
    can never be served for the new one. Derived from the application itself
    rather than hand-bumped, because a namespace literal only protects the
    contract when someone remembers to edit it.
    """
    document = json.dumps(application.openapi(), sort_keys=True, default=str)
    return hashlib.sha256(document.encode("utf-8")).hexdigest()[:16]


@asynccontextmanager
async def _lifespan(application: FastAPI):
    yield
    # Graceful shutdown: return pooled connections before the process exits.
    # Uvicorn drains in-flight requests first (--timeout-graceful-shutdown in
    # the deployment); the cache middleware closes its Redis client on the
    # same lifespan signal.
    dispose_engine()


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the production application with one explicit runtime configuration."""
    configured = settings or get_settings()
    application = FastAPI(
        title=configured.api_title,
        version=configured.api_version,
        description=configured.api_description,
        lifespan=_lifespan,
    )

    @application.exception_handler(ServingContractUnavailable)
    async def _handle_missing_serving_contract(_request, exc):
        return serving_contract_unavailable(exc)

    # Routers first: the cache middleware's contract fingerprint is computed
    # from the served OpenAPI document, which must be complete when hashed.
    for router in PUBLIC_ROUTERS:
        application.include_router(router, prefix=VERSIONED_ROOT)
        application.include_router(router, prefix=API_ROOT)

    application.include_router(health.probe_router)

    # Middleware executes outermost-last-added: telemetry wraps everything
    # (every response carries a request id and is logged, cached or not),
    # then the deprecation signal, then security headers -- all applied to
    # cached bodies too -- then the cache, and innermost the rate limiter, so
    # a cache hit costs no budget and the limits meter exactly the requests
    # that reach the database.
    application.add_middleware(
        RateLimitMiddleware,
        catalog_per_minute=configured.api_rate_limit_catalog_per_minute,
        analysis_per_minute=configured.api_rate_limit_analysis_per_minute,
    )
    application.add_middleware(
        RedisResponseCacheMiddleware,
        redis_url=configured.redis_url,
        ttl_seconds=configured.api_cache_ttl_seconds,
        contract_fingerprint=contract_fingerprint(application),
        epoch_provider=PublicationEpochProvider(configured.api_cache_freshness_seconds),
    )
    application.add_middleware(SecurityHeadersMiddleware)
    application.add_middleware(LegacyDeprecationMiddleware)
    application.add_middleware(RequestTelemetryMiddleware)

    return application


app = create_app()
