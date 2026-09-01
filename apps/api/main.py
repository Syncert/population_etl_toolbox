from fastapi import APIRouter, FastAPI

from apps.api.dependencies import serving_contract_unavailable
from apps.api.middleware import RedisResponseCacheMiddleware, SecurityHeadersMiddleware
from apps.api.routers import (
    catalog,
    cdc,
    comparison,
    distribution,
    health,
    models,
    observations,
    usda_nass,
)
from apps.api.routers.source_observations import SOURCE_ROUTERS
from apps.api.services.observations_service import ServingContractUnavailable
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
PUBLIC_ROUTERS: tuple[APIRouter, ...] = (
    health.router,
    catalog.router,
    observations.router,
    distribution.router,
    comparison.router,
    models.router,
    # Per-source gold schema routers. The observation pairs are generated from
    # the serving registry; CDC and USDA NASS keep hand-written routers because
    # their source-explorer contracts are not the shared observation shape.
    *SOURCE_ROUTERS,
    cdc.router,
    usda_nass.router,
)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the production application with one explicit runtime configuration."""
    configured = settings or get_settings()
    application = FastAPI(
        title=configured.api_title,
        version=configured.api_version,
        description=configured.api_description,
    )

    application.add_middleware(
        RedisResponseCacheMiddleware,
        redis_url=configured.redis_url,
        ttl_seconds=configured.api_cache_ttl_seconds,
    )
    application.add_middleware(SecurityHeadersMiddleware)
    # Added last so it wraps the cache: the retirement signal describes the route
    # and must survive a cache hit.
    application.add_middleware(LegacyDeprecationMiddleware)

    @application.exception_handler(ServingContractUnavailable)
    async def _handle_missing_serving_contract(_request, exc):
        return serving_contract_unavailable(exc)

    for router in PUBLIC_ROUTERS:
        application.include_router(router, prefix=VERSIONED_ROOT)
        application.include_router(router, prefix=API_ROOT)

    application.include_router(health.probe_router)
    return application


app = create_app()
