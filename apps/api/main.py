from fastapi import FastAPI

from apps.api.middleware import RedisResponseCacheMiddleware, SecurityHeadersMiddleware
from apps.api.routers import (
    bls,
    catalog,
    census,
    comparison,
    distribution,
    fred,
    health,
    models,
    observations,
    pep,
    usda_nass,
)
from data_ingestion_toolbox.config import Settings, get_settings


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

    application.include_router(health.router)
    application.include_router(catalog.router)
    application.include_router(observations.router)
    application.include_router(distribution.router)
    application.include_router(comparison.router)
    application.include_router(models.router)
    # Per-source gold schema routers
    application.include_router(bls.router)
    application.include_router(census.router)
    application.include_router(fred.router)
    application.include_router(pep.router)
    application.include_router(usda_nass.router)
    return application


app = create_app()
