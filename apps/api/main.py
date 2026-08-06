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
)
from data_ingestion_toolbox.config import get_settings

settings = get_settings()

app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description=settings.api_description,
)

app.add_middleware(
    RedisResponseCacheMiddleware,
    redis_url=settings.redis_url,
    ttl_seconds=settings.api_cache_ttl_seconds,
)
app.add_middleware(SecurityHeadersMiddleware)

app.include_router(health.router)
app.include_router(catalog.router)
app.include_router(observations.router)
app.include_router(distribution.router)
app.include_router(comparison.router)
app.include_router(models.router)
# Per-source gold schema routers
app.include_router(bls.router)
app.include_router(census.router)
app.include_router(fred.router)
