from fastapi import FastAPI

from apps.api.routers import catalog, comparison, distribution, health, models, observations
from data_ingestion_toolbox.config import get_settings

settings = get_settings()

app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description=settings.api_description,
)

app.include_router(health.router)
app.include_router(catalog.router)
app.include_router(observations.router)
app.include_router(distribution.router)
app.include_router(comparison.router)
app.include_router(models.router)
