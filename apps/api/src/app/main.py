from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.routers import catalog, comparisons, distributions, health, observations

settings = get_settings()
configure_logging(settings.api_log_level)

app = FastAPI(title="Population ETL Analytical API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(catalog.router)
app.include_router(observations.router)
app.include_router(distributions.router)
app.include_router(comparisons.router)
