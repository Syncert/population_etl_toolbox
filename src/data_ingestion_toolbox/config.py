"""Application settings loaded from environment variables.

Uses ``functools.lru_cache`` so ``get_settings()`` returns the same object
throughout the process lifetime (mirroring the common FastAPI pattern).
"""

from __future__ import annotations

import os
from functools import lru_cache


class Settings:
    """Minimal application settings read from environment variables."""

    def __init__(self) -> None:
        self.api_title: str = os.environ.get(
            "API_TITLE", "Population ETL Toolbox API"
        )
        self.api_version: str = os.environ.get("API_VERSION", "0.1.0")
        self.api_description: str = os.environ.get(
            "API_DESCRIPTION",
            "REST API for Census ACS, BLS, and FRED population data.",
        )
        self.redis_url: str = os.environ.get("REDIS_URL", "")
        self.api_cache_ttl_seconds: int = int(
            os.environ.get("API_CACHE_TTL_SECONDS", "300")
        )
        self.database_url: str = os.environ.get("DATABASE_URL", "")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached application settings singleton."""
    return Settings()
