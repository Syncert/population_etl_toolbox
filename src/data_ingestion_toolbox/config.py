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
        self.api_title: str = os.environ.get("API_TITLE", "Population ETL Toolbox API")
        self.api_version: str = os.environ.get("API_VERSION", "0.1.0")
        self.api_description: str = os.environ.get(
            "API_DESCRIPTION",
            "REST API for Census ACS, BLS, and FRED population data.",
        )
        self.redis_url: str = os.environ.get("REDIS_URL", "")
        self.api_cache_ttl_seconds: int = int(
            os.environ.get("API_CACHE_TTL_SECONDS", "300")
        )
        #: How long a cached publication epoch may be reused before the API
        #: re-reads gold_glossary.publisher_harvest_state. This bounds cache
        #: staleness after a warehouse publication: a republication is served
        #: within this window regardless of the response TTL. 0 re-reads on
        #: every cacheable request (deterministic tests).
        self.api_cache_freshness_seconds: int = int(
            os.environ.get("API_CACHE_FRESHNESS_SECONDS", "15")
        )
        self.database_url: str = os.environ.get("DATABASE_URL", "")
        # -- API-owned database limits (API-006). These configure the API's
        # engine only; ETL connections are owned elsewhere and keep their own
        # budgets.
        self.db_pool_size: int = int(os.environ.get("API_DB_POOL_SIZE", "5"))
        self.db_max_overflow: int = int(os.environ.get("API_DB_MAX_OVERFLOW", "10"))
        #: Seconds a request waits for a pooled connection before failing fast
        #: to the sanitized 503 instead of queueing behind an exhausted pool.
        self.db_pool_timeout_seconds: int = int(
            os.environ.get("API_DB_POOL_TIMEOUT_SECONDS", "5")
        )
        self.db_connect_timeout_seconds: int = int(
            os.environ.get("API_DB_CONNECT_TIMEOUT_SECONDS", "5")
        )
        #: Server-side statement timeout: the cancellation contract for a
        #: runaway query. 0 disables.
        self.db_statement_timeout_ms: int = int(
            os.environ.get("API_DB_STATEMENT_TIMEOUT_MS", "15000")
        )
        self.db_pool_recycle_seconds: int = int(
            os.environ.get("API_DB_POOL_RECYCLE_SECONDS", "1800")
        )
        # -- Rate limiting (API-006). Requests per minute per client, split by
        # cost class; 0 disables a bucket. Off by default so local iteration
        # and deterministic suites are unthrottled; deployment configuration
        # enables it.
        self.api_rate_limit_catalog_per_minute: int = int(
            os.environ.get("API_RATE_LIMIT_CATALOG_PER_MINUTE", "0")
        )
        self.api_rate_limit_analysis_per_minute: int = int(
            os.environ.get("API_RATE_LIMIT_ANALYSIS_PER_MINUTE", "0")
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached application settings singleton."""
    return Settings()
