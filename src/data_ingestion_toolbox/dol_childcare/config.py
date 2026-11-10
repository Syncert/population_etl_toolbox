"""Configuration for the NDCP county childcare prices pipeline (county-childcare-prices).

The U.S. Department of Labor Women's Bureau publishes the National Database
of Childcare Prices (NDCP) as an Excel workbook under
``https://www.dol.gov/sites/dolgov/files/WB/`` and through DOL's data portal
API (``https://apiprod.dol.gov/v4/get/WB/ndcp/json``, data.gov's listing).
www.dol.gov's edge refuses the warehouse's honest client (403, 2026-11-08),
so the database is read from the API with a free registered key, held only
in the deployment's environment as ``DOL_API_KEY`` (Nick, 2026-11-09). The
pages state no licence; it is a federal work and is cited as the Women's
Bureau asks.

Importing this module reads the key from the environment only; no network
or database.
"""

from __future__ import annotations

import os

from pydantic import BaseModel, Field, field_validator

SOURCE_CODE = "DOL_CHILDCARE"
DOL_BASE_URL = "https://www.dol.gov/sites/dolgov/files/WB"
#: DOL's data portal API; a dataset is ``/get/<agency>/<endpoint>/<format>``.
DOL_API_URL = "https://apiprod.dol.gov/v4"

#: The citation the Women's Bureau's own pages use for the database.
NDCP_CREDIT = (
    "Source: U.S. Department of Labor, Women's Bureau, "
    "National Database of Childcare Prices."
)


class ChildcareConfig(BaseModel):
    """NDCP ingestion configuration; the API key comes from the environment."""

    postgres_conn_id: str = "public_data"
    #: The workbook is about 92 MB.
    timeout_seconds: float = 600.0
    #: DOL's API key: sent as the ``X-API-KEY`` query parameter it asks for,
    #: never stored, logged or named in an error.
    api_key: str = Field(
        default_factory=lambda: os.environ.get("DOL_API_KEY", ""), repr=False
    )
    #: Rows per API request: one page per study year (about 3,150
    #: counties, roughly 2.2 MB of the 31 fields asked), under the API's
    #: 10,000-row and 5 MB limit. DOL refuses by number of requests, not
    #: their size, so the fewest pages is best.
    api_page_size: int = 5000
    #: The pause between two requests. DOL's API guide states no per-key
    #: rate, but its gateway let 12 or 13 requests through about 14 seconds
    #: apart, then refused until 8 to 9 minutes after the last one it
    #: answered (2026-11-10): about 13 every ten minutes. A request every
    #: 50 seconds is 12 every ten minutes; the 15 pages take about 12.
    min_spacing_seconds: float = 50.0
    #: Attempts per request. A refusal -- a 429, or the 502, 503 or 504
    #: that came with DOL's throttling -- waits as long as its
    #: ``Retry-After`` asks (DOL sends none), or else
    #: ``refusal_wait_seconds`` doubled each time, at most
    #: ``max_refusal_wait_seconds``: asking again sooner only meets another
    #: refusal. A dropped connection or another 5xx waits
    #: ``retry_backoff_seconds``, doubled each time.
    max_attempts: int = 3
    retry_backoff_seconds: float = 15.0
    refusal_wait_seconds: float = 600.0
    max_refusal_wait_seconds: float = 900.0
    user_agent: str = (
        "population-etl-toolbox DOL childcare prices ingestion (public-data warehouse)"
    )

    @field_validator("postgres_conn_id", "user_agent")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("max_attempts", "api_page_size")
    @classmethod
    def _positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("must be at least 1")
        return value

    @field_validator("timeout_seconds")
    @classmethod
    def _timeout(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("timeout_seconds must be positive")
        return value

    @field_validator(
        "min_spacing_seconds",
        "retry_backoff_seconds",
        "refusal_wait_seconds",
        "max_refusal_wait_seconds",
    )
    @classmethod
    def _spacing(cls, value: float) -> float:
        if value < 0:
            raise ValueError("must not be negative")
        return value
