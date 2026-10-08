"""Configuration for the EIA retail gasoline pipeline (grocery-and-gasoline-prices).

The U.S. Energy Information Administration publishes weekly retail gasoline
prices from its EIA-878 survey through API v2, route
``petroleum/pri/gnd``, for the nation, the Petroleum Administration for
Defense Districts and their sub-districts, nine states and ten cities. The
API is free and needs a key, ``EIA_API_KEY``, read from the environment
only: it travels as a query parameter and is never recorded in an endpoint,
a request's parameters, a log line or an error.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

import os
from datetime import date

from pydantic import BaseModel, Field, field_validator

SOURCE_CODE = "EIA"
EIA_API_BASE_URL = "https://api.eia.gov/v2"
GASOLINE_ROUTE = "petroleum/pri/gnd"
API_KEY_ENVIRONMENT_VARIABLE = "EIA_API_KEY"
#: EIA's citation for reused data (https://www.eia.gov/about/copyrights_reuse.php).
CITATION = (
    "Source: U.S. Energy Information Administration, Gasoline and Diesel Fuel "
    "Update (EIA-878), weekly retail gasoline prices"
)


class EiaConfig(BaseModel):
    """EIA ingestion configuration; the key is read from the environment."""

    #: Kept out of the model's repr, so a logged configuration names no key.
    eia_api_key: str = Field(default="", repr=False)
    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 120.0
    #: EIA allows roughly 9,000 requests an hour; one a second is far inside it.
    min_spacing_seconds: float = 1.0
    max_attempts: int = 4
    #: The most rows API v2 returns in one JSON answer.
    page_length: int = 5000
    #: The first week a full history read asks for.
    history_start: date = date(2015, 1, 5)
    #: How many weeks a routine read goes back, so a revised week is read again.
    refresh_weeks: int = 8
    user_agent: str = (
        "population-etl-toolbox EIA retail gasoline ingestion (public-data warehouse)"
    )

    @classmethod
    def from_environment(cls) -> "EiaConfig":
        return cls(eia_api_key=os.environ.get(API_KEY_ENVIRONMENT_VARIABLE, ""))

    @field_validator("postgres_conn_id", "user_agent")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("max_attempts", "page_length", "refresh_weeks")
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

    @field_validator("min_spacing_seconds")
    @classmethod
    def _spacing(cls, value: float) -> float:
        if value < 0:
            raise ValueError("min_spacing_seconds must not be negative")
        return value
