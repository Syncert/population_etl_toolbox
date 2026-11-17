"""Configuration for the NOAA nClimDiv county monthly series pipeline.

NCEI publishes the county files under
``https://www.ncei.noaa.gov/pub/data/cirs/climdiv/``. They need no
credential, so there is none to hold.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "NOAA_CLIMDIV"

#: How served rows credit NCEI.
NOAA_CLIMDIV_CREDIT = (
    "Source: NOAA National Centers for Environmental Information, "
    "nClimDiv county temperature and precipitation."
)


class ClimdivConfig(BaseModel):
    """nClimDiv ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    #: Each county file is about 41 MB.
    timeout_seconds: float = 600.0
    #: The pause between two requests to NCEI.
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    user_agent: str = (
        "population-etl-toolbox NOAA nClimDiv county ingestion (public-data warehouse)"
    )

    @field_validator("postgres_conn_id", "user_agent")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("max_attempts")
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
