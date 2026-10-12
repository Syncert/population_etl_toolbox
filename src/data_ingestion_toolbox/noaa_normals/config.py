"""Configuration for the NOAA U.S. Climate Normals 1991-2020 pipeline.

NCEI publishes the annual/seasonal normals as one archive of by-station CSVs
under ``https://www.ncei.noaa.gov/data/normals-annualseasonal/1991-2020/``.
It needs no credential, so there is none to hold.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "NOAA_NORMALS"

#: How served rows credit NCEI.
NOAA_CREDIT = "Source: NOAA National Centers for Environmental Information, U.S. Climate Normals 1991-2020."


class NormalsConfig(BaseModel):
    """NOAA climate normals ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 600.0
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    user_agent: str = (
        "population-etl-toolbox NOAA climate normals ingestion (public-data warehouse)"
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
