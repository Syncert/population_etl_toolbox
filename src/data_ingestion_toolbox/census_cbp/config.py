"""Configuration for the County Business Patterns pipeline (census-county-business-patterns).

The Census Bureau publishes County Business Patterns as one zip per year and
level at ``https://www2.census.gov/programs-surveys/cbp/datasets/``. The
files need no credential, unlike the CBP API, so this adapter reads them;
there is no key to hold, and nothing about a key can leak.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "CENSUS_CBP"


class CbpConfig(BaseModel):
    """County Business Patterns ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 300.0
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    user_agent: str = "population-etl-toolbox County Business Patterns ingestion (public-data warehouse)"

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
