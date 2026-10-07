"""Configuration for the LEHD LODES pipeline (census-lehd-lodes).

The Census Bureau publishes LODES as gzipped block-level CSVs per state at
``https://lehd.ces.census.gov/data/lodes/LODES8/``, documented for automated
download. They need no credential, so there is none to hold.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "CENSUS_LODES"


class LodesConfig(BaseModel):
    """LODES ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 300.0
    min_spacing_seconds: float = 1.0
    max_attempts: int = 4
    user_agent: str = (
        "population-etl-toolbox LEHD LODES ingestion (public-data warehouse)"
    )
    #: How many of the newest registered years an ordinary run asks for. A
    #: state-year is four files and the origin-destination file alone is
    #: tens of megabytes for a large state, so the default is the newest.
    recent_years: int = 1

    @field_validator("postgres_conn_id", "user_agent")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("max_attempts", "recent_years")
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
