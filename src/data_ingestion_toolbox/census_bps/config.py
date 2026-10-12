"""Configuration for the Census Building Permits Survey pipeline (census-building-permits).

The Building Permits Survey publishes new privately owned housing units
authorized by building permits, by structure type, as comma-separated text
files under https://www2.census.gov/econ/bps/ (documented at
https://www.census.gov/construction/bps/ ): one county file and one state
file per month, a year-to-date file per month whose December issue is the
year, and per-region place files. No credential is needed.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "CENSUS_BPS"
BPS_BASE_URL = "https://www2.census.gov/econ/bps"

#: The first year of the current county and state file layout (`co0001c.txt`).
FIRST_YEAR = 2000
#: The first year whose place files carry FIPS place codes; earlier place
#: files name places by the Bureau's own IDs, which do not resolve by code
#: (verified 2026-10-06: `so2006a.txt` has no `FIPS Place`, `so2007a.txt` has).
FIRST_PLACE_YEAR = 2007


class BpsConfig(BaseModel):
    """Building Permits Survey ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 120.0
    min_spacing_seconds: float = 1.0
    max_attempts: int = 5
    user_agent: str = (
        "population-etl-toolbox building permits ingestion (public-data warehouse)"
    )
    #: How many calendar months back from the run date an ordinary run asks
    #: for, with the annual files of the years they fall in. A month not yet
    #: published answers 404 and is recorded empty.
    recent_months: int = 6

    @field_validator("postgres_conn_id", "user_agent")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("max_attempts", "recent_months")
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
