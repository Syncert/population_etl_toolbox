"""Configuration for the BLS QCEW pipeline (bls-qcew-county-wages).

The Quarterly Census of Employment and Wages publishes establishment counts,
monthly employment and wages by industry and ownership for every county,
from unemployment-insurance records. Its open-data interface serves one CSV
per (year, quarter or annual, industry) slice at
``https://data.bls.gov/cew/data/api/<year>/<qtr|a>/industry/<code>.csv``,
documented at https://www.bls.gov/cew/additional-resources/open-data/ .
It takes no credential.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "BLS_QCEW"
QCEW_API_BASE_URL = "https://data.bls.gov/cew/data/api"

#: The open-data interface's first year; earlier years answer 404 (verified
#: 2026-10-06: 2013 is 404, 2014 is 200).
FIRST_YEAR = 2014


class QcewConfig(BaseModel):
    """QCEW ingestion configuration; there is no credential to hold."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 120.0
    min_spacing_seconds: float = 1.0
    max_attempts: int = 5
    #: The data.bls.gov front end refuses requests without a descriptive agent.
    user_agent: str = "population-etl-toolbox QCEW ingestion (public-data warehouse)"
    #: How many calendar quarters back from the run date an ordinary run asks
    #: for, with the annual averages of the years they fall in. QCEW publishes
    #: about five months after a quarter ends, so the newest one or two asked
    #: for answer 404 and are recorded empty. A history sweep is a manual run
    #: with ``{"history": true}``.
    recent_quarters: int = 6

    @field_validator("postgres_conn_id", "user_agent")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("max_attempts", "recent_quarters")
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
