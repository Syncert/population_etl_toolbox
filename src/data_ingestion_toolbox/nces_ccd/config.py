"""Configuration for the NCES Common Core of Data school pipeline.

NCES publishes the CCD school-universe files at
``https://nces.ed.gov/ccd/Data/zip/`` and the EDGE school geocodes at
``https://nces.ed.gov/programs/edge/data/``. Both are static public-domain
downloads with no credential, so there is none to hold.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "NCES_CCD"

#: How served rows credit NCES.
NCES_CREDIT = (
    "Source: U.S. Department of Education, National Center for Education Statistics, "
    "Common Core of Data and EDGE school geocodes."
)


class CcdConfig(BaseModel):
    """NCES CCD ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 900.0
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    user_agent: str = "population-etl-toolbox NCES Common Core of Data ingestion (public-data warehouse)"

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
