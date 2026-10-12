"""Configuration for the IRS county migration pipeline (irs-county-migration).

The IRS Statistics of Income (SOI) Division publishes county-to-county
migration as two CSVs per pair of filing years, one of inflows and one of
outflows, at ``https://www.irs.gov/pub/irs-soi/county<flow><YY><YY>.csv``.
They need no credential.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "IRS_MIGRATION"
IRS_SOI_BASE_URL = "https://www.irs.gov/pub/irs-soi"


class IrsMigrationConfig(BaseModel):
    """SOI migration ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 120.0
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    user_agent: str = (
        "population-etl-toolbox IRS SOI migration ingestion (public-data warehouse)"
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
