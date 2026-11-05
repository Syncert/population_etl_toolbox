"""Configuration for the IRS SOI county and state income pipeline (irs-soi).

The IRS Statistics of Income (SOI) Division publishes individual income tax
statistics by county and by state as files under
``https://www.irs.gov/pub/irs-soi/``. They need no credential.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "IRS_SOI"
IRS_SOI_BASE_URL = "https://www.irs.gov/pub/irs-soi"


class IrsSoiConfig(BaseModel):
    """SOI income ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 180.0
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    user_agent: str = (
        "population-etl-toolbox IRS SOI income ingestion (public-data warehouse)"
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
