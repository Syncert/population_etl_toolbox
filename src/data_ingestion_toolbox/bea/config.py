"""Configuration for the BEA regional accounts pipeline (bea-regional-accounts).

The Bureau of Economic Analysis publishes its regional tables -- county
personal income, earnings by industry, and county gross domestic product --
as one zip per table at ``https://apps.bea.gov/regional/zip/<TABLE>.zip``,
each holding an every-area CSV whose footer states the release date. The
bulk files need no credential, unlike the BEA API, so this adapter reads
them; there is no key to hold, and nothing about a key can leak.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "BEA"
BEA_BULK_BASE_URL = "https://apps.bea.gov/regional/zip"


class BeaConfig(BaseModel):
    """BEA regional ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 300.0
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    user_agent: str = (
        "population-etl-toolbox BEA regional ingestion (public-data warehouse)"
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
