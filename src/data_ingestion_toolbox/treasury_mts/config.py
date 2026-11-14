"""Configuration for the Treasury Monthly Treasury Statement pipeline.

Treasury's Fiscal Data API (``api.fiscaldata.treasury.gov``) needs no
credential, so there is none to hold.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "TREASURY_MTS"

#: How served rows credit Treasury.
TREASURY_MTS_CREDIT = (
    "Source: U.S. Department of the Treasury, Bureau of the Fiscal Service, "
    "Monthly Treasury Statement, Table 9."
)


class TreasuryMtsConfig(BaseModel):
    """Monthly Treasury Statement ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 120.0
    #: The pause between two requests to the Fiscal Data API.
    min_spacing_seconds: float = 1.0
    max_attempts: int = 4
    #: Rows per page; the API allows up to 10,000 and the whole table since
    #: March 2015 is about 4,600 rows.
    page_size: int = 10_000
    user_agent: str = (
        "population-etl-toolbox Treasury MTS ingestion (public-data warehouse)"
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

    @field_validator("page_size")
    @classmethod
    def _page_size(cls, value: int) -> int:
        if not 1 <= value <= 10_000:
            raise ValueError("page_size must be between 1 and 10,000")
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
