"""Configuration for the HUD Fair Market Rent and income-limit pipeline.

HUD User publishes each fiscal year's county-level FMR and Section 8
income-limit workbooks as static downloads under
``https://www.huduser.gov/portal/datasets/``. They need no credential; the
HUD User API, which does, is not used. So there is no token to hold.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "HUD_FMR_IL"

#: How served rows credit the source. HUD User's API terms require their own
#: notice for API use; the workbooks state no licence, so the credit is the
#: citation HUD User asks for.
HUD_CREDIT = "Source: U.S. Department of Housing and Urban Development, HUD User."


class HudConfig(BaseModel):
    """HUD workbook ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 300.0
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    user_agent: str = "population-etl-toolbox HUD FMR and income limits ingestion (public-data warehouse)"

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
