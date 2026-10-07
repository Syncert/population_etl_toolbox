"""Configuration for the HUD Fair Market Rent and income-limit pipeline.

HUD User publishes each fiscal year's county-level FMR and Section 8
income-limit workbooks as static downloads under
``https://www.huduser.gov/portal/datasets/``, and the same values through the
HUD User Data API. HUD User challenges automated workbook downloads, so the
scheduled pipeline reads the API, which needs ``HUD_USER_API_TOKEN``. The
token is read only when a request executes, is sent only as a bearer
header, and never reaches a captured parameter set, a fingerprint, a log
line or an exception.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

import os

from pydantic import BaseModel, field_validator

SOURCE_CODE = "HUD_FMR_IL"

#: Environment variable carrying the HUD User Data API token.
API_TOKEN_ENVIRONMENT_VARIABLE = "HUD_USER_API_TOKEN"

#: The notice HUD User's API terms require of services that use it.
HUD_API_NOTICE = "This product uses the HUD User Data API but is not endorsed or certified by HUD User."

#: How served rows credit the source. HUD User's API terms require their own
#: notice for API use; the workbooks state no licence, so the credit is the
#: citation HUD User asks for.
HUD_CREDIT = "Source: U.S. Department of Housing and Urban Development, HUD User."


class HudConfig(BaseModel):
    """HUD ingestion configuration. Construction performs no I/O."""

    hud_user_api_token: str = ""
    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 300.0
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    #: HUD User allows 60 calls a minute.
    api_min_spacing_seconds: float = 1.05
    user_agent: str = "population-etl-toolbox HUD FMR and income limits ingestion (public-data warehouse)"

    @classmethod
    def from_environment(cls, **overrides: object) -> "HudConfig":
        """Read the API token only for an executing task."""
        values: dict[str, object] = {
            "hud_user_api_token": os.environ.get(API_TOKEN_ENVIRONMENT_VARIABLE, "")
        }
        values.update(overrides)
        return cls(**values)

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

    @field_validator("min_spacing_seconds", "api_min_spacing_seconds")
    @classmethod
    def _spacing(cls, value: float) -> float:
        if value < 0:
            raise ValueError("min_spacing_seconds must not be negative")
        return value
