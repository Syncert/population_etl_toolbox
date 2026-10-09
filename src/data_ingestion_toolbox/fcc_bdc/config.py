"""Configuration for the FCC Broadband Data Collection availability pipeline.

The FCC's National Broadband Map publishes its availability summary files
through the public data API at ``https://broadbandmap.fcc.gov/api/public/map``.
Every call needs an FCC account's username and an API token, sent as the
``username`` and ``hash_value`` headers: ``FCC_BDC_USERNAME`` and
``FCC_BDC_API_TOKEN``. Both are read only when a request executes, are sent
only as headers, and never reach a captured parameter set, a fingerprint, a
log line or an exception.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

import os

from pydantic import BaseModel, field_validator

SOURCE_CODE = "FCC_BDC"

USERNAME_ENVIRONMENT_VARIABLE = "FCC_BDC_USERNAME"
API_TOKEN_ENVIRONMENT_VARIABLE = "FCC_BDC_API_TOKEN"

#: How served rows credit the FCC.
FCC_CREDIT = "Source: Federal Communications Commission, National Broadband Map (Broadband Data Collection)."


class BdcConfig(BaseModel):
    """FCC BDC ingestion configuration. Construction performs no I/O."""

    fcc_bdc_username: str = ""
    fcc_bdc_api_token: str = ""
    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 600.0
    #: The API allows 10 calls a minute.
    min_spacing_seconds: float = 6.5
    max_attempts: int = 4
    user_agent: str = "population-etl-toolbox FCC Broadband Data Collection ingestion (public-data warehouse)"

    @classmethod
    def from_environment(cls, **overrides: object) -> "BdcConfig":
        """Read the credentials only for an executing task."""
        values: dict[str, object] = {
            "fcc_bdc_username": os.environ.get(USERNAME_ENVIRONMENT_VARIABLE, ""),
            "fcc_bdc_api_token": os.environ.get(API_TOKEN_ENVIRONMENT_VARIABLE, ""),
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

    @field_validator("min_spacing_seconds")
    @classmethod
    def _spacing(cls, value: float) -> float:
        if value < 0:
            raise ValueError("min_spacing_seconds must not be negative")
        return value
