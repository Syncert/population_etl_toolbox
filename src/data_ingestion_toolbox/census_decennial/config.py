"""Configuration for the 2020 Census counts pipeline (group-quarters-by-type).

The 2020 Census redistricting data (Public Law 94-171) are served by the
Census Data API at ``https://api.census.gov/data/2020/dec/pl``, the host the
ACS and SAIPE/SAHIE adapters already call with the same key.

Importing this module reads nothing: the key is read when a request runs, so
DAG parsing and offline replay need no credential.
"""

from __future__ import annotations

import os

from pydantic import BaseModel, field_validator

SOURCE_CODE = "CENSUS_DECENNIAL"
CENSUS_API_BASE_URL = "https://api.census.gov/data"

#: The environment variable the other Census adapters already use.
API_KEY_ENVIRONMENT_VARIABLE = "CENSUS_API_KEY"


class DecennialConfig(BaseModel):
    """2020 Census ingestion configuration.

    The key is applied to the outgoing HTTP request only. It is never placed
    in captured request parameters, fingerprints, logs or errors.
    """

    census_api_key: str = ""
    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 120.0
    min_spacing_seconds: float = 0.25
    max_attempts: int = 6

    @classmethod
    def from_environment(cls, **overrides: object) -> DecennialConfig:
        values: dict[str, object] = {
            "census_api_key": os.environ.get(API_KEY_ENVIRONMENT_VARIABLE, "")
        }
        values.update(overrides)
        return cls(**values)

    def require_api_key(self) -> str:
        key = self.census_api_key.strip()
        if not key:
            raise ValueError(
                f"{API_KEY_ENVIRONMENT_VARIABLE} is required for Census Data API requests"
            )
        return key

    @field_validator("postgres_conn_id")
    @classmethod
    def _validate_conn_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("postgres_conn_id must not be empty")
        return value

    @field_validator("max_attempts")
    @classmethod
    def _validate_attempts(cls, value: int) -> int:
        if value < 1:
            raise ValueError("max_attempts must be at least 1")
        return value

    @field_validator("timeout_seconds")
    @classmethod
    def _validate_timeout(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("timeout_seconds must be positive")
        return value

    @field_validator("min_spacing_seconds")
    @classmethod
    def _validate_spacing(cls, value: float) -> float:
        if value < 0:
            raise ValueError("min_spacing_seconds must not be negative")
        return value
