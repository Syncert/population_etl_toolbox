"""Configuration for the Census SAIPE and SAHIE pipeline (census-saipe-sahie).

The Small Area Income and Poverty Estimates and the Small Area Health
Insurance Estimates are model-based annual figures the Census Bureau serves
from the same Data API host as the ACS, under their own timeseries datasets:
https://www.census.gov/data/developers/data-sets/Poverty-Statistics.html and
https://www.census.gov/data/developers/data-sets/Health-Insurance-Statistics.html

Importing this module reads nothing: the key is read when a request runs, so
DAG parsing and offline replay need no credential.
"""

from __future__ import annotations

import os

from pydantic import BaseModel, field_validator

from data_ingestion_toolbox.utility.db_connection import warehouse_database

SOURCE_CODE = "CENSUS_SAIPE_SAHIE"
CENSUS_API_BASE_URL = "https://api.census.gov/data"

#: The environment variable the ACS adapter already uses; the Census Data API
#: has required a key since May 2026.
API_KEY_ENVIRONMENT_VARIABLE = "CENSUS_API_KEY"

_TARGET_DATABASE = warehouse_database()


class SaeConfig(BaseModel):
    """Census small-area estimates ingestion configuration.

    The key is applied to the outgoing HTTP request only. It is never placed
    in captured request parameters, fingerprints, logs or errors.
    """

    census_api_key: str = ""
    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 60.0
    min_spacing_seconds: float = 0.25
    max_attempts: int = 6

    @classmethod
    def from_environment(cls, **overrides: object) -> "SaeConfig":
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


def target_database() -> str:
    return _TARGET_DATABASE
