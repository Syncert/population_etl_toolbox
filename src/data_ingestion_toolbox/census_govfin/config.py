"""Configuration for the Census government finance pipeline (taxes-and-public-money T-5).

Three Census Bureau products describe what state and local governments
collect, receive and owe:

- the Annual Survey of State and Local Government Finances, served by the
  Census Data API under ``timeseries/govs`` (the nation and each state, for
  state governments, local governments and the two combined);
- the Quarterly Summary of State and Local Tax Revenue (QTAX), served under
  ``timeseries/eits/qtax`` (state tax collections by type of tax);
- the Census of Governments finance Individual Unit Files, one zip per
  census year (years ending in 2 and 7) at
  ``https://www2.census.gov/programs-surveys/gov-finances/``,
  one amount per government and item code, which the replay rolls up to
  county areas.

Importing this module reads nothing: the API key is read when a request
runs, so DAG parsing and offline replay need no credential. The unit files
need none.
"""

from __future__ import annotations

import os

from pydantic import BaseModel, field_validator

from data_ingestion_toolbox.utility.db_connection import warehouse_database

SOURCE_CODE = "CENSUS_GOVFIN"
CENSUS_API_BASE_URL = "https://api.census.gov/data"
CENSUS_FILES_BASE_URL = "https://www2.census.gov/programs-surveys/gov-finances"

#: The variable the ACS and SAIPE/SAHIE adapters already use.
API_KEY_ENVIRONMENT_VARIABLE = "CENSUS_API_KEY"

_TARGET_DATABASE = warehouse_database()


class GovFinConfig(BaseModel):
    """Census government finance ingestion configuration.

    The key is applied to the outgoing Census Data API request only. It is
    never placed in captured request parameters, fingerprints, logs or errors.
    """

    census_api_key: str = ""
    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 120.0
    min_spacing_seconds: float = 0.5
    max_attempts: int = 5
    user_agent: str = (
        "population-etl-toolbox Census government finance ingestion "
        "(public-data warehouse)"
    )

    @classmethod
    def from_environment(cls, **overrides: object) -> "GovFinConfig":
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

    @field_validator("postgres_conn_id", "user_agent")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("max_attempts")
    @classmethod
    def _attempts(cls, value: int) -> int:
        if value < 1:
            raise ValueError("max_attempts must be at least 1")
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


def target_database() -> str:
    return _TARGET_DATABASE
