"""Configuration for the CDC COVID-19 case surveillance counts (outbreak-calibration-sets).

CDC publishes "COVID-19 Case Surveillance Public Use Data with Geography" on
data.cdc.gov as Socrata dataset ``n8mc-b4w4``: one row per case record with
the state and county of residence, the case month, an age group and whether
the person was hospitalised or died, 2020-01 to 2024-07, frozen, Public
Domain U.S. Government. The pipeline never captures the 106-million-row file:
it asks Socrata for counts of CDC's records grouped by county, age group and
the hospitalisation and death fields, one request per state and month.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "CDC_COVID_CASES"

DATASET_ID = "n8mc-b4w4"
BASE_URL = "https://data.cdc.gov"
RESOURCE_PATH = f"/resource/{DATASET_ID}.json"
METADATA_PATH = f"/api/views/{DATASET_ID}.json"

#: How served rows credit CDC.
CDC_CREDIT = (
    "Source: Centers for Disease Control and Prevention, COVID-19 Case "
    "Surveillance Public Use Data with Geography (data.cdc.gov n8mc-b4w4)."
)


class CdcCovidCasesConfig(BaseModel):
    """CDC COVID-19 case counts ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    base_url: str = BASE_URL
    #: The most groups one state-month request may return; a response that
    #: fills it is refused rather than read short.
    group_limit: int = 50_000
    timeout_seconds: float = 600.0
    min_spacing_seconds: float = 1.0
    max_attempts: int = 4
    user_agent: str = "population-etl-toolbox CDC COVID-19 case counts ingestion (public-data warehouse)"

    @field_validator("postgres_conn_id", "user_agent", "base_url")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("max_attempts", "group_limit")
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
