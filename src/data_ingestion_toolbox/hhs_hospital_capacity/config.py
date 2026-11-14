"""Configuration for the HHS hospital capacity pipeline (hospital-capacity-and-locations).

HHS publishes "COVID-19 Reported Patient Impact and Hospital Capacity by
Facility" on healthdata.gov as Socrata dataset ``anag-cw7u``: one row per
hospital and collection week, 2020-03-29 to 2024-04-21, frozen since
2024-05-03, Public Domain U.S. Government. It is read through the Socrata
resource endpoint in pages; there is no credential to hold.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "HHS_HOSPITAL_CAPACITY"

DATASET_ID = "anag-cw7u"
BASE_URL = "https://healthdata.gov"
RESOURCE_PATH = f"/resource/{DATASET_ID}.json"
METADATA_PATH = f"/api/views/{DATASET_ID}.json"

#: How served rows credit HHS.
HHS_CREDIT = (
    "Source: U.S. Department of Health and Human Services, COVID-19 Reported "
    "Patient Impact and Hospital Capacity by Facility (healthdata.gov anag-cw7u)."
)


class HhsHospitalCapacityConfig(BaseModel):
    """HHS hospital capacity ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    base_url: str = BASE_URL
    page_size: int = 50_000
    timeout_seconds: float = 300.0
    min_spacing_seconds: float = 1.0
    max_attempts: int = 4
    user_agent: str = (
        "population-etl-toolbox HHS hospital capacity ingestion (public-data warehouse)"
    )

    @field_validator("postgres_conn_id", "user_agent", "base_url")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("max_attempts", "page_size")
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
