"""Configuration for the CMS long-term care facility pipeline (nursing-home-locations).

CMS publishes the Long-Term Care Facility Characteristics (CMS Form 671) each
quarter and the Skilled Nursing Facility Cost Report each year on its main
catalog, ``https://data.cms.gov/data.json``; every release names its CSV
distribution there. Both are public with no credential, so there is none to
hold.

CMS's Provider Data Catalog (``data.cms.gov/provider-data``), which holds
Provider Information with coordinates and certified beds, refused this
pipeline's client with HTTP 403 on 2026-11-12. It is not read, and no client
identity is changed to reach it.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "CMS_LTC"

CATALOG_URL = "https://data.cms.gov/data.json"

#: How served rows credit CMS.
CMS_CREDIT = (
    "Source: Centers for Medicare & Medicaid Services, Long-Term Care Facility "
    "Characteristics (CMS Form 671) and Skilled Nursing Facility Cost Report."
)


class CmsLtcConfig(BaseModel):
    """CMS long-term care ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    catalog_url: str = CATALOG_URL
    timeout_seconds: float = 300.0
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    user_agent: str = (
        "population-etl-toolbox CMS long-term care facility ingestion "
        "(public-data warehouse)"
    )

    @field_validator("postgres_conn_id", "user_agent", "catalog_url")
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
