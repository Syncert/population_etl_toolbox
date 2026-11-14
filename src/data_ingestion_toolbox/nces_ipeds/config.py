"""Configuration for the NCES IPEDS pipeline (college-housing-capacity).

NCES publishes each IPEDS survey component as a complete data file: one zip
per component and collection year at
``https://nces.ed.gov/ipeds/datacenter/data/<FILE>.zip``. The files are
public domain with no credential, so there is none to hold. The Data Center's
file list answers a plain client with a redirect to a session page; it is not
read, and the registered file names are requested directly.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "NCES_IPEDS"

DATA_BASE_URL = "https://nces.ed.gov/ipeds/datacenter/data/"

#: How served rows credit NCES.
IPEDS_CREDIT = (
    "Source: U.S. Department of Education, National Center for Education "
    "Statistics, Integrated Postsecondary Education Data System (IPEDS)."
)


class NcesIpedsConfig(BaseModel):
    """IPEDS ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    data_base_url: str = DATA_BASE_URL
    timeout_seconds: float = 300.0
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    user_agent: str = (
        "population-etl-toolbox NCES IPEDS ingestion (public-data warehouse)"
    )

    @field_validator("postgres_conn_id", "user_agent", "data_base_url")
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
