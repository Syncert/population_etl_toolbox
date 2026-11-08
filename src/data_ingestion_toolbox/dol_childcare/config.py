"""Configuration for the NDCP county childcare prices pipeline (county-childcare-prices).

The U.S. Department of Labor Women's Bureau publishes the National Database
of Childcare Prices (NDCP) as an Excel workbook under
``https://www.dol.gov/sites/dolgov/files/WB/``. It needs no credential, so
there is none to hold. The pages state no licence; it is a federal work and
is cited as the Women's Bureau asks.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "DOL_CHILDCARE"
DOL_BASE_URL = "https://www.dol.gov/sites/dolgov/files/WB"

#: The citation the Women's Bureau's own pages use for the database.
NDCP_CREDIT = (
    "Source: U.S. Department of Labor, Women's Bureau, "
    "National Database of Childcare Prices."
)


class ChildcareConfig(BaseModel):
    """NDCP ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    #: The workbook is about 92 MB.
    timeout_seconds: float = 600.0
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    user_agent: str = (
        "population-etl-toolbox DOL childcare prices ingestion (public-data warehouse)"
    )

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
