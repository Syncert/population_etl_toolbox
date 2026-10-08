"""Configuration for the FEMA National Risk Index and declarations pipeline.

FEMA publishes the National Risk Index county table as a keyless ArcGIS
feature layer and disaster declarations through the keyless OpenFEMA API.
Neither needs a credential, so there is none to hold.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

from .registry import DECLARATION_PAGE_SIZE, NRI_PAGE_SIZE

SOURCE_CODE = "FEMA_NRI"

#: OpenFEMA's terms ask services to say this; the NRI asks for attribution.
FEMA_NOTICE = (
    "This product uses the Federal Emergency Management Agency's OpenFEMA API, but is not endorsed by FEMA. "
    "National Risk Index data: Federal Emergency Management Agency, FEMA National Risk Index."
)


class FemaConfig(BaseModel):
    """FEMA ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 300.0
    min_spacing_seconds: float = 1.0
    max_attempts: int = 4
    user_agent: str = "population-etl-toolbox FEMA ingestion (public-data warehouse)"
    nri_page_size: int = NRI_PAGE_SIZE
    declaration_page_size: int = DECLARATION_PAGE_SIZE
    #: A run stops rather than paging forever if a service never says it is done.
    max_pages: int = 50

    @field_validator("postgres_conn_id", "user_agent")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator(
        "max_attempts", "nri_page_size", "declaration_page_size", "max_pages"
    )
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
