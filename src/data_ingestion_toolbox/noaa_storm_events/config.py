"""Configuration for the NOAA Storm Events Database pipeline.

NCEI publishes the yearly event files under
``https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/`` and NWS the
zone-county files linked from ``https://www.weather.gov/gis/ZoneCounty``.
Neither needs a credential, so there is none to hold.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "NOAA_STORM_EVENTS"

#: How served rows credit NCEI.
NOAA_STORM_EVENTS_CREDIT = (
    "Source: NOAA National Centers for Environmental Information, "
    "Storm Events Database; NWS zone-county correlation file."
)


class StormEventsConfig(BaseModel):
    """Storm Events ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    #: The largest yearly file is about 13 MB compressed.
    timeout_seconds: float = 600.0
    #: The pause between two requests to NCEI.
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    user_agent: str = (
        "population-etl-toolbox NOAA Storm Events ingestion (public-data warehouse)"
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
