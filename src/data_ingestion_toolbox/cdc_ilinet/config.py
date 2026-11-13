"""Configuration for CDC FluView ILINet (outbreak-calibration-sets).

CDC publishes ILINet, the U.S. Outpatient Influenza-like Illness
Surveillance Network, through FluView Interactive. The page's own download
service at ``gis.cdc.gov/flu2/`` answers two requests: ``GetPhase02InitApp``
(the seasons, regions and jurisdictions the page offers) and
``PostPhase02DataDownload`` (a zip holding ``ILINet.csv`` for the chosen
region type and seasons). The pipeline sends the same requests the page's
"Download Data" button sends, with its own plain client identity; there is
no key. Public Domain U.S. Government.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "CDC_ILINET"

BASE_URL = "https://gis.cdc.gov"
INIT_PATH = "/flu2/GetPhase02InitApp"
DOWNLOAD_PATH = "/flu2/PostPhase02DataDownload"
APP_VERSION = "Public"

#: How served rows credit CDC.
CDC_CREDIT = (
    "Source: Centers for Disease Control and Prevention, FluView Interactive, "
    "U.S. Outpatient Influenza-like Illness Surveillance Network (ILINet)."
)


class CdcIlinetConfig(BaseModel):
    """CDC ILINet ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    base_url: str = BASE_URL
    timeout_seconds: float = 300.0
    min_spacing_seconds: float = 1.0
    max_attempts: int = 4
    user_agent: str = (
        "population-etl-toolbox CDC ILINet ingestion (public-data warehouse)"
    )

    @field_validator("postgres_conn_id", "user_agent", "base_url")
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
