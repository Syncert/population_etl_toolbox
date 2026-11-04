"""Configuration for the elections pipeline.

Every publisher this source reads is keyless: the congress-legislators files
on GitHub, Wikidata's query service and Harvard Dataverse's public API.
There is no credential to hold.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

SOURCE_CODE = "ELECTIONS"


class ElectionsConfig(BaseModel):
    """Elections ingestion configuration; there is no credential."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 300.0
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    #: Wikidata asks automated clients to name themselves.
    user_agent: str = (
        "population-etl-toolbox elections ingestion (public-data warehouse; "
        "https://github.com/Syncert/population_etl_toolbox)"
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
