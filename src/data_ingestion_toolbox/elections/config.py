"""Configuration for the elections pipeline.

Every publisher this source reads is keyless: the congress-legislators files
on GitHub, Wikidata's query service and Harvard Dataverse's public API.

MEDSL's county presidential file carries a Dataverse guestbook: Dataverse
(6.10 and later) refuses the download until the requester says who they are
(name, email, institution, position). That identity is the operator's, read
from the environment at run time and sent only to Dataverse with the
download; it is never written to the repository or to a capture. An
optional Dataverse API token is honoured when the installation requires a
signed-in user for the guestbook.

Importing this module reads nothing: no network, database or secret.
"""

from __future__ import annotations

import os

from pydantic import BaseModel, Field, field_validator

SOURCE_CODE = "ELECTIONS"

#: Environment variables holding the operator's guestbook answers.
GUESTBOOK_ENVIRONMENT = {
    "guestbook_name": "ELECTIONS_GUESTBOOK_NAME",
    "guestbook_email": "ELECTIONS_GUESTBOOK_EMAIL",
    "guestbook_institution": "ELECTIONS_GUESTBOOK_INSTITUTION",
    "guestbook_position": "ELECTIONS_GUESTBOOK_POSITION",
}
DATAVERSE_TOKEN_ENVIRONMENT_VARIABLE = "DATAVERSE_API_TOKEN"


class ElectionsConfig(BaseModel):
    """Elections ingestion configuration; the guestbook identity comes from the environment."""

    postgres_conn_id: str = "public_data"
    timeout_seconds: float = 300.0
    min_spacing_seconds: float = 2.0
    max_attempts: int = 4
    #: Wikidata asks automated clients to name themselves.
    user_agent: str = (
        "population-etl-toolbox elections ingestion (public-data warehouse; "
        "https://github.com/Syncert/population_etl_toolbox)"
    )

    guestbook_name: str = Field(default="", repr=False)
    guestbook_email: str = Field(default="", repr=False)
    guestbook_institution: str = Field(default="", repr=False)
    guestbook_position: str = Field(default="", repr=False)
    dataverse_api_token: str = Field(default="", repr=False)

    @classmethod
    def from_environment(cls) -> "ElectionsConfig":
        values = {
            field: os.environ.get(variable, "").strip()
            for field, variable in GUESTBOOK_ENVIRONMENT.items()
        }
        values["dataverse_api_token"] = os.environ.get(
            DATAVERSE_TOKEN_ENVIRONMENT_VARIABLE, ""
        ).strip()
        return cls(**values)

    @property
    def guestbook_identity(self) -> dict[str, str] | None:
        """The guestbook's four standard answers, or None when unset."""
        identity = {
            "name": self.guestbook_name.strip(),
            "email": self.guestbook_email.strip(),
            "institution": self.guestbook_institution.strip(),
            "position": self.guestbook_position.strip(),
        }
        if not identity["name"] or not identity["email"]:
            return None
        return identity

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
