"""Configuration for the FBI Uniform Crime Reporting (UCR) pipeline."""

from __future__ import annotations

import os

from pydantic import BaseModel, field_validator

# Official Crime Data Explorer API surface, frozen by FBI-001.
#
# The CDE API serves the newest published data directly at the server root;
# the former ``/LATEST`` alias segment was removed upstream (requests including
# it now return 404). The data remains a mutable alias for the newest release,
# not a warehouse release identity, so every capture retains retrieval time,
# checksum, request fingerprint, and the provider freshness fields so a release
# can be identified after the published data moves.
CDE_SERVER_URL = "https://api.usa.gov/crime/fbi/cde"
CDE_BASE_PATH = ""
CDE_BASE_URL = f"{CDE_SERVER_URL}{CDE_BASE_PATH}"

#: Documented query-parameter name for the api.data.gov key the CDE API
#: requires. Kept for redaction assertions: the value must never appear in
#: request parameters, fingerprints, captures, logs, or error summaries.
API_KEY_PARAMETER = "API_KEY"

#: api.data.gov header used to transmit the key. Header auth keeps the secret
#: out of URLs, so transport logs (httpx request lines, proxies) never see it.
API_KEY_HEADER = "X-Api-Key"

#: Environment secret holding the api.data.gov key for the CDE API.
API_KEY_ENVIRONMENT_VARIABLE = "FBI_CDE_API_KEY"

# Shared target warehouse database.
# Overridable so self-contained stacks can point at their own warehouse
# database; production deployments default to the shared "public_data".
_TARGET_DATABASE = os.environ.get("PUBLIC_DATA_DB_NAME", "public_data")


class FbiUcrConfig(BaseModel):
    """FBI UCR ingestion configuration.

    Design goals (matching the other capture-first sources):

    - One source code: ``FBI_UCR`` in the shared ``raw_capture``/``control``
      layer.
    - A registry-driven product scope: the DAG expands to the configured
      summarized-offense products.
    - Capture-first: raw responses commit before any parsing or silver load.

    ``FBI_CDE_API_KEY`` is required by the provider. It is read only when a
    task executes, applied only to the outgoing request, and validated at
    request execution rather than module or DAG import.
    """

    cde_api_key: str = ""

    # Airflow connection ID to Postgres.
    postgres_conn_id: str = "public_data"

    # Transport / concurrency controls. api.data.gov applies per-key hourly
    # rate limits, so requests are spaced and retried with bounded backoff.
    request_timeout_seconds: float = 60.0
    min_spacing_seconds: float = 0.25
    max_attempts: int = 5

    # Airflow max_active_tis_per_dag for the silver refresh tasks.
    silver_max_active_tis: int = 4

    @property
    def has_api_key(self) -> bool:
        return bool(self.cde_api_key.strip())

    @classmethod
    def from_environment(cls, **overrides: object) -> "FbiUcrConfig":
        """Read the provider key only for an executing request/task."""
        values: dict[str, object] = {
            "cde_api_key": os.environ.get(API_KEY_ENVIRONMENT_VARIABLE, "")
        }
        values.update(overrides)
        return cls(**values)

    @field_validator("postgres_conn_id")
    @classmethod
    def _validate_postgres_conn_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("postgres_conn_id must not be empty")
        return value

    @field_validator("max_attempts", "silver_max_active_tis")
    @classmethod
    def _validate_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("FBI UCR sizing values must be at least 1")
        return value

    @field_validator("request_timeout_seconds")
    @classmethod
    def _validate_timeout(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("request_timeout_seconds must be positive")
        return value

    @field_validator("min_spacing_seconds")
    @classmethod
    def _validate_spacing(cls, value: float) -> float:
        if value < 0:
            raise ValueError("min_spacing_seconds must not be negative")
        return value


CONFIG = FbiUcrConfig()


def target_database() -> str:
    """Return the shared warehouse database name for local/CLI use."""
    return _TARGET_DATABASE
