"""Configuration for the CDC illness and disease pipeline."""

from __future__ import annotations

import os

from pydantic import BaseModel, field_validator

# CDC Open Data (Socrata) public base URL. No credentials are required for
# baseline access; an optional app token only raises the per-page row limit.
SOCRATA_BASE_URL = "https://data.cdc.gov"

# Shared target warehouse database.
_TARGET_DATABASE = "public_data"


class CdcConfig(BaseModel):
    """CDC ingestion configuration.

    Design goals (matching the FRED and Census capture-first sources):
    - One source code: ``CDC`` in the shared ``raw_capture``/``control`` layer.
    - A registry-driven asset scope: the DAG expands to the configured assets.
    - Capture-first: raw responses commit before any parsing or silver load.

    The optional ``CDC_SOCRATA_APP_TOKEN`` only raises Socrata rate/row
    limits. It is applied to the outgoing request header only and is never
    placed in request parameters, fingerprints, captures, logs, or errors.
    """

    socrata_app_token: str = ""

    # Airflow connection ID to Postgres.
    postgres_conn_id: str = "public_data"

    # Socrata pagination. Baseline public access allows at most 1000 rows per
    # page without an app token, so 1000 is the safe universal default.
    socrata_page_size: int = 1000

    # Transport / concurrency controls.
    socrata_timeout_seconds: float = 60.0
    socrata_min_spacing_seconds: float = 0.25
    socrata_max_attempts: int = 8

    # Airflow max_active_tis_per_dag for the silver refresh tasks.
    silver_max_active_tis: int = 4

    # Data-quality thresholds: the relative change in per-release row counts
    # allowed before the affected release is quarantined for review. These
    # tolerate documented source change without silently absorbing regressions.
    row_count_change_threshold: float = 0.5

    @property
    def has_token(self) -> bool:
        return bool(self.socrata_app_token.strip())

    @classmethod
    def from_environment(cls, **overrides: object) -> "CdcConfig":
        """Read the optional token only for an executing request/task."""
        values = {"socrata_app_token": os.environ.get("CDC_SOCRATA_APP_TOKEN", "")}
        values.update(overrides)
        return cls(**values)

    @field_validator("postgres_conn_id")
    @classmethod
    def _validate_postgres_conn_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("postgres_conn_id must not be empty")
        return value

    @field_validator(
        "socrata_page_size", "socrata_max_attempts", "silver_max_active_tis"
    )
    @classmethod
    def _validate_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("CDC sizing values must be at least 1")
        return value

    @field_validator("socrata_timeout_seconds")
    @classmethod
    def _validate_timeout(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("socrata_timeout_seconds must be positive")
        return value

    @field_validator("socrata_min_spacing_seconds")
    @classmethod
    def _validate_spacing(cls, value: float) -> float:
        if value < 0:
            raise ValueError("socrata_min_spacing_seconds must not be negative")
        return value

    @field_validator("row_count_change_threshold")
    @classmethod
    def _validate_threshold(cls, value: float) -> float:
        if value < 0:
            raise ValueError("row_count_change_threshold must not be negative")
        return value


CONFIG = CdcConfig()


def target_database() -> str:
    """Return the shared warehouse database name for local/CLI use."""
    return _TARGET_DATABASE
