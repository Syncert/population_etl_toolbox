"""Configuration for the USDA NASS Quick Stats crop pipeline.

Design goals, matching the other capture-first sources:

- One source code, ``USDA_NASS``, in the shared ``raw_capture``/``control`` layer.
- A registry-driven product scope: the DAG expands to the registered products
  and their deterministic slices, never to an unbounded "all Quick Stats" call.
- Capture-first: raw responses commit before any parsing or silver load.

``USDA_NASS_API_KEY`` is required by the provider. It is read only when a
request executes, is sent only as a transport query parameter, and never
reaches a request fingerprint, a captured parameter set, a log line, or an
exception summary.
"""

from __future__ import annotations

import os

from pydantic import BaseModel, field_validator

#: Public Quick Stats host. No path is embedded here; the client owns the
#: three registered endpoint paths.
QUICK_STATS_BASE_URL = "https://quickstats.nass.usda.gov"

#: The provider refuses any single ``api_GET`` call returning more than this
#: many records, answering ``exceeds limit = 50000`` instead of data. Slices are
#: preflighted through ``get_counts`` so the pipeline never issues such a call.
QUICK_STATS_MAX_RECORDS = 50_000

#: Environment variable carrying the required Quick Stats API key.
API_KEY_ENVIRONMENT_VARIABLE = "USDA_NASS_API_KEY"

# Shared target warehouse database.
# Overridable so self-contained stacks can point at their own warehouse
# database; production deployments default to the shared "public_data".
_TARGET_DATABASE = os.environ.get("PUBLIC_DATA_DB_NAME", "public_data")


class NassConfig(BaseModel):
    """USDA NASS ingestion configuration.

    The model performs no I/O at construction. ``from_environment`` reads the
    API key so a DAG can build the configuration inside an executing task
    without the key being required to import the module.
    """

    usda_nass_api_key: str = ""

    # Airflow connection ID to Postgres.
    postgres_conn_id: str = "public_data"

    # Provider limits. ``slice_record_limit`` is the count above which a slice
    # is refused rather than requested; it can be lowered for a tighter
    # partition contract but never raised past the provider's own ceiling.
    slice_record_limit: int = QUICK_STATS_MAX_RECORDS

    # Transport / concurrency controls.
    request_timeout_seconds: float = 120.0
    # 1s spacing keeps a full-history sweep under Quick Stats' sliding-window
    # rate limit (403s begin near ~6 req/s across concurrent slice tasks).
    request_min_spacing_seconds: float = 1.0
    request_max_attempts: int = 6

    # Airflow max_active_tis_per_dag for the silver refresh tasks.
    silver_max_active_tis: int = 4

    # Reconciliation cadence: a full registered-history sweep runs when the
    # logical date's day-of-month is at or below this value, and the bounded
    # recent window runs otherwise. Both modes use the same registry.
    full_reconciliation_day_of_month: int = 1

    # Data-quality threshold: the relative change in per-slice row counts
    # allowed before the affected release is quarantined for review.
    row_count_change_threshold: float = 0.5

    @property
    def has_api_key(self) -> bool:
        return bool(self.usda_nass_api_key.strip())

    @classmethod
    def from_environment(cls, **overrides: object) -> "NassConfig":
        """Read the required API key only for an executing request/task."""
        values: dict[str, object] = {
            "usda_nass_api_key": os.environ.get(API_KEY_ENVIRONMENT_VARIABLE, "")
        }
        values.update(overrides)
        return cls(**values)

    @field_validator("postgres_conn_id")
    @classmethod
    def _validate_postgres_conn_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("postgres_conn_id must not be empty")
        return value

    @field_validator("slice_record_limit")
    @classmethod
    def _validate_slice_record_limit(cls, value: int) -> int:
        if value < 1:
            raise ValueError("slice_record_limit must be at least 1")
        if value > QUICK_STATS_MAX_RECORDS:
            raise ValueError(
                "slice_record_limit must not exceed the provider record limit"
            )
        return value

    @field_validator("request_max_attempts", "silver_max_active_tis")
    @classmethod
    def _validate_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("USDA NASS sizing values must be at least 1")
        return value

    @field_validator("request_timeout_seconds")
    @classmethod
    def _validate_timeout(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("request_timeout_seconds must be positive")
        return value

    @field_validator("request_min_spacing_seconds")
    @classmethod
    def _validate_spacing(cls, value: float) -> float:
        if value < 0:
            raise ValueError("request_min_spacing_seconds must not be negative")
        return value

    @field_validator("full_reconciliation_day_of_month")
    @classmethod
    def _validate_reconciliation_day(cls, value: int) -> int:
        if not 1 <= value <= 28:
            raise ValueError(
                "full_reconciliation_day_of_month must fall in the first 28 days"
            )
        return value

    @field_validator("row_count_change_threshold")
    @classmethod
    def _validate_threshold(cls, value: float) -> float:
        if value < 0:
            raise ValueError("row_count_change_threshold must not be negative")
        return value


CONFIG = NassConfig()


def target_database() -> str:
    """Return the shared warehouse database name for local/CLI use."""
    return _TARGET_DATABASE
