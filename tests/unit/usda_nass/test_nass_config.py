"""USDA NASS request-time configuration contracts."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from data_ingestion_toolbox.usda_nass.config import (
    API_KEY_ENVIRONMENT_VARIABLE,
    CONFIG,
    QUICK_STATS_BASE_URL,
    QUICK_STATS_MAX_RECORDS,
    NassConfig,
    target_database,
)

pytestmark = pytest.mark.unit


def test_module_defaults_validate_without_io() -> None:
    """Covers: ETL-030 — module defaults validate without I/O."""
    assert isinstance(CONFIG, NassConfig)
    assert CONFIG.postgres_conn_id == "public_data"
    assert CONFIG.usda_nass_api_key == ""
    assert CONFIG.has_api_key is False


def test_endpoint_scope_is_explicit() -> None:
    """Covers: ETL-030 — USDA NASS endpoint scope is explicit."""
    assert QUICK_STATS_BASE_URL == "https://quickstats.nass.usda.gov"
    assert QUICK_STATS_BASE_URL.startswith("https://")
    assert not QUICK_STATS_BASE_URL.endswith("/")


def test_target_database_is_the_shared_warehouse() -> None:
    """Covers: ETL-030 — USDA NASS targets the shared warehouse."""
    assert target_database() == "public_data"


def test_api_key_is_read_only_from_the_environment_at_request_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-030 — the API key is read without import-time need."""
    monkeypatch.delenv(API_KEY_ENVIRONMENT_VARIABLE, raising=False)
    assert NassConfig.from_environment().has_api_key is False

    monkeypatch.setenv(API_KEY_ENVIRONMENT_VARIABLE, "  ")
    assert NassConfig.from_environment().has_api_key is False

    monkeypatch.setenv(API_KEY_ENVIRONMENT_VARIABLE, "ABCDEF-0123456789")
    assert NassConfig.from_environment().has_api_key is True


def test_empty_warehouse_connection_id_is_rejected() -> None:
    """Covers: ETL-030 — USDA NASS rejects an empty warehouse connection ID."""
    with pytest.raises(ValidationError, match="postgres_conn_id must not be empty"):
        NassConfig(postgres_conn_id="   ")


def test_slice_record_limit_cannot_exceed_the_provider_ceiling() -> None:
    """Covers: ETL-030 — the slice limit never exceeds the provider ceiling."""
    assert NassConfig().slice_record_limit == QUICK_STATS_MAX_RECORDS
    assert NassConfig(slice_record_limit=1_000).slice_record_limit == 1_000
    with pytest.raises(ValidationError, match="provider record limit"):
        NassConfig(slice_record_limit=QUICK_STATS_MAX_RECORDS + 1)
    with pytest.raises(ValidationError, match="slice_record_limit must be at least 1"):
        NassConfig(slice_record_limit=0)


@pytest.mark.parametrize(
    "field_name", ["request_max_attempts", "silver_max_active_tis"]
)
def test_sizing_controls_reject_nonpositive_values(field_name: str) -> None:
    """Covers: ETL-030 — USDA NASS sizing controls reject nonpositive values."""
    secret = "must-not-appear"
    with pytest.raises(ValidationError) as caught:
        NassConfig(**{field_name: 0, "usda_nass_api_key": secret})
    assert "at least 1" in str(caught.value)
    assert secret not in str(caught.value)


def test_transport_timeout_must_be_positive() -> None:
    """Covers: ETL-030 — the USDA NASS transport timeout must be positive."""
    with pytest.raises(ValidationError, match="request_timeout_seconds must be"):
        NassConfig(request_timeout_seconds=0)


def test_request_spacing_cannot_be_negative() -> None:
    """Covers: ETL-030 — USDA NASS request spacing cannot be negative."""
    with pytest.raises(ValidationError, match="must not be negative"):
        NassConfig(request_min_spacing_seconds=-0.1)


def test_deterministic_tests_may_disable_request_spacing() -> None:
    """Covers: ETL-030 — deterministic USDA NASS tests may disable spacing."""
    assert NassConfig(request_min_spacing_seconds=0.0).request_min_spacing_seconds == 0


def test_reconciliation_day_stays_inside_every_month() -> None:
    """Covers: ETL-030 — the reconciliation day exists in every month."""
    assert NassConfig().full_reconciliation_day_of_month == 1
    with pytest.raises(ValidationError, match="first 28 days"):
        NassConfig(full_reconciliation_day_of_month=29)
    with pytest.raises(ValidationError, match="first 28 days"):
        NassConfig(full_reconciliation_day_of_month=0)


def test_row_count_change_threshold_cannot_be_negative() -> None:
    """Covers: ETL-030 — the drift threshold cannot be negative."""
    assert NassConfig(row_count_change_threshold=0.0).row_count_change_threshold == 0
    with pytest.raises(ValidationError, match="must not be negative"):
        NassConfig(row_count_change_threshold=-0.01)
