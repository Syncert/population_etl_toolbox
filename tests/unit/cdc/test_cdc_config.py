"""Unit tests for the CDC pipeline configuration module."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from data_ingestion_toolbox.cdc.config import (
    SOCRATA_BASE_URL,
    CONFIG,
    CdcConfig,
    target_database,
)

pytestmark = pytest.mark.unit


class TestCdcConfigDefaults:
    def test_module_config_is_a_valid_cdc_config(self) -> None:
        """Covers: ETL-030 — module defaults validate without I/O."""
        assert isinstance(CONFIG, CdcConfig)

    def test_base_url_is_the_public_cdc_portal(self) -> None:
        """Covers: ETL-030 — CDC endpoint scope is explicit."""
        assert SOCRATA_BASE_URL == "https://data.cdc.gov"

    def test_target_database_is_shared_warehouse(self) -> None:
        """Covers: ETL-030 — CDC targets the shared warehouse."""
        assert target_database() == "public_data"

    def test_defaults_are_capture_safe(self) -> None:
        """Covers: ETL-030 — CDC sizing and retry defaults are bounded."""
        config = CdcConfig(socrata_app_token="")
        assert config.socrata_page_size == 1000
        assert config.socrata_timeout_seconds == 60.0
        assert config.socrata_min_spacing_seconds == 0.25
        assert config.socrata_max_attempts == 8
        assert config.silver_max_active_tis == 4
        assert config.row_count_change_threshold == 0.5
        assert config.postgres_conn_id == "public_data"

    def test_token_defaults_from_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covers: ETL-030 — optional CDC token is read without import-time need."""
        monkeypatch.delenv("CDC_SOCRATA_APP_TOKEN", raising=False)
        assert CdcConfig().has_token is False

        monkeypatch.setenv("CDC_SOCRATA_APP_TOKEN", "CDC:from-env")
        assert CdcConfig().socrata_app_token == "CDC:from-env"
        assert CdcConfig().has_token is True


class TestCdcConfigHasToken:
    @pytest.mark.parametrize(
        "token,expected",
        [
            ("", False),
            ("   ", False),
            ("\t\n", False),
            ("CDC:real", True),
        ],
    )
    def test_has_token_ignores_blank_values(self, token: str, expected: bool) -> None:
        """Covers: ETL-030 — blank CDC token values mean anonymous access."""
        assert CdcConfig(socrata_app_token=token).has_token is expected


class TestCdcConfigValidation:
    def test_empty_postgres_conn_id_rejected(self) -> None:
        """Covers: ETL-030 — CDC rejects an empty warehouse connection ID."""
        with pytest.raises(ValidationError, match="postgres_conn_id"):
            CdcConfig(socrata_app_token="", postgres_conn_id="   ")

    @pytest.mark.parametrize(
        "field",
        ["socrata_page_size", "socrata_max_attempts", "silver_max_active_tis"],
    )
    @pytest.mark.parametrize("value", [0, -1])
    def test_sizing_values_must_be_positive(self, field: str, value: int) -> None:
        """Covers: ETL-030 — CDC sizing controls reject nonpositive values."""
        with pytest.raises(ValidationError, match="at least 1"):
            CdcConfig(socrata_app_token="", **{field: value})

    @pytest.mark.parametrize("value", [0.0, -5.0])
    def test_timeout_must_be_positive(self, value: float) -> None:
        """Covers: ETL-030 — CDC transport timeout must be positive."""
        with pytest.raises(ValidationError, match="socrata_timeout_seconds"):
            CdcConfig(socrata_app_token="", socrata_timeout_seconds=value)

    @pytest.mark.parametrize("value", [-0.1, -1.0])
    def test_spacing_must_not_be_negative(self, value: float) -> None:
        """Covers: ETL-030 — CDC request spacing cannot be negative."""
        with pytest.raises(ValidationError, match="socrata_min_spacing_seconds"):
            CdcConfig(socrata_app_token="", socrata_min_spacing_seconds=value)

    def test_zero_spacing_is_allowed(self) -> None:
        """Covers: ETL-030 — deterministic CDC tests may disable spacing."""
        # Zero spacing is a legitimate fast/local mode; only negative is bad.
        assert CdcConfig(socrata_app_token="", socrata_min_spacing_seconds=0.0)

    @pytest.mark.parametrize("value", [-0.01, -1.0])
    def test_row_count_threshold_must_not_be_negative(self, value: float) -> None:
        """Covers: ETL-030 — CDC reconciliation threshold cannot be negative."""
        with pytest.raises(ValidationError, match="row_count_change_threshold"):
            CdcConfig(socrata_app_token="", row_count_change_threshold=value)

    def test_zero_row_count_threshold_is_allowed(self) -> None:
        """Covers: ETL-030 — strict CDC reconciliation may use zero tolerance."""
        config = CdcConfig(socrata_app_token="", row_count_change_threshold=0.0)
        assert config.row_count_change_threshold == 0.0
