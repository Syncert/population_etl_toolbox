"""Isolated live contract checks for the registered CDC Open Data assets.

These checks make the smallest possible request: one metadata/schema document
per enabled asset, using the registered Socrata identifier and the registered
consumed fields only. They never fetch observations, never write to a
warehouse, and are never a pull-request gate, so live CDC availability can
never become a prerequisite for unit, DAG, integration, or end-to-end evidence.
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.cdc.client import (
    SocrataFetchError,
    SocrataRetryExhausted,
    build_cdc_headers,
    fetch_socrata_metadata,
)
from data_ingestion_toolbox.cdc.config import CdcConfig
from data_ingestion_toolbox.cdc.metadata import parse_metadata
from data_ingestion_toolbox.cdc.registry import CdcAsset, enabled_assets
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

TIMEOUT_SECONDS = 15.0
SENTINEL_TOKEN = "cdc-external-sentinel-token"
LOGGER = logging.getLogger(__name__)


def _live_config(**overrides: object) -> CdcConfig:
    """Read the optional token only when the request is about to execute."""
    values: dict[str, object] = {
        "socrata_timeout_seconds": TIMEOUT_SECONDS,
        "socrata_max_attempts": 3,
    }
    values.update(overrides)
    return CdcConfig.from_environment(**values)


@pytest.mark.parametrize("asset", enabled_assets(), ids=lambda asset: asset.asset_id)
def test_cdc_registered_asset_metadata_matches_the_frozen_contract(
    asset: CdcAsset,
) -> None:
    """Covers: EXT-012 — each enabled CDC asset still serves its contract."""
    metadata, result = observe_external_call(
        f"cdc:{asset.asset_id}",
        lambda: parse_metadata(
            fetch_socrata_metadata(asset, config=_live_config()).raw_bytes, asset
        ),
        logger=LOGGER,
    )

    assert metadata.socrata_id == asset.socrata_id
    assert metadata.title == asset.label
    assert metadata.watermark > 0
    assert metadata.columns == tuple(
        (column.name, column.data_type) for column in asset.expected_columns
    )
    assert result.latency_seconds < TIMEOUT_SECONDS


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            SocrataRetryExhausted(
                "/api/views/hksd-2xuw", code="retry_exhausted", status=503
            ),
            "upstream-unavailable",
        ),
        (
            SocrataFetchError(
                "/api/views/hksd-2xuw", code="retryable_http", status=429
            ),
            "upstream-unavailable",
        ),
        (
            httpx.ConnectTimeout("cdc timed out"),
            "upstream-unavailable",
        ),
        (
            SocrataFetchError("/api/views/hksd-2xuw", code="expected_json_object"),
            "contract-regression",
        ),
    ],
)
def test_cdc_upstream_outage_is_not_reported_as_a_contract_regression(
    error: BaseException, expected: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Covers: EXT-012, EXT-005 — CDC 429/5xx/timeout mean upstream unavailable."""
    assert classify_external_failure(error) == expected

    with caplog.at_level(logging.WARNING), pytest.raises(type(error)):
        observe_external_call(
            "cdc",
            lambda: (_ for _ in ()).throw(error),
            logger=LOGGER,
        )

    assert f"failure_class={expected}" in caplog.text


def test_cdc_optional_app_token_is_absent_when_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: EXT-012, EXT-006 — anonymous CDC reads need no credential."""
    monkeypatch.delenv("CDC_SOCRATA_APP_TOKEN", raising=False)

    headers = build_cdc_headers(CdcConfig.from_environment())

    assert "X-App-Token" not in headers
    assert headers == {"Accept": "application/json"}


def test_cdc_configured_app_token_never_reaches_logs_or_errors(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Covers: EXT-012, EXT-006 — a configured CDC token stays in the header."""
    monkeypatch.setenv("CDC_SOCRATA_APP_TOKEN", SENTINEL_TOKEN)
    config = _live_config()
    assert build_cdc_headers(config)["X-App-Token"] == SENTINEL_TOKEN

    failure = httpx.ConnectError("simulated CDC outage")
    with caplog.at_level(logging.WARNING), pytest.raises(httpx.ConnectError):
        observe_external_call(
            "cdc", lambda: (_ for _ in ()).throw(failure), logger=LOGGER
        )

    assert SENTINEL_TOKEN not in caplog.text
    assert SENTINEL_TOKEN not in str(failure)
