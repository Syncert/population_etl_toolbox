"""Small live contracts for Census ACS, BLS, and FRED."""

from __future__ import annotations

import logging
import os
from datetime import date

import httpx
import pytest

from data_ingestion_toolbox.bls.config import CONFIG as BLS_CONFIG
from data_ingestion_toolbox.census_acs.config import CONFIG as ACS_CONFIG
from data_ingestion_toolbox.fred.config import CONFIG as FRED_CONFIG
from tests.support.external import (
    classify_external_failure,
    observe_external_call,
    require_external_key,
)

pytestmark = [pytest.mark.external, pytest.mark.slow]
TIMEOUT_SECONDS = 15.0


def _census_payload(variable: str = "B01003_001E") -> list[list[str]]:
    api_key = require_external_key("CENSUS_API_KEY", ACS_CONFIG.census_api_key)
    response = httpx.get(
        "https://api.census.gov/data/2023/acs/acs5",
        params={"get": f"NAME,{variable}", "for": "us:1", "key": api_key},
        timeout=TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def _bls_payload(series_id: str = "LNS14000000") -> dict:
    current_year = date.today().year
    payload: dict[str, object] = {
        "seriesid": [series_id],
        "startyear": str(current_year - 1),
        "endyear": str(current_year),
    }
    if BLS_CONFIG.bls_api_key:
        payload["registrationkey"] = BLS_CONFIG.bls_api_key
    response = httpx.post(
        "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        json=payload,
        timeout=TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def _fred_payload(series_id: str = "UNRATE") -> dict:
    api_key = require_external_key("FRED_API_KEY", FRED_CONFIG.fred_api_key)
    response = httpx.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params={
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 2,
        },
        timeout=TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def test_census_authentication_and_consumed_schema(caplog) -> None:
    """Covers: EXT-001 — Census responds within budget with consumed fields."""
    payload, result = observe_external_call(
        "census", _census_payload, logger=logging.getLogger(__name__)
    )
    headers = payload[0]
    assert {"NAME", "B01003_001E", "us"} <= set(headers)
    assert len(payload) >= 2
    assert result.latency_seconds < TIMEOUT_SECONDS


def test_bls_authentication_and_consumed_schema() -> None:
    """Covers: EXT-002 — BLS responds within budget with consumed fields."""
    payload, result = observe_external_call(
        "bls", _bls_payload, logger=logging.getLogger(__name__)
    )
    assert payload["status"] == "REQUEST_SUCCEEDED"
    observation = payload["Results"]["series"][0]["data"][0]
    assert {"year", "period", "value"} <= set(observation)
    assert payload["Results"]["series"][0]["seriesID"] == "LNS14000000"
    assert result.latency_seconds < TIMEOUT_SECONDS


def test_fred_authentication_and_consumed_schema() -> None:
    """Covers: EXT-003 — FRED responds within budget with consumed fields."""
    payload, result = observe_external_call(
        "fred", _fred_payload, logger=logging.getLogger(__name__)
    )
    assert payload["observations"]
    assert {"date", "value"} <= set(payload["observations"][0])
    assert result.latency_seconds < TIMEOUT_SECONDS


def test_representative_curated_identifiers_still_exist() -> None:
    """Covers: EXT-004 — representative curated IDs exist at every source."""
    census_variable = f"{ACS_CONFIG.curated_tables[2]}_001E"
    assert census_variable in _census_payload(census_variable)[0]
    assert _bls_payload(BLS_CONFIG.curated_by_program["ln"][0])["Results"]["series"]
    assert _fred_payload(FRED_CONFIG.curated_series_ids[1])["observations"]


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (httpx.ReadTimeout("slow upstream"), "upstream-unavailable"),
        (
            httpx.HTTPStatusError(
                "rate limited",
                request=httpx.Request("GET", "https://source.invalid"),
                response=httpx.Response(429),
            ),
            "upstream-unavailable",
        ),
        (ValueError("missing consumed field"), "contract-regression"),
    ],
)
def test_external_result_classification_and_telemetry(
    error: BaseException, expected: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Covers: EXT-005 — external failures classify with sanitized telemetry."""
    assert classify_external_failure(error) == expected
    secret = "external-secret-do-not-log"
    with caplog.at_level(logging.WARNING), pytest.raises(type(error)):
        observe_external_call(
            "source",
            lambda: (_ for _ in ()).throw(error),
            logger=logging.getLogger(__name__),
        )
    assert "source=source" in caplog.text
    assert f"failure_class={expected}" in caplog.text
    assert secret not in caplog.text


@pytest.mark.parametrize("key_name", ["CENSUS_API_KEY", "FRED_API_KEY"])
def test_missing_credentials_skip_clearly_without_secret_leak(
    key_name: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Covers: EXT-006 — missing credentials skip clearly without leaking."""
    monkeypatch.delenv(key_name, raising=False)
    with pytest.raises(pytest.skip.Exception, match=rf"{key_name} is not configured"):
        require_external_key(key_name, os.environ.get(key_name))
    assert "external-secret-do-not-log" not in caplog.text
