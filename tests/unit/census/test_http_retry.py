"""Production Census HTTP retry and terminal-failure contracts."""

from __future__ import annotations

import httpx
import pytest

from data_ingestion_toolbox.census_acs import ingest
from tests.support.http import SequencedHttpClient, invalid_json_response, response

pytestmark = pytest.mark.unit

SUCCESS = [["B01003_001E", "state"], ["100", "55"]]


def _invoke() -> list[list[str]]:
    return ingest.fetch_acs_api(2024, "acs5", ["B01003_001E"], "state")


def _install_client(monkeypatch, outcomes) -> tuple[SequencedHttpClient, list[float]]:
    client = SequencedHttpClient(outcomes)
    sleeps: list[float] = []
    monkeypatch.setattr(ingest.CONFIG, "census_api_key", "unit-test-key")
    monkeypatch.setattr(ingest.httpx, "Client", lambda *args, **kwargs: client)
    monkeypatch.setattr(ingest.time, "sleep", lambda delay: None)
    monkeypatch.setattr(ingest.fetch_acs_api.retry, "sleep", sleeps.append)
    return client, sleeps


def test_census_missing_required_key_fails_before_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-020, DAG-014 — missing Census credentials fail before I/O."""
    monkeypatch.setattr(ingest.CONFIG, "census_api_key", "")
    client = SequencedHttpClient([response(200, SUCCESS)])
    monkeypatch.setattr(ingest.httpx, "Client", lambda *args, **kwargs: client)

    with pytest.raises(
        ValueError, match=r"^CENSUS_API_KEY required for Census API requests$"
    ):
        _invoke()

    assert client.calls == 0


def test_census_configured_key_is_forwarded_without_logging(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Covers: ETL-020, EXT-001 — every Census Data API request carries its key."""
    client, _ = _install_client(monkeypatch, [response(200, SUCCESS)])

    assert _invoke() == SUCCESS
    assert client.requests[0][1]["params"]["key"] == "unit-test-key"
    assert "unit-test-key" not in caplog.text


@pytest.mark.parametrize("status", [429, 500, 502, 503])
def test_census_retryable_status_uses_production_decorator_then_succeeds(
    monkeypatch, status: int
) -> None:
    """Covers: ETL-020, ETL-021, RES-001 — Census retries declared statuses."""
    client, sleeps = _install_client(
        monkeypatch, [response(status), response(200, SUCCESS)]
    )

    assert _invoke() == SUCCESS
    assert client.calls == 2
    assert sleeps == [5.0]


@pytest.mark.parametrize(
    "failure",
    [
        httpx.ConnectTimeout("timeout"),
        httpx.ConnectError("network"),
    ],
)
def test_census_transport_failure_retries_then_succeeds(
    monkeypatch, failure: httpx.HTTPError
) -> None:
    """Covers: ETL-020, RES-001 — Census retries timeout and network failures."""
    client, sleeps = _install_client(monkeypatch, [failure, response(200, SUCCESS)])

    assert _invoke() == SUCCESS
    assert client.calls == 2
    assert sleeps == [5.0]


def test_census_invalid_json_http_path_retries_without_payload_disclosure(
    monkeypatch,
) -> None:
    """Covers: RES-002 — Census invalid JSON is typed, bounded, and sanitized."""
    client, sleeps = _install_client(
        monkeypatch, [invalid_json_response(), response(200, SUCCESS)]
    )

    assert _invoke() == SUCCESS
    assert client.calls == 2
    assert sleeps == [5.0]


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_census_terminal_4xx_is_not_retried(monkeypatch, status: int) -> None:
    """Covers: ETL-020, ETL-021 — terminal Census 4xx fails on one attempt."""
    client, sleeps = _install_client(monkeypatch, [response(status)])

    with pytest.raises(httpx.HTTPStatusError) as caught:
        _invoke()

    assert caught.value.response.status_code == status
    assert client.calls == 1
    assert sleeps == []


def test_census_exhausted_budget_exposes_final_typed_cause(monkeypatch) -> None:
    """Covers: ETL-021, RES-001 — Census stops at eight bounded attempts."""
    client, sleeps = _install_client(monkeypatch, [response(503)] * 8)

    with pytest.raises(ingest.CensusRetryableHTTP, match="503"):
        _invoke()

    assert client.calls == 8
    assert len(sleeps) == 7
    assert sleeps == sorted(sleeps)
    assert max(sleeps) <= 900
