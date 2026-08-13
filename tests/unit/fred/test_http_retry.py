"""Production FRED HTTP retry and terminal-failure contracts."""

from __future__ import annotations

import httpx
import pytest

from data_ingestion_toolbox.fred import ingest
from tests.support.http import SequencedHttpClient, invalid_json_response, response

pytestmark = pytest.mark.unit

SUCCESS = {"observations": [{"date": "2024-01-01", "value": "3.5"}]}


def _invoke() -> dict:
    return ingest.fetch_fred_observations("UNRATE", "2024-01-01", "2024-12-31")


def _install_client(monkeypatch, outcomes) -> tuple[SequencedHttpClient, list[float]]:
    client = SequencedHttpClient(outcomes)
    sleeps: list[float] = []
    monkeypatch.setattr(ingest.CONFIG, "fred_api_key", "unit-test-key")
    monkeypatch.setattr(ingest.httpx, "Client", lambda *args, **kwargs: client)
    monkeypatch.setattr(ingest.time, "sleep", lambda delay: None)
    monkeypatch.setattr(ingest.fetch_fred_observations.retry, "sleep", sleeps.append)
    return client, sleeps


@pytest.mark.parametrize("status", [429, 500, 502, 503])
def test_fred_retryable_status_uses_production_decorator_then_succeeds(
    monkeypatch, status: int
) -> None:
    """Covers: ETL-020, ETL-021, RES-001 — FRED retries declared statuses."""
    client, sleeps = _install_client(
        monkeypatch, [response(status), response(200, SUCCESS)]
    )

    assert _invoke() == SUCCESS
    assert client.calls == 2
    assert sleeps == [5.0]


@pytest.mark.parametrize(
    "failure",
    [
        httpx.PoolTimeout("timeout"),
        httpx.ConnectError("network"),
    ],
)
def test_fred_transport_failure_retries_then_succeeds(
    monkeypatch, failure: httpx.HTTPError
) -> None:
    """Covers: ETL-020, RES-001 — FRED retries timeout and network failures."""
    client, sleeps = _install_client(monkeypatch, [failure, response(200, SUCCESS)])

    assert _invoke() == SUCCESS
    assert client.calls == 2
    assert sleeps == [5.0]


def test_fred_invalid_json_uses_production_retry_wrapper(monkeypatch) -> None:
    """Covers: RES-002 — FRED invalid JSON follows the real retry policy."""
    client, sleeps = _install_client(
        monkeypatch, [invalid_json_response(), response(200, SUCCESS)]
    )

    assert _invoke() == SUCCESS
    assert client.calls == 2
    assert sleeps == [5.0]


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_fred_terminal_4xx_is_not_retried(monkeypatch, status: int) -> None:
    """Covers: ETL-020, ETL-021 — terminal FRED 4xx fails on one attempt."""
    client, sleeps = _install_client(monkeypatch, [response(status)])

    with pytest.raises(httpx.HTTPStatusError) as caught:
        _invoke()

    assert caught.value.response.status_code == status
    assert client.calls == 1
    assert sleeps == []


def test_fred_exhausted_budget_exposes_final_typed_cause(monkeypatch) -> None:
    """Covers: ETL-021, RES-001 — FRED stops at eight bounded attempts."""
    client, sleeps = _install_client(monkeypatch, [response(500)] * 8)

    with pytest.raises(ingest.FredRetryableHTTP, match="500"):
        _invoke()

    assert client.calls == 8
    assert len(sleeps) == 7
    assert sleeps == sorted(sleeps)
    assert max(sleeps) <= 900
