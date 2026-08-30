"""Secret-safe, bounded FBI CDE transport contracts."""

from __future__ import annotations

import json

import httpx
import pytest

from data_ingestion_toolbox.fbi_ucr.client import (
    FbiCdeConfigurationError,
    FbiCdeHttpError,
    FbiCdePayloadError,
    FbiCdeRetryExhausted,
    fetch_agency_directory,
    fetch_summarized_observations,
    observation_parameters,
)
from data_ingestion_toolbox.fbi_ucr.config import (
    API_KEY_PARAMETER,
    CDE_BASE_URL,
    FbiUcrConfig,
)
from data_ingestion_toolbox.fbi_ucr.registry import (
    SUMMARIZED_VIOLENT_CRIME,
    FbiSubject,
)

from ._doubles import API_KEY, ScriptedCdeClient, cde_response

pytestmark = pytest.mark.unit

PRODUCT = SUMMARIZED_VIOLENT_CRIME
NATIONAL = FbiSubject("national", "US")


def _config(**overrides: object) -> FbiUcrConfig:
    values: dict[str, object] = {
        "cde_api_key": API_KEY,
        "min_spacing_seconds": 0.0,
        "max_attempts": 3,
    }
    values.update(overrides)
    return FbiUcrConfig(**values)


def _national(fbi_bytes) -> httpx.Response:
    """Return a canned success answer carrying the exact fixture bytes."""
    return cde_response(200, raw=fbi_bytes("summarized_national_V"))


def test_request_applies_the_key_only_to_the_outgoing_request(
    fbi_bytes, monkeypatch
) -> None:
    """Covers: ETL-038 — the provider key never enters captured parameters."""
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    client = ScriptedCdeClient([_national(fbi_bytes)])

    response = fetch_summarized_observations(
        PRODUCT, NATIONAL, config=_config(), client=client
    )

    (_args, kwargs) = client.requests[0]
    assert _args[0] == f"{CDE_BASE_URL}/summarized/national/V"
    assert kwargs["headers"]["X-Api-Key"] == API_KEY
    assert API_KEY_PARAMETER not in kwargs["params"]
    assert kwargs["params"]["from"] == "01-1990"
    assert response.request_parameters == observation_parameters(PRODUCT)
    assert API_KEY_PARAMETER not in response.request_parameters
    assert API_KEY not in json.dumps(dict(response.request_parameters))


def test_only_provenance_response_headers_are_retained(fbi_bytes) -> None:
    """Covers: ETL-038 — session and authorization headers are not captured."""
    client = ScriptedCdeClient(
        [
            cde_response(
                200,
                raw=fbi_bytes("summarized_national_V"),
                headers={
                    "content-type": "application/json",
                    "etag": 'W/"fbi-1"',
                    "set-cookie": "session=secret",
                    "authorization": "Bearer secret",
                },
            )
        ]
    )

    response = fetch_summarized_observations(
        PRODUCT, NATIONAL, config=_config(), client=client
    )

    assert set(response.response_headers) == {"content-type", "etag"}


@pytest.mark.parametrize("key", ["", "   "])
def test_missing_credential_fails_at_request_time(key: str, fbi_bytes) -> None:
    """Covers: ETL-030, DAG-014 — an absent key fails when a request runs."""
    client = ScriptedCdeClient([_national(fbi_bytes)])

    with pytest.raises(FbiCdeConfigurationError) as caught:
        fetch_summarized_observations(
            PRODUCT, NATIONAL, config=_config(cde_api_key=key), client=client
        )

    assert caught.value.code == "missing_api_key"
    assert client.calls == 0


def test_malformed_credential_is_rejected_without_exposing_it() -> None:
    """Covers: ETL-030 — a malformed key never reaches the error message."""
    secret = " leading-space-key "
    client = ScriptedCdeClient([])

    with pytest.raises(FbiCdeConfigurationError) as caught:
        fetch_summarized_observations(
            PRODUCT, NATIONAL, config=_config(cde_api_key=secret), client=client
        )

    assert caught.value.code == "invalid_api_key"
    assert secret.strip() not in str(caught.value)


def test_structured_provider_error_body_is_a_payload_violation(fbi_bytes) -> None:
    """Covers: RES-002 — an error document is never treated as observations."""
    client = ScriptedCdeClient(
        [cde_response(200, raw=fbi_bytes("provider_error_body"))]
    )

    with pytest.raises(FbiCdePayloadError) as caught:
        fetch_summarized_observations(
            PRODUCT, NATIONAL, config=_config(), client=client
        )

    assert caught.value.code == "provider_error_body"


def test_truncated_payload_is_rejected_before_parsing() -> None:
    """Covers: RES-002 — a short body against content-length is rejected."""
    client = ScriptedCdeClient(
        [
            cde_response(
                200,
                raw=b'{"offenses"',
                headers={"content-length": "4096"},
            )
        ]
    )

    with pytest.raises(FbiCdePayloadError) as caught:
        fetch_summarized_observations(
            PRODUCT, NATIONAL, config=_config(), client=client
        )

    assert caught.value.code == "truncated_payload"


def test_invalid_json_is_rejected_without_dumping_the_payload() -> None:
    """Covers: RES-002 — malformed bytes fail with source context only."""
    client = ScriptedCdeClient([cde_response(200, raw=b"<html>service down</html>")])

    with pytest.raises(FbiCdePayloadError) as caught:
        fetch_summarized_observations(
            PRODUCT, NATIONAL, config=_config(), client=client
        )

    assert caught.value.code == "invalid_json"
    assert "service down" not in str(caught.value)


@pytest.mark.parametrize("status", [400, 403, 404])
def test_non_retryable_status_fails_immediately(status: int) -> None:
    """Covers: ETL-020 — client errors are not retried."""
    client = ScriptedCdeClient([cde_response(status, {"error": {"code": "bad"}})])

    with pytest.raises(FbiCdeHttpError) as caught:
        fetch_summarized_observations(
            PRODUCT, NATIONAL, config=_config(), client=client
        )

    assert caught.value.status == status
    assert client.calls == 1


@pytest.mark.parametrize("status", [429, 500, 503])
def test_retryable_status_recovers_inside_the_budget(
    status: int, fbi_bytes, monkeypatch
) -> None:
    """Covers: ETL-020, RES-001 — throttling and 5xx retry then succeed."""
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    retries: list[BaseException] = []
    client = ScriptedCdeClient(
        [
            cde_response(status, {"error": {"code": "OVER_RATE_LIMIT"}}),
            _national(fbi_bytes),
        ]
    )

    response = fetch_summarized_observations(
        PRODUCT,
        NATIONAL,
        config=_config(),
        client=client,
        on_retry=retries.append,
    )

    assert response.http_status == 200
    assert client.calls == 2
    assert len(retries) == 1


def test_retry_budget_is_bounded_and_exposes_the_final_cause(monkeypatch) -> None:
    """Covers: ETL-021, RES-001 — retries stop at the configured attempts."""
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    client = ScriptedCdeClient([cde_response(503) for _ in range(3)])

    with pytest.raises(FbiCdeRetryExhausted) as caught:
        fetch_summarized_observations(
            PRODUCT, NATIONAL, config=_config(max_attempts=3), client=client
        )

    assert client.calls == 3
    assert caught.value.status == 503


def test_transport_failure_is_retried_then_reported(monkeypatch) -> None:
    """Covers: ETL-020, RES-001 — network failures retry then fail typed."""
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    client = ScriptedCdeClient(
        [httpx.ConnectTimeout("timed out"), httpx.ConnectTimeout("timed out")]
    )

    with pytest.raises(FbiCdeRetryExhausted):
        fetch_summarized_observations(
            PRODUCT, NATIONAL, config=_config(max_attempts=2), client=client
        )

    assert client.calls == 2


def test_agency_directory_request_carries_no_period_parameters(fbi_bytes) -> None:
    """Covers: ETL-001 — the reference request uses the documented path only."""
    client = ScriptedCdeClient(
        [cde_response(200, raw=fbi_bytes("agency_directory_WI"))]
    )

    response = fetch_agency_directory("WI", config=_config(), client=client)

    (_args, kwargs) = client.requests[0]
    assert _args[0] == f"{CDE_BASE_URL}/agency/byStateAbbr/WI"
    assert set(kwargs["params"]) == set()
    assert kwargs["headers"]["X-Api-Key"] == API_KEY
    assert response.request_parameters == {}
