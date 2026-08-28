"""Offline contracts for the CDC Socrata transport."""

from __future__ import annotations

import httpx
import pytest

from data_ingestion_toolbox.cdc import client as cdc_client
from data_ingestion_toolbox.cdc.client import (
    SocrataConfigurationError,
    SocrataHttpError,
    SocrataPayloadError,
    SocrataRetryExhausted,
    fetch_socrata_dataset_pages,
    fetch_socrata_metadata,
    fetch_socrata_page,
    page_parameters,
)
from data_ingestion_toolbox.cdc.config import CdcConfig
from data_ingestion_toolbox.cdc.registry import CDI_ASSET

from ._doubles import TOKEN, ScriptedSocrataClient, socrata_response

pytestmark = pytest.mark.unit


def _config(**overrides: object) -> CdcConfig:
    values: dict[str, object] = {
        "socrata_app_token": "",
        "socrata_min_spacing_seconds": 0,
        "socrata_max_attempts": 3,
    }
    values.update(overrides)
    return CdcConfig(**values)


def test_page_parameters_include_registered_select_order_and_position() -> None:
    """Covers: ETL-020 — CDC pages use one explicit deterministic contract."""
    params = page_parameters(CDI_ASSET, page_size=25, offset=50)

    assert params == {
        "$select": ",".join(CDI_ASSET.select_columns),
        "$order": ",".join(CDI_ASSET.stable_order),
        "$limit": 25,
        "$offset": 50,
    }


def test_page_fetch_keeps_token_only_in_header_and_returns_capture_bytes() -> None:
    """Covers: ETL-038 — CDC token never enters parameters or errors."""
    raw = b'[{"locationid":"59"}]'
    scripted = ScriptedSocrataClient([socrata_response(200, raw=raw)])
    config = _config(socrata_app_token=TOKEN)

    page = fetch_socrata_page(CDI_ASSET, config=config, client=scripted)

    assert page.raw_bytes == raw
    assert page.row_count == 1
    _, request = scripted.requests[0]
    assert request["headers"]["X-App-Token"] == TOKEN
    assert TOKEN not in str(request["params"])
    assert TOKEN not in repr(page)


@pytest.mark.parametrize("token", [" bad", "bad ", "a b", "x\nvalue"])
def test_invalid_configured_token_fails_only_when_request_executes(token: str) -> None:
    """Covers: ETL-030 — unsafe CDC credentials fail at request execution."""
    config = _config(socrata_app_token=token)
    scripted = ScriptedSocrataClient([])

    with pytest.raises(SocrataConfigurationError) as error:
        fetch_socrata_page(CDI_ASSET, config=config, client=scripted)

    assert token not in str(error.value)
    assert scripted.calls == 0


def test_retryable_http_failures_exhaust_exact_attempt_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-021, RES-001 — CDC retries are bounded and typed."""
    responses = [socrata_response(code, []) for code in (429, 500, 503)]
    scripted = ScriptedSocrataClient(responses)
    sleeps: list[float] = []
    monkeypatch.setattr(cdc_client.time, "sleep", sleeps.append)

    with pytest.raises(SocrataRetryExhausted, match="retry_exhausted") as error:
        fetch_socrata_page(CDI_ASSET, config=_config(), client=scripted)

    assert scripted.calls == 3
    assert len(sleeps) == 2
    assert error.value.status == 503
    assert all(response.is_closed for response in responses)


def test_retry_after_is_honored_before_eventual_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: RES-001 — CDC honors bounded Retry-After guidance."""
    scripted = ScriptedSocrataClient(
        [
            socrata_response(429, [], headers={"Retry-After": "2"}),
            socrata_response(200, [{"locationid": "59"}]),
        ]
    )
    sleeps: list[float] = []
    monkeypatch.setattr(cdc_client.time, "sleep", sleeps.append)

    page = fetch_socrata_page(CDI_ASSET, config=_config(), client=scripted)

    assert page.row_count == 1
    assert sleeps == [2.0]


def test_non_retryable_http_error_closes_response_without_retry() -> None:
    """Covers: ETL-020 — terminal CDC HTTP failures do not burn retries."""
    response = socrata_response(404, {"message": TOKEN})
    scripted = ScriptedSocrataClient([response])

    with pytest.raises(SocrataHttpError, match="non_retryable_http") as error:
        fetch_socrata_page(CDI_ASSET, config=_config(), client=scripted)

    assert scripted.calls == 1
    assert response.is_closed
    assert TOKEN not in str(error.value)


@pytest.mark.parametrize(
    "raw,code",
    [
        (b"not-json", "invalid_json"),
        (b'{"rows":[]}', "expected_json_list"),
    ],
)
def test_malformed_observation_payload_is_typed_not_a_final_page(
    raw: bytes, code: str
) -> None:
    """Covers: RES-002 — malformed CDC bytes cannot masquerade as final pages."""
    scripted = ScriptedSocrataClient([socrata_response(200, raw=raw)])

    with pytest.raises(SocrataPayloadError, match=code):
        fetch_socrata_page(CDI_ASSET, config=_config(), client=scripted)


def test_declared_content_length_mismatch_is_a_truncated_payload() -> None:
    """Covers: RES-002 — truncated CDC response bytes stop ingestion."""
    response = socrata_response(
        200,
        raw=b"[]",
        headers={"Content-Length": "99"},
    )

    with pytest.raises(SocrataPayloadError, match="truncated_payload"):
        fetch_socrata_page(
            CDI_ASSET,
            config=_config(),
            client=ScriptedSocrataClient([response]),
        )


def test_metadata_requires_a_json_object_and_allowlists_headers() -> None:
    """Covers: RES-002, ETL-038 — metadata is structural and secret-safe."""
    response = socrata_response(
        200,
        {"id": CDI_ASSET.socrata_id},
        headers={"ETag": "contract", "Set-Cookie": TOKEN},
    )

    metadata = fetch_socrata_metadata(
        CDI_ASSET,
        config=_config(),
        client=ScriptedSocrataClient([response]),
    )

    assert metadata.response_headers == {
        "content-type": "application/json",
        "etag": "contract",
    }
    assert TOKEN not in repr(metadata)


def test_owned_client_is_closed_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: ETL-020 — internally owned CDC clients are always closed."""
    scripted = ScriptedSocrataClient([socrata_response(200, [])])
    monkeypatch.setattr(cdc_client.httpx, "Client", lambda **_kwargs: scripted)

    fetch_socrata_page(CDI_ASSET, config=_config())

    assert scripted.closed is True


def test_caller_owned_client_remains_open() -> None:
    """Covers: ETL-020 — externally owned CDC clients remain caller-owned."""
    scripted = ScriptedSocrataClient([socrata_response(200, [])])

    fetch_socrata_page(CDI_ASSET, config=_config(), client=scripted)

    assert scripted.closed is False


def test_dataset_paging_advances_by_exact_rows_and_stops_on_short_page() -> None:
    """Covers: ETL-020 — CDC paging is deterministic, finite, and gap-free."""
    scripted = ScriptedSocrataClient(
        [
            socrata_response(200, [{"row": 1}, {"row": 2}]),
            socrata_response(200, [{"row": 3}]),
        ]
    )

    pages = list(
        fetch_socrata_dataset_pages(
            CDI_ASSET,
            page_size=2,
            config=_config(),
            client=scripted,
        )
    )

    assert [page.row_count for page in pages] == [2, 1]
    assert [page.request_parameters["$offset"] for page in pages] == [0, 2]


def test_transport_errors_are_sanitized_and_chained() -> None:
    """Covers: ETL-021, ETL-038 — transport exhaustion retains no secret text."""
    request = httpx.Request("GET", "https://data.cdc.gov")
    scripted = ScriptedSocrataClient(
        [httpx.ConnectError(TOKEN, request=request) for _ in range(3)]
    )

    with pytest.raises(SocrataRetryExhausted) as error:
        fetch_socrata_page(
            CDI_ASSET,
            config=_config(socrata_app_token=TOKEN),
            client=scripted,
        )

    assert TOKEN not in str(error.value)
    assert isinstance(error.value.__cause__, httpx.ConnectError)
