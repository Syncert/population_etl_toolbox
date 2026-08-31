"""Deterministic, secret-safe USDA NASS Quick Stats transport contracts."""

from __future__ import annotations

import httpx
import pytest

from data_ingestion_toolbox.capture import request_fingerprint
from data_ingestion_toolbox.usda_nass.client import (
    API_COUNT_PATH,
    API_DATA_PATH,
    API_PARAM_VALUES_PATH,
    NassConfigurationError,
    NassHttpError,
    NassOverLimitError,
    NassPayloadError,
    NassRetryExhausted,
    count_parameters,
    data_parameters,
    fetch_param_values,
    fetch_slice_count,
    fetch_slice_records,
    redact,
    transport_query,
    validated_api_key,
)
from data_ingestion_toolbox.usda_nass.registry import NassSlice, get_product

from ._doubles import RecordingClient, deterministic_config, json_response

pytestmark = pytest.mark.unit

PRODUCT = get_product("corn_survey_annual")
SLICE = NassSlice(PRODUCT.product_id, "COUNTY", PRODUCT.year_end)
SECRET = "SECRET-KEY-9999-8888-7777-6666-5555"


def _records(count: int) -> dict[str, object]:
    return {"data": [{"Value": str(index)} for index in range(count)]}


def test_endpoint_scope_is_the_three_registered_paths() -> None:
    """Covers: ETL-030 — USDA NASS uses one explicit endpoint contract."""
    assert API_DATA_PATH == "/api/api_GET"
    assert API_COUNT_PATH == "/api/get_counts"
    assert API_PARAM_VALUES_PATH == "/api/get_param_values"


def test_missing_or_unsafe_credentials_fail_at_request_execution() -> None:
    """Covers: ETL-030 — unsafe USDA NASS credentials fail at execution."""
    with pytest.raises(NassConfigurationError) as missing:
        validated_api_key(deterministic_config(usda_nass_api_key="   "))
    assert missing.value.code == "missing_api_key"

    with pytest.raises(NassConfigurationError) as invalid:
        validated_api_key(deterministic_config(usda_nass_api_key="short key!"))
    assert invalid.value.code == "invalid_api_key"


def test_credential_reaches_the_transport_query_but_never_the_parameters() -> None:
    """Covers: ETL-038 — the API key never enters captured parameters."""
    config = deterministic_config(usda_nass_api_key=SECRET)
    parameters = data_parameters(PRODUCT, SLICE)
    query = transport_query(parameters, config)

    assert query["key"] == SECRET
    assert "key" not in parameters
    assert SECRET not in str(parameters)
    fingerprint = request_fingerprint("USDA_NASS", API_DATA_PATH, parameters)
    assert len(fingerprint) == 64
    assert SECRET not in fingerprint


def test_a_credential_already_in_the_parameters_is_refused() -> None:
    """Covers: ETL-038 — a credential cannot be smuggled into parameters."""
    config = deterministic_config()
    with pytest.raises(NassConfigurationError) as caught:
        transport_query({"year": "2024", "key": SECRET}, config)
    assert caught.value.code == "credential_in_parameters"
    assert SECRET not in str(caught.value)


def test_preflight_and_retrieval_share_one_selection() -> None:
    """Covers: ETL-020 — preflight and retrieval cannot drift apart."""
    counts = count_parameters(PRODUCT, SLICE)
    records = data_parameters(PRODUCT, SLICE)

    assert "format" not in counts
    assert records["format"] == "JSON"
    assert counts == {
        name: value for name, value in records.items() if name != "format"
    }


def test_slice_count_is_parsed_from_the_provider_count_facility() -> None:
    """Covers: ETL-020 — the provider count facility gates every retrieval."""
    client = RecordingClient([json_response({"count": "3143"})])
    response = fetch_slice_count(
        PRODUCT, SLICE, config=deterministic_config(), client=client
    )

    assert response.count == 3143
    assert response.http_status == 200
    assert client.calls[0]["url"].endswith(API_COUNT_PATH)
    assert client.calls[0]["params"]["key"]
    assert response.request_parameters == count_parameters(PRODUCT, SLICE)
    assert "key" not in response.request_parameters


@pytest.mark.parametrize(
    "payload", [{"count": "not-a-number"}, {"count": -1}, {"count": True}, {}]
)
def test_an_unusable_count_stops_ingestion(payload: dict[str, object]) -> None:
    """Covers: RES-002 — an unusable preflight count stops ingestion."""
    client = RecordingClient([json_response(payload)])
    with pytest.raises(NassPayloadError) as caught:
        fetch_slice_count(PRODUCT, SLICE, config=deterministic_config(), client=client)
    assert caught.value.code == "invalid_count"


def test_over_limit_refusal_is_typed_and_never_becomes_data() -> None:
    """Covers: RES-002 — an over-limit refusal cannot masquerade as data."""
    client = RecordingClient(
        [json_response({"error": ["exceeds limit = 50000"]}, status=400)]
    )
    with pytest.raises(NassOverLimitError) as caught:
        fetch_slice_records(
            PRODUCT, SLICE, config=deterministic_config(), client=client
        )
    assert caught.value.code == "exceeds_record_limit"
    assert caught.value.status == 400


def test_a_response_at_the_record_ceiling_is_treated_as_truncated() -> None:
    """Covers: RES-002 — a response at the ceiling is refused, not published."""
    config = deterministic_config(slice_record_limit=5)
    client = RecordingClient([json_response(_records(5))])
    with pytest.raises(NassOverLimitError) as caught:
        fetch_slice_records(PRODUCT, SLICE, config=config, client=client)
    assert caught.value.code == "record_limit_reached"

    ok_client = RecordingClient([json_response(_records(4))])
    assert (
        fetch_slice_records(PRODUCT, SLICE, config=config, client=ok_client).row_count
        == 4
    )


def test_truncated_response_bytes_stop_ingestion() -> None:
    """Covers: RES-002 — truncated USDA NASS response bytes stop ingestion."""
    response = json_response(_records(2), headers={"content-length": "99999"})
    client = RecordingClient([response])
    with pytest.raises(NassPayloadError) as caught:
        fetch_slice_records(
            PRODUCT, SLICE, config=deterministic_config(), client=client
        )
    assert caught.value.code == "truncated_payload"


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        (b"not json", "invalid_json"),
        (b"[1, 2, 3]", "expected_json_object"),
        (b'{"data": {"rows": 1}}', "expected_data_list"),
    ],
)
def test_malformed_bytes_cannot_become_observations(payload: bytes, code: str) -> None:
    """Covers: RES-002 — malformed USDA NASS bytes cannot become records."""
    response = httpx.Response(
        200,
        request=httpx.Request("GET", "https://quickstats.invalid/api/api_GET"),
        headers={"content-type": "application/json"},
        content=payload,
    )
    with pytest.raises(NassPayloadError) as caught:
        fetch_slice_records(
            PRODUCT,
            SLICE,
            config=deterministic_config(),
            client=RecordingClient([response]),
        )
    assert caught.value.code == code


def test_a_no_rows_provider_error_is_a_typed_provider_error() -> None:
    """Covers: RES-002 — a provider error envelope is typed, not silent."""
    client = RecordingClient(
        [
            json_response(
                {"error": ["bad request - unable to find row(s) matching"]},
                status=400,
            )
        ]
    )
    with pytest.raises(NassPayloadError) as caught:
        fetch_slice_records(
            PRODUCT, SLICE, config=deterministic_config(), client=client
        )
    assert caught.value.code == "provider_error"


def test_terminal_http_failures_do_not_burn_retries() -> None:
    """Covers: ETL-020 — terminal USDA NASS HTTP failures skip the retry budget."""
    # 403 is Quick Stats' rate-limit signal and retries; 404 stays terminal.
    client = RecordingClient([json_response({"detail": "not found"}, status=404)])
    with pytest.raises(NassHttpError) as caught:
        fetch_slice_count(PRODUCT, SLICE, config=deterministic_config(), client=client)
    assert caught.value.code == "non_retryable_http"
    assert len(client.calls) == 1


def test_retries_are_bounded_typed_and_recorded() -> None:
    """Covers: ETL-021, RES-001 — USDA NASS retries are bounded and typed."""
    observed: list[BaseException] = []
    client = RecordingClient(
        [
            json_response({"count": "1"}, status=503),
            json_response({"count": "1"}, status=503),
            json_response({"count": "1"}, status=503),
        ]
    )
    with pytest.raises(NassRetryExhausted) as caught:
        fetch_slice_count(
            PRODUCT,
            SLICE,
            config=deterministic_config(request_max_attempts=3),
            client=client,
            on_retry=observed.append,
        )
    assert caught.value.code == "retry_exhausted"
    assert caught.value.status == 503
    assert len(client.calls) == 3
    assert len(observed) == 2


def test_a_transient_failure_recovers_within_the_budget() -> None:
    """Covers: ETL-021, RES-001 — a transient failure recovers in budget."""
    client = RecordingClient(
        [
            httpx.ConnectError("connection reset"),
            json_response({"count": "12"}),
        ]
    )
    response = fetch_slice_count(
        PRODUCT, SLICE, config=deterministic_config(), client=client
    )
    assert response.count == 12
    assert len(client.calls) == 2


def test_transport_exhaustion_retains_no_secret_text() -> None:
    """Covers: ETL-021, ETL-038 — transport exhaustion retains no secret text."""
    client = RecordingClient(
        [json_response({}, status=500), json_response({}, status=500)]
    )
    with pytest.raises(NassRetryExhausted) as caught:
        fetch_slice_count(
            PRODUCT,
            SLICE,
            config=deterministic_config(
                usda_nass_api_key=SECRET, request_max_attempts=2
            ),
            client=client,
        )
    message = str(caught.value)
    assert SECRET not in message
    assert "key=" not in message
    assert API_COUNT_PATH in message


def test_provider_text_echoing_a_query_string_is_redacted() -> None:
    """Covers: ETL-038 — echoed query strings never keep the credential."""
    echoed = f"GET /api/api_GET?year=2024&key={SECRET}&format=JSON failed"
    assert SECRET not in redact(echoed)
    assert "key=***" in redact(echoed)


def test_internally_owned_clients_are_always_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-020 — internally owned USDA NASS clients are always closed."""
    created: list[RecordingClient] = []

    def factory(**_kwargs: object) -> RecordingClient:
        client = RecordingClient([json_response({"count": "1"})])
        created.append(client)
        return client

    monkeypatch.setattr(httpx, "Client", factory)
    fetch_slice_count(PRODUCT, SLICE, config=deterministic_config())
    assert created and created[0].closed is True


def test_externally_owned_clients_remain_caller_owned() -> None:
    """Covers: ETL-020 — externally owned USDA NASS clients stay caller-owned."""
    client = RecordingClient([json_response({"count": "1"})])
    fetch_slice_count(PRODUCT, SLICE, config=deterministic_config(), client=client)
    assert client.closed is False


def test_parameter_discovery_returns_the_provider_domain() -> None:
    """Covers: EXT-004 — the provider's own parameter domain is retrievable."""
    client = RecordingClient(
        [json_response({"agg_level_desc": ["NATIONAL", "STATE", "COUNTY"]})]
    )
    response = fetch_param_values(
        "agg_level_desc", config=deterministic_config(), client=client
    )
    assert response.values == ("NATIONAL", "STATE", "COUNTY")
    assert response.request_parameters == {"param": "agg_level_desc"}


def test_parameter_discovery_rejects_a_wrong_shaped_domain() -> None:
    """Covers: RES-002 — a wrong-shaped parameter domain cannot be consumed."""
    client = RecordingClient([json_response({"agg_level_desc": {"a": 1}})])
    with pytest.raises(NassPayloadError) as caught:
        fetch_param_values(
            "agg_level_desc", config=deterministic_config(), client=client
        )
    assert caught.value.code == "expected_param_value_list"
