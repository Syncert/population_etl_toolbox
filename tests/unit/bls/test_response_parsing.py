"""P0 BLS response parsing and error classification."""

from __future__ import annotations

import uuid

import pytest

from data_ingestion_toolbox.bls.ingest import (
    BlsDailyThresholdExceeded,
    BlsNoContent,
    BlsPayloadError,
    BlsRetryableHTTP,
    CONFIG,
    fetch_bls_api,
    parse_bls_response,
)

pytestmark = pytest.mark.unit


def test_bls_response_parsing_preserves_fields(source_fixture) -> None:
    """ETL-011: monthly rows preserve series, period, value and metadata."""
    frame = parse_bls_response(
        source_fixture("bls", "representative.json"),
        program="la",
        load_batch_id=uuid.UUID(int=0),
    )
    assert frame.height == 2
    first = frame.row(0, named=True)
    assert first["series_id"] == "LAUST060000000000003"
    assert first["year"] == 2024
    assert first["period"] == "M01"
    assert first["value"] == 4.2
    assert first["is_latest"] is True
    assert "Preliminary" in first["footnotes"]
    assert frame.row(1, named=True)["value"] is None


def test_bls_empty_response_is_typed() -> None:
    """ETL-012: empty results map to BlsNoContent."""
    with pytest.raises(BlsNoContent):
        parse_bls_response(
            {"status": "REQUEST_SUCCEEDED", "Results": {"series": []}},
            "la",
            uuid.UUID(int=0),
        )


def test_bls_error_response_is_typed() -> None:
    """ETL-012: application-level errors cannot look like successful empties."""
    with pytest.raises(BlsPayloadError, match="REQUEST_FAILED"):
        parse_bls_response(
            {"status": "REQUEST_FAILED", "Results": {}},
            "la",
            uuid.UUID(int=0),
        )


def test_bls_daily_threshold_has_distinct_exception(monkeypatch) -> None:
    """ETL-012: daily quota exhaustion maps to its non-Tenacity exception."""

    class Response:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "status": "REQUEST_NOT_PROCESSED",
                "message": ["Request could not be serviced due to daily threshold"],
            }

    class Client:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            return None

        def post(self, *args, **kwargs) -> Response:
            return Response()

    monkeypatch.setattr(CONFIG, "bls_api_key", "unit-test-key")
    monkeypatch.setattr("data_ingestion_toolbox.bls.ingest.httpx.Client", Client)
    monkeypatch.setattr("data_ingestion_toolbox.bls.ingest.time.sleep", lambda _: None)
    with pytest.raises(BlsDailyThresholdExceeded):
        fetch_bls_api.__wrapped__(["LNS14000000"], 2024, 2024)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ([], "must be an object"),
        ({"Results": []}, "Results must be an object"),
        ({"Results": {"series": "bad"}}, "series must be a list"),
        ({"Results": {"series": ["bad"]}}, "series entry"),
        ({"Results": {"series": [{"data": []}]}}, "missing seriesID"),
        (
            {"Results": {"series": [{"seriesID": "S1", "data": ["bad"]}]}},
            "observation must be an object",
        ),
    ],
)
def test_bls_malformed_shapes_have_source_specific_errors(
    payload: object, message: str
) -> None:
    with pytest.raises(BlsPayloadError, match=message):
        parse_bls_response(payload, "la", uuid.UUID(int=0))


def test_bls_invalid_numeric_and_empty_observations_are_explicit() -> None:
    frame = parse_bls_response(
        {
            "Results": {
                "series": [
                    {
                        "seriesID": "S1",
                        "data": [{"year": "bad", "period": "M01", "value": "bad"}],
                    }
                ]
            }
        },
        "la",
        uuid.UUID(int=0),
    )
    assert frame.row(0, named=True)["year"] is None
    assert frame.row(0, named=True)["value"] is None

    with pytest.raises(BlsNoContent, match="no observations"):
        parse_bls_response(
            {"Results": {"series": [{"seriesID": "S1", "data": []}]}},
            "la",
            uuid.UUID(int=0),
        )


def test_bls_invalid_json_is_retryable(monkeypatch) -> None:
    class Response:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            raise ValueError("truncated")

    class Client:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            return None

        def post(self, *args, **kwargs) -> Response:
            return Response()

    monkeypatch.setattr(CONFIG, "bls_api_key", "unit-test-key")
    monkeypatch.setattr("data_ingestion_toolbox.bls.ingest.httpx.Client", Client)
    monkeypatch.setattr("data_ingestion_toolbox.bls.ingest.time.sleep", lambda _: None)
    with pytest.raises(BlsRetryableHTTP, match="invalid JSON"):
        fetch_bls_api.__wrapped__(["LNS14000000"], 2024, 2024)
