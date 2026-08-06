"""P0 FRED response parsing contracts."""

from __future__ import annotations

import uuid
from datetime import date

import pytest

from data_ingestion_toolbox.fred.ingest import (
    CONFIG,
    FredNoContent,
    FredPayloadError,
    FredRetryableHTTP,
    fetch_fred_observations,
    parse_fred_response,
)

pytestmark = pytest.mark.unit


def test_fred_response_parsing_preserves_fields(source_fixture) -> None:
    """ETL-016: reviewed observations produce exact normalized fields."""
    frame = parse_fred_response(
        source_fixture("fred", "representative.json"),
        series_id="UNRATE",
        domain="labor_cycle",
        load_batch_id=uuid.UUID(int=0),
    )
    assert frame.height == 2
    first = frame.row(0, named=True)
    assert first["series_id"] == "UNRATE"
    assert first["domain"] == "labor_cycle"
    assert first["obs_date"] == date(2024, 1, 1)
    assert first["value"] == 3.75
    assert first["is_missing"] is False
    second = frame.row(1, named=True)
    assert second["value"] is None
    assert second["is_missing"] is True


@pytest.mark.parametrize("value", [".", "", None, "not-a-number"])
def test_fred_missing_and_malformed_values_are_explicit(value: object) -> None:
    """ETL-017: non-numeric observations become marked missing facts."""
    frame = parse_fred_response(
        {"observations": [{"date": "2024-01-01", "value": value}]},
        "UNRATE",
        "labor_cycle",
        uuid.UUID(int=0),
    )
    assert frame["value"][0] is None
    assert frame["is_missing"][0] is True


def test_fred_empty_and_truncated_payloads_are_typed() -> None:
    """ETL-017: empty and truncated entries raise source-specific errors."""
    with pytest.raises(FredNoContent):
        parse_fred_response({"observations": []}, "UNRATE", None, uuid.UUID(int=0))
    with pytest.raises(FredPayloadError, match="missing date"):
        parse_fred_response(
            {"observations": [{"value": "3.5"}]},
            "UNRATE",
            None,
            uuid.UUID(int=0),
        )


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ([], "must be an object"),
        ({"observations": "bad"}, "observations must be a list"),
        ({"observations": ["bad"]}, "observation must be an object"),
        ({"observations": [{"date": "not-a-date", "value": "3.5"}]}, "invalid date"),
    ],
)
def test_fred_malformed_shapes_have_source_specific_errors(
    payload: object, message: str
) -> None:
    with pytest.raises(FredPayloadError, match=message):
        parse_fred_response(payload, "UNRATE", None, uuid.UUID(int=0))


def test_fred_invalid_json_is_retryable(monkeypatch) -> None:
    class Response:
        status_code = 200
        headers: dict[str, str] = {}

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

        def get(self, *args, **kwargs) -> Response:
            return Response()

    monkeypatch.setattr(CONFIG, "fred_api_key", "unit-test-key")
    monkeypatch.setattr("data_ingestion_toolbox.fred.ingest.httpx.Client", Client)
    monkeypatch.setattr("data_ingestion_toolbox.fred.ingest.time.sleep", lambda _: None)
    with pytest.raises(FredRetryableHTTP, match="invalid JSON"):
        fetch_fred_observations.__wrapped__("UNRATE", "2024-01-01", "2024-12-31")
