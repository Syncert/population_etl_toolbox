"""P0 FRED response parsing contracts."""

from __future__ import annotations

import uuid
import json
import logging
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
from data_ingestion_toolbox.fred.silver_fred.replay import (
    FredCapturePayloadError,
    parse_captured_observations,
)

pytestmark = pytest.mark.unit


def test_fred_http_access_logging_cannot_render_query_credentials() -> None:
    """Covers: ETL-038 — FRED query credentials stay out of HTTP access logs."""
    assert logging.getLogger("httpx").getEffectiveLevel() >= logging.WARNING


def test_fred_response_parsing_preserves_fields(source_fixture) -> None:
    """Covers: ETL-016 — reviewed observations preserve normalized fields."""
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
    """Covers: ETL-017 — nonnumeric observations become explicit missing facts."""
    frame = parse_fred_response(
        {"observations": [{"date": "2024-01-01", "value": value}]},
        "UNRATE",
        "labor_cycle",
        uuid.UUID(int=0),
    )
    assert frame["value"][0] is None
    assert frame["is_missing"][0] is True


def test_fred_empty_and_truncated_payloads_are_typed() -> None:
    """Covers: ETL-017, RES-002 — empty and truncated entries are typed."""
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
    """Covers: RES-002 — malformed FRED schemas raise contextual errors."""
    with pytest.raises(FredPayloadError, match=message):
        parse_fred_response(payload, "UNRATE", None, uuid.UUID(int=0))


def test_fred_invalid_json_is_retryable(monkeypatch) -> None:
    """Covers: RES-002 — invalid FRED JSON raises a typed retryable failure."""

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


def test_fred_capture_parser_retains_exact_source_values() -> None:
    """Covers: ETL-016, ETL-017 — silver parsing retains source strings."""
    payload = json.dumps(
        {
            "observations": [
                {
                    "date": "2024-01-01",
                    "value": ".",
                    "realtime_start": "2024-02-01",
                    "realtime_end": "2024-02-01",
                },
                {"date": "2024-02-01", "value": "not-a-number"},
            ]
        }
    ).encode()

    observations = parse_captured_observations(payload)

    assert observations[0]["value_source"] == "."
    assert observations[0]["value_status"] == "missing"
    assert observations[1]["value_source"] == "not-a-number"
    assert observations[1]["value_status"] == "invalid"


def test_fred_capture_parser_rejects_invalid_dates_without_losing_source_bytes() -> (
    None
):
    """Covers: ETL-017, RES-002 — malformed captured values fail explicitly."""
    with pytest.raises(FredCapturePayloadError, match="invalid date"):
        parse_captured_observations(b'{"observations":[{"date":"bad","value":"3.1"}]}')
