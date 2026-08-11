"""P0 shared retry, numeric, deduplication, mapping and metric contracts."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import httpx
import pytest

from data_ingestion_toolbox.normalization import (
    NumericParseError,
    TransformMetrics,
    call_with_retry_budget,
    deduplicate_latest,
    is_retryable_http_failure,
    map_dimension_keys,
    parse_decimal,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("status", [429, 500, 502, 503])
def test_transient_http_statuses_are_retryable(status: int) -> None:
    """Covers: ETL-020 — rate limits and selected server errors retry."""
    assert is_retryable_http_failure(status_code=status)


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_validation_http_statuses_are_not_retryable(status: int) -> None:
    """Covers: ETL-020 — validation and other 4xx failures are final."""
    assert not is_retryable_http_failure(status_code=status)


@pytest.mark.parametrize(
    "exc", [httpx.ConnectTimeout("timeout"), httpx.ConnectError("network")]
)
def test_timeout_and_network_exceptions_are_retryable(exc: Exception) -> None:
    """Covers: ETL-020 — transport failures are retryable."""
    assert is_retryable_http_failure(exception=exc)


def test_retry_budget_stops_and_exposes_final_cause() -> None:
    """Covers: ETL-021 — retries stop at budget and expose final cause."""

    class TransientError(RuntimeError):
        pass

    attempts = 0
    final = TransientError("final cause")

    def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise final

    with pytest.raises(TransientError) as caught:
        call_with_retry_budget(
            operation,
            max_attempts=3,
            retryable=lambda exc: isinstance(exc, TransientError),
        )
    assert attempts == 3
    assert caught.value is final


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("42", Decimal("42")),
        ("-12.5", Decimal("-12.5")),
        ("0.123456789012", Decimal("0.123456789012")),
        (None, None),
        (".", None),
    ],
)
def test_numeric_values_have_explicit_outcomes(
    raw: object, expected: Decimal | None
) -> None:
    """Covers: ETL-022 — valid, precise, negative, and null values parse."""
    assert parse_decimal(raw) == expected


@pytest.mark.parametrize("raw", ["NaN", "Infinity", "1e40", "bad"])
def test_numeric_overflow_and_invalid_values_are_rejected(raw: str) -> None:
    """Covers: ETL-022 — overflow and malformed values are rejected."""
    with pytest.raises(NumericParseError):
        parse_decimal(raw)


def test_duplicate_natural_keys_keep_latest_deterministically() -> None:
    """Covers: ETL-023 — duplicate keys have one deterministic survivor."""
    records = [
        {"series": "A", "date": "2024-01-01", "revision": 1, "value": 1},
        {"series": "B", "date": "2024-01-01", "revision": 1, "value": 8},
        {"series": "A", "date": "2024-01-01", "revision": 2, "value": 2},
    ]
    result = deduplicate_latest(
        records,
        key_fields=("series", "date"),
        order_fields=("revision",),
    )
    assert result == [records[2], records[1]]


def test_dimension_mapping_attaches_keys_and_counts_misses() -> None:
    """Covers: ETL-024 — matches receive keys and misses reconcile."""
    rows = [
        {"duration_start": date(2024, 1, 1), "geo_id": "state:06", "value": 1},
        {"duration_start": date(2024, 2, 1), "geo_id": "state:99", "value": 2},
    ]
    mapped, metrics = map_dimension_keys(
        rows,
        time_keys={date(2024, 1, 1): 20240101},
        geo_keys={"state:06": 6},
    )
    assert mapped == [{**rows[0], "time_sk": 20240101, "geo_sk": 6}]
    assert metrics.time_dim_hits == 1
    assert metrics.time_dim_misses == 1
    assert metrics.geo_dim_hits == 1
    assert metrics.geo_dim_misses == 1
    assert metrics.dimension_miss_rows == 1


def test_transform_metrics_reconcile_and_reject_invalid_counts() -> None:
    """Covers: ETL-025 — categorized outcomes reconcile exactly to inputs."""
    valid = TransformMetrics(
        input_rows=10,
        output_rows=6,
        null_rows=1,
        duplicate_rows=2,
        dimension_miss_rows=1,
        inserted_rows=6,
    )
    valid.validate()

    with pytest.raises(ValueError, match="do not reconcile"):
        TransformMetrics(input_rows=10, output_rows=9).validate()
    with pytest.raises(ValueError, match="negative"):
        TransformMetrics(input_rows=1, output_rows=2, null_rows=-1).validate()
