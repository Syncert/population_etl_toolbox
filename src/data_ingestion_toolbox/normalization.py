"""Pure normalization primitives shared by deterministic ETL jobs."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, TypeVar

import httpx


class NumericParseError(ValueError):
    """A numeric source value is malformed or outside the warehouse contract."""


DEFAULT_NULL_TOKENS = frozenset({"", ".", "NA", "N/A", "NULL", "-"})
RETRYABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503})


def parse_decimal(
    value: object,
    *,
    null_tokens: Iterable[str] = DEFAULT_NULL_TOKENS,
    max_integral_digits: int = 30,
    max_fractional_digits: int = 12,
) -> Decimal | None:
    """Parse a finite decimal without float precision loss."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise NumericParseError("Boolean values are not numeric observations")

    text = str(value).strip()
    if text.upper() in {token.upper() for token in null_tokens}:
        return None
    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:
        raise NumericParseError(f"Invalid numeric value: {text!r}") from exc
    if not parsed.is_finite():
        raise NumericParseError(f"Non-finite numeric value: {text!r}")

    _, digits, exponent = parsed.as_tuple()
    fractional_digits = max(0, -exponent)
    integral_digits = max(1, len(digits) + exponent)
    if integral_digits > max_integral_digits:
        raise NumericParseError(
            f"Numeric value exceeds {max_integral_digits} integral digits"
        )
    if fractional_digits > max_fractional_digits:
        raise NumericParseError(
            f"Numeric value exceeds {max_fractional_digits} fractional digits"
        )
    return parsed


def is_retryable_http_failure(
    *, status_code: int | None = None, exception: BaseException | None = None
) -> bool:
    """Classify only transient HTTP/network failures as retryable."""
    if status_code is not None:
        return status_code in RETRYABLE_HTTP_STATUSES
    return isinstance(exception, (httpx.TimeoutException, httpx.NetworkError))


Record = TypeVar("Record", bound=Mapping[str, Any])


def deduplicate_latest(
    records: Sequence[Record],
    *,
    key_fields: Sequence[str],
    order_fields: Sequence[str],
) -> list[Record]:
    """Return one deterministic latest record for each natural key."""
    winners: dict[tuple[Any, ...], tuple[tuple[Any, ...], int, Record]] = {}
    first_seen: dict[tuple[Any, ...], int] = {}
    for index, record in enumerate(records):
        key = tuple(record[field] for field in key_fields)
        order = tuple(record[field] for field in order_fields)
        first_seen.setdefault(key, index)
        current = winners.get(key)
        if current is None or (order, index) > (current[0], current[1]):
            winners[key] = (order, index, record)
    return [winners[key][2] for key in sorted(first_seen, key=first_seen.__getitem__)]


@dataclass
class TransformMetrics:
    """Small reconciled metric set for pure transform stages."""

    input_rows: int
    output_rows: int = 0
    null_rows: int = 0
    duplicate_rows: int = 0
    dimension_miss_rows: int = 0
    time_dim_hits: int = 0
    time_dim_misses: int = 0
    geo_dim_hits: int = 0
    geo_dim_misses: int = 0
    inserted_rows: int = 0

    def validate(self) -> None:
        if any(value < 0 for value in vars(self).values()):
            raise ValueError("Transform metrics cannot be negative")
        categorized = (
            self.output_rows
            + self.null_rows
            + self.duplicate_rows
            + self.dimension_miss_rows
        )
        if categorized != self.input_rows:
            raise ValueError(
                f"Transform metrics do not reconcile: input={self.input_rows}, "
                f"categorized={categorized}"
            )
        if self.inserted_rows > self.output_rows:
            raise ValueError("Inserted rows cannot exceed output rows")


def map_dimension_keys(
    records: Sequence[Mapping[str, Any]],
    *,
    time_keys: Mapping[Any, int],
    geo_keys: Mapping[Any, int],
    time_field: str = "duration_start",
    geo_field: str = "geo_id",
) -> tuple[list[dict[str, Any]], TransformMetrics]:
    """Attach surrogate keys and drop rows missing either required dimension."""
    output: list[dict[str, Any]] = []
    metrics = TransformMetrics(input_rows=len(records))
    for record in records:
        time_sk = time_keys.get(record.get(time_field))
        geo_sk = geo_keys.get(record.get(geo_field))
        metrics.time_dim_hits += int(time_sk is not None)
        metrics.time_dim_misses += int(time_sk is None)
        metrics.geo_dim_hits += int(geo_sk is not None)
        metrics.geo_dim_misses += int(geo_sk is None)
        if time_sk is None or geo_sk is None:
            metrics.dimension_miss_rows += 1
            continue
        output.append({**record, "time_sk": time_sk, "geo_sk": geo_sk})

    metrics.output_rows = len(output)
    metrics.inserted_rows = len(output)
    metrics.validate()
    return output, metrics


T = TypeVar("T")


def call_with_retry_budget(
    operation: Callable[[], T],
    *,
    max_attempts: int,
    retryable: Callable[[BaseException], bool],
) -> T:
    """Run an operation with an exact, sleep-free retry budget."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except BaseException as exc:
            if attempt == max_attempts or not retryable(exc):
                raise
    raise AssertionError("unreachable")
