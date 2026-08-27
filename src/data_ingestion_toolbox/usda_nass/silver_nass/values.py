"""Pure USDA NASS value parsing and offline capture replay.

Quick Stats publishes ``Value`` and ``CV (%)`` as text. A value may be a
thousands-separated number, a provider suppression symbol, or empty. The exact
source text is always retained, a symbol is never converted to zero, and a
symbol the registry does not know quarantines its row instead of being guessed
at.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

from ..metadata import decode_data_payload, parse_load_time
from ..registry import QUICK_STATS_FIELDS, NassProduct
from .dimensions import (
    CommodityIdentity,
    DomainIdentity,
    GeographyIdentity,
    NassIdentityError,
    PeriodIdentity,
    StatisticIdentity,
    commodity_identity,
    domain_identity,
    geography_identity,
    period_identity,
    source_record_id,
    statistic_identity,
)

#: Value states this adapter distinguishes. ``valid`` is the only state that
#: carries a number; every other state keeps the exact source text and a NULL
#: numeric value.
VALUE_STATUS_VALID = "valid"
VALUE_STATUS_MISSING = "missing"
VALUE_STATUS_WITHHELD = "withheld"
VALUE_STATUS_INSUFFICIENT = "insufficient_reports"
VALUE_STATUS_NOT_APPLICABLE = "not_applicable"
VALUE_STATUS_NOT_AVAILABLE = "not_available"
VALUE_STATUS_BELOW_ROUNDING_UNIT = "below_rounding_unit"
VALUE_STATUS_QUALITY_FLAGGED = "quality_flagged"

#: Provider symbol to explicit warehouse state. The definitions are recorded in
#: ``tests/fixtures/usda_nass/SOURCE_NOTES.md`` with their NASS sources.
SYMBOL_STATUS: dict[str, str] = {
    "(D)": VALUE_STATUS_WITHHELD,
    "(S)": VALUE_STATUS_INSUFFICIENT,
    "(X)": VALUE_STATUS_NOT_APPLICABLE,
    "(NA)": VALUE_STATUS_NOT_AVAILABLE,
    "(Z)": VALUE_STATUS_BELOW_ROUNDING_UNIT,
    "(H)": VALUE_STATUS_QUALITY_FLAGGED,
    "(L)": VALUE_STATUS_QUALITY_FLAGGED,
}

VALUE_STATUSES: frozenset[str] = frozenset(
    {VALUE_STATUS_VALID, VALUE_STATUS_MISSING, *SYMBOL_STATUS.values()}
)


class NassValueError(ValueError):
    """A source value cannot be represented without inventing meaning."""


@dataclass(frozen=True)
class ParsedValue:
    """One source value with its exact text, typed state, and symbol."""

    source_text: str
    value: Decimal | None
    status: str
    symbol: str | None


@dataclass(frozen=True)
class NassObservation:
    """One source-faithful crop observation at the full Quick Stats grain."""

    product_id: str
    release_watermark: str
    source_record_id: str
    source_row: dict[str, Any]
    slice_key: str
    commodity: CommodityIdentity
    statistic: StatisticIdentity
    domain: DomainIdentity
    geography: GeographyIdentity
    period: PeriodIdentity
    value_source: str
    value: Decimal | None
    value_status: str
    suppression_code: str | None
    cv_source: str
    cv_value: Decimal | None
    cv_status: str
    cv_symbol: str | None
    load_time: datetime | None
    capture_id: UUID | None = None
    source_row_index: int | None = None


@dataclass(frozen=True)
class QuarantinedObservation:
    """One rejected source row, kept as an explicit, inspectable outcome."""

    slice_key: str
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ReplayResult:
    """Reconciled outcome of replaying captured bytes without a network."""

    input_count: int
    observations: tuple[NassObservation, ...]
    quarantined: tuple[QuarantinedObservation, ...]


@dataclass(frozen=True)
class CapturedSlicePayload:
    """One captured slice payload and the control state that describes it."""

    capture_id: UUID
    slice_key: str
    agg_level_desc: str
    year: int
    provider_count: int
    captured_row_count: int
    payload: bytes
    payload_checksum: str


class NassReplayError(ValueError):
    """Captured slices cannot form a complete registered release."""


def parse_source_value(text: object) -> ParsedValue:
    """Parse one ``Value`` or ``CV (%)`` cell without ever inventing a zero."""
    source_text = "" if text is None else str(text).strip()
    if not source_text:
        return ParsedValue(source_text, None, VALUE_STATUS_MISSING, None)
    status = SYMBOL_STATUS.get(source_text)
    if status is not None:
        return ParsedValue(source_text, None, status, source_text)
    candidate = source_text.replace(",", "")
    try:
        value = Decimal(candidate)
    except InvalidOperation as exc:
        raise NassValueError(
            f"unregistered non-numeric source value: {source_text!r}"
        ) from exc
    if not value.is_finite():
        raise NassValueError(f"non-finite source value: {source_text!r}")
    return ParsedValue(source_text, value, VALUE_STATUS_VALID, None)


def _required_fields_present(row: Mapping[str, Any]) -> bool:
    return all(field in row for field in QUICK_STATS_FIELDS)


def parse_slice_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    product: NassProduct,
    release_watermark: str,
    slice_key: str,
) -> ReplayResult:
    """Parse one captured slice into observations and explicit quarantine rows."""
    observations: list[NassObservation] = []
    quarantined: list[QuarantinedObservation] = []
    for index, row in enumerate(rows):
        try:
            if not _required_fields_present(row):
                missing = sorted(
                    field for field in QUICK_STATS_FIELDS if field not in row
                )
                raise NassIdentityError(
                    f"record is missing consumed fields: {missing}"
                )
            observations.append(
                _observation(
                    row,
                    product=product,
                    release_watermark=release_watermark,
                    slice_key=slice_key,
                    index=index,
                )
            )
        except (NassIdentityError, NassValueError) as exc:
            quarantined.append(
                QuarantinedObservation(
                    slice_key=slice_key,
                    source_row_index=index,
                    error_code=_error_code(exc),
                    error_summary=str(exc),
                )
            )
    return ReplayResult(len(rows), tuple(observations), tuple(quarantined))


def _error_code(error: Exception) -> str:
    if isinstance(error, NassValueError):
        return "unregistered_value"
    return "unresolvable_identity"


def _observation(
    row: Mapping[str, Any],
    *,
    product: NassProduct,
    release_watermark: str,
    slice_key: str,
    index: int,
) -> NassObservation:
    commodity = commodity_identity(row)
    statistic = statistic_identity(row, product)
    domain = domain_identity(row)
    geography = geography_identity(row)
    period = period_identity(row)
    if commodity.commodity_desc != product.commodity_desc:
        raise NassIdentityError(
            f"record commodity {commodity.commodity_desc!r} is outside "
            f"product {product.product_id!r}"
        )
    if statistic.source_desc != product.source_desc:
        raise NassIdentityError(
            f"record source program {statistic.source_desc!r} is outside "
            f"product {product.product_id!r}"
        )
    value = parse_source_value(row.get("Value"))
    cv = parse_source_value(row.get("CV (%)"))
    return NassObservation(
        product_id=product.product_id,
        release_watermark=release_watermark,
        source_record_id=source_record_id(row),
        source_row={key: row[key] for key in sorted(row)},
        slice_key=slice_key,
        commodity=commodity,
        statistic=statistic,
        domain=domain,
        geography=geography,
        period=period,
        value_source=value.source_text,
        value=value.value,
        value_status=value.status,
        suppression_code=value.symbol,
        cv_source=cv.source_text,
        cv_value=cv.value,
        cv_status=cv.status,
        cv_symbol=cv.symbol,
        load_time=parse_load_time(row.get("load_time")),
        source_row_index=index,
    )


def replay_slices(
    product: NassProduct,
    slices: Sequence[CapturedSlicePayload],
    *,
    release_watermark: str,
) -> ReplayResult:
    """Replay a complete registered slice set without any network access.

    Every slice must checksum, decode, and match both its recorded capture row
    count and its provider preflight count. A release missing a registered slice
    or carrying a partial one cannot replay, so it cannot publish.
    """
    if not slices:
        raise NassReplayError("USDA NASS release has no captured slices")
    ordered = sorted(slices, key=lambda item: item.slice_key)
    seen: set[str] = set()
    observations: list[NassObservation] = []
    quarantined: list[QuarantinedObservation] = []
    input_count = 0
    for item in ordered:
        if item.slice_key in seen:
            raise NassReplayError(
                f"USDA NASS slice captured more than once: {item.slice_key}"
            )
        seen.add(item.slice_key)
        if hashlib.sha256(item.payload).hexdigest() != item.payload_checksum:
            raise NassReplayError(
                f"USDA NASS capture checksum mismatch: {item.capture_id}"
            )
        rows = decode_data_payload(item.payload)
        if len(rows) != item.captured_row_count:
            raise NassReplayError(
                f"USDA NASS captured row count does not match payload: "
                f"{item.slice_key}"
            )
        if len(rows) != item.provider_count:
            raise NassReplayError(
                f"USDA NASS slice is partial against its preflight: {item.slice_key}"
            )
        result = parse_slice_rows(
            rows,
            product=product,
            release_watermark=release_watermark,
            slice_key=item.slice_key,
        )
        input_count += result.input_count
        observations.extend(
            replace(observation, capture_id=item.capture_id)
            for observation in result.observations
        )
        quarantined.extend(result.quarantined)
    if input_count != len(observations) + len(quarantined):
        raise NassReplayError("USDA NASS replay reconciliation failed")
    return ReplayResult(input_count, tuple(observations), tuple(quarantined))
