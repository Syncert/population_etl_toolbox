"""Pure USDA NASS release identity, preflight, and change decisions.

Quick Stats publishes no dataset-metadata document, so a release is identified
by evidence the provider does expose: the ``get_counts`` preflight for every
registered slice, the record field signature, and the maximum ``load_time``
across the captured rows. Every function here is pure except the final loader,
which reads previously accepted control state.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from .config import NassConfig
from .registry import QUICK_STATS_FIELDS, NassProduct

#: Quick Stats stamps ``load_time`` as a naive local timestamp, with or without
#: fractional seconds depending on the row.
_LOAD_TIME_FORMATS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")


class NassMetadataError(ValueError):
    """A captured payload cannot support a safe release decision."""


class ReleaseDecision(StrEnum):
    """Typed outcome of comparing provider evidence with accepted state."""

    UNCHANGED = "unchanged"
    INGEST = "ingest"
    OVER_LIMIT_QUARANTINE = "over_limit_quarantine"
    PARTIAL_SLICE_QUARANTINE = "partial_slice_quarantine"
    ROW_COUNT_DRIFT_QUARANTINE = "row_count_drift_quarantine"
    SCHEMA_CHANGE_QUARANTINE = "schema_change_quarantine"
    BACKWARD_WATERMARK_QUARANTINE = "backward_watermark_quarantine"
    INVALID_WATERMARK_QUARANTINE = "invalid_watermark_quarantine"


#: Decisions that permit a release to reach silver and gold.
PUBLISHABLE_DECISIONS = frozenset({ReleaseDecision.INGEST, ReleaseDecision.UNCHANGED})


@dataclass(frozen=True)
class NassSliceCount:
    """One preflighted slice and the record count the provider reported."""

    slice_key: str
    agg_level_desc: str
    year: int
    provider_count: int
    capture_id: str | None = None


@dataclass(frozen=True)
class NassReleaseContract:
    """Allowlisted evidence required to identify and version one extraction."""

    product_id: str
    parser_contract_version: str
    extraction_watermark: str
    total_row_count: int
    slice_counts: tuple[tuple[str, int], ...]
    field_signature: tuple[str, ...]

    @property
    def release_version(self) -> str:
        return self.extraction_watermark


def parse_load_time(value: object) -> datetime | None:
    """Parse one Quick Stats ``load_time`` stamp without guessing a format."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    for pattern in _LOAD_TIME_FORMATS:
        try:
            return datetime.strptime(text, pattern)
        except ValueError:
            continue
    return None


def format_watermark(moment: datetime) -> str:
    """Return the canonical textual watermark for one parsed ``load_time``."""
    return moment.strftime("%Y-%m-%d %H:%M:%S.%f")


def decode_data_payload(payload: bytes) -> list[Mapping[str, Any]]:
    """Decode one captured ``api_GET`` payload into its record list."""
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NassMetadataError("USDA NASS capture is not valid JSON") from exc
    if not isinstance(document, dict):
        raise NassMetadataError("USDA NASS capture must be a JSON object")
    rows = document.get("data")
    if not isinstance(rows, list):
        raise NassMetadataError("USDA NASS capture must carry a data list")
    for row in rows:
        if not isinstance(row, dict):
            raise NassMetadataError("USDA NASS records must be JSON objects")
    return rows


def decode_count_payload(payload: bytes) -> int:
    """Decode one captured ``get_counts`` payload into its record count."""
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NassMetadataError("USDA NASS count capture is not valid JSON") from exc
    if not isinstance(document, dict):
        raise NassMetadataError("USDA NASS count capture must be a JSON object")
    raw_count = document.get("count")
    if isinstance(raw_count, bool) or not isinstance(raw_count, (int, str)):
        raise NassMetadataError("USDA NASS count capture has no usable count")
    try:
        count = int(str(raw_count).strip())
    except ValueError as exc:
        raise NassMetadataError("USDA NASS count capture has no usable count") from exc
    if count < 0:
        raise NassMetadataError("USDA NASS count capture is negative")
    return count


def field_signature(rows: Iterable[Mapping[str, Any]]) -> tuple[str, ...]:
    """Return the sorted union of provider keys observed across records."""
    observed: set[str] = set()
    for row in rows:
        observed.update(str(key) for key in row)
    return tuple(sorted(observed))


def summarize_release(
    product: NassProduct,
    *,
    payloads: Sequence[bytes],
    slice_counts: Sequence[NassSliceCount],
) -> NassReleaseContract:
    """Build the release contract from captured bytes alone.

    The summary is derived after every capture has committed, so it never gates
    the raw-before-parse boundary; it only decides whether the committed
    evidence may progress to silver.
    """
    rows: list[Mapping[str, Any]] = []
    for payload in payloads:
        rows.extend(decode_data_payload(payload))
    watermarks = [
        moment
        for moment in (parse_load_time(row.get("load_time")) for row in rows)
        if moment is not None
    ]
    watermark = format_watermark(max(watermarks)) if watermarks else ""
    return NassReleaseContract(
        product_id=product.product_id,
        parser_contract_version=product.parser_contract_version,
        extraction_watermark=watermark,
        total_row_count=len(rows),
        slice_counts=tuple(
            (item.slice_key, item.provider_count) for item in slice_counts
        ),
        field_signature=field_signature(rows),
    )


def decide_preflight(
    product: NassProduct,
    config: NassConfig,
    slice_counts: Sequence[NassSliceCount],
    previous: NassReleaseContract | None,
) -> ReleaseDecision:
    """Return the safe next action using only preflight evidence.

    This runs before any record is retrieved, so an over-limit partition is
    refused instead of producing a truncated capture.
    """
    if any(item.provider_count > config.slice_record_limit for item in slice_counts):
        return ReleaseDecision.OVER_LIMIT_QUARANTINE
    current_counts = tuple(
        (item.slice_key, item.provider_count) for item in slice_counts
    )
    if previous is None:
        return ReleaseDecision.INGEST
    if previous.parser_contract_version != product.parser_contract_version:
        return ReleaseDecision.INGEST
    if current_counts == previous.slice_counts:
        return ReleaseDecision.UNCHANGED
    # Drift is only meaningful over slices both contracts registered: a
    # reviewed window expansion contributes new slice keys, and a recent-mode
    # run preflights a subset of a full-mode contract; neither is provider
    # row-count drift. Compare totals over the common slice keys only.
    previous_counts = dict(previous.slice_counts)
    overlap_previous = sum(
        previous_counts[key] for key, _ in current_counts if key in previous_counts
    )
    overlap_current = sum(
        count for key, count in current_counts if key in previous_counts
    )
    if overlap_previous > 0:
        drift = abs(overlap_current - overlap_previous) / overlap_previous
        if drift > config.row_count_change_threshold:
            return ReleaseDecision.ROW_COUNT_DRIFT_QUARANTINE
    return ReleaseDecision.INGEST


def decide_release(
    product: NassProduct,
    current: NassReleaseContract,
    previous: NassReleaseContract | None,
) -> ReleaseDecision:
    """Return the safe next action after every slice has been captured."""
    expected_fields = tuple(sorted(QUICK_STATS_FIELDS))
    if current.total_row_count and current.field_signature != expected_fields:
        return ReleaseDecision.SCHEMA_CHANGE_QUARANTINE
    if current.total_row_count and not current.extraction_watermark:
        return ReleaseDecision.INVALID_WATERMARK_QUARANTINE
    if previous is None:
        return ReleaseDecision.INGEST
    if previous.parser_contract_version != product.parser_contract_version:
        return ReleaseDecision.INGEST
    if current.extraction_watermark < previous.extraction_watermark:
        return ReleaseDecision.BACKWARD_WATERMARK_QUARANTINE
    if (
        current.extraction_watermark == previous.extraction_watermark
        and current.slice_counts == previous.slice_counts
    ):
        return ReleaseDecision.UNCHANGED
    return ReleaseDecision.INGEST


def load_latest_accepted_release(
    connection_factory: Callable[[], Any],
    product: NassProduct,
) -> NassReleaseContract | None:
    """Load the latest safe release contract for change comparison."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT product_id, parser_contract_version, extraction_watermark,
                       total_row_count, slice_counts, field_signature
                FROM control.usda_nass_release
                WHERE product_id = %s
                  AND decision IN ('ingest', 'unchanged')
                  AND status IN ('captured', 'silver_ready', 'published')
                ORDER BY extraction_watermark DESC, created_at DESC
                LIMIT 1
                """,
                (product.product_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return NassReleaseContract(
            product_id=row[0],
            parser_contract_version=row[1],
            extraction_watermark=row[2],
            total_row_count=int(row[3]),
            slice_counts=tuple((str(key), int(value)) for key, value in row[4]),
            field_signature=tuple(str(name) for name in row[5]),
        )
    finally:
        database_connection.close()
