"""Checksum-backed, complete-sequence offline replay for CDC captures."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any
from uuid import UUID

from psycopg2.extras import Json, execute_values

from ..registry import CdcAsset
from .cdi import parse_cdi_rows
from .models import CdcObservation, QuarantinedObservation, ReplayResult
from .places_county import parse_places_county_rows


class CdcReplayError(ValueError):
    """Captured pages cannot form a complete registered release."""


@dataclass(frozen=True)
class CapturedPage:
    capture_id: UUID
    offset: int
    limit: int
    row_count: int
    payload: bytes
    payload_checksum: str


def _rows(page: CapturedPage) -> list[object]:
    if hashlib.sha256(page.payload).hexdigest() != page.payload_checksum:
        raise CdcReplayError(f"CDC capture checksum mismatch: {page.capture_id}")
    try:
        value = json.loads(page.payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CdcReplayError("CDC capture is not valid JSON") from exc
    if not isinstance(value, list):
        raise CdcReplayError("CDC observation capture must be a JSON list")
    if len(value) != page.row_count:
        raise CdcReplayError("CDC captured row count does not match payload")
    return value


def replay_pages(
    asset: CdcAsset,
    pages: list[CapturedPage],
    *,
    release_watermark: str,
) -> ReplayResult:
    """Replay a complete deterministic page sequence without network access."""
    if not pages:
        raise CdcReplayError("CDC release has no captured observation pages")
    ordered = sorted(pages, key=lambda page: page.offset)
    expected_offset = 0
    parsed: list[tuple[CapturedPage, list[object]]] = []
    for page in ordered:
        if page.limit < 1 or page.offset != expected_offset:
            raise CdcReplayError("CDC capture page sequence is incomplete")
        rows = _rows(page)
        parsed.append((page, rows))
        expected_offset += len(rows)
    if ordered[-1].row_count >= ordered[-1].limit:
        raise CdcReplayError("CDC release lacks a terminating short page")

    observations: list[CdcObservation] = []
    quarantined: list[QuarantinedObservation] = []
    input_count = 0
    for page, rows in parsed:
        result = parse_page(
            asset,
            capture_id=page.capture_id,
            offset=page.offset,
            rows=rows,
            release_watermark=release_watermark,
        )
        input_count += result.input_count
        observations.extend(result.observations)
        quarantined.extend(result.quarantined)
    if input_count != len(observations) + len(quarantined):
        raise CdcReplayError("CDC replay reconciliation failed")
    return ReplayResult(input_count, tuple(observations), tuple(quarantined))


def parse_page(
    asset: CdcAsset,
    *,
    capture_id: UUID,
    offset: int,
    rows: list[object],
    release_watermark: str,
) -> ReplayResult:
    """Parse one captured page; row indexes count from the release's first row."""
    if asset.asset_id == "cdi":
        result = parse_cdi_rows(rows, release_watermark=release_watermark, asset=asset)
    elif asset.asset_id in ("places_county", "places_tract"):
        result = parse_places_county_rows(
            rows, release_watermark=release_watermark, asset=asset
        )
    else:
        raise CdcReplayError(f"no CDC parser registered for asset: {asset.asset_id}")
    return ReplayResult(
        result.input_count,
        tuple(
            replace(
                observation,
                capture_id=capture_id,
                source_row_index=offset + int(observation.source_row_index or 0),
            )
            for observation in result.observations
        ),
        tuple(
            replace(item, source_row_index=offset + item.source_row_index)
            for item in result.quarantined
        ),
    )


def load_captured_pages(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    asset: CdcAsset,
) -> list[CapturedPage]:
    """Load exact page bytes and recorded positions for offline replay."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT capture.capture_id,
                       (capture.request_parameters ->> '$offset')::INTEGER,
                       (capture.request_parameters ->> '$limit')::INTEGER,
                       blob.payload, blob.payload_checksum
                FROM raw_capture.response_capture AS capture
                JOIN raw_capture.payload_blob AS blob USING (payload_checksum)
                WHERE capture.run_id = %s
                  AND capture.source_code = 'CDC'
                  AND capture.endpoint = %s
                  AND capture.request_parameters ? '$offset'
                ORDER BY (capture.request_parameters ->> '$offset')::INTEGER
                """,
                (str(run_id), asset.api_path),
            )
            rows = cursor.fetchall()
    finally:
        database_connection.close()
    pages: list[CapturedPage] = []
    for capture_id, offset, limit, payload, checksum in rows:
        raw = bytes(payload)
        try:
            decoded = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CdcReplayError("CDC capture is not valid JSON") from exc
        if not isinstance(decoded, list):
            raise CdcReplayError("CDC observation capture must be a JSON list")
        pages.append(
            CapturedPage(
                capture_id=capture_id,
                offset=offset,
                limit=limit,
                row_count=len(decoded),
                payload=raw,
                payload_checksum=checksum,
            )
        )
    return pages


def replay_captured_run(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    asset: CdcAsset,
    release_watermark: str,
) -> ReplayResult:
    """Replay one complete run solely from durable capture bytes."""
    return replay_pages(
        asset,
        load_captured_pages(connection_factory, run_id=run_id, asset=asset),
        release_watermark=release_watermark,
    )


@dataclass(frozen=True)
class RecordedPage:
    """One page a run recorded in ``control.cdc_release_page``, bytes not loaded."""

    capture_id: UUID
    offset: int
    limit: int
    row_count: int


@dataclass(frozen=True)
class StreamedReplay:
    """What a page-by-page replay read and stored."""

    input_count: int
    observation_count: int
    quarantine_count: int


def recorded_pages(
    connection_factory: Callable[[], Any], *, run_id: UUID
) -> list[RecordedPage]:
    """The pages a run recorded, in offset order (empty for an older run)."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT capture_id::TEXT, page_offset, page_limit, row_count
                FROM control.cdc_release_page
                WHERE run_id = %s ORDER BY page_offset
                """,
                (str(run_id),),
            )
            rows = cursor.fetchall()
    finally:
        database_connection.close()
    return [
        RecordedPage(UUID(capture_id), int(offset), int(limit), int(count))
        for capture_id, offset, limit, count in rows
    ]


def _page_payload(
    connection_factory: Callable[[], Any], capture_id: UUID
) -> tuple[bytes, str]:
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT blob.payload, blob.payload_checksum
                FROM raw_capture.response_capture AS capture
                JOIN raw_capture.payload_blob AS blob USING (payload_checksum)
                WHERE capture.capture_id = %s AND capture.source_code = 'CDC'
                """,
                (str(capture_id),),
            )
            row = cursor.fetchone()
    finally:
        database_connection.close()
    if row is None:
        raise CdcReplayError(f"CDC page capture is absent: {capture_id}")
    return bytes(row[0]), row[1]


def replay_and_persist_run(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    asset: CdcAsset,
    release_watermark: str,
) -> StreamedReplay:
    """Replay a run page by page, storing each page before reading the next.

    A run that recorded its pages (every run since the tract release) is read
    from ``control.cdc_release_page`` one page's bytes at a time, so a
    three-million-row release never sits in memory whole; the sequence is
    checked complete before any page is parsed. An older run without page
    rows is replayed whole, as before. Storing is idempotent, so a replay
    that stops part way is simply run again.
    """
    pages = recorded_pages(connection_factory, run_id=run_id)
    if not pages:
        result = replay_captured_run(
            connection_factory,
            run_id=run_id,
            asset=asset,
            release_watermark=release_watermark,
        )
        persist_replay_result(
            connection_factory,
            run_id=run_id,
            asset=asset,
            release_watermark=release_watermark,
            result=result,
        )
        return StreamedReplay(
            result.input_count, len(result.observations), len(result.quarantined)
        )
    expected = 0
    for page in pages:
        if page.offset != expected or page.row_count > page.limit:
            raise CdcReplayError("CDC capture page sequence is incomplete")
        expected += page.row_count
    if pages[-1].row_count >= pages[-1].limit:
        raise CdcReplayError("CDC release lacks a terminating short page")
    totals = [0, 0, 0]
    for page in pages:
        payload, checksum = _page_payload(connection_factory, page.capture_id)
        rows = _rows(
            CapturedPage(
                page.capture_id,
                page.offset,
                page.limit,
                page.row_count,
                payload,
                checksum,
            )
        )
        result = parse_page(
            asset,
            capture_id=page.capture_id,
            offset=page.offset,
            rows=rows,
            release_watermark=release_watermark,
        )
        if result.input_count != len(result.observations) + len(result.quarantined):
            raise CdcReplayError("CDC replay reconciliation failed")
        persist_replay_result(
            connection_factory,
            run_id=run_id,
            asset=asset,
            release_watermark=release_watermark,
            result=result,
        )
        totals[0] += result.input_count
        totals[1] += len(result.observations)
        totals[2] += len(result.quarantined)
    return StreamedReplay(*totals)


def _stratum_id(observation: CdcObservation) -> str:
    canonical = json.dumps(
        observation.strata, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def persist_replay_result(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    asset: CdcAsset,
    release_watermark: str,
    result: ReplayResult,
) -> None:
    """Persist capture-scoped source revisions and explicit quarantine rows."""
    if result.input_count != len(result.observations) + len(result.quarantined):
        raise CdcReplayError("CDC replay reconciliation failed before persistence")
    observations = [
        (
            str(item.capture_id),
            item.source_row_index,
            str(run_id),
            item.dataset,
            item.release_watermark,
            item.source_record_id,
            Json(item.source_row),
            item.measure_id,
            item.measure_label,
            item.topic,
            item.period_start,
            item.period_end,
            item.geo_source_code,
            item.geo_source_label,
            item.geo_type,
            item.geo_id,
            item.value_source,
            item.value,
            item.value_status,
            item.unit,
            item.value_type_id,
            item.value_type_label,
            item.adjustment_status,
            item.confidence_lower,
            item.confidence_upper,
            item.footnote_code,
            item.footnote_text,
            _stratum_id(item),
            Json(item.strata),
            item.estimate_method,
            item.population_basis,
            item.total_population,
            item.population_18_plus,
        )
        for item in result.observations
    ]
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            if observations:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_cdc.observation_revision (
                        capture_id, source_row_index, run_id, asset_id,
                        release_watermark, source_record_id, source_record,
                        measure_id, measure_label, topic, period_start, period_end,
                        geo_source_code, geo_source_label, geo_type, geo_id,
                        value_source, value, value_status, unit, value_type_id,
                        value_type_label, adjustment_status, confidence_lower,
                        confidence_upper, footnote_code, footnote_text, stratum_id,
                        strata, estimate_method, population_basis,
                        total_population, population_18_plus
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index) DO NOTHING
                    """,
                    observations,
                )
            if result.quarantined:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_cdc.observation_quarantine (
                        run_id, asset_id, release_watermark, source_row_index,
                        error_code, error_summary
                    ) VALUES %s
                    ON CONFLICT (
                        run_id, asset_id, release_watermark,
                        source_row_index, error_code
                    ) DO NOTHING
                    """,
                    [
                        (
                            str(run_id),
                            asset.asset_id,
                            release_watermark,
                            item.source_row_index,
                            item.error_code,
                            item.error_summary,
                        )
                        for item in result.quarantined
                    ],
                )
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
