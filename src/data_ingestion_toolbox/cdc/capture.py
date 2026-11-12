"""CDC-specific orchestration over the shared raw/control foundation."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from data_ingestion_toolbox.capture import (
    CaptureControl,
    CaptureReceipt,
    ResponseCapture,
    persist_response_capture,
)

from .client import (
    fetch_socrata_count,
    fetch_socrata_metadata,
    fetch_socrata_page,
    page_parameters,
)
from .config import CdcConfig
from .metadata import CdcMetadata, MetadataDecision, decide_metadata, parse_metadata
from .registry import CdcAsset

SOURCE_CODE = "CDC"


class CdcCaptureError(RuntimeError):
    """A captured release that cannot be complete: the run fails, nothing replays."""

    def __init__(self, asset_id: str, *, code: str, detail: str) -> None:
        self.asset_id = asset_id
        self.code = code
        super().__init__(f"CDC {asset_id} capture refused ({code}): {detail}")


@dataclass(frozen=True)
class CapturedCdcRelease:
    """Durable capture lineage for one metadata decision and page sequence."""

    run_id: UUID
    asset_id: str
    metadata_capture_id: UUID
    page_capture_ids: tuple[UUID, ...]
    metadata: CdcMetadata
    decision: MetadataDecision
    row_count: int
    complete: bool
    #: The provider's own ``count(*)`` for a bulk asset; None otherwise.
    counted_row_count: int | None = None


def persist_release_state(
    connection_factory: Callable[[], Any],
    release: CapturedCdcRelease,
) -> None:
    """Persist one typed metadata/capture decision in the control plane."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.cdc_dataset_release (
                    run_id, asset_id, socrata_id, title, release_watermark,
                    schema_contract, provider_row_count, license_id,
                    metadata_capture_id, decision, status, captured_row_count,
                    page_count, complete, counted_row_count
                ) VALUES (
                    %s, %s, %s, %s, %s, %s::JSONB, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    decision = EXCLUDED.decision,
                    status = EXCLUDED.status,
                    captured_row_count = EXCLUDED.captured_row_count,
                    page_count = EXCLUDED.page_count,
                    complete = EXCLUDED.complete,
                    counted_row_count = EXCLUDED.counted_row_count,
                    updated_at = NOW()
                """,
                (
                    str(release.run_id),
                    release.asset_id,
                    release.metadata.socrata_id,
                    release.metadata.title,
                    release.metadata.watermark,
                    json.dumps(release.metadata.columns),
                    release.metadata.row_count,
                    release.metadata.license_id,
                    str(release.metadata_capture_id),
                    release.decision.value,
                    (
                        "captured"
                        if release.decision
                        in (MetadataDecision.INGEST, MetadataDecision.UNCHANGED)
                        else "quarantined"
                    ),
                    release.row_count,
                    len(release.page_capture_ids),
                    release.complete,
                    release.counted_row_count,
                ),
            )
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()


def _capture(
    connection_factory: Callable[[], Any],
    *,
    control: CaptureControl,
    run_id: UUID,
    endpoint: str,
    parameters: dict[str, object],
    payload: bytes,
    response_headers: dict[str, str],
    http_status: int,
    parser_version: str,
    source_revision: str | None,
    request_id: UUID,
    persist_capture: Callable[[Callable[[], Any], ResponseCapture], CaptureReceipt],
) -> UUID:
    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=request_id,
        run_id=run_id,
        source_code=SOURCE_CODE,
        endpoint=endpoint,
        request_parameters=parameters,
        retrieved_at=datetime.now(UTC),
        http_status=http_status,
        response_headers=response_headers,
        media_type="application/json",
        payload=payload,
        payload_schema_version=parser_version,
        source_revision=source_revision,
    )
    receipt = persist_capture(connection_factory, capture)
    control.finish_request(request_id, status="captured")
    return receipt.capture_id


def _record_page(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    asset: CdcAsset,
    watermark: int,
    offset: int,
    limit: int,
    row_count: int,
    capture_id: UUID,
    resumed_from_run_id: UUID | None = None,
) -> None:
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.cdc_release_page (
                    run_id, asset_id, release_watermark, page_offset, page_limit,
                    row_count, capture_id, resumed_from_run_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(run_id),
                    asset.asset_id,
                    watermark,
                    offset,
                    limit,
                    row_count,
                    str(capture_id),
                    str(resumed_from_run_id) if resumed_from_run_id else None,
                ),
            )
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()


def resumable_pages(
    connection_factory: Callable[[], Any],
    asset: CdcAsset,
    *,
    watermark: int,
    page_size: int,
) -> tuple[UUID | None, list[tuple[int, int, UUID]]]:
    """The full pages an unfinished run of the same release already captured.

    Returns (that run, [(offset, rows, capture)]) for the contiguous run of
    full pages from offset 0 of the newest run of this asset, watermark and
    page size that never recorded a release; (None, []) when there is none.
    A short page is never reused: it is where the release ended, so its run
    either finished or failed after reading it, and is read again.
    """
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT page.run_id::TEXT
                FROM control.cdc_release_page AS page
                WHERE page.asset_id = %s
                  AND page.release_watermark = %s
                  AND page.page_limit = %s
                  AND NOT EXISTS (
                      SELECT 1 FROM control.cdc_dataset_release AS release
                      WHERE release.run_id = page.run_id
                  )
                ORDER BY page.created_at DESC
                LIMIT 1
                """,
                (asset.asset_id, watermark, page_size),
            )
            row = cursor.fetchone()
            if row is None:
                return None, []
            cursor.execute(
                """
                SELECT page_offset, row_count, capture_id::TEXT
                FROM control.cdc_release_page
                WHERE run_id = %s ORDER BY page_offset
                """,
                (row[0],),
            )
            pages = cursor.fetchall()
    finally:
        database_connection.close()
    reused: list[tuple[int, int, UUID]] = []
    expected = 0
    for offset, rows, capture_id in pages:
        if int(offset) != expected or int(rows) != page_size:
            break
        reused.append((int(offset), int(rows), UUID(capture_id)))
        expected += int(rows)
    return (UUID(row[0]) if reused else None), reused


class PageLedger:
    """Where a run records its pages and finds an unfinished run to resume.

    The default writes ``control.cdc_release_page``; a test passes its own.
    """

    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        self.connection_factory = connection_factory

    def record(self, **page: Any) -> None:
        _record_page(self.connection_factory, **page)

    def resumable(
        self, asset: CdcAsset, *, watermark: int, page_size: int
    ) -> tuple[UUID | None, list[tuple[int, int, UUID]]]:
        return resumable_pages(
            self.connection_factory, asset, watermark=watermark, page_size=page_size
        )


def _count_rows(payload: bytes, asset: CdcAsset) -> int:
    try:
        value = json.loads(payload)
        return int(value[0]["count"])
    except (ValueError, KeyError, IndexError, TypeError) as exc:
        raise CdcCaptureError(
            asset.asset_id, code="unreadable_count", detail="count(*) answer"
        ) from exc


def capture_asset_release(
    connection_factory: Callable[[], Any],
    asset: CdcAsset,
    *,
    previous_metadata: CdcMetadata | None = None,
    config: CdcConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
    page_ledger: PageLedger | None = None,
) -> CapturedCdcRelease:
    """Capture metadata and, when changed, every deterministic source page."""
    ledger = page_ledger or PageLedger(connection_factory)
    runtime_config = config or CdcConfig.from_environment()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(watermark={"asset_id": asset.asset_id})
    active_request_id: UUID | None = None
    try:
        request = capture_control.start_request(
            run_id=run_id,
            endpoint=asset.metadata_path,
            parameters={},
            max_attempts=runtime_config.socrata_max_attempts,
        )
        active_request_id = request.request_id
        metadata_response = fetch_socrata_metadata(
            asset,
            config=runtime_config,
            client=client,
            on_retry=lambda error: capture_control.record_request_retry(
                request.request_id, error=error
            ),
        )
        metadata_capture_id = _capture(
            connection_factory,
            control=capture_control,
            run_id=run_id,
            endpoint=asset.metadata_path,
            parameters={},
            payload=metadata_response.raw_bytes,
            response_headers=dict(metadata_response.response_headers),
            http_status=metadata_response.http_status,
            parser_version=asset.parser_contract_version,
            source_revision=None,
            request_id=request.request_id,
            persist_capture=persist_capture,
        )
        active_request_id = None
        metadata = parse_metadata(metadata_response.raw_bytes, asset)
        watermark = {
            "asset_id": asset.asset_id,
            "release_watermark": metadata.watermark,
        }
        capture_control.set_run_watermark(run_id, watermark=watermark)
        decision = decide_metadata(asset, metadata, previous_metadata)
        if decision is MetadataDecision.UNCHANGED:
            capture_control.finish_run(run_id, status="success")
            return CapturedCdcRelease(
                run_id,
                asset.asset_id,
                metadata_capture_id,
                (),
                metadata,
                decision,
                0,
                True,
            )
        if decision is not MetadataDecision.INGEST:
            capture_control.quarantine(
                capture_id=metadata_capture_id,
                run_id=run_id,
                parser_version=asset.parser_contract_version,
                error_code=decision.value,
                error=decision.value,
            )
            capture_control.finish_run(run_id, status="partial")
            return CapturedCdcRelease(
                run_id,
                asset.asset_id,
                metadata_capture_id,
                (),
                metadata,
                decision,
                0,
                False,
            )

        if asset.bulk:
            runtime_config = runtime_config.model_copy(
                update={
                    "socrata_page_size": runtime_config.socrata_bulk_page_size,
                    "socrata_timeout_seconds": max(
                        runtime_config.socrata_timeout_seconds,
                        runtime_config.socrata_bulk_timeout_seconds,
                    ),
                }
            )
        offset = 0
        row_count = 0
        page_capture_ids: list[UUID] = []
        if asset.bulk:
            resumed_from, reused = ledger.resumable(
                asset,
                watermark=metadata.watermark,
                page_size=runtime_config.socrata_page_size,
            )
            for reused_offset, reused_rows, reused_capture in reused:
                ledger.record(
                    run_id=run_id,
                    asset=asset,
                    watermark=metadata.watermark,
                    offset=reused_offset,
                    limit=runtime_config.socrata_page_size,
                    row_count=reused_rows,
                    capture_id=reused_capture,
                    resumed_from_run_id=resumed_from,
                )
                page_capture_ids.append(reused_capture)
                row_count += reused_rows
                offset = reused_offset + reused_rows
        while True:
            if asset.bulk and page_capture_ids:
                time.sleep(runtime_config.socrata_min_spacing_seconds)
            parameters = page_parameters(
                asset,
                page_size=runtime_config.socrata_page_size,
                offset=offset,
            )
            page_request = capture_control.start_request(
                run_id=run_id,
                endpoint=asset.api_path,
                parameters=parameters,
                max_attempts=runtime_config.socrata_max_attempts,
            )
            active_request_id = page_request.request_id
            page = fetch_socrata_page(
                asset,
                offset=offset,
                page_size=runtime_config.socrata_page_size,
                config=runtime_config,
                client=client,
                on_retry=lambda error, request_id=page_request.request_id: (
                    capture_control.record_request_retry(request_id, error=error)
                ),
            )
            page_capture_ids.append(
                _capture(
                    connection_factory,
                    control=capture_control,
                    run_id=run_id,
                    endpoint=asset.api_path,
                    parameters=dict(page.request_parameters),
                    payload=page.raw_bytes,
                    response_headers=dict(page.response_headers),
                    http_status=page.http_status,
                    parser_version=asset.parser_contract_version,
                    source_revision=metadata.release_version,
                    request_id=page_request.request_id,
                    persist_capture=persist_capture,
                )
            )
            active_request_id = None
            ledger.record(
                run_id=run_id,
                asset=asset,
                watermark=metadata.watermark,
                offset=offset,
                limit=runtime_config.socrata_page_size,
                row_count=page.row_count,
                capture_id=page_capture_ids[-1],
            )
            row_count += page.row_count
            if page.row_count < runtime_config.socrata_page_size:
                break
            offset += page.row_count
        counted: int | None = None
        if asset.bulk:
            count_request = capture_control.start_request(
                run_id=run_id,
                endpoint=asset.api_path,
                parameters={"$select": "count(*)"},
                max_attempts=runtime_config.socrata_max_attempts,
            )
            active_request_id = count_request.request_id
            answer = fetch_socrata_count(
                asset,
                config=runtime_config,
                client=client,
                on_retry=lambda error, request_id=count_request.request_id: (
                    capture_control.record_request_retry(request_id, error=error)
                ),
            )
            _capture(
                connection_factory,
                control=capture_control,
                run_id=run_id,
                endpoint=asset.api_path,
                parameters=dict(answer.request_parameters),
                payload=answer.raw_bytes,
                response_headers=dict(answer.response_headers),
                http_status=answer.http_status,
                parser_version=asset.parser_contract_version,
                source_revision=metadata.release_version,
                request_id=count_request.request_id,
                persist_capture=persist_capture,
            )
            active_request_id = None
            counted = _count_rows(answer.raw_bytes, asset)
            if counted != row_count:
                raise CdcCaptureError(
                    asset.asset_id,
                    code="paged_total_differs",
                    detail=f"pages hold {row_count} rows, count(*) answers {counted}",
                )
        capture_control.finish_run(run_id, status="success")
        return CapturedCdcRelease(
            run_id,
            asset.asset_id,
            metadata_capture_id,
            tuple(page_capture_ids),
            metadata,
            decision,
            row_count,
            True,
            counted,
        )
    except BaseException as exc:
        if active_request_id is not None:
            capture_control.finish_request(
                active_request_id, status="failed", error=exc
            )
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
