"""CDC-specific orchestration over the shared raw/control foundation."""

from __future__ import annotations

import json
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

from .client import fetch_socrata_metadata, fetch_socrata_page, page_parameters
from .config import CdcConfig
from .metadata import CdcMetadata, MetadataDecision, decide_metadata, parse_metadata
from .registry import CdcAsset

SOURCE_CODE = "CDC"


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
                    page_count, complete
                ) VALUES (
                    %s, %s, %s, %s, %s, %s::JSONB, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    decision = EXCLUDED.decision,
                    status = EXCLUDED.status,
                    captured_row_count = EXCLUDED.captured_row_count,
                    page_count = EXCLUDED.page_count,
                    complete = EXCLUDED.complete,
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
) -> CapturedCdcRelease:
    """Capture metadata and, when changed, every deterministic source page."""
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

        offset = 0
        row_count = 0
        page_capture_ids: list[UUID] = []
        while True:
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
            row_count += page.row_count
            if page.row_count < runtime_config.socrata_page_size:
                break
            offset += page.row_count
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
        )
    except BaseException as exc:
        if active_request_id is not None:
            capture_control.finish_request(
                active_request_id, status="failed", error=exc
            )
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
