"""Capture-first orchestration for the registered IPEDS files.

Each file a run reads is its own ingestion run: one request and, when NCES
serves the zip, one committed capture of its bytes before anything parses
them. NCES answering 404 records the file ``not_published`` with no capture.
A zip whose bytes and parser contract equal the same file's last published
capture is recorded ``unchanged`` and replays nothing (utility.unchanged).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from data_ingestion_toolbox.capture import (
    CaptureControl,
    CaptureReceipt,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.utility.unchanged import repeats_last_publication

from .client import IpedsNotPublished, fetch_file
from .config import SOURCE_CODE, NcesIpedsConfig
from .registry import Component

#: The file's newest published capture.
LATEST_PUBLISHED_SQL = """
    SELECT response.payload_checksum, response.payload_schema_version, file.published_at
    FROM control.nces_ipeds_file AS file
    JOIN raw_capture.response_capture AS response ON response.capture_id = file.capture_id
    WHERE file.component_id = %s AND file.collection_year = %s AND file.status = 'published'
    ORDER BY file.published_at DESC
    LIMIT 1
"""

PersistCapture = Callable[[Callable[[], Any], ResponseCapture], CaptureReceipt]


def pause_between_files(config: NcesIpedsConfig) -> None:
    """Wait the configured spacing before the next file of a run."""
    time.sleep(config.min_spacing_seconds)


def file_url(component: Component, year: int, config: NcesIpedsConfig) -> str:
    return f"{config.data_base_url}{component.file_stem(year)}.zip"


def _record(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    component: Component,
    year: int,
    url: str,
    capture_id: UUID | None,
    checksum: str | None,
    status: str,
) -> None:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.nces_ipeds_file (
                    run_id, component_id, collection_year, file_stem, download_url,
                    capture_id, payload_checksum, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(run_id),
                    component.component_id,
                    year,
                    component.file_stem(year),
                    url,
                    str(capture_id) if capture_id else None,
                    checksum,
                    status,
                ),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def capture_file(
    connection_factory: Callable[[], Any],
    component: Component,
    year: int,
    *,
    config: NcesIpedsConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: PersistCapture = persist_response_capture,
) -> tuple[UUID, str]:
    """Capture one component's file for one year; return (run_id, file status)."""
    runtime = config or NcesIpedsConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    url = file_url(component, year, runtime)
    parameters = {"component": component.component_id, "collection_year": year}
    run_id = capture_control.start_run(watermark=dict(parameters))
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=url,
        parameters=parameters,
        max_attempts=runtime.max_attempts,
    )
    try:
        try:
            response = fetch_file(
                url,
                config=runtime,
                client=client,
                on_retry=lambda error: capture_control.record_request_retry(
                    request.request_id, error=error
                ),
            )
        except IpedsNotPublished as absent:
            capture_control.finish_request(
                request.request_id, status="empty", error=absent
            )
            _record(
                connection_factory,
                run_id=run_id,
                component=component,
                year=year,
                url=url,
                capture_id=None,
                checksum=None,
                status="not_published",
            )
            capture_control.finish_run(run_id, status="success")
            return run_id, "not_published"
        receipt = persist_capture(
            connection_factory,
            ResponseCapture(
                capture_id=uuid4(),
                request_id=request.request_id,
                run_id=run_id,
                source_code=SOURCE_CODE,
                endpoint=url,
                request_parameters=parameters,
                retrieved_at=datetime.now(UTC),
                http_status=response.http_status,
                response_headers=dict(response.response_headers),
                media_type="application/zip",
                payload=response.raw_bytes,
                payload_schema_version=component.parser_contract_version,
                source_revision=response.response_headers.get("last-modified"),
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
        unchanged = repeats_last_publication(
            connection_factory,
            LATEST_PUBLISHED_SQL,
            (component.component_id, year),
            payload_checksum=receipt.payload_checksum,
            parser_contract=component.parser_contract_version,
        )
        status = "unchanged" if unchanged else "captured"
        _record(
            connection_factory,
            run_id=run_id,
            component=component,
            year=year,
            url=url,
            capture_id=receipt.capture_id,
            checksum=receipt.payload_checksum,
            status=status,
        )
        capture_control.finish_run(run_id, status="success")
        return run_id, status
    except BaseException as exc:
        capture_control.finish_request(request.request_id, status="failed", error=exc)
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
