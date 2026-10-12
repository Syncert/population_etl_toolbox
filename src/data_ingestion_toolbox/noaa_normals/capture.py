"""Capture-first orchestration for the NOAA climate normals archive.

One ingestion run per read: one request and one committed capture of the
archive, before anything parses it. NCEI publishes a new archive name for a
new version; a read whose bytes equal the last published capture is recorded
``unchanged`` and replays nothing.
"""

from __future__ import annotations

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

from .client import fetch_archive
from .config import SOURCE_CODE, NormalsConfig
from .registry import ARCHIVE_PATH, ARCHIVE_VERSION, NORMALS_BASE_URL

PARSER_CONTRACT_VERSION = "noaa_normals_csv:v1"


def _execute(
    connection_factory: Callable[[], Any], sql: str, parameters: tuple
) -> list[tuple]:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, parameters)
            rows = cursor.fetchall() if cursor.description else []
        connection.commit()
        return rows
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def capture_archive(
    connection_factory: Callable[[], Any],
    *,
    config: NormalsConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, str]:
    """Capture the registered archive; return (run_id, file status)."""
    runtime = config or NormalsConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    parameters = {"archive": ARCHIVE_VERSION}
    run_id = capture_control.start_run(watermark=dict(parameters))
    endpoint = f"{NORMALS_BASE_URL}{ARCHIVE_PATH}"
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=endpoint,
        parameters=parameters,
        max_attempts=runtime.max_attempts,
    )
    try:
        response = fetch_archive(
            config=runtime,
            client=client,
            on_retry=lambda error: capture_control.record_request_retry(
                request.request_id, error=error
            ),
        )
        receipt = persist_capture(
            connection_factory,
            ResponseCapture(
                capture_id=uuid4(),
                request_id=request.request_id,
                run_id=run_id,
                source_code=SOURCE_CODE,
                endpoint=endpoint,
                request_parameters=parameters,
                retrieved_at=datetime.now(UTC),
                http_status=response.http_status,
                response_headers=dict(response.response_headers),
                media_type="application/gzip",
                payload=response.raw_bytes,
                payload_schema_version=PARSER_CONTRACT_VERSION,
                source_revision=ARCHIVE_VERSION,
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
        previous = _execute(
            connection_factory,
            """
            SELECT payload_checksum FROM control.noaa_normals_file
            WHERE status = 'published' ORDER BY published_at DESC LIMIT 1
            """,
            (),
        )
        status = (
            "unchanged"
            if previous and previous[0][0] == receipt.payload_checksum
            else "captured"
        )
        _execute(
            connection_factory,
            """
            INSERT INTO control.noaa_normals_file (run_id, archive_version, capture_id, payload_checksum, status)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                str(run_id),
                ARCHIVE_VERSION,
                str(receipt.capture_id),
                receipt.payload_checksum,
                status,
            ),
        )
        capture_control.finish_run(run_id, status="success")
        return run_id, status
    except BaseException as exc:
        capture_control.finish_request(request.request_id, status="failed", error=exc)
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
