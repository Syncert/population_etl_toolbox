"""Capture-first orchestration for the FHFA annual HPI workbooks.

One ingestion run per read of a file: one request and one committed capture
of the workbook, before anything parses it. FHFA revises every year's index
in each new file and sends no validator, so every read downloads the file;
a read whose bytes equal the last published file's is recorded
``unchanged`` and replays nothing (the payload store keeps one copy of the
bytes either way).
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

from .client import fetch_file
from .config import SOURCE_CODE, HpiConfig
from .registry import FHFA_BASE_URL, HpiFile

PARSER_CONTRACT_VERSION = "fhfa_hpi_xlsx:v1"


def _latest_published_checksum(
    connection_factory: Callable[[], Any], kind: str
) -> str | None:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT payload_checksum FROM control.fhfa_hpi_file
                WHERE kind = %s AND status = 'published'
                ORDER BY published_at DESC LIMIT 1
                """,
                (kind,),
            )
            row = cursor.fetchone()
            return row[0] if row else None
    finally:
        connection.close()


def capture_file(
    connection_factory: Callable[[], Any],
    item: HpiFile,
    *,
    config: HpiConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, str]:
    """Capture one registered file; return (run_id, file status)."""
    runtime = config or HpiConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(watermark={"kind": item.kind})
    endpoint = f"{FHFA_BASE_URL}{item.path}"
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=endpoint,
        parameters={"kind": item.kind},
        max_attempts=runtime.max_attempts,
    )
    try:
        response = fetch_file(
            item,
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
                request_parameters={"kind": item.kind},
                retrieved_at=datetime.now(UTC),
                http_status=response.http_status,
                response_headers=dict(response.response_headers),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                payload=response.raw_bytes,
                payload_schema_version=PARSER_CONTRACT_VERSION,
                source_revision=item.key,
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
        unchanged = (
            _latest_published_checksum(connection_factory, item.kind)
            == receipt.payload_checksum
        )
        status = "unchanged" if unchanged else "captured"
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.fhfa_hpi_file (run_id, kind, capture_id, payload_checksum, status)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        str(run_id),
                        item.kind,
                        str(receipt.capture_id),
                        receipt.payload_checksum,
                        status,
                    ),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        capture_control.finish_run(run_id, status="success")
        return run_id, status
    except BaseException as exc:
        capture_control.finish_request(request.request_id, status="failed", error=exc)
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
