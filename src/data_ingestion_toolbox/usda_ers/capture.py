"""Capture-first orchestration for the USDA ERS county files.

One ingestion run per read of a registered file: one request and one
committed capture, before anything parses it. ERS replaces files in place
and sends no validator, so every read downloads the file; a read whose bytes
equal that file's last published capture is recorded ``unchanged`` and
replays nothing (the payload store keeps one copy of the bytes either way).
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
from .config import SOURCE_CODE, ErsConfig
from .registry import ERS_BASE_URL, ErsFile

PARSER_CONTRACT_VERSION = "usda_ers_csv:v1"


def _latest_published_checksum(
    connection_factory: Callable[[], Any], item: ErsFile
) -> str | None:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT payload_checksum FROM control.usda_ers_file
                WHERE product = %s AND edition = %s AND status = 'published'
                ORDER BY published_at DESC LIMIT 1
                """,
                (item.product, item.edition),
            )
            row = cursor.fetchone()
            return row[0] if row else None
    finally:
        connection.close()


def capture_file(
    connection_factory: Callable[[], Any],
    item: ErsFile,
    *,
    config: ErsConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, str]:
    """Capture one registered file; return (run_id, file status)."""
    runtime = config or ErsConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    parameters = {"product": item.product, "edition": item.edition}
    run_id = capture_control.start_run(watermark=dict(parameters))
    endpoint = f"{ERS_BASE_URL}{item.path}"
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=endpoint,
        parameters=parameters,
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
                request_parameters=parameters,
                retrieved_at=datetime.now(UTC),
                http_status=response.http_status,
                response_headers=dict(response.response_headers),
                media_type="application/zip" if item.member else "text/csv",
                payload=response.raw_bytes,
                payload_schema_version=PARSER_CONTRACT_VERSION,
                source_revision=item.key,
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
        unchanged = (
            _latest_published_checksum(connection_factory, item)
            == receipt.payload_checksum
        )
        status = "unchanged" if unchanged else "captured"
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.usda_ers_file (
                        run_id, product, edition, capture_id, payload_checksum, status
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(run_id),
                        item.product,
                        item.edition,
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
