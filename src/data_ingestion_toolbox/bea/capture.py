"""Capture-first orchestration for BEA regional tables.

One ingestion run per table: one request and one committed capture of the
table's zip. The bytes are committed before anything parses them.
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

from .client import fetch_table
from .config import BEA_BULK_BASE_URL, SOURCE_CODE, BeaConfig
from .registry import BeaTable

PARSER_CONTRACT_VERSION = "bea_regional_bulk_csv:v1"


def capture_table(
    connection_factory: Callable[[], Any],
    table: BeaTable,
    *,
    config: BeaConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, UUID]:
    """Capture one registered table; return (run_id, capture_id)."""
    runtime = config or BeaConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(watermark={"table_code": table.code})
    endpoint = f"{BEA_BULK_BASE_URL}{table.path}"
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=endpoint,
        parameters={"table": table.code},
        max_attempts=runtime.max_attempts,
    )
    try:
        response = fetch_table(
            table,
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
                request_parameters=dict(response.request_parameters),
                retrieved_at=datetime.now(UTC),
                http_status=response.http_status,
                response_headers=dict(response.response_headers),
                media_type="application/zip",
                payload=response.raw_bytes,
                payload_schema_version=PARSER_CONTRACT_VERSION,
                source_revision=table.code,
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.bea_table_capture (run_id, table_code, capture_id, status)
                    VALUES (%s, %s, %s, 'captured')
                    """,
                    (str(run_id), table.code, str(receipt.capture_id)),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        capture_control.finish_run(run_id, status="success")
        return run_id, receipt.capture_id
    except BaseException as exc:
        capture_control.finish_request(request.request_id, status="failed", error=exc)
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
