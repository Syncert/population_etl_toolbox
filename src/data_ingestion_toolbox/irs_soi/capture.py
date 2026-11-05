"""Capture-first orchestration for SOI income files.

One ingestion run per registered file (product and tax year): one request
and one committed capture. The bytes are committed before anything parses
them, and the control row records the capture before replay counts it.
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
from .config import IRS_SOI_BASE_URL, SOURCE_CODE, IrsSoiConfig
from .registry import CSV, SoiFile

PARSER_CONTRACT_VERSION = "irs_soi_income:v1"


def capture_file(
    connection_factory: Callable[[], Any],
    item: SoiFile,
    *,
    config: IrsSoiConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, UUID]:
    """Capture one registered file; return (run_id, capture_id)."""
    runtime = config or IrsSoiConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    parameters = {"kind": item.kind, "tax_year": str(item.tax_year)}
    run_id = capture_control.start_run(watermark=dict(parameters))
    endpoint = f"{IRS_SOI_BASE_URL}{item.path}"
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=endpoint,
        parameters=dict(parameters),
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
                request_parameters=dict(response.request_parameters),
                retrieved_at=datetime.now(UTC),
                http_status=response.http_status,
                response_headers=dict(response.response_headers),
                media_type="text/csv" if item.container == CSV else "application/zip",
                payload=response.raw_bytes,
                payload_schema_version=PARSER_CONTRACT_VERSION,
                source_revision=item.key,
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.irs_soi_file (
                        run_id, file_kind, tax_year, capture_id, status
                    ) VALUES (%s, %s, %s, %s, 'captured')
                    """,
                    (
                        str(run_id),
                        item.kind,
                        item.tax_year,
                        str(receipt.capture_id),
                    ),
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
