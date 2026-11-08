"""Capture-first orchestration for the NDCP workbook.

One ingestion run per read of a registered release: one request and one
committed capture, before anything parses it. The Women's Bureau replaces
the file in place, so every read downloads it; a read whose bytes repeat
the release's last published capture is recorded ``unchanged`` and replays
nothing (``utility.unchanged``). The payload store keeps one copy of the
bytes either way.
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
from data_ingestion_toolbox.utility.unchanged import repeats_last_publication

from .client import fetch_release
from .config import DOL_BASE_URL, SOURCE_CODE, ChildcareConfig
from .registry import NdcpRelease

PARSER_CONTRACT_VERSION = "dol_ndcp_xlsx:v1"

XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

#: The release's newest published capture. The revision table stores only
#: prices that changed (CHURN RULE), so it cannot list every geography a
#: capture names: any geography added since forces a replay, which then
#: stores nothing that repeats.
LATEST_PUBLISHED_SQL = """
    SELECT response.payload_checksum, response.payload_schema_version, file.published_at
    FROM control.dol_childcare_file AS file
    JOIN raw_capture.response_capture AS response ON response.capture_id = file.capture_id
    WHERE file.release = %s AND file.status = 'published'
    ORDER BY file.published_at DESC
    LIMIT 1
"""


def capture_release(
    connection_factory: Callable[[], Any],
    release: NdcpRelease,
    *,
    config: ChildcareConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, str]:
    """Capture one registered workbook; return (run_id, file status)."""
    runtime = config or ChildcareConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    parameters = {"release": release.key}
    run_id = capture_control.start_run(watermark=dict(parameters))
    endpoint = f"{DOL_BASE_URL}{release.path}"
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=endpoint,
        parameters=dict(parameters),
        max_attempts=runtime.max_attempts,
    )
    try:
        response = fetch_release(
            release,
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
                request_parameters=dict(parameters),
                retrieved_at=datetime.now(UTC),
                http_status=response.http_status,
                response_headers=dict(response.response_headers),
                media_type=XLSX_MEDIA_TYPE,
                payload=response.raw_bytes,
                payload_schema_version=PARSER_CONTRACT_VERSION,
                source_revision=release.key,
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
        status = (
            "unchanged"
            if repeats_last_publication(
                connection_factory,
                LATEST_PUBLISHED_SQL,
                (release.key,),
                payload_checksum=receipt.payload_checksum,
                parser_contract=PARSER_CONTRACT_VERSION,
            )
            else "captured"
        )
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.dol_childcare_file (
                        run_id, release, capture_id, status
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (str(run_id), release.key, str(receipt.capture_id), status),
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
