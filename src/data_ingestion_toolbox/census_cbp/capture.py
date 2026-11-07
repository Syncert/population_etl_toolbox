"""Capture-first orchestration for County Business Patterns files.

One ingestion run per file (level and year): one request and one committed
capture of the zip. The bytes are committed before anything parses them.
A read whose bytes repeat the file's last published capture is recorded
``unchanged`` and replays nothing (``utility.unchanged``).
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

from .client import fetch_file
from .config import SOURCE_CODE, CbpConfig
from .registry import CBP_BASE_URL, CbpFile

PARSER_CONTRACT_VERSION = "census_cbp_csv:v1"

#: The file's newest published capture.
LATEST_PUBLISHED_SQL = """
    SELECT response.payload_checksum, response.payload_schema_version, file.published_at,
           response.capture_id
    FROM control.census_cbp_file AS file
    JOIN raw_capture.response_capture AS response ON response.capture_id = file.capture_id
    WHERE file.kind = %s AND file.year = %s AND file.status = 'published'
    ORDER BY file.published_at DESC
    LIMIT 1
"""

#: Its revision rows name the geographies a replay would resolve (utility.unchanged).
REVISION_TABLE = "silver_census_cbp.observation_revision"


def capture_file(
    connection_factory: Callable[[], Any],
    item: CbpFile,
    *,
    config: CbpConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, UUID]:
    """Capture one registered file; return (run_id, capture_id)."""
    runtime = config or CbpConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(watermark={"kind": item.kind, "year": item.year})
    endpoint = f"{CBP_BASE_URL}{item.path}"
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=endpoint,
        parameters={"kind": item.kind, "year": str(item.year)},
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
                media_type="application/zip",
                payload=response.raw_bytes,
                payload_schema_version=PARSER_CONTRACT_VERSION,
                source_revision=item.key,
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
        status = (
            "unchanged"
            if repeats_last_publication(
                connection_factory,
                LATEST_PUBLISHED_SQL,
                (item.kind, item.year),
                payload_checksum=receipt.payload_checksum,
                parser_contract=PARSER_CONTRACT_VERSION,
                revision_table=REVISION_TABLE,
            )
            else "captured"
        )
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.census_cbp_file (run_id, kind, year, capture_id, status)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        str(run_id),
                        item.kind,
                        item.year,
                        str(receipt.capture_id),
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
        return run_id, receipt.capture_id
    except BaseException as exc:
        capture_control.finish_request(request.request_id, status="failed", error=exc)
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
