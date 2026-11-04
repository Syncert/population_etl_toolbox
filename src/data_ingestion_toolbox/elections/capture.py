"""Capture-first orchestration for the elections streams.

One ingestion run per read of a stream: one request and one committed
capture per file, before anything parses it. A results stream reads its
Dataverse file list first and then the one registered file it names. A read
whose files are byte-for-byte the last published read's is recorded
``unchanged`` and replays nothing; a failed read is withdrawn from the
stream's ledger and keeps its errors in the ingestion ledger.
"""

from __future__ import annotations

import hashlib
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

from .client import (
    ElectionFile,
    FileRequest,
    data_request,
    fetch_file,
    governor_request,
    legislator_requests,
    listing_request,
    select_data_file,
)
from .config import SOURCE_CODE, ElectionsConfig
from .registry import GOVERNORS, LEGISLATORS, STREAMS, result_dataset

PARSER_CONTRACT_VERSION = "elections_files:v1"


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


def capture_stream(
    connection_factory: Callable[[], Any],
    stream: str,
    *,
    config: ElectionsConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, str]:
    """Capture every file of one stream; return (run_id, run status)."""
    if stream not in STREAMS:
        raise ValueError(f"unregistered elections stream: {stream}")
    runtime = config or ElectionsConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(watermark={"stream": stream})
    _execute(
        connection_factory,
        "INSERT INTO control.elections_run (run_id, stream, status) VALUES (%s, %s, 'capturing')",
        (str(run_id), stream),
    )
    checksums: list[str] = []
    source_file: str | None = None
    file_index = 0

    def capture(request: FileRequest) -> ElectionFile:
        nonlocal file_index
        control_request = capture_control.start_request(
            run_id=run_id,
            endpoint=request.url,
            parameters=dict(request.parameters),
            max_attempts=runtime.max_attempts,
        )
        try:
            fetched = fetch_file(
                request,
                config=runtime,
                client=client,
                on_retry=lambda error: capture_control.record_request_retry(
                    control_request.request_id, error=error
                ),
            )
            receipt = persist_capture(
                connection_factory,
                ResponseCapture(
                    capture_id=uuid4(),
                    request_id=control_request.request_id,
                    run_id=run_id,
                    source_code=SOURCE_CODE,
                    endpoint=request.url,
                    request_parameters=dict(request.parameters),
                    retrieved_at=datetime.now(UTC),
                    http_status=fetched.http_status,
                    response_headers=dict(fetched.response_headers),
                    media_type=request.media_type,
                    payload=fetched.raw_bytes,
                    payload_schema_version=PARSER_CONTRACT_VERSION,
                    source_revision=f"{stream}:{request.file_key}",
                ),
            )
        except BaseException as exc:
            capture_control.finish_request(
                control_request.request_id, status="failed", error=exc
            )
            raise
        capture_control.finish_request(control_request.request_id, status="captured")
        _execute(
            connection_factory,
            """
            INSERT INTO control.elections_file (run_id, file_index, file_key, file_kind, capture_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                str(run_id),
                file_index,
                request.file_key,
                request.kind,
                str(receipt.capture_id),
            ),
        )
        file_index += 1
        if request.kind != "listing":
            checksums.append(receipt.payload_checksum)
        return fetched

    try:
        if stream == LEGISLATORS:
            for request in legislator_requests():
                capture(request)
        elif stream == GOVERNORS:
            capture(governor_request())
        else:
            dataset = result_dataset(stream)
            listing = capture(listing_request(dataset))
            file_id, source_file = select_data_file(dataset, listing.raw_bytes)
            capture(data_request(dataset, file_id))
        run_checksum = hashlib.sha256("|".join(checksums).encode()).hexdigest()
        previous = _execute(
            connection_factory,
            """
            SELECT run_checksum FROM control.elections_run
            WHERE stream = %s AND status = 'published'
            ORDER BY published_at DESC LIMIT 1
            """,
            (stream,),
        )
        status = (
            "unchanged" if previous and previous[0][0] == run_checksum else "captured"
        )
        _execute(
            connection_factory,
            """
            UPDATE control.elections_run
               SET status = %s, run_checksum = %s, source_file = %s, file_count = %s,
                   updated_at = NOW()
             WHERE run_id = %s
            """,
            (status, run_checksum, source_file, file_index, str(run_id)),
        )
        capture_control.finish_run(run_id, status="success")
        return run_id, status
    except BaseException as exc:
        _execute(
            connection_factory,
            "DELETE FROM control.elections_file WHERE run_id = %s",
            (str(run_id),),
        )
        _execute(
            connection_factory,
            "DELETE FROM control.elections_run WHERE run_id = %s",
            (str(run_id),),
        )
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
