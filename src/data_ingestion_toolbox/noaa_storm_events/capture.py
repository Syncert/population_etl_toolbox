"""Capture-first orchestration for the Storm Events and zone-county files.

One ingestion run per read: the NWS page names the zone-county files and
the NCEI listing the newest details file of every year. A file whose name was
already published (NCEI stamps each yearly file with its creation day, NWS
each zone file with its valid day) is recorded ``already_loaded`` and not
downloaded again. Every other file is one request and one committed capture
before anything parses it; one whose bytes equal the last published capture
of its year (or zone file) is ``unchanged`` and replays nothing.
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

from .client import SourceFile, fetch_file, fetch_listing
from .config import SOURCE_CODE, StormEventsConfig

PARSER_CONTRACT_VERSION = "noaa_storm_events:v1"


def _pause(seconds: float) -> None:
    """The spacing between two downloads."""
    time.sleep(seconds)


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


def capture_release(
    connection_factory: Callable[[], Any],
    *,
    config: StormEventsConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
    sleep: Callable[[float], None] | None = None,
) -> tuple[UUID, dict[str, str]]:
    """Capture every listed file not yet published; return (run_id, status by file name)."""
    runtime = config or StormEventsConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    pause = sleep or _pause
    files = fetch_listing(config=runtime, client=client, sleep=pause)
    run_id = capture_control.start_run(
        watermark={"files": [file.name for file in files]}
    )
    statuses: dict[str, str] = {}
    try:
        for file in files:
            statuses[file.name] = _capture_file(
                connection_factory,
                file,
                run_id=run_id,
                runtime=runtime,
                client=client,
                capture_control=capture_control,
                persist_capture=persist_capture,
            )
            if statuses[file.name] != "already_loaded":
                pause(runtime.min_spacing_seconds)
        capture_control.finish_run(run_id, status="success")
    except BaseException as exc:
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
    return run_id, statuses


def _insert_file_row(
    connection_factory: Callable[[], Any],
    run_id: UUID,
    file: SourceFile,
    *,
    status: str,
    capture_id: UUID | None = None,
    checksum: str | None = None,
) -> None:
    _execute(
        connection_factory,
        """
        INSERT INTO control.noaa_storm_events_file (
            run_id, file_kind, file_name, data_year, stamp, capture_id, payload_checksum, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            str(run_id),
            file.kind,
            file.name,
            file.data_year,
            file.stamp,
            str(capture_id) if capture_id else None,
            checksum,
            status,
        ),
    )


def _capture_file(
    connection_factory: Callable[[], Any],
    file: SourceFile,
    *,
    run_id: UUID,
    runtime: StormEventsConfig,
    client: Any | None,
    capture_control: CaptureControl,
    persist_capture: Callable[[Callable[[], Any], ResponseCapture], CaptureReceipt],
) -> str:
    loaded = _execute(
        connection_factory,
        """
        SELECT 1 FROM control.noaa_storm_events_file
        WHERE file_name = %s AND status = 'published' LIMIT 1
        """,
        (file.name,),
    )
    if loaded:
        _insert_file_row(connection_factory, run_id, file, status="already_loaded")
        return "already_loaded"
    parameters = {"file": file.name}
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=file.url,
        parameters=parameters,
        max_attempts=runtime.max_attempts,
    )
    try:
        response = fetch_file(
            file,
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
                endpoint=file.url,
                request_parameters=parameters,
                retrieved_at=datetime.now(UTC),
                http_status=response.http_status,
                response_headers=dict(response.response_headers),
                media_type="application/gzip"
                if file.kind == "details"
                else "text/plain",
                payload=response.raw_bytes,
                payload_schema_version=PARSER_CONTRACT_VERSION,
                source_revision=file.name,
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
    except BaseException as exc:
        capture_control.finish_request(request.request_id, status="failed", error=exc)
        raise
    if file.kind == "details":
        previous = _execute(
            connection_factory,
            """
            SELECT payload_checksum FROM control.noaa_storm_events_file
            WHERE status = 'published' AND file_kind = 'details' AND data_year = %s
            ORDER BY published_at DESC LIMIT 1
            """,
            (file.data_year,),
        )
    else:
        previous = _execute(
            connection_factory,
            """
            SELECT payload_checksum FROM control.noaa_storm_events_file
            WHERE status = 'published' AND file_name = %s
            ORDER BY published_at DESC LIMIT 1
            """,
            (file.name,),
        )
    status = (
        "unchanged"
        if previous and previous[0][0] == receipt.payload_checksum
        else "captured"
    )
    _insert_file_row(
        connection_factory,
        run_id,
        file,
        status=status,
        capture_id=receipt.capture_id,
        checksum=receipt.payload_checksum,
    )
    return status
