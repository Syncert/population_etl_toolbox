"""Capture-first orchestration for the NOAA nClimDiv county files.

One ingestion run per read: the directory listing names the current file
of each registered element. A file whose name was already published (NCEI
names each release by its day) is recorded ``already_loaded`` and not
downloaded again. Every other file is one request and one committed capture
before anything parses it; one whose bytes equal the element's last
published capture is ``unchanged`` and replays nothing.
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

from .client import ClimdivFile, fetch_county_file, fetch_listing
from .config import SOURCE_CODE, ClimdivConfig
from .registry import CLIMDIV_DIRECTORY_URL

PARSER_CONTRACT_VERSION = "noaa_climdiv_county:v1"


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
    config: ClimdivConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, dict[str, str]]:
    """Capture every registered county file listed now; return (run_id, status by element)."""
    runtime = config or ClimdivConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    files = fetch_listing(config=runtime, client=client)
    run_id = capture_control.start_run(
        watermark={"files": [file.name for file in files]}
    )
    statuses: dict[str, str] = {}
    try:
        for file in files:
            statuses[file.file_element] = _capture_file(
                connection_factory,
                file,
                run_id=run_id,
                runtime=runtime,
                client=client,
                capture_control=capture_control,
                persist_capture=persist_capture,
            )
        capture_control.finish_run(run_id, status="success")
    except BaseException as exc:
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
    return run_id, statuses


def _capture_file(
    connection_factory: Callable[[], Any],
    file: ClimdivFile,
    *,
    run_id: UUID,
    runtime: ClimdivConfig,
    client: Any | None,
    capture_control: CaptureControl,
    persist_capture: Callable[[Callable[[], Any], ResponseCapture], CaptureReceipt],
) -> str:
    loaded = _execute(
        connection_factory,
        """
        SELECT 1 FROM control.noaa_climdiv_file
        WHERE file_name = %s AND status = 'published' LIMIT 1
        """,
        (file.name,),
    )
    if loaded:
        _execute(
            connection_factory,
            """
            INSERT INTO control.noaa_climdiv_file (
                run_id, file_element, file_name, version, release_stamp, status
            ) VALUES (%s, %s, %s, %s, %s, 'already_loaded')
            """,
            (str(run_id), file.file_element, file.name, file.version, file.stamp),
        )
        return "already_loaded"
    endpoint = CLIMDIV_DIRECTORY_URL + file.name
    parameters = {"file": file.name}
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=endpoint,
        parameters=parameters,
        max_attempts=runtime.max_attempts,
    )
    try:
        response = fetch_county_file(
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
                endpoint=endpoint,
                request_parameters=parameters,
                retrieved_at=datetime.now(UTC),
                http_status=response.http_status,
                response_headers=dict(response.response_headers),
                media_type="text/plain",
                payload=response.raw_bytes,
                payload_schema_version=PARSER_CONTRACT_VERSION,
                source_revision=f"v{file.version}-{file.stamp}",
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
    except BaseException as exc:
        capture_control.finish_request(request.request_id, status="failed", error=exc)
        raise
    previous = _execute(
        connection_factory,
        """
        SELECT payload_checksum FROM control.noaa_climdiv_file
        WHERE status = 'published' AND file_element = %s
        ORDER BY published_at DESC LIMIT 1
        """,
        (file.file_element,),
    )
    status = (
        "unchanged"
        if previous and previous[0][0] == receipt.payload_checksum
        else "captured"
    )
    _execute(
        connection_factory,
        """
        INSERT INTO control.noaa_climdiv_file (
            run_id, file_element, file_name, version, release_stamp, capture_id,
            payload_checksum, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            str(run_id),
            file.file_element,
            file.name,
            file.version,
            file.stamp,
            str(receipt.capture_id),
            receipt.payload_checksum,
            status,
        ),
    )
    return status
