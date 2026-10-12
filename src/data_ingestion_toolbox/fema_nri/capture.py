"""Capture-first orchestration for FEMA's two streams.

One ingestion run per read of a stream: one request and one committed
capture per page, before anything parses it, until the service says no more
records follow. A National Risk Index read whose pages are byte-for-byte the
last published read's is recorded ``unchanged`` and replays nothing.
Declarations always replay: a declaration revision is a new ``hash`` for an
``id``, and replay keeps only revisions it has not seen.
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

from .client import FemaPayloadError, fetch_page, page_parameters
from .config import SOURCE_CODE, FemaConfig
from .registry import DECLARATIONS_URL, NRI, NRI_LAYER_URL

PARSER_CONTRACT_VERSION = "fema_nri_json:v1"


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
    config: FemaConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, str]:
    """Capture every page of one stream; return (run_id, run status)."""
    runtime = config or FemaConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(watermark={"stream": stream})
    _execute(
        connection_factory,
        "INSERT INTO control.fema_nri_run (run_id, stream, status) VALUES (%s, %s, 'capturing')",
        (str(run_id), stream),
    )
    checksums: list[str] = []
    version: str | None = None
    page_index = 0
    try:
        while True:
            if page_index >= runtime.max_pages:
                raise FemaPayloadError(
                    f"{stream}:page:{page_index}", code="too_many_pages"
                )
            url = NRI_LAYER_URL if stream == NRI else DECLARATIONS_URL
            parameters = page_parameters(stream, page_index, runtime)
            request = capture_control.start_request(
                run_id=run_id,
                endpoint=url,
                parameters=parameters,
                max_attempts=runtime.max_attempts,
            )
            try:
                page = fetch_page(
                    stream,
                    page_index,
                    config=runtime,
                    client=client,
                    on_retry=lambda error, request=request: (
                        capture_control.record_request_retry(
                            request.request_id, error=error
                        )
                    ),
                )
                receipt = persist_capture(
                    connection_factory,
                    ResponseCapture(
                        capture_id=uuid4(),
                        request_id=request.request_id,
                        run_id=run_id,
                        source_code=SOURCE_CODE,
                        endpoint=url,
                        request_parameters=parameters,
                        retrieved_at=datetime.now(UTC),
                        http_status=page.http_status,
                        response_headers=dict(page.response_headers),
                        media_type="application/json",
                        payload=page.raw_bytes,
                        payload_schema_version=PARSER_CONTRACT_VERSION,
                        source_revision=f"{stream}:{page_index}",
                    ),
                )
            except BaseException as exc:
                capture_control.finish_request(
                    request.request_id, status="failed", error=exc
                )
                raise
            capture_control.finish_request(request.request_id, status="captured")
            if stream == NRI and version is None and page.records:
                version = str(page.records[0].get("NRI_VER") or "") or None
            checksums.append(receipt.payload_checksum)
            _execute(
                connection_factory,
                """
                INSERT INTO control.fema_nri_page (run_id, page_index, capture_id, record_count)
                VALUES (%s, %s, %s, %s)
                """,
                (str(run_id), page_index, str(receipt.capture_id), len(page.records)),
            )
            page_index += 1
            if not page.more:
                break
        run_checksum = hashlib.sha256("|".join(checksums).encode()).hexdigest()
        previous = _execute(
            connection_factory,
            """
            SELECT run_checksum FROM control.fema_nri_run
            WHERE stream = %s AND status = 'published'
            ORDER BY published_at DESC LIMIT 1
            """,
            (stream,),
        )
        unchanged = stream == NRI and bool(previous) and previous[0][0] == run_checksum
        status = "unchanged" if unchanged else "captured"
        _execute(
            connection_factory,
            """
            UPDATE control.fema_nri_run
               SET status = %s, run_checksum = %s, nri_version = %s, page_count = %s, updated_at = NOW()
             WHERE run_id = %s
            """,
            (status, run_checksum, version, page_index, str(run_id)),
        )
        capture_control.finish_run(run_id, status="success")
        return run_id, status
    except BaseException as exc:
        # The failed run keeps its errors in the ingestion ledger; its
        # half-captured pages are withdrawn so no rule reads them as work.
        _execute(
            connection_factory,
            "DELETE FROM control.fema_nri_page WHERE run_id = %s",
            (str(run_id),),
        )
        _execute(
            connection_factory,
            "DELETE FROM control.fema_nri_run WHERE run_id = %s",
            (str(run_id),),
        )
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
