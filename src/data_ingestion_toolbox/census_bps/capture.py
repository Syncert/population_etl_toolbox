"""Capture-first orchestration for the Building Permits Survey.

One ingestion run per (frequency, year, month); one request and one committed
capture per registered file. The bytes are committed before anything parses
them, and each file's control row records whether it was published.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
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
from .config import BPS_BASE_URL, SOURCE_CODE, BpsConfig
from .registry import BpsSlice, slices_for

PARSER_CONTRACT_VERSION = "census_bps_text:v1"


def pause(seconds: float) -> None:
    """The spacing between file requests; a seam the orchestrated tests replace."""
    time.sleep(seconds)


@dataclass(frozen=True)
class CapturedFile:
    run_id: UUID
    item: BpsSlice
    capture_id: UUID
    published: bool


def record_file(connection_factory: Callable[[], Any], captured: CapturedFile) -> None:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.census_bps_slice (
                    run_id, slice_key, frequency, year, month, capture_id, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, slice_key) DO NOTHING
                """,
                (
                    str(captured.run_id),
                    captured.item.slice_key,
                    captured.item.frequency,
                    captured.item.year,
                    captured.item.month,
                    str(captured.capture_id),
                    "captured" if captured.published else "empty",
                ),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def capture_period(
    connection_factory: Callable[[], Any],
    frequency: str,
    year: int,
    month: int,
    *,
    files: Sequence[BpsSlice] | None = None,
    config: BpsConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
    sleep: Callable[[float], None] | None = None,
) -> tuple[UUID, list[CapturedFile]]:
    """Capture every registered file of one (frequency, year, month)."""
    runtime = config or BpsConfig()
    items = tuple(files) if files is not None else slices_for(frequency, year, month)
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(
        watermark={"frequency": frequency, "year": year, "month": month}
    )
    captured: list[CapturedFile] = []
    active_request: UUID | None = None
    try:
        for index, item in enumerate(items):
            if index:
                (sleep or pause)(runtime.min_spacing_seconds)
            endpoint = f"{BPS_BASE_URL}/{item.kind}"
            request = capture_control.start_request(
                run_id=run_id,
                endpoint=endpoint,
                parameters={
                    "slice": item.slice_key,
                    "frequency": frequency,
                    "year": str(year),
                    "month": str(month),
                },
                max_attempts=runtime.max_attempts,
            )
            active_request = request.request_id
            response = fetch_file(
                item,
                config=runtime,
                client=client,
                on_retry=lambda error, request_id=request.request_id: (
                    capture_control.record_request_retry(request_id, error=error)
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
                    media_type="text/csv",
                    payload=response.raw_bytes,
                    payload_schema_version=PARSER_CONTRACT_VERSION,
                    source_revision=item.path,
                ),
            )
            capture_control.finish_request(
                request.request_id, status="captured" if response.published else "empty"
            )
            active_request = None
            captured_file = CapturedFile(
                run_id, item, receipt.capture_id, response.published
            )
            record_file(connection_factory, captured_file)
            captured.append(captured_file)
        capture_control.finish_run(run_id, status="success")
        return run_id, captured
    except BaseException as exc:
        if active_request is not None:
            capture_control.finish_request(active_request, status="failed", error=exc)
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
