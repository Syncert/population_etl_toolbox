"""Capture-first orchestration for QCEW over the shared raw/control plane.

One ingestion run per (year, period); one request and one committed capture
per registered industry. The bytes are committed before anything parses
them, and each slice's control row records whether the period was published.
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

from .client import fetch_slice
from .config import QCEW_API_BASE_URL, SOURCE_CODE, QcewConfig
from .registry import INDUSTRIES, QcewIndustry

PARSER_CONTRACT_VERSION = "bls_qcew_csv:v1"


def pause(seconds: float) -> None:
    """The spacing between slice requests; a seam the orchestrated tests replace."""
    time.sleep(seconds)


@dataclass(frozen=True)
class CapturedSlice:
    run_id: UUID
    year: int
    period: str
    industry_code: str
    capture_id: UUID
    published: bool


def record_slice(
    connection_factory: Callable[[], Any], captured: CapturedSlice
) -> None:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.bls_qcew_slice (
                    run_id, year, period, industry_code, capture_id, status
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, industry_code) DO NOTHING
                """,
                (
                    str(captured.run_id),
                    captured.year,
                    captured.period,
                    captured.industry_code,
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
    year: int,
    period: str,
    *,
    industries: Sequence[QcewIndustry] = INDUSTRIES,
    config: QcewConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
    sleep: Callable[[float], None] | None = None,
) -> tuple[UUID, list[CapturedSlice]]:
    """Capture every registered industry slice of one (year, period)."""
    runtime = config or QcewConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(watermark={"year": year, "period": period})
    captured: list[CapturedSlice] = []
    active_request: UUID | None = None
    try:
        for index, industry in enumerate(industries):
            if index:
                (sleep or pause)(runtime.min_spacing_seconds)
            parameters = {
                "year": str(year),
                "period": period,
                "industry_code": industry.code,
            }
            endpoint = f"{QCEW_API_BASE_URL}/{year}/{period}/industry"
            request = capture_control.start_request(
                run_id=run_id,
                endpoint=endpoint,
                parameters=parameters,
                max_attempts=runtime.max_attempts,
            )
            active_request = request.request_id
            response = fetch_slice(
                year,
                period,
                industry,
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
                    source_revision=f"{year}-{period}",
                ),
            )
            capture_control.finish_request(
                request.request_id, status="captured" if response.published else "empty"
            )
            active_request = None
            slice_capture = CapturedSlice(
                run_id,
                year,
                period,
                industry.code,
                receipt.capture_id,
                response.published,
            )
            record_slice(connection_factory, slice_capture)
            captured.append(slice_capture)
        capture_control.finish_run(run_id, status="success")
        return run_id, captured
    except BaseException as exc:
        if active_request is not None:
            capture_control.finish_request(active_request, status="failed", error=exc)
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
