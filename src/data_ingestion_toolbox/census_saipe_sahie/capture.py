"""Capture-first orchestration for SAIPE and SAHIE over the shared raw/control plane.

One ingestion run per (dataset, year); one request and one committed capture
per grain. The bytes are committed before anything parses them, and the
control row for each slice records whether the API published it.
"""

from __future__ import annotations

from collections.abc import Callable
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
from .config import SOURCE_CODE, SaeConfig
from .registry import GEO_LEVELS, SaeDataset


@dataclass(frozen=True)
class CapturedSlice:
    run_id: UUID
    dataset_id: str
    estimate_year: int
    geo_level: str
    capture_id: UUID
    published: bool


def record_slice(
    connection_factory: Callable[[], Any], captured: CapturedSlice, row_count: int
) -> None:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.census_sae_slice (
                    run_id, dataset_id, estimate_year, geo_level, capture_id,
                    captured_row_count, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, geo_level) DO NOTHING
                """,
                (
                    str(captured.run_id),
                    captured.dataset_id,
                    captured.estimate_year,
                    captured.geo_level,
                    str(captured.capture_id),
                    row_count,
                    "captured" if captured.published else "empty",
                ),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def capture_dataset_year(
    connection_factory: Callable[[], Any],
    dataset: SaeDataset,
    year: int,
    *,
    config: SaeConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, list[CapturedSlice]]:
    """Capture every registered grain of one dataset year."""
    runtime = config or SaeConfig.from_environment()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(
        watermark={"dataset_id": dataset.dataset_id, "estimate_year": year}
    )
    captured: list[CapturedSlice] = []
    active_request: UUID | None = None
    try:
        for geo_level in GEO_LEVELS:
            parameters = dataset.request_parameters(year=year, geo_level=geo_level)
            request = capture_control.start_request(
                run_id=run_id,
                endpoint=dataset.api_path,
                parameters=parameters,
                max_attempts=runtime.max_attempts,
            )
            active_request = request.request_id
            response = fetch_slice(
                dataset,
                year=year,
                geo_level=geo_level,
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
                    endpoint=dataset.api_path,
                    request_parameters=dict(response.request_parameters),
                    retrieved_at=datetime.now(UTC),
                    http_status=response.http_status,
                    response_headers=dict(response.response_headers),
                    media_type="application/json",
                    payload=response.raw_bytes,
                    payload_schema_version=dataset.parser_contract_version,
                    source_revision=str(year),
                ),
            )
            capture_control.finish_request(request.request_id, status="captured")
            active_request = None
            slice_capture = CapturedSlice(
                run_id,
                dataset.dataset_id,
                year,
                geo_level,
                receipt.capture_id,
                response.published,
            )
            # Recorded before anything parses the bytes; the replay counts
            # the rows and moves the status on.
            record_slice(connection_factory, slice_capture, 0)
            captured.append(slice_capture)
        capture_control.finish_run(run_id, status="success")
        return run_id, captured
    except BaseException as exc:
        if active_request is not None:
            capture_control.finish_request(active_request, status="failed", error=exc)
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
