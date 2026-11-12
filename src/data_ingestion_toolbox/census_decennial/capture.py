"""Capture-first orchestration for the 2020 Census over the shared raw/control plane.

One ingestion run per dataset load; one request and one committed capture
per slice (a grain and the area it covers). The bytes are committed before
anything parses them, and the control row for each slice records whether
the API published it.
"""

from __future__ import annotations

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
from data_ingestion_toolbox.utility.unchanged import repeats_last_publication

from .client import fetch_slice
from .config import SOURCE_CODE, DecennialConfig
from .registry import CENSUS_YEAR, DecennialDataset, DecennialSlice


@dataclass(frozen=True)
class CapturedSlice:
    run_id: UUID
    dataset_id: str
    geo_level: str
    scope: str
    capture_id: UUID
    published: bool
    #: The bytes repeat the slice's last published capture (utility.unchanged).
    unchanged: bool = False


#: The slice's newest published capture.
LATEST_PUBLISHED_SQL = """
    SELECT response.payload_checksum, response.payload_schema_version, slice.published_at,
           response.capture_id
    FROM control.census_decennial_slice AS slice
    JOIN raw_capture.response_capture AS response ON response.capture_id = slice.capture_id
    WHERE slice.dataset_id = %s AND slice.geo_level = %s AND slice.scope = %s
      AND slice.status = 'published'
    ORDER BY slice.published_at DESC
    LIMIT 1
"""

#: Its revision rows name the geographies a replay would resolve (utility.unchanged).
REVISION_TABLE = "silver_census_decennial.observation_revision"


def slice_status(captured: CapturedSlice) -> str:
    """The control status a freshly captured slice is recorded with."""
    if not captured.published:
        return "empty"
    return "unchanged" if captured.unchanged else "captured"


def record_slice(
    connection_factory: Callable[[], Any], captured: CapturedSlice, row_count: int
) -> None:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.census_decennial_slice (
                    run_id, dataset_id, geo_level, scope, capture_id,
                    captured_row_count, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, geo_level, scope) DO NOTHING
                """,
                (
                    str(captured.run_id),
                    captured.dataset_id,
                    captured.geo_level,
                    captured.scope,
                    str(captured.capture_id),
                    row_count,
                    slice_status(captured),
                ),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def capture_slices(
    connection_factory: Callable[[], Any],
    dataset: DecennialDataset,
    slices: Sequence[DecennialSlice],
    *,
    config: DecennialConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, list[CapturedSlice]]:
    """Capture the given slices of one dataset in one ingestion run."""
    runtime = config or DecennialConfig.from_environment()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(
        watermark={
            "dataset_id": dataset.dataset_id,
            "census_year": CENSUS_YEAR,
            "slices": len(slices),
        }
    )
    captured: list[CapturedSlice] = []
    active_request: UUID | None = None
    try:
        for slice_ in slices:
            parameters = dataset.request_parameters(slice_)
            request = capture_control.start_request(
                run_id=run_id,
                endpoint=dataset.api_path,
                parameters=parameters,
                max_attempts=runtime.max_attempts,
            )
            active_request = request.request_id
            response = fetch_slice(
                dataset,
                slice_=slice_,
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
                    source_revision=str(CENSUS_YEAR),
                ),
            )
            capture_control.finish_request(request.request_id, status="captured")
            active_request = None
            slice_capture = CapturedSlice(
                run_id,
                dataset.dataset_id,
                slice_.geo_level,
                slice_.scope,
                receipt.capture_id,
                response.published,
                unchanged=response.published
                and repeats_last_publication(
                    connection_factory,
                    LATEST_PUBLISHED_SQL,
                    (dataset.dataset_id, slice_.geo_level, slice_.scope),
                    payload_checksum=receipt.payload_checksum,
                    parser_contract=dataset.parser_contract_version,
                    revision_table=REVISION_TABLE,
                ),
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
