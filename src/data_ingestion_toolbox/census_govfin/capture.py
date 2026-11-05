"""Capture-first orchestration for Census government finance.

One ingestion run per (dataset, period); one request and one committed
capture per declared slice. The bytes are committed before anything parses
them, and the control row for each slice records whether Census published it.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
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

from .client import GovFinResponse, fetch_api_slice, fetch_archive
from .config import SOURCE_CODE, GovFinConfig
from .registry import api_request, get_edition, slice_keys


@dataclass(frozen=True)
class CapturedSlice:
    run_id: UUID
    dataset_id: str
    period: int
    slice_key: str
    capture_id: UUID
    published: bool
    #: The bytes repeat the slice's last published capture (utility.unchanged).
    unchanged: bool = False


#: The slice's newest published capture.
LATEST_PUBLISHED_SQL = """
    SELECT response.payload_checksum, response.payload_schema_version, slice.published_at
    FROM control.census_govfin_slice AS slice
    JOIN raw_capture.response_capture AS response ON response.capture_id = slice.capture_id
    WHERE slice.dataset_id = %s AND slice.period = %s AND slice.slice_key = %s
      AND slice.status = 'published'
    ORDER BY slice.published_at DESC
    LIMIT 1
"""


def slice_status(captured: CapturedSlice) -> str:
    """The control status a freshly captured slice is recorded with."""
    if not captured.published:
        return "empty"
    return "unchanged" if captured.unchanged else "captured"


def parser_contract_version(dataset_id: str, period: int) -> str:
    if dataset_id == "cog_units":
        return get_edition(period).parser_contract_version
    return f"census_govfin_{dataset_id}:v1"


def record_slice(
    connection_factory: Callable[[], Any], captured: CapturedSlice
) -> None:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.census_govfin_slice (
                    run_id, dataset_id, period, slice_key, capture_id, status
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, slice_key) DO NOTHING
                """,
                (
                    str(captured.run_id),
                    captured.dataset_id,
                    captured.period,
                    captured.slice_key,
                    str(captured.capture_id),
                    slice_status(captured),
                ),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _request_identity(
    dataset_id: str, period: int, slice_key: str
) -> tuple[str, dict[str, str]]:
    if dataset_id == "cog_units":
        archive = next(
            item for item in get_edition(period).archives if item.slice_key == slice_key
        )
        return archive.path, {"census_year": str(period), "slice": slice_key}
    return api_request(dataset_id, period, slice_key)


def _fetch(
    dataset_id: str,
    period: int,
    slice_key: str,
    *,
    config: GovFinConfig,
    client: Any | None,
    on_retry: Callable[[BaseException], None],
) -> GovFinResponse:
    if dataset_id == "cog_units":
        archive = next(
            item for item in get_edition(period).archives if item.slice_key == slice_key
        )
        return fetch_archive(
            archive, period, config=config, client=client, on_retry=on_retry
        )
    return fetch_api_slice(
        dataset_id, period, slice_key, config=config, client=client, on_retry=on_retry
    )


def capture_period(
    connection_factory: Callable[[], Any],
    dataset_id: str,
    period: int,
    *,
    config: GovFinConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, list[CapturedSlice]]:
    """Capture every declared slice of one dataset period."""
    runtime = config or GovFinConfig.from_environment()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(
        watermark={"dataset_id": dataset_id, "period": period}
    )
    captured: list[CapturedSlice] = []
    active_request: UUID | None = None
    try:
        for slice_key in slice_keys(dataset_id, period):
            endpoint, parameters = _request_identity(dataset_id, period, slice_key)
            request = capture_control.start_request(
                run_id=run_id,
                endpoint=endpoint,
                parameters=parameters,
                max_attempts=runtime.max_attempts,
            )
            active_request = request.request_id
            response = _fetch(
                dataset_id,
                period,
                slice_key,
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
                    endpoint=response.endpoint,
                    request_parameters=dict(response.request_parameters),
                    retrieved_at=datetime.now(UTC),
                    http_status=response.http_status,
                    response_headers=dict(response.response_headers),
                    media_type=response.media_type,
                    payload=response.raw_bytes,
                    payload_schema_version=parser_contract_version(dataset_id, period),
                    source_revision=str(period),
                ),
            )
            capture_control.finish_request(request.request_id, status="captured")
            active_request = None
            captured.append(
                CapturedSlice(
                    run_id,
                    dataset_id,
                    period,
                    slice_key,
                    receipt.capture_id,
                    response.published,
                    unchanged=response.published
                    and repeats_last_publication(
                        connection_factory,
                        LATEST_PUBLISHED_SQL,
                        (dataset_id, period, slice_key),
                        payload_checksum=receipt.payload_checksum,
                        parser_contract=parser_contract_version(dataset_id, period),
                    ),
                )
            )
        if dataset_id == "cog_units" and not all(
            item.unchanged for item in captured if item.published
        ):
            # An edition's county areas sum its unit file over its directory:
            # one changed member is a replay of all of them.
            captured = [replace(item, unchanged=False) for item in captured]
        # Recorded before anything parses the bytes; the replay counts the
        # rows and moves the status on.
        for item in captured:
            record_slice(connection_factory, item)
        capture_control.finish_run(run_id, status="success")
        return run_id, captured
    except BaseException as exc:
        if active_request is not None:
            capture_control.finish_request(active_request, status="failed", error=exc)
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
