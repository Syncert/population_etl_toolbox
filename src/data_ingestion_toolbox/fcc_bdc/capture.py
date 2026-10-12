"""Capture-first orchestration for FCC availability vintages.

One ingestion run per read of a registered vintage: the vintage's file
listing and every registered file -- the national other-geographies summary
and each state's place summary -- each its own request and committed
capture, before anything parses it. The run's checksum is the checksum of
the files' checksums in order; a read equal to the vintage's last published
read is recorded ``unchanged`` and replays nothing. A new FCC revision is a
new file name, so it differs.

The credentials travel only as headers; captured endpoints and parameters
name the path, the vintage and the file, never the token.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any
from uuid import UUID, uuid4

from data_ingestion_toolbox.capture import (
    CaptureControl,
    CaptureReceipt,
    ResponseCapture,
    persist_response_capture,
)

from .client import (
    BdcClient,
    BdcPayloadError,
    BdcResponse,
    check_summary,
    listing_files,
)
from .config import SOURCE_CODE, BdcConfig
from .registry import (
    CENSUS_PLACE,
    FCC_API_BASE_URL,
    OTHER_GEOGRAPHIES,
    SummaryFile,
    vintage_key,
)

PARSER_CONTRACT_VERSION = "fcc_bdc_summary_csv:v1"


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


def capture_vintage(
    connection_factory: Callable[[], Any],
    as_of_date: date,
    *,
    config: BdcConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
    sleep: Callable[[float], None] | None = None,
) -> tuple[UUID, str]:
    """Capture one registered vintage's listing and files; return (run_id, status)."""
    runtime = config or BdcConfig.from_environment()
    # Missing credentials fail here, before any run is recorded.
    api = BdcClient(runtime, client=client, **({"sleep": sleep} if sleep else {}))
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(watermark={"as_of_date": as_of_date.isoformat()})
    captured: list[tuple[SummaryFile | None, CaptureReceipt]] = []
    try:

        def answer(
            path: str,
            parameters: dict[str, str],
            *,
            query: dict[str, str] | None = None,
            check: Callable[[bytes], None] | None = None,
            media_type: str,
            item: SummaryFile | None,
        ) -> BdcResponse:
            endpoint = f"{FCC_API_BASE_URL}/{path}"
            request = capture_control.start_request(
                run_id=run_id,
                endpoint=endpoint,
                parameters=parameters,
                max_attempts=runtime.max_attempts,
            )
            try:
                response = api.get(
                    path,
                    params=query,
                    check=check,
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
                        media_type=media_type,
                        payload=response.raw_bytes,
                        payload_schema_version=PARSER_CONTRACT_VERSION,
                        source_revision=item.file_name
                        if item
                        else vintage_key(as_of_date),
                    ),
                )
            except BaseException as exc:
                capture_control.finish_request(
                    request.request_id, status="failed", error=exc
                )
                raise
            capture_control.finish_request(request.request_id, status="captured")
            captured.append((item, receipt))
            return response

        listing_path = f"downloads/listAvailabilityData/{as_of_date.isoformat()}"
        listing = answer(
            listing_path,
            {"as_of_date": as_of_date.isoformat(), "category": "Summary"},
            query={"category": "Summary"},
            media_type="application/json",
            item=None,
        )
        files = listing_files(
            listing.raw_bytes, as_of_date, frozenset({OTHER_GEOGRAPHIES, CENSUS_PLACE})
        )
        if not any(item.subcategory == OTHER_GEOGRAPHIES for item in files):
            raise BdcPayloadError(listing_path, code="no_registered_files")
        for item in files:
            path = f"downloads/downloadFile/availability/{item.file_id}"
            answer(
                path,
                {
                    "as_of_date": as_of_date.isoformat(),
                    "file_id": str(item.file_id),
                    "file_name": item.file_name,
                },
                check=lambda raw, name=item.file_name: check_summary(raw, name),
                media_type="application/zip",
                item=item,
            )
        files_captured = [
            (item, receipt) for item, receipt in captured if item is not None
        ]
        checksum = hashlib.sha256(
            "\n".join(
                f"{item.slice_key}={receipt.payload_checksum}"
                for item, receipt in files_captured
            ).encode()
        ).hexdigest()
        previous = _execute(
            connection_factory,
            """
            SELECT payload_checksum FROM control.fcc_bdc_read
            WHERE as_of_date = %s AND status = 'published'
            ORDER BY published_at DESC LIMIT 1
            """,
            (as_of_date,),
        )
        status = "unchanged" if previous and previous[0][0] == checksum else "captured"
        listing_receipt = next(receipt for item, receipt in captured if item is None)
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.fcc_bdc_read (
                        run_id, as_of_date, listing_capture_id, payload_checksum, file_count, status
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(run_id),
                        as_of_date,
                        str(listing_receipt.capture_id),
                        checksum,
                        len(files_captured),
                        status,
                    ),
                )
                for item, receipt in files_captured:
                    cursor.execute(
                        """
                        INSERT INTO control.fcc_bdc_file (
                            run_id, slice_key, subcategory, state_fips, file_id, file_name, revision,
                            capture_id, payload_checksum
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            str(run_id),
                            item.slice_key,
                            item.kind,
                            item.state_fips,
                            item.file_id,
                            item.file_name,
                            item.revision,
                            str(receipt.capture_id),
                            receipt.payload_checksum,
                        ),
                    )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        capture_control.finish_run(run_id, status="success")
        return run_id, status
    except BaseException as exc:
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
    finally:
        api.close()
