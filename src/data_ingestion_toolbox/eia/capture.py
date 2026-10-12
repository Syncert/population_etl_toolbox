"""Capture-first orchestration for EIA weekly retail gasoline prices.

One ingestion run per read of a window of weeks: every page of the answer
is its own request and committed capture before anything parses it. The
pages are sorted by week and series, so paging is deterministic. The key
travels only inside the client; captured endpoints and parameters name the
route, the window and the page.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from data_ingestion_toolbox.capture import (
    CaptureControl,
    CaptureReceipt,
    ResponseCapture,
    persist_response_capture,
)

from .client import EiaClient, EiaPayloadError, page_rows
from .config import EIA_API_BASE_URL, GASOLINE_ROUTE, SOURCE_CODE, EiaConfig
from .registry import PROCESS, PRODUCTS

PARSER_CONTRACT_VERSION = "eia_v2_petroleum_pri_gnd_json:v1"
DATA_ROUTE = f"{GASOLINE_ROUTE}/data/"


def window_parameters(
    start: date, end: date | None, *, offset: int, length: int
) -> list[tuple[str, str]]:
    """The query for one page of a window, without the key."""
    parameters = [
        ("frequency", "weekly"),
        ("data[0]", "value"),
        *(("facets[product][]", product) for product in sorted(PRODUCTS)),
        ("facets[process][]", PROCESS),
        ("start", start.isoformat()),
    ]
    if end is not None:
        parameters.append(("end", end.isoformat()))
    parameters += [
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "asc"),
        ("sort[1][column]", "series"),
        ("sort[1][direction]", "asc"),
        ("offset", str(offset)),
        ("length", str(length)),
    ]
    return parameters


def capture_window(
    connection_factory: Callable[[], Any],
    start: date,
    end: date | None = None,
    *,
    config: EiaConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
    sleep: Callable[[float], None] | None = None,
) -> UUID:
    """Capture every page of one window of weeks; return the run id."""
    runtime = config or EiaConfig.from_environment()
    # A missing key fails here, before any run is recorded.
    api = EiaClient(runtime, client=client, **({"sleep": sleep} if sleep else {}))
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(
        watermark={
            "start_week": start.isoformat(),
            "end_week": end.isoformat() if end else None,
        }
    )
    endpoint = f"{EIA_API_BASE_URL}/{DATA_ROUTE}"
    receipts: list[CaptureReceipt] = []
    try:
        offset, total = 0, None
        while total is None or offset < total:
            parameters = window_parameters(
                start, end, offset=offset, length=runtime.page_length
            )
            # What the request asked, as recorded with the request and its
            # capture alike (the capture's fingerprint must match); the key
            # is not among it.
            recorded = {
                "start": start.isoformat(),
                "end": end.isoformat() if end else None,
                "offset": offset,
                "length": runtime.page_length,
                "products": sorted(PRODUCTS),
                "process": PROCESS,
            }
            request = capture_control.start_request(
                run_id=run_id,
                endpoint=endpoint,
                parameters=recorded,
                max_attempts=runtime.max_attempts,
            )
            try:
                response = api.get(
                    DATA_ROUTE,
                    parameters,
                    on_retry=lambda error, request_id=request.request_id: (
                        capture_control.record_request_retry(request_id, error=error)
                    ),
                )
                # Kept before it is read: a page that is not the registered
                # answer is still evidence of what EIA sent.
                receipt = persist_capture(
                    connection_factory,
                    ResponseCapture(
                        capture_id=uuid4(),
                        request_id=request.request_id,
                        run_id=run_id,
                        source_code=SOURCE_CODE,
                        endpoint=endpoint,
                        request_parameters=recorded,
                        retrieved_at=datetime.now(UTC),
                        http_status=response.http_status,
                        response_headers=dict(response.response_headers),
                        media_type="application/json",
                        payload=response.raw_bytes,
                        payload_schema_version=PARSER_CONTRACT_VERSION,
                        source_revision=f"{start.isoformat()}+{offset}",
                    ),
                )
            except BaseException as exc:
                capture_control.finish_request(
                    request.request_id, status="failed", error=exc
                )
                raise
            capture_control.finish_request(request.request_id, status="captured")
            receipts.append(receipt)
            page_total, rows = page_rows(response.raw_bytes, DATA_ROUTE)
            if total is None:
                total = page_total
            elif page_total != total:
                # The answer changed under the read: pages would not join up.
                raise EiaPayloadError(DATA_ROUTE, code="total_changed_while_paging")
            if not rows:
                break
            offset += len(rows)
        if total and offset < total:
            raise EiaPayloadError(DATA_ROUTE, code="short_read")
        checksum = hashlib.sha256(
            "\n".join(receipt.payload_checksum for receipt in receipts).encode()
        ).hexdigest()
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.eia_read (
                        run_id, start_week, end_week, page_count, row_total,
                        payload_checksum, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, 'captured')
                    """,
                    (str(run_id), start, end, len(receipts), total or 0, checksum),
                )
                for index, receipt in enumerate(receipts):
                    cursor.execute(
                        """
                        INSERT INTO control.eia_page (run_id, page_index, capture_id, payload_checksum)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            str(run_id),
                            index,
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
        return run_id
    except BaseException as exc:
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
    finally:
        api.close()


def plan_window(connection_factory: Callable[[], Any], config: EiaConfig) -> date:
    """The first week the next read asks for.

    The whole history on a warehouse that has published none; otherwise the
    newest published week less ``refresh_weeks``, so a week EIA revised is
    read again and a week it has not yet published is asked for.
    """
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(fact.week_start)
                FROM silver_eia.fact_retail_price AS fact
                JOIN control.eia_read AS read ON read.run_id = fact.run_id
                WHERE read.status = 'published'
                """
            )
            newest = cursor.fetchone()[0]
    finally:
        connection.close()
    if newest is None:
        return config.history_start
    return max(config.history_start, newest - timedelta(weeks=config.refresh_weeks))
