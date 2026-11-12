"""Capture-first orchestration for Monthly Treasury Statement Table 9.

One ingestion run per read. The first read asks for every statement month
from March 2015; later reads ask from three months before the newest month
already published (Treasury can restate a recent month), or for everything
again with ``{"history": true}``. Every page is one request and one
committed capture before anything parses it, recorded in
``control.treasury_mts_page`` so an interrupted read is visible page by page.
"""

from __future__ import annotations

import time
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

from .client import TreasuryMtsPage, fetch_page, page_parameters
from .config import SOURCE_CODE, TreasuryMtsConfig
from .registry import (
    FIRST_RECORD_DATE,
    FISCAL_DATA_BASE_URL,
    REVISION_WINDOW_MONTHS,
    TABLE_PATH,
)

PARSER_CONTRACT_VERSION = "treasury_mts_table9:v1"


def _execute(
    connection_factory: Callable[[], Any], sql: str, parameters: tuple = ()
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


def window_start(newest_published: date | None, *, history: bool = False) -> str:
    """The first record date a read asks for."""
    if history or newest_published is None:
        return FIRST_RECORD_DATE
    month = (
        newest_published.year * 12 + newest_published.month - 1 - REVISION_WINDOW_MONTHS
    )
    start = date(month // 12, month % 12 + 1, 1)
    return max(start.isoformat(), FIRST_RECORD_DATE)


def newest_published_record_date(connection_factory: Callable[[], Any]) -> date | None:
    rows = _execute(
        connection_factory,
        "SELECT MAX(record_date) FROM silver_treasury_mts.statement_line",
    )
    return rows[0][0] if rows else None


def capture_statements(
    connection_factory: Callable[[], Any],
    *,
    conf: dict[str, Any] | None = None,
    config: TreasuryMtsConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    sleep: Callable[[float], None] = time.sleep,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, int]:
    """Capture every page of the read; return (run_id, pages captured)."""
    runtime = config or TreasuryMtsConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    since = window_start(
        newest_published_record_date(connection_factory),
        history=bool(conf and conf.get("history")),
    )
    run_id = capture_control.start_run(watermark={"since": since})
    pages = 0
    try:
        page_number, total_pages = 1, 1
        while page_number <= total_pages:
            if page_number > 1:
                sleep(runtime.min_spacing_seconds)
            page = _capture_page(
                connection_factory,
                page_number,
                since=since,
                run_id=run_id,
                runtime=runtime,
                client=client,
                capture_control=capture_control,
                persist_capture=persist_capture,
                sleep=sleep,
            )
            total_pages = max(page.total_pages, 1)
            pages += 1
            page_number += 1
        capture_control.finish_run(run_id, status="success")
    except BaseException as exc:
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
    return run_id, pages


def _capture_page(
    connection_factory: Callable[[], Any],
    page_number: int,
    *,
    since: str,
    run_id: UUID,
    runtime: TreasuryMtsConfig,
    client: Any | None,
    capture_control: CaptureControl,
    persist_capture: Callable[[Callable[[], Any], ResponseCapture], CaptureReceipt],
    sleep: Callable[[float], None],
) -> TreasuryMtsPage:
    endpoint = FISCAL_DATA_BASE_URL + TABLE_PATH
    request_parameters = page_parameters(
        page_number, page_size=runtime.page_size, since=since
    )
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=endpoint,
        parameters=request_parameters,
        max_attempts=runtime.max_attempts,
    )
    try:
        page = fetch_page(
            page_number,
            since=since,
            config=runtime,
            client=client,
            on_retry=lambda error: capture_control.record_request_retry(
                request.request_id, error=error
            ),
            sleep=sleep,
        )
        receipt = persist_capture(
            connection_factory,
            ResponseCapture(
                capture_id=uuid4(),
                request_id=request.request_id,
                run_id=run_id,
                source_code=SOURCE_CODE,
                endpoint=endpoint,
                request_parameters=page.parameters,
                retrieved_at=datetime.now(UTC),
                http_status=page.http_status,
                response_headers=dict(page.response_headers),
                media_type="application/json",
                payload=page.raw_bytes,
                payload_schema_version=PARSER_CONTRACT_VERSION,
                source_revision=f"since {since} page {page_number}",
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
    except BaseException as exc:
        capture_control.finish_request(request.request_id, status="failed", error=exc)
        raise
    _execute(
        connection_factory,
        """
        INSERT INTO control.treasury_mts_page (
            run_id, page_number, since_record_date, capture_id, payload_checksum,
            row_count, status
        ) VALUES (%s, %s, %s, %s, %s, %s, 'captured')
        """,
        (
            str(run_id),
            page_number,
            since,
            str(receipt.capture_id),
            receipt.payload_checksum,
            page.row_count,
        ),
    )
    return page
