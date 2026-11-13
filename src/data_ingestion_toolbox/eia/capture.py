"""Capture-first orchestration for EIA prices, one registered route at a time.

One ingestion run per read of a window of periods of one route
(``routes.ROUTES``): every page of the answer is its own request and
committed capture before anything parses it. The pages are sorted by period
and series (or area and product), so paging is deterministic. The key
travels only inside the client; captured endpoints and parameters name the
route, the window and the page. Retail gasoline's request, record and
capture are exactly what they were before the registry.
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
from .config import EIA_API_BASE_URL, SOURCE_CODE, EiaConfig
from .routes import (
    GASOLINE,
    EiaRoute,
    recorded_parameters,
    request_parameters,
    route_for,
)

PARSER_CONTRACT_VERSION = route_for(GASOLINE).parser_contract_version
DATA_ROUTE = route_for(GASOLINE).data_route


def period_text(route: EiaRoute, day: date | None) -> str | None:
    """A period as the route's API reads it: ``YYYY-MM-DD`` weekly, ``YYYY-MM`` monthly."""
    if day is None:
        return None
    return day.isoformat() if route.frequency == "weekly" else day.strftime("%Y-%m")


def window_parameters(
    start: date,
    end: date | None,
    *,
    offset: int,
    length: int,
    route: str = GASOLINE,
) -> list[tuple[str, str]]:
    """The query for one page of a window, without the key."""
    entry = route_for(route)
    return request_parameters(
        entry,
        str(period_text(entry, start)),
        period_text(entry, end),
        offset=offset,
        length=length,
    )


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
    route: str = GASOLINE,
) -> UUID:
    """Capture every page of one window of one route's periods; return the run id."""
    entry = route_for(route)
    data_route = entry.data_route
    runtime = config or EiaConfig.from_environment()
    # A missing key fails here, before any run is recorded.
    api = EiaClient(runtime, client=client, **({"sleep": sleep} if sleep else {}))
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    watermark = {
        "start_week": start.isoformat(),
        "end_week": end.isoformat() if end else None,
    }
    if entry.key != GASOLINE:
        watermark["route"] = entry.key
    run_id = capture_control.start_run(watermark=watermark)
    endpoint = f"{EIA_API_BASE_URL}/{data_route}"
    receipts: list[CaptureReceipt] = []
    try:
        offset, total = 0, None
        while total is None or offset < total:
            first, last = period_text(entry, start), period_text(entry, end)
            parameters = request_parameters(
                entry, str(first), last, offset=offset, length=runtime.page_length
            )
            # What the request asked, as recorded with the request and its
            # capture alike (the capture's fingerprint must match); the key
            # is not among it.
            recorded = recorded_parameters(
                entry, str(first), last, offset=offset, length=runtime.page_length
            )
            request = capture_control.start_request(
                run_id=run_id,
                endpoint=endpoint,
                parameters=recorded,
                max_attempts=runtime.max_attempts,
            )
            try:
                response = api.get(
                    data_route,
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
                        payload_schema_version=entry.parser_contract_version,
                        source_revision=(
                            f"{start.isoformat()}+{offset}"
                            if entry.key == GASOLINE
                            else f"{entry.key}:{start.isoformat()}+{offset}"
                        ),
                    ),
                )
            except BaseException as exc:
                capture_control.finish_request(
                    request.request_id, status="failed", error=exc
                )
                raise
            capture_control.finish_request(request.request_id, status="captured")
            receipts.append(receipt)
            page_total, rows = page_rows(response.raw_bytes, data_route)
            if total is None:
                total = page_total
            elif page_total != total:
                # The answer changed under the read: pages would not join up.
                raise EiaPayloadError(data_route, code="total_changed_while_paging")
            if not rows:
                break
            offset += len(rows)
        if total and offset < total:
            raise EiaPayloadError(data_route, code="short_read")
        checksum = hashlib.sha256(
            "\n".join(receipt.payload_checksum for receipt in receipts).encode()
        ).hexdigest()
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.eia_read (
                        run_id, route, start_week, end_week, page_count, row_total,
                        payload_checksum, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'captured')
                    """,
                    (
                        str(run_id),
                        entry.key,
                        start,
                        end,
                        len(receipts),
                        total or 0,
                        checksum,
                    ),
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


def _months_before(day: date, months: int) -> date:
    index = day.year * 12 + day.month - 1 - months
    return date(index // 12, index % 12 + 1, 1)


def plan_window(
    connection_factory: Callable[[], Any],
    config: EiaConfig,
    route: str = GASOLINE,
) -> date:
    """The first period the next read of a route asks for.

    The whole history on a warehouse that has published none of the route;
    otherwise the newest published period less the route's refresh window,
    so a period EIA revised is read again and every period not yet loaded
    is asked for (the LOADER RULE: a scheduled run loads what was never
    loaded). Gasoline keeps its own setting, ``refresh_weeks``.
    """
    entry = route_for(route)
    if entry.key == GASOLINE:
        query = """
            SELECT MAX(fact.week_start)
            FROM silver_eia.fact_retail_price AS fact
            JOIN control.eia_read AS read ON read.run_id = fact.run_id
            WHERE read.status = 'published'
        """
        parameters: tuple = ()
    else:
        query = """
            SELECT MAX(fact.period_start)
            FROM silver_eia.fact_energy_price AS fact
            JOIN control.eia_read AS read ON read.run_id = fact.run_id
            WHERE read.status = 'published' AND fact.route = %s
        """
        parameters = (entry.key,)
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            newest = cursor.fetchone()[0]
    finally:
        connection.close()
    first = config.history_start if entry.key == GASOLINE else entry.history_start
    if newest is None:
        return first
    if entry.key == GASOLINE:
        back = newest - timedelta(weeks=config.refresh_weeks)
    elif entry.frequency == "weekly":
        back = newest - timedelta(weeks=entry.refresh_periods)
    else:
        back = _months_before(newest, entry.refresh_periods)
    return max(first, back)
