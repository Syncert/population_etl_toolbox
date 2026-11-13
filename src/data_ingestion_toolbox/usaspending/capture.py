"""Capture-first orchestration for USAspending geography reads.

One ingestion run per read. Each fiscal year a run works on is cut four ways
and read for the state and county layers: every award, each award type
group, and each awarding agency the year's agency list names (that list is
read first, page by page). Every request is one committed capture before
anything parses it, recorded in ``control.usaspending_request``.

An ordinary run reads the current and prior fiscal years again (agencies are
still reporting them); a retry of the same scheduled run skips what its
earlier attempts already captured. ``{"history": true}`` also reads, oldest year first,
every request of fiscal 2008 onward never captured before; a run stops at
``max_requests_per_run`` and the next history run continues from the
requests still never captured, so an interrupted load resumes rather than
restarts.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any
from uuid import UUID, uuid4

from data_ingestion_toolbox.capture import (
    CaptureControl,
    CaptureReceipt,
    ResponseCapture,
    persist_response_capture,
)

from .client import UsaspendingResponse, post_search
from .config import SOURCE_CODE, UsaspendingConfig
from .registry import (
    AGENCY_PATH,
    API_BASE_URL,
    AWARD_GROUPS,
    CUT_AGENCY,
    CUT_ALL,
    CUT_AWARD_TYPE,
    FIRST_FISCAL_YEAR,
    GEO_LAYERS,
    GEOGRAPHY_PATH,
    REFRESH_FISCAL_YEARS,
    fiscal_year_period,
)

PARSER_CONTRACT_VERSION = "usaspending_geography:v1"
CUT_AGENCY_LIST = "agency_list"
AGENCY_PAGE_SIZE = 100


@dataclass(frozen=True)
class RequestKey:
    fiscal_year: int
    geo_layer: str
    cut_kind: str
    cut_key: str
    agency_name: str | None = None

    @property
    def identity(self) -> tuple[int, str, str, str]:
        return (self.fiscal_year, self.geo_layer, self.cut_kind, self.cut_key)


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


def current_fiscal_year(today: date) -> int:
    """The federal fiscal year ``today`` falls in (October starts the next)."""
    return today.year + (1 if today.month >= 10 else 0)


def fiscal_years(today: date, *, history: bool) -> list[int]:
    """The years a run works on: the refresh years, then (history) the rest oldest first."""
    newest = current_fiscal_year(today)
    refresh = list(range(newest, newest - REFRESH_FISCAL_YEARS, -1))
    if not history:
        return refresh
    return refresh + list(range(FIRST_FISCAL_YEAR, newest - REFRESH_FISCAL_YEARS + 1))


def geography_body(key: RequestKey) -> dict[str, Any]:
    """The request body of one geography read."""
    filters: dict[str, Any] = {"time_period": [fiscal_year_period(key.fiscal_year)]}
    if key.cut_kind == CUT_AWARD_TYPE:
        group = next(group for group in AWARD_GROUPS if group.key == key.cut_key)
        filters["award_type_codes"] = list(group.codes)
    elif key.cut_kind == CUT_AGENCY:
        filters["agencies"] = [
            {"type": "awarding", "tier": "toptier", "name": key.agency_name}
        ]
    return {
        "scope": "place_of_performance",
        "geo_layer": key.geo_layer,
        "filters": filters,
    }


def agency_list_body(fiscal_year: int, page: int) -> dict[str, Any]:
    return {
        "filters": {"time_period": [fiscal_year_period(fiscal_year)]},
        "limit": AGENCY_PAGE_SIZE,
        "page": page,
    }


def planned_geography_keys(
    fiscal_year: int, agencies: list[tuple[str, str]]
) -> Iterator[RequestKey]:
    """Every geography read of a year: all awards, each group, each agency."""
    for layer in GEO_LAYERS:
        yield RequestKey(fiscal_year, layer, CUT_ALL, "")
        for group in AWARD_GROUPS:
            yield RequestKey(fiscal_year, layer, CUT_AWARD_TYPE, group.key)
        for code, name in agencies:
            yield RequestKey(fiscal_year, layer, CUT_AGENCY, code, name)


def agencies_of(raw_bytes: bytes) -> list[tuple[str, str]]:
    """(code, name) of each agency an awarding-agency page names, as published."""
    seen: dict[str, str] = {}
    for row in json.loads(raw_bytes)["results"]:
        code = str(row.get("code") or "").strip()
        name = str(row.get("name") or "").strip()
        if code and name and code not in seen:
            seen[code] = name
    return sorted(seen.items())


def captured_identities(
    connection_factory: Callable[[], Any],
    fiscal_year: int,
    *,
    since: datetime | None = None,
) -> set[tuple[int, str, str, str]]:
    """The request keys of a year some run captured and did not refuse.

    ``since`` keeps only captures recorded at or after that time.
    """
    rows = _execute(
        connection_factory,
        """
        SELECT DISTINCT fiscal_year, geo_layer, cut_kind, cut_key
          FROM control.usaspending_request
         WHERE fiscal_year = %s
           AND status IN ('captured', 'silver_ready', 'published')
           AND (%s::timestamptz IS NULL OR created_at >= %s::timestamptz)
        """,
        (fiscal_year, since, since),
    )
    return {(int(row[0]), row[1], row[2], row[3]) for row in rows}


def capture_awards(
    connection_factory: Callable[[], Any],
    *,
    conf: dict[str, Any] | None = None,
    config: UsaspendingConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    today: date | None = None,
    refresh_since: datetime | None = None,
    sleep: Callable[[float], None] = time.sleep,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, int]:
    """Capture the run's requests; return (run_id, requests captured).

    A refresh year is read again in full, except the requests captured at or
    after ``refresh_since``: the DAG passes its run's start, so a retry of the
    same scheduled run continues where the failed attempt stopped instead of
    reading the year again.
    """
    runtime = config or UsaspendingConfig()
    settings = conf or {}
    history = bool(settings.get("history"))
    budget = int(settings.get("max_requests") or runtime.max_requests_per_run)
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    years = fiscal_years(today or datetime.now(UTC).date(), history=history)
    refresh = set(years[:REFRESH_FISCAL_YEARS])
    run_id = capture_control.start_run(
        watermark={"fiscal_years": years, "history": history, "max_requests": budget}
    )
    made = 0

    def capture(
        key: RequestKey, path: str, body: dict[str, Any]
    ) -> UsaspendingResponse:
        nonlocal made
        if made:
            sleep(runtime.min_spacing_seconds)
        made += 1
        return _capture_request(
            connection_factory,
            key,
            path,
            body,
            run_id=run_id,
            runtime=runtime,
            client=client,
            capture_control=capture_control,
            persist_capture=persist_capture,
            sleep=sleep,
        )

    try:
        for fiscal_year in years:
            if made >= budget:
                break
            if fiscal_year not in refresh:
                done = captured_identities(connection_factory, fiscal_year)
            elif refresh_since is not None:
                done = captured_identities(
                    connection_factory, fiscal_year, since=refresh_since
                )
            else:
                done = set()
            # The year's agency list decides its agency reads, so it is read
            # whenever any read of the year is still owed.
            agencies: list[tuple[str, str]] = []
            page, has_next = 1, True
            while has_next:
                response = capture(
                    RequestKey(fiscal_year, "nation", CUT_AGENCY_LIST, str(page)),
                    AGENCY_PATH,
                    agency_list_body(fiscal_year, page),
                )
                agencies.extend(agencies_of(response.raw_bytes))
                has_next = response.has_next
                page += 1
            agencies = sorted(dict(agencies).items())
            for key in planned_geography_keys(fiscal_year, agencies):
                if made >= budget:
                    break
                if key.identity in done:
                    continue
                capture(key, GEOGRAPHY_PATH, geography_body(key))
        capture_control.finish_run(run_id, status="success")
    except BaseException as exc:
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
    return run_id, made


def _capture_request(
    connection_factory: Callable[[], Any],
    key: RequestKey,
    path: str,
    body: dict[str, Any],
    *,
    run_id: UUID,
    runtime: UsaspendingConfig,
    client: Any | None,
    capture_control: CaptureControl,
    persist_capture: Callable[[Callable[[], Any], ResponseCapture], CaptureReceipt],
    sleep: Callable[[float], None],
) -> UsaspendingResponse:
    endpoint = API_BASE_URL + path
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=endpoint,
        parameters=body,
        max_attempts=runtime.max_attempts,
    )
    try:
        response = post_search(
            path,
            body,
            geo_layer=key.geo_layer if path == GEOGRAPHY_PATH else None,
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
                request_parameters=body,
                retrieved_at=datetime.now(UTC),
                http_status=response.http_status,
                response_headers=dict(response.response_headers),
                media_type="application/json",
                payload=response.raw_bytes,
                payload_schema_version=PARSER_CONTRACT_VERSION,
                source_revision=(
                    f"FY{key.fiscal_year} {key.geo_layer} {key.cut_kind} {key.cut_key}".strip()
                ),
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
    except BaseException as exc:
        capture_control.finish_request(request.request_id, status="failed", error=exc)
        raise
    _execute(
        connection_factory,
        """
        INSERT INTO control.usaspending_request (
            run_id, fiscal_year, geo_layer, cut_kind, cut_key, agency_name,
            capture_id, payload_checksum, row_count, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'captured')
        """,
        (
            str(run_id),
            key.fiscal_year,
            key.geo_layer,
            key.cut_kind,
            key.cut_key,
            key.agency_name,
            str(receipt.capture_id),
            receipt.payload_checksum,
            response.row_count,
        ),
    )
    return response
