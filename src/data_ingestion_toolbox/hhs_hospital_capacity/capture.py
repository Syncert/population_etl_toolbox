"""Capture-first orchestration for the HHS hospital capacity dataset.

A snapshot is one complete read: the dataset's view document as its own run,
then every page in order, each page its own run with one request and one
committed capture before anything parses it. The snapshot is complete when a
page holds fewer rows than were asked for. A run that stops part way resumes
at the next page of the same snapshot. The dataset is frozen, so once a
snapshot is published no run reads it again unless it asks for history; a
history snapshot whose every page has the bytes of the published one's page
at the same place is recorded ``unchanged`` and replays nothing (CHURN RULE).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from data_ingestion_toolbox.capture import (
    CaptureControl,
    CaptureReceipt,
    ResponseCapture,
    persist_response_capture,
)

from .client import fetch_metadata, fetch_page, page_parameters, rows_updated_at
from .config import (
    METADATA_PATH,
    RESOURCE_PATH,
    SOURCE_CODE,
    HhsHospitalCapacityConfig,
)
from .registry import PARSER_CONTRACT_VERSION

METADATA_CONTRACT_VERSION = "socrata_view:v1"

PersistCapture = Callable[[Callable[[], Any], ResponseCapture], CaptureReceipt]


def _query(factory: Callable[[], Any], sql: str, parameters: tuple = ()) -> list[tuple]:
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        connection.close()


def _execute(factory: Callable[[], Any], sql: str, parameters: tuple) -> None:
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, parameters)
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _capture(
    factory: Callable[[], Any],
    *,
    control: CaptureControl,
    persist_capture: PersistCapture,
    config: HhsHospitalCapacityConfig,
    parameters: dict[str, object],
    fetch: Callable[[Callable[[BaseException], None]], Any],
    contract: str,
) -> tuple[UUID, Any, CaptureReceipt]:
    run_id = control.start_run(watermark=dict(parameters))
    request = None
    try:

        def on_retry(error: BaseException) -> None:
            control.record_request_retry(request.request_id, error=error)

        endpoint = f"{config.base_url}{parameters['path']}"
        request = control.start_request(
            run_id=run_id,
            endpoint=endpoint,
            parameters=parameters,
            max_attempts=config.max_attempts,
        )
        response = fetch(on_retry)
        receipt = persist_capture(
            factory,
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
                media_type="application/json",
                payload=response.raw_bytes,
                payload_schema_version=contract,
                source_revision=response.response_headers.get("last-modified"),
            ),
        )
        control.finish_request(request.request_id, status="captured")
        return run_id, response, receipt
    except BaseException as exc:
        if request is not None:
            control.finish_request(request.request_id, status="failed", error=exc)
        control.finish_run(run_id, status="failed", error=exc)
        raise


def start_snapshot(
    factory: Callable[[], Any],
    *,
    config: HhsHospitalCapacityConfig,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: PersistCapture = persist_response_capture,
) -> UUID:
    """Capture the view document and open a new snapshot; return its id."""
    capture_control = control or CaptureControl(factory, source_code=SOURCE_CODE)
    run_id, response, receipt = _capture(
        factory,
        control=capture_control,
        persist_capture=persist_capture,
        config=config,
        parameters={"path": METADATA_PATH, "document": "view"},
        fetch=lambda on_retry: fetch_metadata(
            config=config, client=client, on_retry=on_retry
        ),
        contract=METADATA_CONTRACT_VERSION,
    )
    snapshot_id = uuid4()
    try:
        _execute(
            factory,
            """
            INSERT INTO control.hhs_hospital_capacity_snapshot (
                snapshot_id, metadata_run_id, metadata_capture_id, rows_updated_at,
                page_size, status
            ) VALUES (%s, %s, %s, %s, %s, 'capturing')
            """,
            (
                str(snapshot_id),
                str(run_id),
                str(receipt.capture_id),
                rows_updated_at(response.raw_bytes),
                config.page_size,
            ),
        )
    except BaseException as exc:
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
    capture_control.finish_run(run_id, status="success")
    return snapshot_id


def capture_page(
    factory: Callable[[], Any],
    snapshot_id: UUID,
    page_index: int,
    *,
    config: HhsHospitalCapacityConfig,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: PersistCapture = persist_response_capture,
) -> tuple[UUID, int]:
    """Capture one page of a snapshot; return (run_id, rows on the page)."""
    capture_control = control or CaptureControl(factory, source_code=SOURCE_CODE)
    offset = page_index * config.page_size
    run_id, response, receipt = _capture(
        factory,
        control=capture_control,
        persist_capture=persist_capture,
        config=config,
        parameters={
            "path": RESOURCE_PATH,
            "snapshot_id": str(snapshot_id),
            "page_index": page_index,
            **page_parameters(offset=offset, page_size=config.page_size),
        },
        fetch=lambda on_retry: fetch_page(
            offset, config=config, client=client, on_retry=on_retry
        ),
        contract=PARSER_CONTRACT_VERSION,
    )
    try:
        _execute(
            factory,
            """
            INSERT INTO control.hhs_hospital_capacity_page (
                run_id, snapshot_id, page_index, row_offset, capture_id,
                payload_checksum, row_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(run_id),
                str(snapshot_id),
                page_index,
                offset,
                str(receipt.capture_id),
                receipt.payload_checksum,
                response.row_count,
            ),
        )
    except BaseException as exc:
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
    capture_control.finish_run(run_id, status="success")
    return run_id, int(response.row_count)


#: Whether every page of a captured snapshot has the bytes of the published
#: snapshot's page at the same place, with the same page size and count.
_REPEATS_PUBLISHED_SQL = """
    WITH published AS (
        SELECT snapshot_id, page_size, page_count FROM control.hhs_hospital_capacity_snapshot
        WHERE status = 'published' ORDER BY published_at DESC LIMIT 1
    ), candidate AS (
        SELECT snapshot_id, page_size, page_count FROM control.hhs_hospital_capacity_snapshot
        WHERE snapshot_id = %s
    )
    SELECT EXISTS (SELECT 1 FROM published)
       AND (SELECT page_size FROM published) = (SELECT page_size FROM candidate)
       AND (SELECT page_count FROM published) = (SELECT page_count FROM candidate)
       AND NOT EXISTS (
           SELECT 1
           FROM control.hhs_hospital_capacity_page AS page
           LEFT JOIN control.hhs_hospital_capacity_page AS before
             ON before.snapshot_id = (SELECT snapshot_id FROM published)
            AND before.page_index = page.page_index
            AND before.payload_checksum = page.payload_checksum
           JOIN raw_capture.response_capture AS capture ON capture.capture_id = page.capture_id
           LEFT JOIN raw_capture.response_capture AS before_capture
             ON before_capture.capture_id = before.capture_id
           WHERE page.snapshot_id = (SELECT snapshot_id FROM candidate)
             AND (before.run_id IS NULL
                  OR before_capture.payload_schema_version IS DISTINCT FROM capture.payload_schema_version)
       )
"""


def plan_snapshot(
    factory: Callable[[], Any], conf: dict[str, Any] | None
) -> tuple[str, UUID | None]:
    """What this run does: ('nothing'|'resume'|'replay'|'start', snapshot)."""
    history = bool(conf and conf.get("history"))
    rows = _query(
        factory,
        """
        SELECT snapshot_id::TEXT, status FROM control.hhs_hospital_capacity_snapshot
        ORDER BY created_at DESC LIMIT 1
        """,
    )
    published = _query(
        factory,
        "SELECT 1 FROM control.hhs_hospital_capacity_snapshot WHERE status = 'published' LIMIT 1",
    )
    if rows:
        snapshot_id, status = UUID(rows[0][0]), rows[0][1]
        if status == "capturing":
            return "resume", snapshot_id
        if status in ("captured", "silver_ready"):
            return "replay", snapshot_id
    if published and not history:
        return "nothing", None
    if rows and rows[0][1] == "quarantined" and not history:
        return "nothing", None
    return "start", None


def capture_snapshot(
    factory: Callable[[], Any],
    *,
    conf: dict[str, Any] | None = None,
    config: HhsHospitalCapacityConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: PersistCapture = persist_response_capture,
    sleep: Callable[[float], None] = time.sleep,
) -> UUID | None:
    """Capture (or resume) a snapshot; return it when it is ready to replay."""
    runtime = config or HhsHospitalCapacityConfig()
    action, snapshot_id = plan_snapshot(factory, conf)
    if action == "nothing":
        return None
    if action == "replay":
        return snapshot_id
    if action == "start":
        snapshot_id = start_snapshot(
            factory,
            config=runtime,
            client=client,
            control=control,
            persist_capture=persist_capture,
        )
    assert snapshot_id is not None
    page_size, pages = _query(
        factory,
        """
        SELECT snapshot.page_size, COUNT(page.run_id)
        FROM control.hhs_hospital_capacity_snapshot AS snapshot
        LEFT JOIN control.hhs_hospital_capacity_page AS page
          ON page.snapshot_id = snapshot.snapshot_id
        WHERE snapshot.snapshot_id = %s
        GROUP BY snapshot.page_size
        """,
        (str(snapshot_id),),
    )[0]
    if page_size != runtime.page_size:
        runtime = runtime.model_copy(update={"page_size": page_size})
    last = _query(
        factory,
        """
        SELECT row_count FROM control.hhs_hospital_capacity_page
        WHERE snapshot_id = %s ORDER BY page_index DESC LIMIT 1
        """,
        (str(snapshot_id),),
    )
    page_index = int(pages)
    complete = bool(last) and int(last[0][0]) < page_size
    while not complete:
        if page_index:
            sleep(runtime.min_spacing_seconds)
        _run_id, row_count = capture_page(
            factory,
            snapshot_id,
            page_index,
            config=runtime,
            client=client,
            control=control,
            persist_capture=persist_capture,
        )
        page_index += 1
        complete = row_count < page_size
    _execute(
        factory,
        """
        UPDATE control.hhs_hospital_capacity_snapshot AS snapshot
           SET page_count = pages.page_count, row_count = pages.row_count,
               status = 'captured', updated_at = NOW()
          FROM (
              SELECT COUNT(*) AS page_count, COALESCE(SUM(row_count), 0) AS row_count
              FROM control.hhs_hospital_capacity_page WHERE snapshot_id = %s
          ) AS pages
         WHERE snapshot.snapshot_id = %s
        """,
        (str(snapshot_id), str(snapshot_id)),
    )
    (repeats,) = _query(factory, _REPEATS_PUBLISHED_SQL, (str(snapshot_id),))[0]
    if repeats:
        _execute(
            factory,
            """
            UPDATE control.hhs_hospital_capacity_snapshot
               SET status = 'unchanged', updated_at = NOW() WHERE snapshot_id = %s
            """,
            (str(snapshot_id),),
        )
        return None
    return snapshot_id
