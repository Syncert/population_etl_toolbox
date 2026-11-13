"""Capture-first orchestration for the CDC COVID-19 case surveillance counts.

A snapshot is one complete read, in three steps, each request its own run and
each response committed before anything parses it:

1. the dataset's view document;
2. the slice index: CDC's records per state of residence and case month,
   grouped by Socrata; every state-month becomes a pending slice;
3. one request per slice, grouped by county, age group, hospitalisation and
   death, a pause apart.

A slice is accepted only when its groups add up to the records the index
counted for it and it did not fill the group limit; otherwise the snapshot
is ``quarantined`` and nothing from it is served. A run that stops part way
resumes at the next pending slice. The dataset is frozen, so once a snapshot
is published no run reads it again unless it asks for history; a history
snapshot whose index and every slice repeat the published bytes is recorded
``unchanged`` and replays nothing (CHURN RULE).
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

from .client import (
    CdcCovidPayloadError,
    fetch_index,
    fetch_metadata,
    fetch_slice,
    grouped_rows,
    index_parameters,
    record_count,
    rows_updated_at,
    slice_parameters,
)
from .config import METADATA_PATH, RESOURCE_PATH, SOURCE_CODE, CdcCovidCasesConfig
from .registry import PARSER_CONTRACT_VERSION

METADATA_CONTRACT_VERSION = "socrata_view:v1"
INDEX_CONTRACT_VERSION = "cdc_n8mc_b4w4_slice_index_json:v1"

PersistCapture = Callable[[Callable[[], Any], ResponseCapture], CaptureReceipt]


class CdcCovidSliceError(RuntimeError):
    """A captured response that cannot be accepted as a complete slice."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        super().__init__(f"CDC case surveillance {code}: {detail}")


def _query(factory: Callable[[], Any], sql: str, parameters: tuple = ()) -> list[tuple]:
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        connection.close()


def _execute(factory: Callable[[], Any], statements: list[tuple[str, Any]]) -> None:
    connection = factory()
    try:
        with connection.cursor() as cursor:
            for sql, parameters in statements:
                if isinstance(parameters, list):
                    cursor.executemany(sql, parameters)
                else:
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
    config: CdcCovidCasesConfig,
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


def _text(row: dict[str, Any], key: str) -> str | None:
    value = row.get(key)
    if value is None:
        return None
    return str(value)


def index_slices(
    raw_bytes: bytes, *, group_limit: int
) -> list[tuple[str | None, str | None, int]]:
    """The (state, month, records) of every slice the index names, in its order."""
    rows = grouped_rows(raw_bytes, RESOURCE_PATH)
    if len(rows) >= group_limit:
        raise CdcCovidSliceError(
            "index_filled_limit", f"the index returned {len(rows)} groups"
        )
    slices = []
    seen: set[tuple[str | None, str | None]] = set()
    for row in rows:
        records = record_count(row)
        key = (_text(row, "res_state"), _text(row, "case_month"))
        if records is None:
            raise CdcCovidPayloadError(RESOURCE_PATH, code="unreadable_index_count")
        if key in seen:
            raise CdcCovidPayloadError(RESOURCE_PATH, code="repeated_index_slice")
        seen.add(key)
        slices.append((key[0], key[1], records))
    return slices


def start_snapshot(
    factory: Callable[[], Any],
    *,
    config: CdcCovidCasesConfig,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: PersistCapture = persist_response_capture,
    sleep: Callable[[float], None] = time.sleep,
) -> UUID:
    """Capture the view document and the slice index; open a snapshot."""
    capture_control = control or CaptureControl(factory, source_code=SOURCE_CODE)
    meta_run, meta_response, meta_receipt = _capture(
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
    capture_control.finish_run(meta_run, status="success")
    sleep(config.min_spacing_seconds)
    index_run, index_response, index_receipt = _capture(
        factory,
        control=capture_control,
        persist_capture=persist_capture,
        config=config,
        parameters={
            "path": RESOURCE_PATH,
            "document": "slice_index",
            **index_parameters(limit=config.group_limit),
        },
        fetch=lambda on_retry: fetch_index(
            config=config, client=client, on_retry=on_retry
        ),
        contract=INDEX_CONTRACT_VERSION,
    )
    snapshot_id = uuid4()
    try:
        slices = index_slices(index_response.raw_bytes, group_limit=config.group_limit)
        _execute(
            factory,
            [
                (
                    """
                    INSERT INTO control.cdc_covid_cases_snapshot (
                        snapshot_id, metadata_run_id, metadata_capture_id,
                        index_run_id, index_capture_id, index_payload_checksum,
                        rows_updated_at, group_limit, slice_count,
                        index_record_count, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'capturing')
                    """,
                    (
                        str(snapshot_id),
                        str(meta_run),
                        str(meta_receipt.capture_id),
                        str(index_run),
                        str(index_receipt.capture_id),
                        index_receipt.payload_checksum,
                        rows_updated_at(meta_response.raw_bytes),
                        config.group_limit,
                        len(slices),
                        sum(records for _state, _month, records in slices),
                    ),
                ),
                (
                    """
                    INSERT INTO control.cdc_covid_cases_slice (
                        snapshot_id, slice_index, res_state, case_month,
                        index_record_count
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    [
                        (str(snapshot_id), position, state, month, records)
                        for position, (state, month, records) in enumerate(slices)
                    ],
                ),
            ],
        )
    except BaseException as exc:
        capture_control.finish_run(index_run, status="failed", error=exc)
        raise
    capture_control.finish_run(index_run, status="success")
    return snapshot_id


def capture_slice(
    factory: Callable[[], Any],
    snapshot_id: UUID,
    slice_index: int,
    res_state: str | None,
    case_month: str | None,
    index_records: int,
    *,
    config: CdcCovidCasesConfig,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: PersistCapture = persist_response_capture,
) -> UUID:
    """Capture one state-month slice and check it against the index; return its run."""
    capture_control = control or CaptureControl(factory, source_code=SOURCE_CODE)
    run_id, response, receipt = _capture(
        factory,
        control=capture_control,
        persist_capture=persist_capture,
        config=config,
        parameters={
            "path": RESOURCE_PATH,
            "snapshot_id": str(snapshot_id),
            "slice_index": slice_index,
            **slice_parameters(
                res_state=res_state, case_month=case_month, limit=config.group_limit
            ),
        },
        fetch=lambda on_retry: fetch_slice(
            res_state, case_month, config=config, client=client, on_retry=on_retry
        ),
        contract=PARSER_CONTRACT_VERSION,
    )
    try:
        rows = grouped_rows(response.raw_bytes, RESOURCE_PATH)
        counts = [record_count(row) for row in rows]
        readable = all(count is not None for count in counts)
        records = sum(count for count in counts if count is not None)
        problem = None
        if len(rows) >= config.group_limit:
            problem = ("slice_filled_limit", f"{len(rows)} groups")
        elif readable and records != index_records:
            problem = (
                "slice_total_differs",
                f"{res_state} {case_month}: groups add to {records}, index {index_records}",
            )
        statements: list[tuple[str, Any]] = [
            (
                """
                UPDATE control.cdc_covid_cases_slice
                   SET run_id = %s, capture_id = %s, payload_checksum = %s,
                       group_count = %s, record_count = %s, captured_at = NOW()
                 WHERE snapshot_id = %s AND slice_index = %s
                """,
                (
                    str(run_id),
                    str(receipt.capture_id),
                    receipt.payload_checksum,
                    len(rows),
                    records,
                    str(snapshot_id),
                    slice_index,
                ),
            )
        ]
        if problem is not None:
            statements.append(
                (
                    """
                    UPDATE control.cdc_covid_cases_snapshot
                       SET status = 'quarantined', updated_at = NOW()
                     WHERE snapshot_id = %s
                    """,
                    (str(snapshot_id),),
                )
            )
        _execute(factory, statements)
        if problem is not None:
            raise CdcCovidSliceError(*problem)
    except BaseException as exc:
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
    capture_control.finish_run(run_id, status="success")
    return run_id


#: Whether the index and every slice of a captured snapshot repeat the
#: published snapshot's bytes for the same state and month.
_REPEATS_PUBLISHED_SQL = """
    WITH published AS (
        SELECT snapshot_id, index_payload_checksum, slice_count, group_limit
        FROM control.cdc_covid_cases_snapshot
        WHERE status = 'published' ORDER BY published_at DESC LIMIT 1
    ), candidate AS (
        SELECT snapshot_id, index_payload_checksum, slice_count, group_limit
        FROM control.cdc_covid_cases_snapshot WHERE snapshot_id = %s
    )
    SELECT EXISTS (SELECT 1 FROM published)
       AND (SELECT index_payload_checksum FROM published)
           = (SELECT index_payload_checksum FROM candidate)
       AND (SELECT slice_count FROM published) = (SELECT slice_count FROM candidate)
       AND (SELECT group_limit FROM published) = (SELECT group_limit FROM candidate)
       AND NOT EXISTS (
           SELECT 1
           FROM control.cdc_covid_cases_slice AS slice
           JOIN raw_capture.response_capture AS capture ON capture.capture_id = slice.capture_id
           LEFT JOIN control.cdc_covid_cases_slice AS before
             ON before.snapshot_id = (SELECT snapshot_id FROM published)
            AND before.res_state IS NOT DISTINCT FROM slice.res_state
            AND before.case_month IS NOT DISTINCT FROM slice.case_month
            AND before.payload_checksum = slice.payload_checksum
           LEFT JOIN raw_capture.response_capture AS before_capture
             ON before_capture.capture_id = before.capture_id
           WHERE slice.snapshot_id = (SELECT snapshot_id FROM candidate)
             AND (before.run_id IS NULL
                  OR before_capture.payload_schema_version
                     IS DISTINCT FROM capture.payload_schema_version)
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
        SELECT snapshot_id::TEXT, status FROM control.cdc_covid_cases_snapshot
        ORDER BY created_at DESC LIMIT 1
        """,
    )
    published = _query(
        factory,
        "SELECT 1 FROM control.cdc_covid_cases_snapshot WHERE status = 'published' LIMIT 1",
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
    config: CdcCovidCasesConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: PersistCapture = persist_response_capture,
    sleep: Callable[[float], None] = time.sleep,
) -> UUID | None:
    """Capture (or resume) a snapshot; return it when it is ready to replay."""
    runtime = config or CdcCovidCasesConfig()
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
            sleep=sleep,
        )
    assert snapshot_id is not None
    (group_limit,) = _query(
        factory,
        "SELECT group_limit FROM control.cdc_covid_cases_snapshot WHERE snapshot_id = %s",
        (str(snapshot_id),),
    )[0]
    if group_limit != runtime.group_limit:
        runtime = runtime.model_copy(update={"group_limit": group_limit})
    pending = _query(
        factory,
        """
        SELECT slice_index, res_state, case_month, index_record_count
        FROM control.cdc_covid_cases_slice
        WHERE snapshot_id = %s AND run_id IS NULL
        ORDER BY slice_index
        """,
        (str(snapshot_id),),
    )
    for slice_index, res_state, case_month, index_records in pending:
        sleep(runtime.min_spacing_seconds)
        capture_slice(
            factory,
            snapshot_id,
            int(slice_index),
            res_state,
            case_month,
            int(index_records),
            config=runtime,
            client=client,
            control=control,
            persist_capture=persist_capture,
        )
    _execute(
        factory,
        [
            (
                """
                UPDATE control.cdc_covid_cases_snapshot AS snapshot
                   SET group_count = slices.group_count,
                       captured_record_count = slices.record_count,
                       status = 'captured', updated_at = NOW()
                  FROM (
                      SELECT COALESCE(SUM(group_count), 0) AS group_count,
                             COALESCE(SUM(record_count), 0) AS record_count
                      FROM control.cdc_covid_cases_slice WHERE snapshot_id = %s
                  ) AS slices
                 WHERE snapshot.snapshot_id = %s AND snapshot.status = 'capturing'
                """,
                (str(snapshot_id), str(snapshot_id)),
            )
        ],
    )
    (repeats,) = _query(factory, _REPEATS_PUBLISHED_SQL, (str(snapshot_id),))[0]
    if repeats:
        _execute(
            factory,
            [
                (
                    """
                    UPDATE control.cdc_covid_cases_snapshot
                       SET status = 'unchanged', updated_at = NOW() WHERE snapshot_id = %s
                    """,
                    (str(snapshot_id),),
                )
            ],
        )
        return None
    return snapshot_id
