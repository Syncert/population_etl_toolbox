"""Capture-first orchestration for CDC FluView ILINet.

A release is one run: FluView's init document, then one download per region
type (national, HHS regions, states), each a pause apart and each committed
before anything parses it. The national and HHS-region downloads ask for
every season the init lists; the state download every season the init marks
as having state data. A release whose three ``ILINet.csv`` members repeat the
published release's bytes is recorded ``unchanged`` and replays nothing
(CHURN RULE); CDC revises recent weeks, so a changed file replays and keeps
only the figures that changed.
"""

from __future__ import annotations

import hashlib
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

from .client import download_body, fetch_download, fetch_init, ilinet_csv, read_init
from .config import DOWNLOAD_PATH, INIT_PATH, SOURCE_CODE, CdcIlinetConfig
from .registry import PARSER_CONTRACT_VERSION, REGION_TYPES

INIT_CONTRACT_VERSION = "cdc_fluview_init_json:v1"

PersistCapture = Callable[[Callable[[], Any], ResponseCapture], CaptureReceipt]


def _query(factory: Callable[[], Any], sql: str, parameters: tuple = ()) -> list[tuple]:
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        connection.close()


def _execute(factory: Callable[[], Any], statements: list[tuple[str, tuple]]) -> None:
    connection = factory()
    try:
        with connection.cursor() as cursor:
            for sql, parameters in statements:
                cursor.execute(sql, parameters)
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _persist(
    factory: Callable[[], Any],
    *,
    control: CaptureControl,
    persist_capture: PersistCapture,
    config: CdcIlinetConfig,
    run_id: UUID,
    path: str,
    parameters: dict[str, object],
    fetch: Callable[[Callable[[BaseException], None]], Any],
    contract: str,
) -> tuple[Any, CaptureReceipt]:
    endpoint = f"{config.base_url}{path}"
    request = control.start_request(
        run_id=run_id,
        endpoint=endpoint,
        parameters=parameters,
        max_attempts=config.max_attempts,
    )
    try:
        response = fetch(
            lambda error: control.record_request_retry(request.request_id, error=error)
        )
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
                media_type=response.media_type,
                payload=response.raw_bytes,
                payload_schema_version=contract,
                source_revision=response.response_headers.get("last-modified"),
            ),
        )
    except BaseException as exc:
        control.finish_request(request.request_id, status="failed", error=exc)
        raise
    control.finish_request(request.request_id, status="captured")
    return response, receipt


#: Whether every download of a captured release repeats the published
#: release's ``ILINet.csv`` bytes for the same region type.
_REPEATS_PUBLISHED_SQL = """
    WITH published AS (
        SELECT release_id FROM control.cdc_ilinet_release
        WHERE status = 'published' ORDER BY published_at DESC LIMIT 1
    )
    SELECT EXISTS (SELECT 1 FROM published)
       AND NOT EXISTS (
           SELECT 1
           FROM control.cdc_ilinet_download AS download
           JOIN raw_capture.response_capture AS capture
             ON capture.capture_id = download.capture_id
           LEFT JOIN control.cdc_ilinet_download AS before
             ON before.release_id = (SELECT release_id FROM published)
            AND before.region_type = download.region_type
            AND before.csv_checksum = download.csv_checksum
           LEFT JOIN raw_capture.response_capture AS before_capture
             ON before_capture.capture_id = before.capture_id
           WHERE download.release_id = %s
             AND (before.release_id IS NULL
                  OR before_capture.payload_schema_version
                     IS DISTINCT FROM capture.payload_schema_version)
       )
"""


def pending_release(factory: Callable[[], Any]) -> UUID | None:
    """A captured release not yet published, which a run replays before reading."""
    rows = _query(
        factory,
        """
        SELECT release_id::TEXT FROM control.cdc_ilinet_release
        WHERE status IN ('captured', 'silver_ready')
        ORDER BY created_at DESC LIMIT 1
        """,
    )
    return UUID(rows[0][0]) if rows else None


def capture_release(
    factory: Callable[[], Any],
    *,
    config: CdcIlinetConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: PersistCapture = persist_response_capture,
    sleep: Callable[[float], None] = time.sleep,
) -> UUID | None:
    """Capture one release; return it when it is ready to replay."""
    runtime = config or CdcIlinetConfig()
    pending = pending_release(factory)
    if pending is not None:
        return pending
    capture_control = control or CaptureControl(factory, source_code=SOURCE_CODE)
    release_id = uuid4()
    run_id = capture_control.start_run(watermark={"release_id": str(release_id)})
    try:
        init_response, init_receipt = _persist(
            factory,
            control=capture_control,
            persist_capture=persist_capture,
            config=runtime,
            run_id=run_id,
            path=INIT_PATH,
            parameters={"path": INIT_PATH, "appVersion": "Public"},
            fetch=lambda on_retry: fetch_init(
                config=runtime, client=client, on_retry=on_retry, sleep=sleep
            ),
            contract=INIT_CONTRACT_VERSION,
        )
        _execute(
            factory,
            [
                (
                    """
                    INSERT INTO control.cdc_ilinet_release (
                        release_id, run_id, init_capture_id, status
                    ) VALUES (%s, %s, %s, 'capturing')
                    """,
                    (str(release_id), str(run_id), str(init_receipt.capture_id)),
                )
            ],
        )
        init = read_init(init_response.raw_bytes)
        for region_type in REGION_TYPES:
            sleep(runtime.min_spacing_seconds)
            body = download_body(region_type, init)
            response, receipt = _persist(
                factory,
                control=capture_control,
                persist_capture=persist_capture,
                config=runtime,
                run_id=run_id,
                path=DOWNLOAD_PATH,
                parameters={"path": DOWNLOAD_PATH, **body},
                fetch=lambda on_retry, body=body: fetch_download(
                    body, config=runtime, client=client, on_retry=on_retry, sleep=sleep
                ),
                contract=PARSER_CONTRACT_VERSION,
            )
            csv_bytes = ilinet_csv(response.raw_bytes)
            _execute(
                factory,
                [
                    (
                        """
                        INSERT INTO control.cdc_ilinet_download (
                            release_id, region_type, capture_id, payload_checksum,
                            csv_checksum, season_count
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            str(release_id),
                            region_type.key,
                            str(receipt.capture_id),
                            receipt.payload_checksum,
                            hashlib.sha256(csv_bytes).hexdigest(),
                            len(body["SeasonsDT"]),
                        ),
                    )
                ],
            )
        (repeats,) = _query(factory, _REPEATS_PUBLISHED_SQL, (str(release_id),))[0]
        _execute(
            factory,
            [
                (
                    """
                    UPDATE control.cdc_ilinet_release
                       SET status = %s, updated_at = NOW()
                     WHERE release_id = %s
                    """,
                    ("unchanged" if repeats else "captured", str(release_id)),
                )
            ],
        )
    except BaseException as exc:
        try:
            _execute(
                factory,
                [
                    (
                        """
                        UPDATE control.cdc_ilinet_release
                           SET status = 'failed', updated_at = NOW()
                         WHERE release_id = %s AND status = 'capturing'
                        """,
                        (str(release_id),),
                    )
                ],
            )
        finally:
            capture_control.finish_run(run_id, status="failed", error=exc)
        raise
    capture_control.finish_run(run_id, status="success")
    return None if repeats else release_id
