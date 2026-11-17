"""Capture-first orchestration for vehicle registrations (vehicle-fleet-tables).

A release is one run. It reads WisDOT's two vehicle-information pages, then
every Report 28 calendar year they link, then FHWA's MV-1 workbook for each
year from the configured first year to the current one, each request a pause
apart and each body committed before anything parses it. An MV-1 year FHWA
has not posted answers 404 and is recorded as not published. A document whose
bytes repeat its newest published capture is recorded ``unchanged`` and is
not parsed again; a release whose every document is unchanged is itself
``unchanged`` and writes nothing (CHURN RULE).
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

from .client import NotPublished, fetch_document, report28_links
from .config import (
    FHWA_MV1_PATH,
    SOURCE_CODE,
    WISDOT_INDEX_PATHS,
    VehicleRegistrationsConfig,
)
from .registry import (
    INDEX_CONTRACT_VERSION,
    MV1_CONTRACT_VERSION,
    RPT28_CONTRACT_VERSION,
)

PersistCapture = Callable[[Callable[[], Any], ResponseCapture], CaptureReceipt]

CONTRACTS = {
    "index": INDEX_CONTRACT_VERSION,
    "rpt28": RPT28_CONTRACT_VERSION,
    "mv1": MV1_CONTRACT_VERSION,
}


class VehicleRegistrationsCaptureError(RuntimeError):
    """A read that cannot find what the publishers registered."""


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


def pending_release(factory: Callable[[], Any]) -> UUID | None:
    """A captured release not yet published, which a run replays before reading."""
    rows = _query(
        factory,
        """
        SELECT release_id::TEXT FROM control.vehicle_registrations_release
        WHERE status IN ('captured', 'silver_ready')
        ORDER BY created_at DESC LIMIT 1
        """,
    )
    return UUID(rows[0][0]) if rows else None


#: The checksum and contract of a document's newest published capture.
_PUBLISHED_SQL = """
    SELECT document.payload_checksum, capture.payload_schema_version
    FROM control.vehicle_registrations_document AS document
    JOIN control.vehicle_registrations_release AS release
      ON release.release_id = document.release_id
    JOIN raw_capture.response_capture AS capture
      ON capture.capture_id = document.capture_id
    WHERE release.status = 'published'
      AND document.document_key = %s
      AND document.status IN ('parsed', 'unchanged')
    ORDER BY release.published_at DESC
    LIMIT 1
"""


def capture_release(
    factory: Callable[[], Any],
    *,
    config: VehicleRegistrationsConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: PersistCapture = persist_response_capture,
    sleep: Callable[[float], None] = time.sleep,
    today: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> UUID | None:
    """Capture one release; return it when it holds anything to replay."""
    runtime = config or VehicleRegistrationsConfig()
    pending = pending_release(factory)
    if pending is not None:
        return pending
    capture_control = control or CaptureControl(factory, source_code=SOURCE_CODE)
    release_id = uuid4()
    run_id = capture_control.start_run(watermark={"release_id": str(release_id)})
    changed = 0
    documents = 0
    first = True

    def read(
        base_url: str,
        path: str,
        kind: str,
        key: str,
        year: int | None,
        *,
        optional: bool,
    ) -> bytes | None:
        nonlocal changed, documents, first
        if not first:
            sleep(runtime.min_spacing_seconds)
        first = False
        endpoint = f"{base_url}{path}"
        parameters = {"path": path, "document_key": key}
        request = capture_control.start_request(
            run_id=run_id,
            endpoint=endpoint,
            parameters=parameters,
            max_attempts=runtime.max_attempts,
        )
        try:
            fetched = fetch_document(
                base_url,
                path,
                kind,
                config=runtime,
                client=client,
                not_found_is_unpublished=optional,
                on_retry=lambda error: capture_control.record_request_retry(
                    request.request_id, error=error
                ),
                sleep=sleep,
            )
        except NotPublished:
            capture_control.finish_request(request.request_id, status="empty")
            return None
        except BaseException as exc:
            capture_control.finish_request(
                request.request_id, status="failed", error=exc
            )
            raise
        try:
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
                    http_status=fetched.http_status,
                    response_headers=dict(fetched.response_headers),
                    media_type=fetched.media_type,
                    payload=fetched.raw_bytes,
                    payload_schema_version=CONTRACTS[kind],
                    source_revision=fetched.response_headers.get("last-modified"),
                ),
            )
        except BaseException as exc:
            capture_control.finish_request(
                request.request_id, status="failed", error=exc
            )
            raise
        capture_control.finish_request(request.request_id, status="captured")
        status = "captured"
        if kind != "index":
            published = _query(factory, _PUBLISHED_SQL, (key,))
            if published and published[0] == (
                receipt.payload_checksum,
                CONTRACTS[kind],
            ):
                status = "unchanged"
            else:
                changed += 1
            documents += 1
        _execute(
            factory,
            """
            INSERT INTO control.vehicle_registrations_document (
                release_id, document_key, document_kind, data_year, source_url,
                capture_id, payload_checksum, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(release_id),
                key,
                kind,
                year,
                endpoint,
                str(receipt.capture_id),
                receipt.payload_checksum,
                status,
            ),
        )
        return fetched.raw_bytes

    try:
        _execute(
            factory,
            """
            INSERT INTO control.vehicle_registrations_release (release_id, run_id, status)
            VALUES (%s, %s, 'capturing')
            """,
            (str(release_id), str(run_id)),
        )
        links: dict[int, str] = {}
        for path in WISDOT_INDEX_PATHS:
            page = read(
                runtime.wisdot_base_url,
                path,
                "index",
                f"wisdot_index:{path}",
                None,
                optional=False,
            )
            links.update(report28_links(page or b""))
        if not links:
            raise VehicleRegistrationsCaptureError(
                "WisDOT's vehicle-information pages link no Report 28 calendar year"
            )
        for year, path in sorted(links.items()):
            read(
                runtime.wisdot_base_url,
                path,
                "rpt28",
                f"wisdot_rpt28:cal:{year}",
                year,
                optional=False,
            )
        for year in range(runtime.fhwa_first_year, today().year + 1):
            read(
                runtime.fhwa_base_url,
                FHWA_MV1_PATH.format(year=year),
                "mv1",
                f"fhwa_mv1:{year}",
                year,
                optional=True,
            )
        status = "captured" if changed else "unchanged"
        _execute(
            factory,
            """
            UPDATE control.vehicle_registrations_release
               SET status = %s, document_count = %s, updated_at = NOW()
             WHERE release_id = %s
            """,
            (status, documents, str(release_id)),
        )
    except BaseException as exc:
        try:
            _execute(
                factory,
                """
                UPDATE control.vehicle_registrations_release
                   SET status = 'failed', updated_at = NOW()
                 WHERE release_id = %s AND status = 'capturing'
                """,
                (str(release_id),),
            )
        finally:
            capture_control.finish_run(run_id, status="failed", error=exc)
        raise
    capture_control.finish_run(run_id, status="success")
    return release_id if changed else None
