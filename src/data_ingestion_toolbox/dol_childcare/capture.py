"""Capture-first orchestration for the NDCP release.

One ingestion run per read of a registered release: one request and one
committed capture, before anything parses it. A release read from DOL's API
is captured as one JSON document holding every page as the API sent it; a
workbook as its bytes. The Women's Bureau replaces the database in place,
so every read fetches it; a read whose bytes repeat
the release's last published capture is recorded ``unchanged`` and replays
nothing (``utility.unchanged``). The payload store keeps one copy of the
bytes either way.
"""

from __future__ import annotations

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
from data_ingestion_toolbox.utility.unchanged import repeats_last_publication

from .client import fetch_release
from .config import DOL_API_URL, DOL_BASE_URL, SOURCE_CODE, ChildcareConfig
from .registry import API, NdcpRelease

PARSER_CONTRACT_VERSION = "dol_ndcp_xlsx:v1"
API_PARSER_CONTRACT_VERSION = "dol_ndcp_api:v1"

XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
JSON_MEDIA_TYPE = "application/json"


def parser_contract(release: NdcpRelease) -> str:
    return (
        API_PARSER_CONTRACT_VERSION if release.kind == API else PARSER_CONTRACT_VERSION
    )


def release_endpoint(release: NdcpRelease) -> str:
    """The release's address, never with the key."""
    base = DOL_API_URL if release.kind == API else DOL_BASE_URL
    return f"{base}{release.path}"


#: The release's newest published capture. The revision table stores only
#: prices that changed (CHURN RULE), so it cannot list every geography a
#: capture names: any geography added since forces a replay, which then
#: stores nothing that repeats.
#: A kept page older than this is asked for afresh.
PAGE_KEEP_DAYS = 7


class StoredPages:
    """The release's answered API pages, kept in ``control.dol_childcare_page``.

    Each page is committed as soon as it is answered, so a read stopped by a
    refusal resumes where it stopped on the next attempt.
    """

    def __init__(self, connection_factory: Callable[[], Any], release: str) -> None:
        self._factory = connection_factory
        self._release = release

    def _run(self, sql: str, parameters: tuple) -> list[tuple]:
        connection = self._factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, parameters)
                rows = list(cursor.fetchall()) if cursor.description else []
            connection.commit()
            return rows
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def forget_stale(self) -> None:
        self._run(
            """
            DELETE FROM control.dol_childcare_page
            WHERE release = %s AND retrieved_at < NOW() - make_interval(days => %s)
            """,
            (self._release, PAGE_KEEP_DAYS),
        )

    def get(self, request_key: str) -> bytes | None:
        rows = self._run(
            """
            SELECT body FROM control.dol_childcare_page
            WHERE release = %s AND request_key = %s
              AND retrieved_at >= NOW() - make_interval(days => %s)
            """,
            (self._release, request_key, PAGE_KEEP_DAYS),
        )
        return bytes(rows[0][0]) if rows else None

    def put(self, request_key: str, *, year: int, offset: int, body: bytes) -> None:
        self._run(
            """
            INSERT INTO control.dol_childcare_page (
                release, request_key, study_year, page_offset, body
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (release, request_key) DO UPDATE
               SET body = EXCLUDED.body, retrieved_at = NOW()
            """,
            (self._release, request_key, year, offset, body),
        )


_FORGET_PAGES_SQL = "DELETE FROM control.dol_childcare_page WHERE release = %s"


LATEST_PUBLISHED_SQL = """
    SELECT response.payload_checksum, response.payload_schema_version, file.published_at
    FROM control.dol_childcare_file AS file
    JOIN raw_capture.response_capture AS response ON response.capture_id = file.capture_id
    WHERE file.release = %s AND file.status = 'published'
    ORDER BY file.published_at DESC
    LIMIT 1
"""


def capture_release(
    connection_factory: Callable[[], Any],
    release: NdcpRelease,
    *,
    config: ChildcareConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, str]:
    """Capture one registered workbook; return (run_id, file status)."""
    runtime = config or ChildcareConfig()
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    parameters = {"release": release.key}
    run_id = capture_control.start_run(watermark=dict(parameters))
    endpoint = release_endpoint(release)
    contract = parser_contract(release)
    request = capture_control.start_request(
        run_id=run_id,
        endpoint=endpoint,
        parameters=dict(parameters),
        max_attempts=runtime.max_attempts,
    )
    pages = (
        StoredPages(connection_factory, release.key) if release.kind == API else None
    )
    try:
        if pages is not None:
            pages.forget_stale()
        response = fetch_release(
            release,
            config=runtime,
            client=client,
            on_retry=lambda error: capture_control.record_request_retry(
                request.request_id, error=error
            ),
            pages=pages,
        )
        receipt = persist_capture(
            connection_factory,
            ResponseCapture(
                capture_id=uuid4(),
                request_id=request.request_id,
                run_id=run_id,
                source_code=SOURCE_CODE,
                endpoint=endpoint,
                request_parameters=dict(parameters),
                retrieved_at=datetime.now(UTC),
                http_status=response.http_status,
                response_headers=dict(response.response_headers),
                media_type=JSON_MEDIA_TYPE if release.kind == API else XLSX_MEDIA_TYPE,
                payload=response.raw_bytes,
                payload_schema_version=contract,
                source_revision=release.key,
            ),
        )
        capture_control.finish_request(request.request_id, status="captured")
        status = (
            "unchanged"
            if repeats_last_publication(
                connection_factory,
                LATEST_PUBLISHED_SQL,
                (release.key,),
                payload_checksum=receipt.payload_checksum,
                parser_contract=contract,
            )
            else "captured"
        )
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.dol_childcare_file (
                        run_id, release, capture_id, status
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (str(run_id), release.key, str(receipt.capture_id), status),
                )
                # The capture holds every page now; the kept ones are waste.
                cursor.execute(_FORGET_PAGES_SQL, (release.key,))
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        capture_control.finish_run(run_id, status="success")
        return run_id, status
    except BaseException as exc:
        capture_control.finish_request(request.request_id, status="failed", error=exc)
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
