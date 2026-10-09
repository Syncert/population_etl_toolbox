"""Capture-first orchestration for one LODES state-year.

One ingestion run per (state, year): the state's ``version.txt`` and
checksum list are captured first; when the data vintage is the one already
published for that state-year, the run records ``unchanged`` and fetches
nothing else. Otherwise each registered data file is fetched, checked
against the published checksum and committed before anything parses it. A
file the checksum list does not name is recorded as not published (Alaska
publishes no workplace files for recent years), never fetched blind.
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

from .client import LodesIntegrityError, fetch, parse_checksums, parse_version, verify
from .config import SOURCE_CODE, LodesConfig
from .registry import (
    FORMAT_VERSION,
    LODES_BASE_URL,
    checksum_path,
    files_for,
    version_path,
)

PARSER_CONTRACT_VERSION = "census_lodes8_csv:v1"


def _latest_published_vintage(
    connection_factory: Callable[[], Any], state: str, year: int
) -> str | None:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT data_vintage FROM control.census_lodes_slice
                WHERE state = %s AND year = %s AND status = 'published'
                ORDER BY published_at DESC LIMIT 1
                """,
                (state, year),
            )
            row = cursor.fetchone()
            return row[0] if row else None
    finally:
        connection.close()


def _execute(
    connection_factory: Callable[[], Any], sql: str, parameters: tuple
) -> None:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, parameters)
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def capture_state_year(
    connection_factory: Callable[[], Any],
    state: str,
    year: int,
    *,
    config: LodesConfig | None = None,
    client: Any | None = None,
    control: CaptureControl | None = None,
    persist_capture: Callable[
        [Callable[[], Any], ResponseCapture], CaptureReceipt
    ] = persist_response_capture,
) -> tuple[UUID, str]:
    """Capture one state-year; return (run_id, slice status)."""
    runtime = config or LodesConfig()
    files = files_for(state, year)
    capture_control = control or CaptureControl(
        connection_factory, source_code=SOURCE_CODE
    )
    run_id = capture_control.start_run(watermark={"state": state, "year": year})

    def capture(
        path: str,
        media_type: str,
        revision: str,
        check: Callable[[bytes], None] | None = None,
    ) -> UUID:
        endpoint = f"{LODES_BASE_URL}{path}"
        request = capture_control.start_request(
            run_id=run_id,
            endpoint=endpoint,
            parameters={"state": state, "year": str(year), "path": path},
            max_attempts=runtime.max_attempts,
        )
        try:
            response = fetch(
                path,
                config=runtime,
                client=client,
                on_retry=lambda error: capture_control.record_request_retry(
                    request.request_id, error=error
                ),
            )
            if check is not None:
                check(response.raw_bytes)
            receipt = persist_capture(
                connection_factory,
                ResponseCapture(
                    capture_id=uuid4(),
                    request_id=request.request_id,
                    run_id=run_id,
                    source_code=SOURCE_CODE,
                    endpoint=endpoint,
                    request_parameters={
                        "state": state,
                        "year": str(year),
                        "path": path,
                    },
                    retrieved_at=datetime.now(UTC),
                    http_status=response.http_status,
                    response_headers=dict(response.response_headers),
                    media_type=media_type,
                    payload=response.raw_bytes,
                    payload_schema_version=PARSER_CONTRACT_VERSION,
                    source_revision=revision,
                ),
            )
        except BaseException as exc:
            capture_control.finish_request(
                request.request_id, status="failed", error=exc
            )
            raise
        capture_control.finish_request(request.request_id, status="captured")
        return receipt.capture_id

    try:
        texts: dict[str, str] = {}

        def keep_text(name: str) -> Callable[[bytes], None]:
            def check(raw: bytes) -> None:
                texts[name] = raw.decode("latin-1")

            return check

        version_capture = capture(
            version_path(state), "text/plain", f"{state}:version", keep_text("version")
        )
        vintage, format_version = parse_version(texts["version"])
        if format_version != FORMAT_VERSION:
            raise LodesIntegrityError(
                version_path(state), code="unregistered_format_version"
            )
        checksum_capture = capture(
            checksum_path(state),
            "text/plain",
            f"{state}:{vintage}",
            keep_text("checksums"),
        )
        listed = parse_checksums(texts["checksums"])
        unchanged = (
            _latest_published_vintage(connection_factory, state, year) == vintage
        )
        _execute(
            connection_factory,
            """
            INSERT INTO control.census_lodes_slice (
                run_id, state, year, data_vintage, format_version,
                version_capture_id, checksum_capture_id, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(run_id),
                state,
                year,
                vintage,
                format_version,
                str(version_capture),
                str(checksum_capture),
                "unchanged" if unchanged else "captured",
            ),
        )
        if not unchanged:
            for item in files:
                expected = listed.get(item.name)
                if expected is None:
                    _execute(
                        connection_factory,
                        """
                        INSERT INTO control.census_lodes_file (run_id, family, file_name, status)
                        VALUES (%s, %s, %s, 'not_published')
                        """,
                        (str(run_id), item.family, item.name),
                    )
                    continue
                capture_id = capture(
                    item.path,
                    "application/gzip",
                    f"{state}:{vintage}:{item.name}",
                    lambda raw, item=item, expected=expected: (
                        verify(raw, item.path, expected=expected) and None
                    ),
                )
                _execute(
                    connection_factory,
                    """
                    INSERT INTO control.census_lodes_file (
                        run_id, family, file_name, capture_id, listed_sha256, status
                    ) VALUES (%s, %s, %s, %s, %s, 'captured')
                    """,
                    (str(run_id), item.family, item.name, str(capture_id), expected),
                )
        capture_control.finish_run(run_id, status="success")
        return run_id, "unchanged" if unchanged else "captured"
    except BaseException as exc:
        # The failed run stays in the ingestion ledger with its error; its
        # half-registered slice is withdrawn so no rule mistakes it for a
        # state-year waiting to be replayed.
        _execute(
            connection_factory,
            "DELETE FROM control.census_lodes_file WHERE run_id = %s",
            (str(run_id),),
        )
        _execute(
            connection_factory,
            "DELETE FROM control.census_lodes_slice WHERE run_id = %s",
            (str(run_id),),
        )
        capture_control.finish_run(run_id, status="failed", error=exc)
        raise
