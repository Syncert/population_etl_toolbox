"""Shared immutable-response capture and offline replay helpers."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID
from uuid import uuid4

from data_ingestion_toolbox.normalization import sanitize_error_message

logger = logging.getLogger(__name__)

SENSITIVE_NAMES = {
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "password",
    "proxy_authorization",
    "secret",
    "set_cookie",
    "token",
}
ALLOWED_RESPONSE_HEADERS = {
    "content-type",
    "etag",
    "last-modified",
    "request-id",
    "retry-after",
    "x-request-id",
}

#: Run outcomes that mean the run stopped before finishing its registered work.
#: A run that ends this way owns the state it started: its requests must reach a
#: terminal status, or the slice-ledger and lineage rules count an abandoned
#: attempt as missing work forever, and a later successful run cannot clear it.
#: ``success`` is deliberately absent -- a successful run that still holds
#: unfinished requests is a defect the assessment must keep reporting.
ABORTED_RUN_STATUSES: frozenset[str] = frozenset({"failed", "cancelled", "partial"})

#: Request statuses that have not reached an outcome.
UNFINISHED_REQUEST_STATUSES: tuple[str, ...] = ("planned", "running")

#: Recorded against a request the aborting run never resolved.
ABORTED_REQUEST_ERROR = "run aborted before this request reached an outcome"


def _normalized_name(name: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")


def _reject_sensitive_keys(value: object, *, path: str = "parameters") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = _normalized_name(key)
            if normalized in SENSITIVE_NAMES or any(
                normalized.endswith(f"_{suffix}")
                for suffix in ("api_key", "password", "secret", "token")
            ):
                raise ValueError(f"sensitive field is forbidden in {path}: {key}")
            _reject_sensitive_keys(child, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _reject_sensitive_keys(child, path=f"{path}[{index}]")


def request_fingerprint(
    source_code: str,
    endpoint: str,
    parameters: Mapping[str, object],
) -> str:
    """Return a stable SHA-256 identity for a sanitized source request."""
    _reject_sensitive_keys(parameters)
    canonical = json.dumps(
        {
            "source_code": source_code.strip().upper(),
            "endpoint": endpoint.strip(),
            "parameters": parameters,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def allowlisted_response_headers(headers: Mapping[str, object]) -> dict[str, str]:
    """Return provenance headers while excluding authorization/session material."""
    return {
        str(name).strip().lower(): str(value)
        for name, value in headers.items()
        if str(name).strip().lower() in ALLOWED_RESPONSE_HEADERS
    }


@dataclass(frozen=True)
class ResponseCapture:
    """Lossless payload and immutable retrieval-envelope values."""

    capture_id: UUID
    request_id: UUID
    run_id: UUID
    source_code: str
    endpoint: str
    request_parameters: Mapping[str, object]
    retrieved_at: datetime
    http_status: int
    response_headers: Mapping[str, object]
    media_type: str
    payload: bytes
    payload_schema_version: str | None = None
    source_revision: str | None = None

    @property
    def request_fingerprint(self) -> str:
        return request_fingerprint(
            self.source_code,
            self.endpoint,
            self.request_parameters,
        )

    @property
    def payload_checksum(self) -> str:
        return hashlib.sha256(self.payload).hexdigest()


@dataclass(frozen=True)
class CaptureReceipt:
    capture_id: UUID
    payload_checksum: str


@dataclass(frozen=True)
class ControlRequest:
    request_id: UUID
    request_fingerprint: str


@dataclass(frozen=True, slots=True)
class RunFinalization:
    """What finalizing one aborted run's control rows changed."""

    run_id: UUID
    source_code: str
    #: Requests whose bytes were already durable when the run stopped.
    captured: int = 0
    #: Requests that stopped with nothing captured.
    failed: int = 0

    @property
    def changed(self) -> int:
        return self.captured + self.failed


#: Finalize a stopped run's unfinished requests in one statement per outcome.
#:
#: A request that already holds durable bytes is finished as ``captured``: the
#: payload is committed, checksummed, and replayable, so the honest terminal
#: state is the one the successful path would have written. Everything else is
#: ``failed`` -- it produced nothing and never will.
#:
#: ``started_at`` is coalesced because a request abandoned while ``planned``
#: may never have started, and the table requires a start before a finish.
_FINALIZE_CAPTURED_REQUESTS = """
    UPDATE control.ingestion_request AS request
       SET status = 'captured',
           started_at = COALESCE(request.started_at, NOW()),
           finished_at = NOW(),
           updated_at = NOW()
     WHERE request.run_id = %s
       AND request.source_code = %s
       AND request.status = ANY(%s)
       AND EXISTS (
           SELECT 1 FROM raw_capture.response_capture AS capture
            WHERE capture.request_id = request.request_id
       )
"""

_FINALIZE_FAILED_REQUESTS = """
    UPDATE control.ingestion_request AS request
       SET status = 'failed',
           started_at = COALESCE(request.started_at, NOW()),
           finished_at = NOW(),
           last_error = COALESCE(request.last_error, %s),
           updated_at = NOW()
     WHERE request.run_id = %s
       AND request.source_code = %s
       AND request.status = ANY(%s)
       AND NOT EXISTS (
           SELECT 1 FROM raw_capture.response_capture AS capture
            WHERE capture.request_id = request.request_id
       )
"""


def finalize_run_requests(
    cursor: Any, run_id: UUID, source_code: str
) -> RunFinalization:
    """Bring one stopped run's unfinished requests to a terminal status.

    The caller owns the transaction, so this composes with the statement that
    stops the run: a run and the requests it abandons reach their terminal
    states together, or neither does.
    """
    unfinished = list(UNFINISHED_REQUEST_STATUSES)
    cursor.execute(_FINALIZE_CAPTURED_REQUESTS, (str(run_id), source_code, unfinished))
    captured = max(cursor.rowcount, 0)
    cursor.execute(
        _FINALIZE_FAILED_REQUESTS,
        (ABORTED_REQUEST_ERROR, str(run_id), source_code, unfinished),
    )
    failed = max(cursor.rowcount, 0)
    return RunFinalization(run_id, source_code, captured=captured, failed=failed)


class CaptureControl:
    """Provider-neutral committed run/request/quarantine transitions."""

    def __init__(
        self,
        connection_factory: Callable[[], Any],
        *,
        source_code: str,
    ) -> None:
        self.connection_factory = connection_factory
        self.source_code = source_code.strip().upper()
        if not re.fullmatch(r"[A-Z0-9][A-Z0-9_-]*", self.source_code):
            raise ValueError("invalid capture source code")

    def _execute(self, statement: str, parameters: tuple[object, ...]) -> None:
        database_connection = self.connection_factory()
        try:
            with database_connection.cursor() as cursor:
                cursor.execute(statement, parameters)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    def start_run(self, *, watermark: Mapping[str, object] | None = None) -> UUID:
        run_id = uuid4()
        self._execute(
            """
            INSERT INTO control.ingestion_run (
                run_id, source_code, status, started_at, source_watermark
            ) VALUES (%s, %s, 'running', NOW(), %s::JSONB)
            """,
            (
                str(run_id),
                self.source_code,
                json.dumps(watermark) if watermark is not None else None,
            ),
        )
        return run_id

    def finish_run(
        self,
        run_id: UUID,
        *,
        status: str,
        error: BaseException | str | None = None,
    ) -> RunFinalization:
        """Stop a run, finalizing the requests an aborted run abandons.

        A run that stops without finishing its work owns the control rows it
        started. Left unfinished, they are indistinguishable from work the
        warehouse still owes: the slice-ledger and lineage rules count them as
        missing forever, so a later successful run over the same partition can
        never clear the assessment. Finalizing them here, in the transaction
        that stops the run, keeps the control plane honest without hiding
        anything -- the run keeps its ``failed`` status and error summary, and
        the requests keep theirs.
        """
        summary = sanitize_error_message(error) if error is not None else None
        finalization = RunFinalization(run_id, self.source_code)
        database_connection = self.connection_factory()
        try:
            with database_connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE control.ingestion_run
                       SET status = %s, finished_at = NOW(), error_summary = %s,
                           updated_at = NOW()
                     WHERE run_id = %s AND source_code = %s
                    """,
                    (status, summary, str(run_id), self.source_code),
                )
                if status in ABORTED_RUN_STATUSES:
                    finalization = finalize_run_requests(
                        cursor, run_id, self.source_code
                    )
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()
        if finalization.changed:
            logger.warning(
                "Run %s stopped as '%s'; finalized %d abandoned request(s) "
                "(%d already captured, %d failed)",
                run_id,
                status,
                finalization.changed,
                finalization.captured,
                finalization.failed,
            )
        return finalization

    def set_run_watermark(
        self,
        run_id: UUID,
        *,
        watermark: Mapping[str, object],
    ) -> None:
        """Persist a provider watermark discovered after raw metadata capture."""
        _reject_sensitive_keys(watermark, path="watermark")
        self._execute(
            """
            UPDATE control.ingestion_run
               SET source_watermark = %s::JSONB, updated_at = NOW()
             WHERE run_id = %s AND source_code = %s
            """,
            (json.dumps(watermark, sort_keys=True), str(run_id), self.source_code),
        )

    def start_request(
        self,
        *,
        run_id: UUID,
        endpoint: str,
        parameters: Mapping[str, object],
        max_attempts: int = 1,
    ) -> ControlRequest:
        fingerprint = request_fingerprint(self.source_code, endpoint, parameters)
        request_id = uuid4()
        self._execute(
            """
            INSERT INTO control.ingestion_request (
                request_id, run_id, source_code, endpoint,
                request_parameters, request_fingerprint, status,
                attempt_count, max_attempts, started_at
            ) VALUES (%s, %s, %s, %s, %s::JSONB, %s,
                      'running', 1, %s, NOW())
            """,
            (
                str(request_id),
                str(run_id),
                self.source_code,
                endpoint.strip(),
                json.dumps(parameters, sort_keys=True),
                fingerprint,
                max_attempts,
            ),
        )
        return ControlRequest(request_id, fingerprint)

    def finish_request(
        self,
        request_id: UUID,
        *,
        status: str,
        error: BaseException | str | None = None,
    ) -> None:
        summary = sanitize_error_message(error) if error is not None else None
        self._execute(
            """
            UPDATE control.ingestion_request
               SET status = %s, finished_at = NOW(), last_error = %s,
                   updated_at = NOW()
             WHERE request_id = %s AND source_code = %s
            """,
            (status, summary, str(request_id), self.source_code),
        )

    def record_request_retry(
        self, request_id: UUID, *, error: BaseException | str
    ) -> None:
        """Record a bounded transport retry without completing the request."""
        self._execute(
            """
            UPDATE control.ingestion_request
               SET attempt_count = attempt_count + 1,
                   last_error = %s,
                   updated_at = NOW()
             WHERE request_id = %s AND source_code = %s
               AND attempt_count < max_attempts
            """,
            (sanitize_error_message(error), str(request_id), self.source_code),
        )

    def quarantine(
        self,
        *,
        capture_id: UUID,
        run_id: UUID,
        parser_version: str,
        error_code: str,
        error: BaseException | str,
    ) -> None:
        self._execute(
            """
            INSERT INTO control.capture_quarantine (
                quarantine_id, capture_id, run_id, source_code,
                parser_version, error_code, error_summary
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (capture_id, parser_version, error_code) DO NOTHING
            """,
            (
                str(uuid4()),
                str(capture_id),
                str(run_id),
                self.source_code,
                parser_version,
                error_code,
                sanitize_error_message(error),
            ),
        )


def persist_response_capture(
    connection_factory: Callable[[], Any],
    capture: ResponseCapture,
) -> CaptureReceipt:
    """Commit one capture using a dedicated connection before parsing begins.

    The referenced control run/request must already be committed. The function owns
    and closes the returned connection so downstream parser rollback cannot affect it.
    """
    safe_headers = allowlisted_response_headers(capture.response_headers)
    fingerprint = capture.request_fingerprint
    checksum = capture.payload_checksum
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_capture.payload_blob (
                    payload_checksum, payload, payload_size
                ) VALUES (%s, %s, %s)
                ON CONFLICT (payload_checksum) DO NOTHING
                """,
                (checksum, capture.payload, len(capture.payload)),
            )
            cursor.execute(
                """
                INSERT INTO raw_capture.response_capture (
                    capture_id, request_id, run_id, source_code, endpoint,
                    request_parameters, request_fingerprint, retrieved_at,
                    http_status, response_headers, media_type,
                    payload_schema_version, source_revision, payload_checksum
                ) VALUES (
                    %s, %s, %s, %s, %s, %s::JSONB, %s, %s,
                    %s, %s::JSONB, %s, %s, %s, %s
                )
                """,
                (
                    str(capture.capture_id),
                    str(capture.request_id),
                    str(capture.run_id),
                    capture.source_code.strip().upper(),
                    capture.endpoint.strip(),
                    json.dumps(capture.request_parameters, sort_keys=True),
                    fingerprint,
                    capture.retrieved_at,
                    capture.http_status,
                    json.dumps(safe_headers, sort_keys=True),
                    capture.media_type,
                    capture.payload_schema_version,
                    capture.source_revision,
                    checksum,
                ),
            )
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return CaptureReceipt(capture.capture_id, checksum)


def load_captured_payload(
    connection_factory: Callable[[], Any],
    capture_id: UUID,
) -> bytes:
    """Load and checksum-verify response bytes without network access."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT blob.payload, blob.payload_checksum
                FROM raw_capture.response_capture AS capture
                JOIN raw_capture.payload_blob AS blob USING (payload_checksum)
                WHERE capture.capture_id = %s
                """,
                (str(capture_id),),
            )
            row = cursor.fetchone()
        if row is None:
            raise LookupError(f"capture not found: {capture_id}")
        payload = bytes(row[0])
        actual_checksum = hashlib.sha256(payload).hexdigest()
        if actual_checksum != row[1]:
            raise ValueError(f"capture checksum mismatch: {capture_id}")
        return payload
    finally:
        database_connection.close()
