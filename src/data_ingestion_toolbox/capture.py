"""Shared immutable-response capture and offline replay helpers."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

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
                    capture.capture_id,
                    capture.request_id,
                    capture.run_id,
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
                (capture_id,),
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
