"""Shared immutable response-capture helper contracts."""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime

import pytest

from data_ingestion_toolbox.capture import (
    ResponseCapture,
    allowlisted_response_headers,
    load_captured_payload,
    persist_response_capture,
    request_fingerprint,
)

pytestmark = pytest.mark.unit


class _Cursor:
    def __init__(self, connection: "_Connection") -> None:
        self.connection = connection

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, parameters: tuple[object, ...]) -> None:
        self.connection.executions.append((sql, parameters))
        if self.connection.fail_on_execution == len(self.connection.executions):
            raise RuntimeError("injected database failure")

    def fetchone(self) -> tuple[object, ...] | None:
        return self.connection.row


class _Connection:
    def __init__(
        self,
        *,
        row: tuple[object, ...] | None = None,
        fail_on_execution: int | None = None,
    ) -> None:
        self.row = row
        self.fail_on_execution = fail_on_execution
        self.executions: list[tuple[str, tuple[object, ...]]] = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def _capture() -> ResponseCapture:
    return ResponseCapture(
        capture_id=uuid.UUID(int=1),
        request_id=uuid.UUID(int=2),
        run_id=uuid.UUID(int=3),
        source_code="fixture",
        endpoint="https://api.example.test/observations",
        request_parameters={"series": ["A", "B"], "start": "2024-01-01"},
        retrieved_at=datetime(2026, 8, 17, 12, tzinfo=UTC),
        http_status=200,
        response_headers={
            "Content-Type": "application/json",
            "ETag": "fixture-etag",
            "Set-Cookie": "must-not-persist",
        },
        media_type="application/json",
        payload=b'{"value":"."}',
        payload_schema_version="fixture-v1",
    )


def test_request_fingerprint_is_stable_and_rejects_secrets() -> None:
    """Covers: ETL-038 — fingerprints are canonical and secret-safe."""
    first = request_fingerprint(
        "fixture",
        "https://api.example.test/data",
        {"series": ["A", "B"], "start": 2024},
    )
    reordered = request_fingerprint(
        "FIXTURE",
        "https://api.example.test/data",
        {"start": 2024, "series": ["A", "B"]},
    )

    assert first == reordered
    with pytest.raises(ValueError, match="sensitive field"):
        request_fingerprint(
            "FIXTURE",
            "https://api.example.test/data",
            {"query": {"api_key": "do-not-store"}},
        )
    assert allowlisted_response_headers(
        {"Content-Type": "application/json", "Authorization": "secret"}
    ) == {"content-type": "application/json"}


def test_capture_writer_commits_dedicated_transaction_and_sanitizes_headers() -> None:
    """Covers: ETL-039 — capture commit completes independently before parsing."""
    database_connection = _Connection()
    capture = _capture()

    receipt = persist_response_capture(lambda: database_connection, capture)

    assert receipt.capture_id == capture.capture_id
    assert receipt.payload_checksum == hashlib.sha256(capture.payload).hexdigest()
    assert database_connection.committed is True
    assert database_connection.rolled_back is False
    assert database_connection.closed is True
    assert len(database_connection.executions) == 2
    response_parameters = database_connection.executions[1][1]
    assert "set-cookie" not in str(response_parameters).lower()
    assert "must-not-persist" not in str(response_parameters)


def test_offline_replay_verifies_payload_checksum() -> None:
    """Covers: ETL-040 — offline replay returns only checksum-verified bytes."""
    payload = b'{"value":"3.2"}'
    checksum = hashlib.sha256(payload).hexdigest()
    valid_connection = _Connection(row=(memoryview(payload), checksum))

    assert load_captured_payload(lambda: valid_connection, uuid.UUID(int=1)) == payload
    assert valid_connection.closed is True

    corrupt_connection = _Connection(row=(memoryview(payload), "0" * 64))
    with pytest.raises(ValueError, match="checksum mismatch"):
        load_captured_payload(lambda: corrupt_connection, uuid.UUID(int=1))
    assert corrupt_connection.closed is True


def test_capture_writer_rolls_back_failed_insert() -> None:
    """Covers: ETL-039 — a failed envelope insert rolls back its payload insert."""
    database_connection = _Connection(fail_on_execution=2)

    with pytest.raises(RuntimeError, match="injected database failure"):
        persist_response_capture(lambda: database_connection, _capture())

    assert database_connection.committed is False
    assert database_connection.rolled_back is True
    assert database_connection.closed is True
