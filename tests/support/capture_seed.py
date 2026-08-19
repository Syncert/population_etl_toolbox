"""Small database helpers for capture-first integration fixtures."""

from __future__ import annotations

import hashlib
from uuid import UUID, uuid4

from psycopg2.extensions import cursor


def seed_capture(db_cursor: cursor, source_code: str, payload: bytes = b"{}") -> UUID:
    """Insert the minimum valid run/request/capture graph and return capture_id."""
    run_id, request_id, capture_id = uuid4(), uuid4(), uuid4()
    fingerprint = uuid4().hex * 2
    checksum = hashlib.sha256(payload).hexdigest()
    db_cursor.execute(
        "INSERT INTO control.ingestion_run (run_id, source_code, status) VALUES (%s, %s, 'success')",
        (run_id, source_code),
    )
    db_cursor.execute(
        """INSERT INTO control.ingestion_request
           (request_id, run_id, source_code, endpoint, request_parameters,
            request_fingerprint, status) VALUES
           (%s, %s, %s, 'test://capture', '{}'::jsonb, %s, 'captured')""",
        (request_id, run_id, source_code, fingerprint),
    )
    db_cursor.execute(
        """INSERT INTO raw_capture.payload_blob
           (payload_checksum, payload, payload_size) VALUES (%s, %s, %s)
           ON CONFLICT (payload_checksum) DO NOTHING""",
        (checksum, payload, len(payload)),
    )
    db_cursor.execute(
        """INSERT INTO raw_capture.response_capture
           (capture_id, request_id, run_id, source_code, endpoint,
            request_parameters, request_fingerprint, retrieved_at, http_status,
            response_headers, media_type, payload_checksum) VALUES
           (%s, %s, %s, %s, 'test://capture', '{}'::jsonb, %s, NOW(), 200,
            '{}'::jsonb, 'application/json', %s)""",
        (capture_id, request_id, run_id, source_code, fingerprint, checksum),
    )
    return capture_id
