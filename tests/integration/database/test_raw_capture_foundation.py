"""ARCH-004 immutable capture and control-plane database contracts."""

from __future__ import annotations

import hashlib
import uuid

import psycopg2
import pytest
from psycopg2.extensions import connection

pytestmark = [pytest.mark.integration, pytest.mark.database]


def _insert_request(
    database_connection: connection,
    *,
    source_code: str = "FIXTURE",
) -> tuple[str, str, str]:
    run_id = str(uuid.uuid4())
    request_id = str(uuid.uuid4())
    request_fingerprint = hashlib.sha256(b"fixture-request").hexdigest()
    with database_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.ingestion_run (
                run_id, source_code, status, started_at
            ) VALUES (%s, %s, 'running', NOW())
            """,
            (run_id, source_code),
        )
        cursor.execute(
            """
            INSERT INTO control.ingestion_request (
                request_id, run_id, source_code, endpoint,
                request_parameters, request_fingerprint, status,
                attempt_count, max_attempts, started_at
            ) VALUES (
                %s, %s, %s, 'https://api.example.test/observations',
                '{"series":"TEST"}'::JSONB, %s, 'running', 1, 3, NOW()
            )
            """,
            (request_id, run_id, source_code, request_fingerprint),
        )
    return run_id, request_id, request_fingerprint


def _insert_capture(
    database_connection: connection,
    *,
    run_id: str,
    request_id: str,
    request_fingerprint: str,
    payload: bytes,
    retrieved_at: str,
    source_revision: str,
    source_code: str = "FIXTURE",
) -> tuple[str, str]:
    capture_id = str(uuid.uuid4())
    checksum = hashlib.sha256(payload).hexdigest()
    with database_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO raw_capture.payload_blob (
                payload_checksum, payload, payload_size
            ) VALUES (%s, %s, %s)
            ON CONFLICT (payload_checksum) DO NOTHING
            """,
            (checksum, psycopg2.Binary(payload), len(payload)),
        )
        cursor.execute(
            """
            INSERT INTO raw_capture.response_capture (
                capture_id, request_id, run_id, source_code, endpoint,
                request_parameters, request_fingerprint, retrieved_at,
                http_status, response_headers, media_type,
                payload_schema_version, source_revision, payload_checksum
            ) VALUES (
                %s, %s, %s, %s, 'https://api.example.test/observations',
                '{"series":"TEST"}'::JSONB, %s, %s, 200,
                '{"content-type":"application/json"}'::JSONB,
                'application/json', 'fixture-v1', %s, %s
            )
            """,
            (
                capture_id,
                request_id,
                run_id,
                source_code,
                request_fingerprint,
                retrieved_at,
                source_revision,
                checksum,
            ),
        )
    return capture_id, checksum


def test_capture_and_control_foundation_bootstraps(
    postgres_connection: connection,
) -> None:
    """Covers: DB-019 — foundation creates separated capture/control contracts."""
    expected_tables = {
        ("raw_capture", "payload_blob"),
        ("raw_capture", "response_capture"),
        ("control", "ingestion_run"),
        ("control", "ingestion_request"),
        ("control", "capture_quarantine"),
        ("control", "publisher_ready_event"),
        ("control", "schema_migration_state"),
        ("control", "serving_refresh_state"),
        ("control", "serving_refresh_chunk_state"),
        ("control", "acs_ingestion_slices"),
        ("control", "bls_ingestion_slices"),
        ("control", "fred_ingestion_slices"),
        ("control", "cdc_dataset_release"),
        ("control", "fbi_ucr_release"),
        ("control", "usda_nass_release"),
        ("control", "usda_nass_slice"),
        ("control", "data_quality_run"),
        ("control", "data_quality_result"),
    }
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('raw_capture', 'control')
              AND table_type = 'BASE TABLE'
            """
        )
        assert set(cursor.fetchall()) == expected_tables


def test_capture_round_trips_exact_response_bytes(
    postgres_connection: connection,
) -> None:
    """Covers: DB-020 — persisted payload and envelope replay losslessly."""
    payload = b'{"observations":[{"date":"2024-01-01","value":"."}]}'
    run_id, request_id, fingerprint = _insert_request(postgres_connection)
    capture_id, checksum = _insert_capture(
        postgres_connection,
        run_id=run_id,
        request_id=request_id,
        request_fingerprint=fingerprint,
        payload=payload,
        retrieved_at="2026-08-17T12:00:00Z",
        source_revision="revision-a",
    )

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT blob.payload, blob.payload_size, capture.request_fingerprint,
                   capture.source_revision, capture.media_type
            FROM raw_capture.response_capture AS capture
            JOIN raw_capture.payload_blob AS blob USING (payload_checksum)
            WHERE capture.capture_id = %s
            """,
            (capture_id,),
        )
        stored_payload, size, stored_fingerprint, revision, media_type = (
            cursor.fetchone()
        )

    assert bytes(stored_payload) == payload
    assert size == len(payload)
    assert stored_fingerprint == fingerprint
    assert revision == "revision-a"
    assert media_type == "application/json"
    assert checksum == hashlib.sha256(payload).hexdigest()


def test_capture_relations_reject_update_and_delete(
    postgres_connection: connection,
) -> None:
    """Covers: DB-021 — normal DML cannot mutate captured evidence."""
    payload = b'{"value":"3.1"}'
    run_id, request_id, fingerprint = _insert_request(postgres_connection)
    capture_id, checksum = _insert_capture(
        postgres_connection,
        run_id=run_id,
        request_id=request_id,
        request_fingerprint=fingerprint,
        payload=payload,
        retrieved_at="2026-08-17T12:00:00Z",
        source_revision="revision-a",
    )

    statements = (
        (
            "UPDATE raw_capture.response_capture SET http_status = 201 WHERE capture_id = %s",
            (capture_id,),
            {"55000"},
        ),
        (
            "DELETE FROM raw_capture.payload_blob WHERE payload_checksum = %s",
            (checksum,),
            {"55000"},
        ),
        (
            "TRUNCATE raw_capture.response_capture",
            None,
            # PostgreSQL may reject the referencing quarantine FK before the
            # append-only trigger runs; either error prevents truncation.
            {"55000", "0A000"},
        ),
    )
    with postgres_connection.cursor() as cursor:
        for statement, parameters, expected_codes in statements:
            cursor.execute("SAVEPOINT before_forbidden_mutation")
            with pytest.raises(psycopg2.DatabaseError) as error:
                cursor.execute(statement, parameters)
            assert error.value.pgcode in expected_codes
            cursor.execute("ROLLBACK TO SAVEPOINT before_forbidden_mutation")


def test_changed_response_retains_both_retrieval_events(
    postgres_connection: connection,
) -> None:
    """Covers: DB-022 — changed responses retain both versions and retrievals."""
    run_id, request_id, fingerprint = _insert_request(postgres_connection)
    first_capture, first_checksum = _insert_capture(
        postgres_connection,
        run_id=run_id,
        request_id=request_id,
        request_fingerprint=fingerprint,
        payload=b'{"value":"3.1"}',
        retrieved_at="2026-08-17T12:00:00Z",
        source_revision="revision-a",
    )
    second_capture, second_checksum = _insert_capture(
        postgres_connection,
        run_id=run_id,
        request_id=request_id,
        request_fingerprint=fingerprint,
        payload=b'{"value":"3.2"}',
        retrieved_at="2026-08-18T12:00:00Z",
        source_revision="revision-b",
    )

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT capture_id::TEXT, payload_checksum, source_revision
            FROM raw_capture.response_capture
            WHERE request_id = %s
            ORDER BY retrieved_at
            """,
            (request_id,),
        )
        versions = cursor.fetchall()

    assert versions == [
        (first_capture, first_checksum, "revision-a"),
        (second_capture, second_checksum, "revision-b"),
    ]
    assert first_checksum != second_checksum


def test_parser_failure_is_quarantined_without_losing_capture(
    postgres_connection: connection,
) -> None:
    """Covers: DB-023 — sanitized quarantine state retains capture lineage."""
    run_id, request_id, fingerprint = _insert_request(postgres_connection)
    capture_id, _ = _insert_capture(
        postgres_connection,
        run_id=run_id,
        request_id=request_id,
        request_fingerprint=fingerprint,
        payload=b'{"observations":"unexpected-shape"}',
        retrieved_at="2026-08-17T12:00:00Z",
        source_revision="revision-bad",
    )
    quarantine_id = str(uuid.uuid4())

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.capture_quarantine (
                quarantine_id, capture_id, run_id, source_code,
                parser_version, error_code, error_summary
            ) VALUES (%s, %s, %s, 'FIXTURE', 'parser-v1', 'INVALID_SHAPE',
                      'observations field has an unsupported type')
            """,
            (quarantine_id, capture_id, run_id),
        )
        cursor.execute(
            """
            SELECT quarantine.status, quarantine.error_code,
                   capture.payload_checksum
            FROM control.capture_quarantine AS quarantine
            JOIN raw_capture.response_capture AS capture USING (capture_id)
            WHERE quarantine.quarantine_id = %s
            """,
            (quarantine_id,),
        )
        result = cursor.fetchone()

    assert result[0:2] == ("pending", "INVALID_SHAPE")
    assert len(result[2]) == 64
