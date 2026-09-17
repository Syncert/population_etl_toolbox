"""The capture export and restore, against a real warehouse.

Covers: DB-047 -- an export restored into a re-bootstrapped warehouse verifies
        under DQ-SHARED-001, and the append-only triggers are still in place
        afterwards.

The unit tier proves the export's shape, its refusals and its statement order
against a stand-in connection. What only a database can answer is whether the
statements it produces are accepted by the schema they target -- the foreign
keys between a capture, its request and its run, the `payload_size` CHECK that
compares a column to `OCTET_LENGTH(payload)`, and above all whether the
append-only triggers let a restore through without being disabled. That last
one is the point of ADR-0006: a restore that has to turn the triggers off is a
restore that could rewrite history.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.capture_export import (
    ExportError,
    export_captures,
    payload_path,
    restore_captures,
)
from data_ingestion_toolbox.quality.reconciliation import verify_capture_checksums

pytestmark = [pytest.mark.integration, pytest.mark.database]

SOURCE = "CENSUS_ACS"


def _seed_capture(cursor, payload: bytes) -> dict[str, str]:
    """One run, one request, one capture, and the payload it points at."""
    run_id = str(uuid4())
    request_id = str(uuid4())
    capture_id = str(uuid4())
    checksum = hashlib.sha256(payload).hexdigest()
    endpoint = f"/data/2023/acs/acs5/{capture_id[:8]}"
    fingerprint = hashlib.sha256(endpoint.encode()).hexdigest()

    cursor.execute(
        "INSERT INTO control.ingestion_run (run_id, source_code, status) "
        "VALUES (%s, %s, 'success')",
        (run_id, SOURCE),
    )
    cursor.execute(
        "INSERT INTO control.ingestion_request "
        "(request_id, run_id, source_code, endpoint, request_fingerprint, status) "
        "VALUES (%s, %s, %s, %s, %s, 'captured')",
        (request_id, run_id, SOURCE, endpoint, fingerprint),
    )
    cursor.execute(
        "INSERT INTO raw_capture.payload_blob "
        "(payload_checksum, payload, payload_size) VALUES (%s, %s, %s) "
        "ON CONFLICT DO NOTHING",
        (checksum, payload, len(payload)),
    )
    cursor.execute(
        "INSERT INTO raw_capture.response_capture "
        "(capture_id, request_id, run_id, source_code, endpoint, request_fingerprint, "
        " retrieved_at, http_status, media_type, payload_checksum) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, 200, 'application/json', %s)",
        (
            capture_id,
            request_id,
            run_id,
            SOURCE,
            endpoint,
            fingerprint,
            datetime(2026, 3, 1, 12, tzinfo=timezone.utc),
            checksum,
        ),
    )
    return {
        "run_id": run_id,
        "request_id": request_id,
        "capture_id": capture_id,
        "checksum": checksum,
    }


def _append_only_triggers(cursor) -> set[tuple[str, str]]:
    cursor.execute(
        """
        SELECT tgrelid::regclass::text, tgname
          FROM pg_trigger
         WHERE NOT tgisinternal
           AND tgrelid::regclass::text IN (
               'raw_capture.payload_blob', 'raw_capture.response_capture'
           )
        """
    )
    return {(relation, name) for relation, name in cursor.fetchall()}


def test_an_exported_capture_restores_and_verifies(
    tmp_path: Path,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-047 — the round trip, end to end, with the triggers on."""
    payloads = [
        b'[{"NAME":"Dane County, Wisconsin","B01003_001E":"561504"}]',
        b'[{"NAME":"Dane County, Wisconsin","B01003_001E":"565000"}]',
    ]

    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            seeded = [_seed_capture(cursor, payload) for payload in payloads]
            triggers_before = _append_only_triggers(cursor)
        database.commit()

        assert len(triggers_before) == 2, triggers_before

        export_captures(database, tmp_path)
        for record, payload in zip(seeded, payloads):
            assert payload_path(tmp_path, record["checksum"]).read_bytes() == payload
    finally:
        database.close()

    # A reset destroys silver and gold and re-creates the schema. The captures
    # are gone from the database and present in the export.
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            for record in seeded:
                cursor.execute(
                    "DELETE FROM control.ingestion_request WHERE request_id = %s",
                    (record["request_id"],),
                )
        database.rollback()
    finally:
        database.close()

    # The restore. Nothing is disabled, nothing is dropped: the triggers that
    # were there before are there during, and every statement is an INSERT.
    database = postgres_connection_factory()
    try:
        loaded = restore_captures(database, tmp_path)
        database.commit()
        assert loaded["raw_capture.response_capture"] >= len(seeded)

        with database.cursor() as cursor:
            assert _append_only_triggers(cursor) == triggers_before

            # DQ-SHARED-001 over every capture in scope, not a window: this is
            # the verdict a release certification uses.
            outcomes = verify_capture_checksums(
                cursor, {"cadence": "release", "source_code": SOURCE}
            )
            assert outcomes, "the rule returned no outcome at all"
            for outcome in outcomes:
                assert outcome.status == "PASS", outcome

            cursor.execute(
                "SELECT COUNT(*) FROM raw_capture.response_capture WHERE capture_id = ANY(%s)",
                ([record["capture_id"] for record in seeded],),
            )
            assert cursor.fetchone()[0] == len(seeded)
    finally:
        database.close()


def test_restoring_the_same_export_twice_changes_nothing(
    tmp_path: Path,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-047 — a re-run is safe, because it inserts and never updates.

    An operator who is not sure whether the restore completed must be able to
    run it again. `ON CONFLICT DO NOTHING` makes the second run a no-op rather
    than an update the triggers would refuse -- and refusing it is the correct
    behaviour, so a restore built on upserts would fail here instead.
    """
    payload = b'[{"NAME":"Dane County, Wisconsin","B01003_001E":"561504"}]'

    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            record = _seed_capture(cursor, payload)
        database.commit()
        export_captures(database, tmp_path)

        restore_captures(database, tmp_path)
        database.commit()
        restore_captures(database, tmp_path)
        database.commit()

        with database.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM raw_capture.response_capture WHERE capture_id = %s",
                (record["capture_id"],),
            )
            assert cursor.fetchone()[0] == 1
    finally:
        database.close()


def test_a_corrupted_export_never_reaches_the_database(
    tmp_path: Path,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-047 — verification happens before the first statement."""
    payload = b'[{"NAME":"Dane County, Wisconsin","B01003_001E":"561504"}]'

    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            record = _seed_capture(cursor, payload)
        database.commit()
        export_captures(database, tmp_path)

        payload_path(tmp_path, record["checksum"]).write_bytes(b"substituted")
        with pytest.raises(ExportError):
            restore_captures(database, tmp_path)
        database.rollback()
    finally:
        database.close()
