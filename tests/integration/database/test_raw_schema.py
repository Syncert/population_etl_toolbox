"""Warehouse bootstrap and capture-first schema contracts."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from tests.support.postgres import apply_sql_files

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_database_service_versions(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ENV-008 — database integration runs expected service versions."""
    conn = postgres_connection_factory()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW server_version_num")
            assert int(cursor.fetchone()[0]) >= 160000
            cursor.execute("SELECT postgis_lib_version()")
            assert cursor.fetchone()[0].startswith("3.")
    finally:
        conn.close()


def test_bootstrap_creates_metadata_capture_and_revision_tables(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-001 — bootstrap creates every required owned table."""
    expected = {
        "raw_capture.response_capture",
        "control.ingestion_run",
        "silver_census.observation_revision",
        "silver_bls.observation_revision",
        "silver_fred.observation_revision",
        "raw_census.acs_datasets",
        "raw_bls.bls_datasets",
        "raw_fred.fred_datasets",
        "silver_pep.pep_dataset",
        "silver_pep.pep_release",
        "silver_pep.observation_revision",
    }
    conn = postgres_connection_factory()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT to_regclass(name) FROM unnest(%s::text[]) AS name",
                (sorted(expected),),
            )
            assert {str(row[0]) for row in cursor.fetchall()} == expected
            cursor.execute("""SELECT to_regclass('raw_census.acs_long'),
                                      to_regclass('raw_bls.bls_long'),
                                      to_regclass('raw_fred.fred_long')""")
            assert cursor.fetchone() == (None, None, None)
    finally:
        conn.close()


def test_warehouse_manifest_is_idempotent(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-002 — checked-in warehouse DDL can be applied twice safely."""
    conn = postgres_connection_factory()
    try:
        apply_sql_files(conn)
        apply_sql_files(conn)
        conn.commit()
    finally:
        conn.close()


def test_revision_tables_expose_capture_scoped_primary_keys(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-003 — parsed revisions are keyed to immutable capture positions."""
    conn = postgres_connection_factory()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT n.nspname || '.' || c.relname, pg_get_constraintdef(k.oid)
                FROM pg_constraint k
                JOIN pg_class c ON c.oid = k.conrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE k.contype = 'p' AND (n.nspname, c.relname) IN
                  (('silver_census','observation_revision'),
                   ('silver_bls','observation_revision'),
                   ('silver_fred','observation_revision'))
            """)
            definitions = dict(cursor.fetchall())
            assert set(definitions) == {
                "silver_census.observation_revision",
                "silver_bls.observation_revision",
                "silver_fred.observation_revision",
            }
            assert all("capture_id" in value for value in definitions.values())
    finally:
        conn.close()


def test_control_status_constraints_and_rollback(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-005, DB-014 — control domains reject invalid state transactionally."""
    conn = postgres_connection_factory()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SAVEPOINT before_invalid")
            with pytest.raises(Exception):
                cursor.execute(
                    "INSERT INTO control.ingestion_run (source_code, status) VALUES ('TEST', 'invalid')"
                )
            cursor.execute("ROLLBACK TO SAVEPOINT before_invalid")
            cursor.execute(
                "SELECT COUNT(*) FROM control.ingestion_run WHERE source_code = 'TEST'"
            )
            assert cursor.fetchone() == (0,)
        conn.rollback()
    finally:
        conn.close()
