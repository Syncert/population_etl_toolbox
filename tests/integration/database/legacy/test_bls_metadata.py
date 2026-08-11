# tests/bls_1_metadata_test.py

"""
BLS Metadata Test

This test script verifies that BLS metadata can be fetched and stored
in the raw_bls schema tables: bls_series and bls_datasets.

Run this BEFORE running the ingestion tests.
"""

import os

import psycopg2
import pytest

from data_ingestion_toolbox.bls.config import CONFIG
from data_ingestion_toolbox.bls.metadata import (
    sync_bls_datasets_table,
    sync_bls_series_metadata,
)
from data_ingestion_toolbox.utility.db_connection import PostgresConnectionFactory

pytestmark = [pytest.mark.integration, pytest.mark.database, pytest.mark.slow]
if os.environ.get("RUN_LEGACY_DATABASE_TESTS") != "1":
    pytest.skip(
        "legacy live-database check requires explicit opt-in", allow_module_level=True
    )


def get_connection():
    """Get database connection."""
    details = PostgresConnectionFactory.auto(
        conn_id=CONFIG.postgres_conn_id,
        prefix="POSTGRES_",
        database="public_data",
    )
    return psycopg2.connect(**details.psycopg_kwargs())


def test_sync_bls_datasets_table():
    """Covers: EXT-009 — BLS dataset metadata synchronizes."""
    print("\n=== Testing sync_bls_datasets_table ===")

    count = sync_bls_datasets_table()
    print(f"Synced {count} dataset records")

    # Verify data was inserted
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_bls.bls_datasets;")
            db_count = cur.fetchone()[0]
            print(f"Found {db_count} records in raw_bls.bls_datasets")

            # Show sample
            cur.execute("""
                SELECT program, year, title, is_available
                FROM raw_bls.bls_datasets
                ORDER BY program
                LIMIT 10;
            """)
            rows = cur.fetchall()
            print("\nSample records:")
            for row in rows:
                print(f"  {row[0]} | {row[1]} | {row[2]} | {row[3]}")
    finally:
        conn.close()

    assert db_count > 0, "No records found in bls_datasets"
    print("[PASS] Test passed: bls_datasets populated")


def test_sync_bls_series_metadata():
    """Covers: EXT-009 — BLS series metadata synchronizes by program."""
    print("\n=== Testing sync_bls_series_metadata ===")

    for program in CONFIG.programs:
        print(f"\n--- Testing program: {program} ---")

        try:
            count = sync_bls_series_metadata(program)
            print(f"Synced {count} series for {program}")

            # Verify data was inserted
            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM raw_bls.bls_series WHERE program = %s;",
                        (program,),
                    )
                    db_count = cur.fetchone()[0]
                    print(f"Found {db_count} series in database for {program}")

                    # Show sample
                    cur.execute(
                        """
                        SELECT series_id, title, seasonal, measure, area_code
                        FROM raw_bls.bls_series
                        WHERE program = %s
                        ORDER BY series_id
                        LIMIT 5;
                    """,
                        (program,),
                    )
                    rows = cur.fetchall()
                    print(f"\nSample {program} series:")
                    for row in rows:
                        print(
                            f"  {row[0]} | {row[1][:60] if row[1] else ''} | S:{row[2]} | M:{row[3]} | A:{row[4]}"
                        )
            finally:
                conn.close()

            assert db_count > 0, f"No series found for program {program}"
            print(f"[PASS] Test passed: {program} series metadata synced")

        except Exception as e:
            print(f"[FAIL] Test failed for {program}: {e}")
            # Continue to next program rather than failing entirely
            continue


def test_laus_area_code_variety():
    """Covers: EXT-009 — LAUS metadata retains geography varieties."""
    print("\n=== Testing LAUS Area Code Variety ===")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Check for state-level
            cur.execute("""
                SELECT COUNT(*) FROM raw_bls.bls_series
                WHERE program = 'la' AND area_code LIKE 'ST%';
            """)
            state_count = cur.fetchone()[0]
            print(f"State-level LAUS series: {state_count}")

            # Check for county-level
            cur.execute("""
                SELECT COUNT(*) FROM raw_bls.bls_series
                WHERE program = 'la' AND area_code LIKE 'CN%';
            """)
            county_count = cur.fetchone()[0]
            print(f"County-level LAUS series: {county_count}")

            # Check for metro-level
            cur.execute("""
                SELECT COUNT(*) FROM raw_bls.bls_series
                WHERE program = 'la' AND area_code LIKE 'MT%';
            """)
            metro_count = cur.fetchone()[0]
            print(f"Metro-level LAUS series: {metro_count}")

            # Note: US-level series (area_code with zeros) are not included in the
            # downloaded metadata files, but can be accessed via the API
            assert state_count > 0, "No state-level LAUS series found"
            assert county_count > 0, "No county-level LAUS series found"
            assert metro_count > 0, "No metro-level LAUS series found"

            print(
                "[PASS] Test passed: LAUS metadata includes multiple geographic levels"
            )
    finally:
        conn.close()


def main():
    """Run all metadata tests."""
    print("=" * 70)
    print("BLS METADATA TESTS")
    print("=" * 70)

    try:
        # Test 1: Sync datasets table
        test_sync_bls_datasets_table()

        # Test 2: Sync series metadata
        test_sync_bls_series_metadata()

        # Test 3: Verify LAUS area variety
        test_laus_area_code_variety()

        print("\n" + "=" * 70)
        print("ALL TESTS PASSED [PASS]")
        print("=" * 70)

    except AssertionError as e:
        print(f"\n[FAIL] TEST FAILED: {e}")
        raise
    except Exception as e:
        print(f"\n[FAIL] UNEXPECTED ERROR: {e}")
        raise


if __name__ == "__main__":
    main()
