# tests/fred_1_metadata_test.py

"""
FRED Metadata Test

This test script verifies that FRED metadata can be fetched and stored
in the raw_fred schema tables: fred_series and fred_datasets.

Run this BEFORE running the ingestion tests.
"""

from data_ingestion_toolbox.fred.metadata import sync_fred_series_metadata, sync_fred_datasets_table
from data_ingestion_toolbox.fred.config import CONFIG
import psycopg2
from data_ingestion_toolbox.utility.db_connection import PostgresConnectionFactory


def get_connection():
    """Get database connection."""
    details = PostgresConnectionFactory.auto(
        conn_id=CONFIG.postgres_conn_id,
        prefix="POSTGRES_",
        database="public_data",
    )
    return psycopg2.connect(**details.psycopg_kwargs())


def test_sync_fred_datasets_table():
    """Test syncing FRED datasets table."""
    print("\n=== Testing sync_fred_datasets_table ===")
    
    count = sync_fred_datasets_table()
    print(f"Synced {count} dataset records")
    
    # Verify data was inserted
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_fred.fred_datasets;")
            db_count = cur.fetchone()[0]
            print(f"Found {db_count} records in raw_fred.fred_datasets")
            
            # Show sample by domain
            cur.execute("""
                SELECT domain, COUNT(*) as series_count
                FROM raw_fred.fred_datasets
                GROUP BY domain
                ORDER BY domain;
            """)
            rows = cur.fetchall()
            print("\nRecords by domain:")
            for row in rows:
                print(f"  {row[0]}: {row[1]} series")
    finally:
        conn.close()
    
    assert db_count > 0, "No records found in fred_datasets"
    print("[PASS] Test passed: fred_datasets populated")


def test_sync_fred_series_metadata():
    """Test syncing FRED series metadata."""
    print("\n=== Testing sync_fred_series_metadata ===")
    
    # Test with a small subset first
    test_series = ["UNRATE", "PAYEMS", "GDPC1"]
    print(f"Testing with series: {test_series}")
    
    count = sync_fred_series_metadata(test_series)
    print(f"Synced {count} series metadata records")
    
    # Verify data was inserted
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM raw_fred.fred_series
                WHERE series_id = ANY(%s);
                """,
                (test_series,)
            )
            db_count = cur.fetchone()[0]
            print(f"Found {db_count} series in database")
            
            # Show sample
            cur.execute("""
                SELECT series_id, title, units, frequency, seasonal_adjustment,
                       observation_start, observation_end
                FROM raw_fred.fred_series
                WHERE series_id = ANY(%s)
                ORDER BY series_id;
            """, (test_series,))
            rows = cur.fetchall()
            print("\nSample series metadata:")
            for row in rows:
                print(f"  {row[0]}")
                print(f"    Title: {row[1]}")
                print(f"    Units: {row[2]}, Freq: {row[3]}, Seasonal Adj: {row[4]}")
                print(f"    Obs Range: {row[5]} to {row[6]}")
    finally:
        conn.close()
    
    assert db_count == len(test_series), f"Expected {len(test_series)} series, found {db_count}"
    print("[PASS] Test passed: fred_series metadata synced")


def test_sync_all_curated_series():
    """Test syncing all curated FRED series metadata."""
    print("\n=== Testing sync_all_curated_series ===")
    
    count = sync_fred_series_metadata()  # Uses CONFIG.curated_series_ids by default
    print(f"Synced {count} curated series")
    
    # Verify data
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_fred.fred_series;")
            db_count = cur.fetchone()[0]
            print(f"Total series in database: {db_count}")
            
            # Check that all curated series are present
            cur.execute("""
                SELECT series_id, title
                FROM raw_fred.fred_series
                WHERE series_id = ANY(%s)
                ORDER BY series_id;
            """, (CONFIG.curated_series_ids,))
            rows = cur.fetchall()
            
            print(f"\nFound {len(rows)}/{len(CONFIG.curated_series_ids)} curated series:")
            for row in rows:
                print(f"  {row[0]}: {row[1][:60]}")
    finally:
        conn.close()
    
    assert db_count >= len(CONFIG.curated_series_ids), \
        f"Expected at least {len(CONFIG.curated_series_ids)} series, found {db_count}"
    print("[PASS] Test passed: all curated series metadata synced")


def test_series_metadata_fields():
    """Verify that series metadata has expected fields populated."""
    print("\n=== Testing Series Metadata Fields ===")
    
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Check for series with complete metadata
            cur.execute("""
                SELECT series_id, title, units, frequency
                FROM raw_fred.fred_series
                WHERE title IS NOT NULL
                  AND title != ''
                  AND units IS NOT NULL
                  AND frequency IS NOT NULL
                LIMIT 5;
            """)
            rows = cur.fetchall()
            
            print("Sample series with complete metadata:")
            for row in rows:
                print(f"  {row[0]}: {row[1][:50]} | Units: {row[2]} | Freq: {row[3]}")
            
            assert len(rows) > 0, "No series with complete metadata found"
            print("[PASS] Test passed: series metadata fields are populated")
    finally:
        conn.close()


def main():
    """Run all metadata tests."""
    print("=" * 70)
    print("FRED METADATA TESTS")
    print("=" * 70)
    
    if not CONFIG.has_api_key:
        print("\n[FAIL] ERROR: FRED_API_KEY not set!")
        print("Please set the FRED_API_KEY environment variable to run metadata tests.")
        return
    
    try:
        # Test 1: Sync datasets table
        test_sync_fred_datasets_table()
        
        # Test 2: Sync a few test series
        test_sync_fred_series_metadata()
        
        # Test 3: Sync all curated series
        test_sync_all_curated_series()
        
        # Test 4: Verify metadata fields
        test_series_metadata_fields()
        
        print("\n" + "=" * 70)
        print("ALL TESTS PASSED [PASS]")
        print("=" * 70)
    
    except AssertionError as e:
        print(f"\n[FAIL] TEST FAILED: {e}")
        raise
    except Exception as e:
        print(f"\n[FAIL] UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
