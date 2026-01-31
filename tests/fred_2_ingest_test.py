# tests/fred_2_ingest_test.py

"""
FRED Ingestion Test

This test script verifies that FRED data can be ingested from the FRED API
into the raw_fred.fred_long table.

Run this AFTER running fred_1_metadata_test.py.

This test will make real API calls to the FRED API, so ensure you have:
1. FRED_API_KEY environment variable set
2. Database connection configured
3. Metadata tables populated (run fred_1_metadata_test.py first)
"""

from fred.ingest import ingest_slice
from fred.config import CONFIG
import psycopg2
from utility.db_connection import PostgresConnectionFactory


def get_connection():
    """Get database connection."""
    details = PostgresConnectionFactory.auto(
        conn_id=CONFIG.postgres_conn_id,
        prefix="POSTGRES_",
        database="public_data",
    )
    return psycopg2.connect(**details.psycopg_kwargs())


def test_single_series_ingestion():
    """Test ingestion of a single FRED series."""
    print("\n=== Testing Single Series Ingestion (UNRATE) ===")
    
    # Ingest unemployment rate for recent 2 years
    rows = ingest_slice(
        domain="labor_cycle",
        series_ids=["UNRATE"],
        date_start="2022-01-01",
        date_end="2023-12-31"
    )
    
    print(f"Ingested {rows} rows for UNRATE (2022-2023)")
    
    # Verify data in database
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_fred.fred_long
                WHERE series_id = 'UNRATE'
                  AND obs_date BETWEEN '2022-01-01' AND '2023-12-31';
            """)
            
            db_count = cur.fetchone()[0]
            print(f"Found {db_count} UNRATE observations in database")
            
            # Show sample data
            cur.execute("""
                SELECT series_id, obs_date, value, is_missing
                FROM raw_fred.fred_long
                WHERE series_id = 'UNRATE'
                  AND obs_date BETWEEN '2022-01-01' AND '2023-12-31'
                ORDER BY obs_date DESC
                LIMIT 12;
            """)
            
            rows_sample = cur.fetchall()
            print("\nSample UNRATE data (most recent 12 months):")
            for row in rows_sample:
                print(f"  {row[0]} | {row[1]} | {row[2]} | Missing: {row[3]}")
    finally:
        conn.close()
    
    assert db_count > 0, "No UNRATE data found in database"
    assert db_count >= 24, f"Expected at least 24 monthly observations, found {db_count}"
    print("✓ Test passed: single series ingestion successful")


def test_labor_cycle_domain():
    """Test ingestion of labor_cycle domain series."""
    print("\n=== Testing Labor Cycle Domain Ingestion ===")
    
    # Ingest recent 1 year for labor cycle series
    rows = ingest_slice(
        domain="labor_cycle",
        date_start="2023-01-01",
        date_end="2023-12-31"
    )
    
    print(f"Ingested {rows} rows for labor_cycle domain (2023)")
    
    # Verify data in database
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_fred.fred_long
                WHERE domain = 'labor_cycle'
                  AND obs_date BETWEEN '2023-01-01' AND '2023-12-31';
            """)
            
            count = cur.fetchone()[0]
            print(f"Found {count} labor_cycle observations in database")
            
            # Count distinct series
            cur.execute("""
                SELECT COUNT(DISTINCT series_id)
                FROM raw_fred.fred_long
                WHERE domain = 'labor_cycle'
                  AND obs_date BETWEEN '2023-01-01' AND '2023-12-31';
            """)
            
            series_count = cur.fetchone()[0]
            print(f"Found data for {series_count} distinct series")
            
            # Show series breakdown
            cur.execute("""
                SELECT series_id, COUNT(*) as obs_count
                FROM raw_fred.fred_long
                WHERE domain = 'labor_cycle'
                  AND obs_date BETWEEN '2023-01-01' AND '2023-12-31'
                GROUP BY series_id
                ORDER BY series_id;
            """)
            
            rows_sample = cur.fetchall()
            print("\nSeries observation counts:")
            for row in rows_sample:
                print(f"  {row[0]}: {row[1]} observations")
    finally:
        conn.close()
    
    assert count > 0, "No labor_cycle data found"
    assert series_count > 0, "No distinct series found"
    print("✓ Test passed: labor_cycle domain ingestion successful")


def test_housing_domain():
    """Test ingestion of housing domain series."""
    print("\n=== Testing Housing Domain Ingestion ===")
    
    # Ingest recent 1 year for housing series
    rows = ingest_slice(
        domain="housing",
        date_start="2023-01-01",
        date_end="2023-12-31"
    )
    
    print(f"Ingested {rows} rows for housing domain (2023)")
    
    # Verify data in database
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_fred.fred_long
                WHERE domain = 'housing'
                  AND obs_date BETWEEN '2023-01-01' AND '2023-12-31';
            """)
            
            count = cur.fetchone()[0]
            print(f"Found {count} housing observations in database")
            
            # Show sample
            cur.execute("""
                SELECT series_id, obs_date, value
                FROM raw_fred.fred_long
                WHERE domain = 'housing'
                  AND obs_date BETWEEN '2023-01-01' AND '2023-12-31'
                ORDER BY series_id, obs_date DESC
                LIMIT 10;
            """)
            
            rows_sample = cur.fetchall()
            print("\nSample housing data:")
            for row in rows_sample:
                print(f"  {row[0]} | {row[1]} | {row[2]}")
    finally:
        conn.close()
    
    assert count > 0, "No housing data found"
    print("✓ Test passed: housing domain ingestion successful")


def test_macro_domain():
    """Test ingestion of macro domain series."""
    print("\n=== Testing Macro Domain Ingestion ===")
    
    # Ingest recent 2 years for macro series (GDP is quarterly, so needs longer range)
    rows = ingest_slice(
        domain="macro",
        date_start="2022-01-01",
        date_end="2023-12-31"
    )
    
    print(f"Ingested {rows} rows for macro domain (2022-2023)")
    
    # Verify data in database
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_fred.fred_long
                WHERE domain = 'macro'
                  AND obs_date BETWEEN '2022-01-01' AND '2023-12-31';
            """)
            
            count = cur.fetchone()[0]
            print(f"Found {count} macro observations in database")
            
            # Show GDP data specifically
            cur.execute("""
                SELECT series_id, obs_date, value
                FROM raw_fred.fred_long
                WHERE series_id = 'GDPC1'
                  AND obs_date BETWEEN '2022-01-01' AND '2023-12-31'
                ORDER BY obs_date DESC;
            """)
            
            gdp_rows = cur.fetchall()
            print("\nGDP (GDPC1) quarterly data:")
            for row in gdp_rows:
                print(f"  {row[1]} | {row[2]}")
    finally:
        conn.close()
    
    assert count > 0, "No macro data found"
    print("✓ Test passed: macro domain ingestion successful")


def test_missing_data_handling():
    """Verify that missing data (.) is properly handled."""
    print("\n=== Testing Missing Data Handling ===")
    
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Check if we have any missing data flagged
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_fred.fred_long
                WHERE is_missing = TRUE;
            """)
            
            missing_count = cur.fetchone()[0]
            print(f"Found {missing_count} observations flagged as missing")
            
            # Show examples if any
            if missing_count > 0:
                cur.execute("""
                    SELECT series_id, obs_date, value, is_missing
                    FROM raw_fred.fred_long
                    WHERE is_missing = TRUE
                    LIMIT 5;
                """)
                
                rows = cur.fetchall()
                print("\nSample missing data records:")
                for row in rows:
                    print(f"  {row[0]} | {row[1]} | Value: {row[2]} | Missing: {row[3]}")
            
            # Also check for NULL values (should have is_missing=TRUE)
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_fred.fred_long
                WHERE value IS NULL AND is_missing = FALSE;
            """)
            
            incorrect_missing = cur.fetchone()[0]
            
            if incorrect_missing > 0:
                print(f"\n⚠ Warning: {incorrect_missing} NULL values not flagged as missing")
            
            assert incorrect_missing == 0, \
                f"Found {incorrect_missing} NULL values not properly flagged as missing"
    finally:
        conn.close()
    
    print("✓ Test passed: missing data handling is correct")


def test_all_curated_series():
    """Test ingestion of all curated series (shorter time range)."""
    print("\n=== Testing All Curated Series Ingestion ===")
    
    # Ingest just 6 months for all series (to keep test fast)
    rows = ingest_slice(
        domain=None,  # All domains
        series_ids=CONFIG.curated_series_ids,
        date_start="2023-07-01",
        date_end="2023-12-31"
    )
    
    print(f"Ingested {rows} rows for all curated series (Jul-Dec 2023)")
    
    # Verify data in database
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Count distinct series
            cur.execute("""
                SELECT COUNT(DISTINCT series_id)
                FROM raw_fred.fred_long
                WHERE obs_date BETWEEN '2023-07-01' AND '2023-12-31';
            """)
            
            series_count = cur.fetchone()[0]
            print(f"Found data for {series_count} distinct series")
            
            # Show coverage
            cur.execute("""
                SELECT series_id, COUNT(*) as obs_count
                FROM raw_fred.fred_long
                WHERE obs_date BETWEEN '2023-07-01' AND '2023-12-31'
                GROUP BY series_id
                ORDER BY series_id;
            """)
            
            rows_sample = cur.fetchall()
            print("\nSeries coverage (Jul-Dec 2023):")
            for row in rows_sample:
                print(f"  {row[0]}: {row[1]} observations")
    finally:
        conn.close()
    
    assert series_count > 0, "No series data found"
    print("✓ Test passed: all curated series ingestion successful")


def main():
    """Run all ingestion tests."""
    print("=" * 70)
    print("FRED INGESTION TESTS")
    print("=" * 70)
    
    if not CONFIG.has_api_key:
        print("\n✗ ERROR: FRED_API_KEY not set!")
        print("Please set the FRED_API_KEY environment variable to run ingestion tests.")
        return
    
    try:
        # Test individual series and domains
        test_single_series_ingestion()
        test_labor_cycle_domain()
        test_housing_domain()
        test_macro_domain()
        
        # Test data quality
        test_missing_data_handling()
        
        # Test comprehensive ingestion
        test_all_curated_series()
        
        print("\n" + "=" * 70)
        print("ALL TESTS PASSED ✓")
        print("=" * 70)
    
    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}")
        raise
    except Exception as e:
        print(f"\n✗ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
