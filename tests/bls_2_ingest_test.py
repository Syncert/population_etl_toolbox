# tests/bls_2_ingest_test.py

"""
BLS Ingestion Test

This test script verifies that BLS data can be ingested from the BLS API
into the raw_bls.bls_long table.

Run this AFTER running bls_1_metadata_test.py.

This test will make real API calls to the BLS API, so ensure you have:
1. BLS_API_KEY environment variable set
2. Database connection configured
3. Metadata tables populated (run bls_1_metadata_test.py first)
"""

from bls.ingest import ingest_slice
from bls.config import CONFIG
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


def test_laus_state_level():
    """Test LAUS ingestion at state level (all states)."""
    print("\n=== Testing LAUS State-Level Ingestion (All States) ===")
    
    # Note: LAUS (Local Area Unemployment Statistics) does not provide national-level data.
    # National unemployment data comes from CPS (Current Population Survey) series like LNS14000000.
    # LAUS covers states, counties, metros, and cities.
    
    year_start = 2022
    year_end = 2023
    
    rows = ingest_slice(
        program="la",
        start_year=year_start,
        end_year=year_end,
        geo_level="state",
        state_fips=None  # Get all states
    )
    
    print(f"Ingested {rows} rows for LAUS state-level ({year_start}-{year_end})")
    
    # Verify data in database
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_bls.bls_long
                WHERE program = 'la'
                  AND geo_level = 'state'
                  AND year BETWEEN %s AND %s;
            """, (year_start, year_end))
            
            db_count = cur.fetchone()[0]
            print(f"Found {db_count} LAUS state-level rows in database")
            
            # Show sample data
            cur.execute("""
                SELECT series_id, year, period, value, geo_level, state_fips
                FROM raw_bls.bls_long
                WHERE program = 'la'
                  AND geo_level = 'state'
                  AND year BETWEEN %s AND %s
                ORDER BY year DESC, period DESC
                LIMIT 10;
            """, (year_start, year_end))
            
            rows_sample = cur.fetchall()
            print("\nSample state-level LAUS data:")
            for row in rows_sample:
                print(f"  {row[0]} | {row[1]}/{row[2]} | {row[3]} | {row[4]} | State:{row[5]}")
    finally:
        conn.close()
    
    assert db_count > 0, "No state-level LAUS data found in database"
    print("✓ Test passed: LAUS state-level ingestion successful")


def test_laus_county_level():
    """Test LAUS ingestion at county level (Wisconsin)."""
    print("\n=== Testing LAUS County-Level Ingestion (Wisconsin) ===")
    
    year_start = 2022
    year_end = 2023
    
    rows = ingest_slice(
        program="la",
        start_year=year_start,
        end_year=year_end,
        geo_level="county",
        state_fips="55"  # Wisconsin
    )
    
    print(f"Ingested {rows} rows for LAUS Wisconsin counties ({year_start}-{year_end})")
    
    # Verify data in database
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_bls.bls_long
                WHERE program = 'la'
                  AND geo_level = 'county'
                  AND state_fips = '55'
                  AND year BETWEEN %s AND %s;
            """, (year_start, year_end))
            
            db_count = cur.fetchone()[0]
            print(f"Found {db_count} LAUS county-level rows in database for Wisconsin")
            
            # Show sample data
            cur.execute("""
                SELECT series_id, year, period, value, geo_level, state_fips, county_fips
                FROM raw_bls.bls_long
                WHERE program = 'la'
                  AND geo_level = 'county'
                  AND state_fips = '55'
                  AND year BETWEEN %s AND %s
                ORDER BY year DESC, period DESC, county_fips
                LIMIT 10;
            """, (year_start, year_end))
            
            rows_sample = cur.fetchall()
            print("\nSample Wisconsin county LAUS data:")
            for row in rows_sample:
                print(f"  {row[0]} | {row[1]}/{row[2]} | {row[3]} | {row[4]} | State:{row[5]} County:{row[6]}")
    finally:
        conn.close()
    
    assert db_count > 0, "No county-level LAUS data found in database"
    print("✓ Test passed: LAUS county-level ingestion successful")


def test_old_laus_us_level():
    """Test LAUS ingestion at US national level."""
    print("\n=== SKIPPING: LAUS US-Level Test ===")
    print("Note: LAUS does not provide US national-level data.")
    print("Use CPS (Current Population Survey) series like LNS14000000 for national unemployment.")
    print("Test skipped (expected)")


def test_laus_state_level_old():
    """Test LAUS ingestion at state level (Wisconsin)."""
    print("\n=== Testing LAUS State-Level Ingestion (Wisconsin - OLD) ===")
    
    year_start = 2022
    year_end = 2023
    
    rows = ingest_slice(
        program="la",
        start_year=year_start,
        end_year=year_end,
        geo_level="state",
        state_fips=None  # Get all states
    )
    
    print(f"Ingested {rows} rows for LAUS state-level ({year_start}-{year_end})")
    
    # Verify data in database
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Check for Wisconsin specifically
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_bls.bls_long
                WHERE program = 'la'
                  AND geo_level = 'state'
                  AND state_fips = '55'
                  AND year BETWEEN %s AND %s;
            """, (year_start, year_end))
            
            wi_count = cur.fetchone()[0]
            print(f"Found {wi_count} Wisconsin state-level rows in database")
            
            # Check total states
            cur.execute("""
                SELECT COUNT(DISTINCT state_fips)
                FROM raw_bls.bls_long
                WHERE program = 'la'
                  AND geo_level = 'state'
                  AND year BETWEEN %s AND %s;
            """, (year_start, year_end))
            
            state_count = cur.fetchone()[0]
            print(f"Found data for {state_count} distinct states")
            
            # Show sample Wisconsin data
            cur.execute("""
                SELECT series_id, year, period, value, geo_level, state_fips
                FROM raw_bls.bls_long
                WHERE program = 'la'
                  AND state_fips = '55'
                  AND year = %s
                ORDER BY period DESC
                LIMIT 10;
            """, (year_end,))
            
            rows_sample = cur.fetchall()
            print("\nSample Wisconsin state-level data:")
            for row in rows_sample:
                print(f"  {row[0]} | {row[1]}/{row[2]} | {row[3]} | State:{row[5]}")
    finally:
        conn.close()
    
    assert wi_count > 0, "No Wisconsin state-level data found"
    print("✓ Test passed: LAUS state-level ingestion successful")


def test_laus_county_level():
    """Test LAUS ingestion at county level (Wisconsin counties)."""
    print("\n=== Testing LAUS County-Level Ingestion (Wisconsin) ===")
    
    year_start = 2022
    year_end = 2023
    
    rows = ingest_slice(
        program="la",
        start_year=year_start,
        end_year=year_end,
        geo_level="county",
        state_fips="55"  # Wisconsin
    )
    
    print(f"Ingested {rows} rows for Wisconsin county-level ({year_start}-{year_end})")
    
    # Verify data in database
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_bls.bls_long
                WHERE program = 'la'
                  AND geo_level = 'county'
                  AND state_fips = '55'
                  AND year BETWEEN %s AND %s;
            """, (year_start, year_end))
            
            count = cur.fetchone()[0]
            print(f"Found {count} Wisconsin county rows in database")
            
            # Count distinct counties
            cur.execute("""
                SELECT COUNT(DISTINCT county_fips)
                FROM raw_bls.bls_long
                WHERE program = 'la'
                  AND geo_level = 'county'
                  AND state_fips = '55'
                  AND year BETWEEN %s AND %s;
            """, (year_start, year_end))
            
            county_count = cur.fetchone()[0]
            print(f"Found data for {county_count} distinct Wisconsin counties")
            
            # Show sample county data
            cur.execute("""
                SELECT series_id, year, period, value, county_fips
                FROM raw_bls.bls_long
                WHERE program = 'la'
                  AND geo_level = 'county'
                  AND state_fips = '55'
                  AND year = %s
                ORDER BY county_fips, period DESC
                LIMIT 10;
            """, (year_end,))
            
            rows_sample = cur.fetchall()
            print("\nSample Wisconsin county data:")
            for row in rows_sample:
                print(f"  {row[0]} | {row[1]}/{row[2]} | {row[3]} | County:{row[4]}")
    finally:
        conn.close()
    
    assert count > 0, "No Wisconsin county data found"
    assert county_count > 0, "No distinct counties found"
    print("✓ Test passed: LAUS county-level ingestion successful")


def test_cps_ingestion():
    """Test CPS/LN (Current Population Survey - National) ingestion."""
    print("\n=== Testing CPS/LN Ingestion ===")
    
    year_start = 2022
    year_end = 2023
    
    rows = ingest_slice(
        program="ln",
        start_year=year_start,
        end_year=year_end
    )
    
    print(f"Ingested {rows} rows for CPS/LN ({year_start}-{year_end})")
    
    # Verify data in database
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_bls.bls_long
                WHERE program = 'ln'
                  AND year BETWEEN %s AND %s;
            """, (year_start, year_end))
            
            count = cur.fetchone()[0]
            print(f"Found {count} CPS/LN rows in database")
            
            # Show sample including LNS14000000 (unemployment rate)
            cur.execute("""
                SELECT series_id, year, period, value
                FROM raw_bls.bls_long
                WHERE program = 'ln'
                  AND year = %s
                  AND series_id = 'LNS14000000'
                ORDER BY period DESC
                LIMIT 12;
            """, (year_end,))
            
            rows_sample = cur.fetchall()
            print("\nSample CPS/LN data (LNS14000000 - Unemployment Rate):")
            for row in rows_sample:
                print(f"  {row[0]} | {row[1]}/{row[2]} | {row[3]}%")
    finally:
        conn.close()
    
    assert count > 0, "No CPS/LN data found"
    print("✓ Test passed: CPS/LN ingestion successful")


def test_ces_ingestion():
    """Test CES (Current Employment Statistics) ingestion."""
    print("\n=== Testing CES Ingestion ===")
    
    year_start = 2022
    year_end = 2023
    
    rows = ingest_slice(
        program="ce",
        start_year=year_start,
        end_year=year_end
    )
    
    print(f"Ingested {rows} rows for CES ({year_start}-{year_end})")
    
    # Verify data in database
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_bls.bls_long
                WHERE program = 'ce'
                  AND year BETWEEN %s AND %s;
            """, (year_start, year_end))
            
            count = cur.fetchone()[0]
            print(f"Found {count} CES rows in database")
            
            # Show sample
            cur.execute("""
                SELECT series_id, year, period, value
                FROM raw_bls.bls_long
                WHERE program = 'ce'
                  AND year = %s
                ORDER BY period DESC
                LIMIT 10;
            """, (year_end,))
            
            rows_sample = cur.fetchall()
            print("\nSample CES data:")
            for row in rows_sample:
                print(f"  {row[0]} | {row[1]}/{row[2]} | {row[3]}")
    finally:
        conn.close()
    
    assert count > 0, "No CES data found"
    print("✓ Test passed: CES ingestion successful")


def test_cpi_ingestion():
    """Test CPI (Consumer Price Index) ingestion."""
    print("\n=== Testing CPI Ingestion ===")
    
    year_start = 2022
    year_end = 2023
    
    rows = ingest_slice(
        program="cu",
        start_year=year_start,
        end_year=year_end
    )
    
    print(f"Ingested {rows} rows for CPI ({year_start}-{year_end})")
    
    # Verify data in database
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_bls.bls_long
                WHERE program = 'cu'
                  AND year BETWEEN %s AND %s;
            """, (year_start, year_end))
            
            count = cur.fetchone()[0]
            print(f"Found {count} CPI rows in database")
            
            # Show sample
            cur.execute("""
                SELECT series_id, year, period, value
                FROM raw_bls.bls_long
                WHERE program = 'cu'
                  AND year = %s
                ORDER BY period DESC
                LIMIT 10;
            """, (year_end,))
            
            rows_sample = cur.fetchall()
            print("\nSample CPI data:")
            for row in rows_sample:
                print(f"  {row[0]} | {row[1]}/{row[2]} | {row[3]}")
    finally:
        conn.close()
    
    assert count > 0, "No CPI data found"
    print("✓ Test passed: CPI ingestion successful")


def test_jolts_ingestion():
    """Test JOLTS (Job Openings and Labor Turnover Survey) ingestion."""
    print("\n=== Testing JOLTS Ingestion ===")
    
    year_start = 2022
    year_end = 2023
    
    rows = ingest_slice(
        program="jt",
        start_year=year_start,
        end_year=year_end
    )
    
    print(f"Ingested {rows} rows for JOLTS ({year_start}-{year_end})")
    
    # Verify data in database
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM raw_bls.bls_long
                WHERE program = 'jt'
                  AND year BETWEEN %s AND %s;
            """, (year_start, year_end))
            
            count = cur.fetchone()[0]
            print(f"Found {count} JOLTS rows in database")
            
            # Show sample
            cur.execute("""
                SELECT series_id, year, period, value
                FROM raw_bls.bls_long
                WHERE program = 'jt'
                  AND year = %s
                ORDER BY period DESC
                LIMIT 10;
            """, (year_end,))
            
            rows_sample = cur.fetchall()
            print("\nSample JOLTS data:")
            for row in rows_sample:
                print(f"  {row[0]} | {row[1]}/{row[2]} | {row[3]}")
    finally:
        conn.close()
    
    assert count > 0, "No JOLTS data found"
    print("✓ Test passed: JOLTS ingestion successful")


def main():
    """Run all ingestion tests."""
    print("=" * 70)
    print("BLS INGESTION TESTS")
    print("=" * 70)
    
    if not CONFIG.has_api_key:
        print("\n✗ ERROR: BLS_API_KEY not set!")
        print("Please set the BLS_API_KEY environment variable to run ingestion tests.")
        return
    
    try:
        # LAUS tests (geographic hierarchy)
        # Skip US-level as LAUS doesn't provide national data
        test_old_laus_us_level()  # This just prints a skip message
        test_laus_state_level()
        test_laus_county_level()
        
        # Other program tests
        test_cps_ingestion()  # National unemployment/employment (LN series)
        test_ces_ingestion()
        test_cpi_ingestion()
        test_jolts_ingestion()
        
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
