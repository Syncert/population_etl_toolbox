#!/usr/bin/env python3
"""
Diagnostic script: Identify missing geographic surrogate keys (geo_sk) in silver_ref.dim_geo

This script helps troubleshoot why rows are being dropped from BLS and Census transforms
due to missing geo_sk values.

Run this after a failed transform to diagnose issues.
"""

from datetime import date
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def diagnose_bls_missing_geos(postgres_conn_id: str = 'public_data'):
    """Diagnose missing BLS geographies."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    logger.info("=" * 80)
    logger.info("BLS GEOGRAPHY DIAGNOSTIC")
    logger.info("=" * 80)
    
    # 1. Count total unique geographies in raw BLS data
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(DISTINCT (series_id, year, period)) as total_records
            FROM raw_bls.bls_long;
        """)
        total_bls = cur.fetchone()[0]
    
    logger.info(f"Total BLS data records: {total_bls:,}")
    
    # 2. Check what geographic levels are in BLS data
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT 
                CASE 
                    WHEN series_id LIKE 'LNS%' THEN 'US'
                    WHEN series_id ~ '^LA[SU]ST[0-9]{2}0{11}[0-9]{2}$' THEN 'State'
                    WHEN series_id ~ '^LA[SU]CN[0-9]{5}0{8}[0-9]{2}$' THEN 'County'
                    ELSE 'Unknown'
                END as geo_type,
                COUNT(*) as record_count
            FROM raw_bls.bls_long
            GROUP BY geo_type
            ORDER BY geo_type;
        """)
        logger.info("\nBLS geographic distribution:")
        for geo_type, count in cur.fetchall():
            logger.info(f"  {geo_type}: {count:,}")
    
    # 3. Check what's in dim_geo
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT 
                geo_level,
                COUNT(*) as count
            FROM silver_ref.dim_geo
            GROUP BY geo_level
            ORDER BY geo_level;
        """)
        logger.info("\nsilver_ref.dim_geo contents:")
        for geo_level, count in cur.fetchall():
            logger.info(f"  {geo_level}: {count:,}")
    
    # 4. Audit stored raw LAUS geography against the canonical 20-character ID.
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM raw_bls.bls_long
            WHERE program = 'la'
              AND series_id ~ '^LA[SU]CN[0-9]{5}0{8}[0-9]{2}$'
              AND (
                  geo_level IS DISTINCT FROM 'county'
                  OR state_fips IS DISTINCT FROM SUBSTRING(series_id, 6, 2)
                  OR county_fips IS DISTINCT FROM SUBSTRING(series_id, 8, 3)
                  OR geo_id IS DISTINCT FROM (
                      'state:' || SUBSTRING(series_id, 6, 2)
                      || '|county:' || SUBSTRING(series_id, 8, 3)
                  )
              );
        """)
        malformed_raw_count = cur.fetchone()[0]

    if malformed_raw_count:
        logger.warning(
            "Found %s raw LAUS county rows with stale derived geography fields. "
            "The stored series IDs and observations are valid; rerun raw enrichment "
            "or update the derived fields, then rerun the LA silver transform.",
            f"{malformed_raw_count:,}",
        )
    else:
        logger.info("Raw LAUS county geography fields match canonical series parsing.")

    # 5. Find canonical BLS geographies that are not in dim_geo.
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            WITH bls_geo AS (
                SELECT DISTINCT
                    CASE 
                        WHEN series_id LIKE 'LNS%' THEN 'us'
                        WHEN series_id ~ '^LA[SU]ST[0-9]{2}0{11}[0-9]{2}$' THEN 'state'
                        WHEN series_id ~ '^LA[SU]CN[0-9]{5}0{8}[0-9]{2}$' THEN 'county'
                        ELSE NULL
                    END AS geo_level,
                    CASE 
                        WHEN series_id LIKE 'LNS%' THEN 'us:1'
                        WHEN series_id ~ '^LA[SU]ST[0-9]{2}0{11}[0-9]{2}$'
                            THEN 'state:' || SUBSTRING(series_id, 6, 2)
                        WHEN series_id ~ '^LA[SU]CN[0-9]{5}0{8}[0-9]{2}$' THEN
                            'state:' || SUBSTRING(series_id, 6, 2) || '|county:' || SUBSTRING(series_id, 8, 3)
                        ELSE NULL
                    END AS geo_id
                FROM raw_bls.bls_long
            )
            SELECT 
                bls.geo_level,
                bls.geo_id,
                COUNT(*) as bls_record_count,
                CASE WHEN dg.geo_sk IS NULL THEN 'MISSING' ELSE 'OK' END as status
            FROM bls_geo bls
            LEFT JOIN silver_ref.dim_geo dg 
                ON bls.geo_level = dg.geo_level AND bls.geo_id = dg.geo_id
            WHERE bls.geo_level IS NOT NULL AND bls.geo_id IS NOT NULL
            GROUP BY bls.geo_level, bls.geo_id, dg.geo_sk
            ORDER BY status DESC, bls.geo_level, bls.geo_id;
        """)
        
        missing_count = 0
        ok_count = 0
        missing_geo_list = []
        
        logger.info("\nGeography lookup status:")
        for geo_level, geo_id, record_count, status in cur.fetchall():
            if status == 'MISSING':
                missing_count += 1
                missing_geo_list.append((geo_level, geo_id, record_count))
                logger.warning(f"  {status} - {geo_level:10} {geo_id:40} ({record_count:,} records)")
            else:
                ok_count += 1
        
        if missing_geo_list:
            logger.error(f"\nFound {missing_count} missing geographic combinations!")
            logger.error("These would cause row drops during transform:")
            total_missing_records = sum(rc for _, _, rc in missing_geo_list)
            logger.error(f"  Total records affected: {total_missing_records:,}")
        else:
            logger.info(f"\nAll {ok_count} BLS geographies are present in dim_geo ✓")


def diagnose_census_missing_geos(postgres_conn_id: str = 'public_data'):
    """Diagnose missing Census geographies."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    logger.info("\n" + "=" * 80)
    logger.info("CENSUS ACS GEOGRAPHY DIAGNOSTIC")
    logger.info("=" * 80)
    
    # 1. Count total unique geographies in Census data
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) as total_records
            FROM raw_census.acs_long;
        """)
        total_census = cur.fetchone()[0]
    
    logger.info(f"Total Census ACS records: {total_census:,}")
    
    # 2. Check what geographic levels are in Census data
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT 
                geo_level,
                COUNT(DISTINCT (state_fips, county_fips)) as unique_geos,
                COUNT(*) as record_count
            FROM raw_census.acs_long
            GROUP BY geo_level
            ORDER BY geo_level;
        """)
        logger.info("\nCensus geographic distribution:")
        for geo_level, unique_geos, count in cur.fetchall():
            logger.info(f"  {geo_level}: {unique_geos:,} unique geographies, {count:,} records")
    
    # 3. Find missing geographies
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            WITH census_geo AS (
                SELECT DISTINCT
                    geo_level,
                    state_fips,
                    county_fips
                FROM raw_census.acs_long
            )
            SELECT 
                cg.geo_level,
                cg.state_fips,
                cg.county_fips,
                CASE 
                    WHEN cg.geo_level = 'us' THEN 'us:1'
                    WHEN cg.geo_level = 'state' THEN 'state:' || cg.state_fips
                    WHEN cg.geo_level = 'county' THEN 'state:' || cg.state_fips || '|county:' || cg.county_fips
                    ELSE NULL
                END as expected_geo_id,
                CASE WHEN dg.geo_sk IS NULL THEN 'MISSING' ELSE 'OK' END as status
            FROM census_geo cg
            LEFT JOIN silver_ref.dim_geo dg 
                ON cg.geo_level = dg.geo_level AND 
                   CASE 
                       WHEN cg.geo_level = 'us' THEN dg.geo_id = 'us:1'
                       WHEN cg.geo_level = 'state' THEN dg.geo_id = 'state:' || cg.state_fips
                       WHEN cg.geo_level = 'county' THEN dg.geo_id = 'state:' || cg.state_fips || '|county:' || cg.county_fips
                   END
            ORDER BY status DESC, cg.geo_level;
        """)
        
        missing_count = 0
        logger.info("\nGeography lookup status:")
        for geo_level, state_fips, county_fips, expected_geo_id, status in cur.fetchall():
            geo_display = f"{geo_level}:{state_fips or 'NULL'}:{county_fips or 'NULL'}"
            if status == 'MISSING':
                missing_count += 1
                logger.warning(f"  {status} - {expected_geo_id}")
            
        if missing_count > 0:
            logger.error(f"\nFound {missing_count} missing geographic combinations!")
        else:
            logger.info("\nAll Census geographies are present in dim_geo ✓")


def check_dimension_sync_status(postgres_conn_id: str = 'public_data'):
    """Check when dimensions were last synced."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    logger.info("\n" + "=" * 80)
    logger.info("DIMENSION SYNC STATUS")
    logger.info("=" * 80)
    
    with hook.get_conn() as conn, conn.cursor() as cur:
        # Check dim_geo last update
        cur.execute("""
            SELECT MAX(ingested_at) as last_sync FROM silver_ref.dim_geo;
        """)
        last_geo_sync = cur.fetchone()[0]
        logger.info(f"dim_geo last synced: {last_geo_sync}")
        
        # Check dim_time last update
        cur.execute("""
            SELECT MAX(ingested_at) as last_sync FROM silver_ref.dim_time;
        """)
        last_time_sync = cur.fetchone()[0]
        logger.info(f"dim_time last synced: {last_time_sync}")
        
        # Check date range coverage
        cur.execute("""
            SELECT MIN(date_key) as earliest, MAX(date_key) as latest 
            FROM silver_ref.dim_time;
        """)
        earliest, latest = cur.fetchone()
        logger.info(f"dim_time coverage: {earliest} to {latest}")


if __name__ == '__main__':
    logger.info("Starting geography diagnostics...\n")
    
    postgres_conn_id = 'public_data'  # Default Airflow connection ID
    
    try:
        diagnose_bls_missing_geos(postgres_conn_id)
        diagnose_census_missing_geos(postgres_conn_id)
        check_dimension_sync_status(postgres_conn_id)
        
        logger.info("\n" + "=" * 80)
        logger.info("DIAGNOSTIC COMPLETE")
        logger.info("=" * 80)
    except Exception as e:
        logger.error(f"Diagnostic failed: {e}", exc_info=True)
