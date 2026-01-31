# bls/geography.py

from __future__ import annotations

import logging
from typing import List, Optional

import psycopg2

from utility.db_connection import PostgresConnectionFactory
from .config import CONFIG

logger = logging.getLogger(__name__)

_TARGET_DATABASE = "public_data"


def _get_pg_connection():
    """Get database connection."""
    details = PostgresConnectionFactory.auto(
        conn_id=getattr(CONFIG, "postgres_conn_id", None),
        prefix="POSTGRES_",
        database=_TARGET_DATABASE,
    )
    return psycopg2.connect(**details.psycopg_kwargs())


def get_laus_area_codes(
    geo_level: str,
    state_fips: Optional[str] = None
) -> List[str]:
    """
    Get LAUS area codes for a given geographic level.
    
    LAUS area code format (15 characters):
    - US:      000000000000000  (15 zeros)
    - State:   ST##000000000000  (ST + 2-digit state FIPS + 12 zeros)
    - County:  CN##NNNNN0000000  (CN + 2-digit state FIPS + 5-digit county + 7 zeros)
    - Metro:   MT#############   (MT + 13 digits)
    - City:    CI#############   (CI + 13 digits)
    
    This function queries raw_bls.bls_series metadata to get valid area codes
    that have been synced from BLS download.bls.gov metadata files.
    
    Args:
        geo_level: One of 'us', 'state', 'county', 'metro', 'city'
        state_fips: Required for county level, 2-digit state FIPS code
    
    Returns:
        List of 15-character LAUS area codes
    """
    if geo_level == "us":
        # US national level always uses area code with 15 zeros
        # This code may not exist in metadata but is valid for API queries
        return ["000000000000000"]
    
    elif geo_level == "state":
        # Query all state-level area codes from metadata
        # State area codes: ST##000000 where ## is state FIPS
        return _get_area_codes_from_db(prefix="ST")
    
    elif geo_level == "county":
        # Query county-level area codes for a specific state
        # County area codes: CN##NNNNN where ## is state FIPS
        if not state_fips:
            raise ValueError("state_fips required for county-level LAUS area codes")
        
        # Ensure state_fips is 2 digits zero-padded
        state_fips_padded = str(state_fips).zfill(2)
        
        # Get counties for this state
        return _get_area_codes_from_db(prefix=f"CN{state_fips_padded}")
    
    elif geo_level == "metro":
        # Metro area codes: MT#######
        return _get_area_codes_from_db(prefix="MT")
    
    elif geo_level == "city":
        # City area codes: CI#######
        return _get_area_codes_from_db(prefix="CI")
    
    else:
        raise ValueError(f"Unsupported geo_level: {geo_level}")


def _get_area_codes_from_db(prefix: str) -> List[str]:
    """
    Query raw_bls.bls_series for area codes matching a prefix pattern.
    
    Args:
        prefix: Area code prefix (e.g., 'ST', 'CN06', 'MT', 'CI')
    
    Returns:
        List of distinct area codes
    """
    conn = _get_pg_connection()
    
    try:
        with conn.cursor() as cur:
            # Area codes are stored in the area_code column for LAUS series
            sql = """
                SELECT DISTINCT area_code
                FROM raw_bls.bls_series
                WHERE program = 'la'
                  AND area_code LIKE %s
                  AND area_code IS NOT NULL
                ORDER BY area_code;
            """
            
            cur.execute(sql, (f"{prefix}%",))
            rows = cur.fetchall()
            
            area_codes = [row[0] for row in rows]
            
            logger.info(f"Found {len(area_codes)} LAUS area codes with prefix '{prefix}'")
            return area_codes
    
    finally:
        conn.close()


def parse_laus_series_id(series_id: str) -> dict:
    """
    Parse a LAUS series ID into its components.
    
    LAUS series ID format: LA{S|U}{area_code}{measure_code}
    - Positions 0-1:  'LA' (program identifier)
    - Position 2:     'S' (seasonally adjusted) or 'U' (not seasonally adjusted)
    - Positions 3-12: 10-digit area code
    - Positions 13-14: 2-digit measure code (03, 04, 05, 06, 07, 08, 09)
    
    Returns:
        Dict with keys: program, seasonal, area_code, measure_code, geo_level, state_fips, county_fips
    """
    if not series_id or len(series_id) < 15:
        return {
            "program": None,
            "seasonal": None,
            "area_code": None,
            "measure_code": None,
            "geo_level": None,
            "state_fips": None,
            "county_fips": None,
        }
    
    program = series_id[0:2]  # 'LA'
    seasonal = series_id[2]    # 'S' or 'U'
    area_code = series_id[3:13]  # 10 digits
    measure_code = series_id[13:15]  # 2 digits
    
    # Parse geography from area code
    geo_level = None
    state_fips = None
    county_fips = None
    
    if area_code == "0000000000":
        geo_level = "us"
    else:
        prefix = area_code[:2]
        
        if prefix == "ST":
            geo_level = "state"
            state_fips = area_code[2:4]
        elif prefix == "CN":
            geo_level = "county"
            state_fips = area_code[2:4]
            county_fips = area_code[4:9]
        elif prefix == "MT":
            geo_level = "metro"
        elif prefix == "CI":
            geo_level = "city"
    
    return {
        "program": program,
        "seasonal": seasonal,
        "area_code": area_code,
        "measure_code": measure_code,
        "geo_level": geo_level,
        "state_fips": state_fips,
        "county_fips": county_fips,
    }


def build_laus_series_id(
    area_code: str,
    measure_code: str,
    seasonal: str = "U"
) -> str:
    """
    Build a LAUS series ID from components.
    
    Args:
        area_code: 10-digit LAUS area code
        measure_code: 2-digit measure code (03, 04, 05, 06, 07, 08, 09)
        seasonal: 'S' (seasonally adjusted) or 'U' (not seasonally adjusted)
    
    Returns:
        Full 15-character LAUS series ID
    """
    if len(area_code) != 10:
        raise ValueError(f"area_code must be 10 digits, got: {area_code}")
    if len(measure_code) != 2:
        raise ValueError(f"measure_code must be 2 digits, got: {measure_code}")
    if seasonal not in ("S", "U"):
        raise ValueError(f"seasonal must be 'S' or 'U', got: {seasonal}")
    
    return f"LA{seasonal}{area_code}{measure_code}"


# Measure code descriptions (for reference)
LAUS_MEASURE_CODES = {
    "03": "Unemployment rate (% of labor force)",
    "04": "Unemployment level (count)",
    "05": "Employment level (count)",
    "06": "Labor force level (count)",
    "07": "Employment-population ratio",
    "08": "Labor force participation rate (% of population)",
    "09": "Civilian noninstitutional population",
}
