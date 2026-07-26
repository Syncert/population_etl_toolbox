# bls/geography.py

from __future__ import annotations

import logging
import re
from typing import List, Optional

import psycopg2

from data_ingestion_toolbox.utility.db_connection import PostgresConnectionFactory
from .config import CONFIG

logger = logging.getLogger(__name__)

_TARGET_DATABASE = "public_data"

LAUS_AREA_CODE_LENGTH = 15
LAUS_SERIES_ID_LENGTH = 20
LAUS_NATIONAL_AREA_CODE = "0" * LAUS_AREA_CODE_LENGTH
_LAUS_AREA_CODE_RE = re.compile(r"^[A-Z0-9]{15}$")
_LAUS_MEASURE_CODE_RE = re.compile(r"^[0-9]{2}$")
_LAUS_SERIES_ID_RE = re.compile(r"^LA[SU][A-Z0-9]{15}[0-9]{2}$")


def _empty_laus_parse() -> dict:
    return {
        "program": None,
        "seasonal": None,
        "area_code": None,
        "measure_code": None,
        "geo_level": None,
        "geo_id": None,
        "state_fips": None,
        "county_fips": None,
    }


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
    - State:   ST##00000000000   (ST + 2-digit state FIPS + 11 zeros)
    - County:  CN#####00000000   (CN + 5-digit state/county FIPS + 8 zeros)
    - Metro:   MT#############   (MT + 13 digits)
    - City:    CT#############   (CT + 13 digits)
    
    This function queries raw_bls.bls_series metadata to get valid area codes
    that have been synced from BLS download.bls.gov metadata files.
    
    Args:
        geo_level: One of 'us', 'state', 'county', 'metro', 'city'
        state_fips: Required for county level, 2-digit state FIPS code
    
    Returns:
        List of 15-character LAUS area codes
    """
    if geo_level == "us":
        # LAUS metadata does not publish a national series, but retaining the
        # all-zero code makes the official-width grammar explicit and supports
        # parsing legacy national rows. CPS/LN remains the national data source.
        return [LAUS_NATIONAL_AREA_CODE]
    
    elif geo_level == "state":
        # Query all state-level area codes from metadata
        # State area codes: ST##00000000000 where ## is state FIPS
        return _get_area_codes_from_db(prefix="ST")
    
    elif geo_level == "county":
        # Query county-level area codes for a specific state
        # County area codes: CN#####00000000 where ##### is state+county FIPS
        if not state_fips:
            raise ValueError("state_fips required for county-level LAUS area codes")
        
        # Ensure state_fips is 2 digits zero-padded
        state_fips_padded = str(state_fips).zfill(2)
        
        # Get counties for this state
        return _get_area_codes_from_db(prefix=f"CN{state_fips_padded}")
    
    elif geo_level == "metro":
        # Metro area codes: MT + 13 digits
        return _get_area_codes_from_db(prefix="MT")
    
    elif geo_level == "city":
        # Official LAUS city area codes use CT, not CI.
        return _get_area_codes_from_db(prefix="CT")
    
    else:
        raise ValueError(f"Unsupported geo_level: {geo_level}")


def _get_area_codes_from_db(prefix: str) -> List[str]:
    """
    Query raw_bls.bls_series for area codes matching a prefix pattern.
    
    Args:
        prefix: Area code prefix (e.g., 'ST', 'CN06', 'MT', 'CT')
    
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
                  AND LENGTH(area_code) = 15
                  AND area_code ~ '^[A-Z0-9]{15}$'
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
    
    Official LAUS series ID format: LA{S|U}{area_code}{measure_code}
    - Positions 0-1:  'LA' (program identifier)
    - Position 2:     'S' (seasonally adjusted) or 'U' (not seasonally adjusted)
    - Positions 3-17: 15-character area code
    - Positions 18-19: 2-digit measure code (03, 04, 05, 06, 07, 08, 09)
    
    Returns:
        Parsed components and normalized geography fields. Invalid or legacy
        non-20-character IDs return the same keys with null values.
    """
    if not isinstance(series_id, str) or not _LAUS_SERIES_ID_RE.fullmatch(series_id):
        return _empty_laus_parse()
    
    program = series_id[0:2]  # 'LA'
    seasonal = series_id[2]  # 'S' or 'U'
    area_code = series_id[3:18]  # 15 characters
    measure_code = series_id[18:20]  # 2 digits
    
    # Parse geography from area code
    geo_level = None
    geo_id = None
    state_fips = None
    county_fips = None
    
    if area_code == LAUS_NATIONAL_AREA_CODE:
        geo_level = "us"
        geo_id = "us:1"
    else:
        prefix = area_code[:2]
        
        if prefix == "ST" and re.fullmatch(r"ST\d{2}0{11}", area_code):
            geo_level = "state"
            state_fips = area_code[2:4]
            geo_id = f"state:{state_fips}"
        elif prefix == "CN" and re.fullmatch(r"CN\d{5}0{8}", area_code):
            geo_level = "county"
            state_fips = area_code[2:4]
            county_fips = area_code[4:7]
            geo_id = f"state:{state_fips}|county:{county_fips}"
        elif prefix == "MT" and re.fullmatch(r"MT\d{7}0{6}", area_code):
            geo_level = "metro"
            state_fips = area_code[2:4]
            geo_id = f"metro:{area_code[4:9]}"
        elif prefix == "CT" and re.fullmatch(r"CT\d{7}0{6}", area_code):
            geo_level = "city"
            state_fips = area_code[2:4]
            geo_id = f"state:{state_fips}|city:{area_code[4:9]}"
    
    return {
        "program": program,
        "seasonal": seasonal,
        "area_code": area_code,
        "measure_code": measure_code,
        "geo_level": geo_level,
        "geo_id": geo_id,
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
        area_code: Official 15-character LAUS area code
        measure_code: 2-digit measure code (03, 04, 05, 06, 07, 08, 09)
        seasonal: 'S' (seasonally adjusted) or 'U' (not seasonally adjusted)
    
    Returns:
        Full 20-character LAUS series ID
    """
    if not isinstance(area_code, str) or not _LAUS_AREA_CODE_RE.fullmatch(area_code):
        raise ValueError(
            "area_code must be exactly 15 uppercase alphanumeric characters, "
            f"got: {area_code!r}"
        )
    if not isinstance(measure_code, str) or not _LAUS_MEASURE_CODE_RE.fullmatch(measure_code):
        raise ValueError(f"measure_code must be exactly 2 digits, got: {measure_code!r}")
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
