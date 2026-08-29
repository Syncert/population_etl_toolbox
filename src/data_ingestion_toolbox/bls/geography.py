# data_ingestion_toolbox/bls/geography.py

from __future__ import annotations

import os
import logging
import re
from typing import List, Optional

from data_ingestion_toolbox.utility.db_connection import PostgresConnectionFactory
from .config import CONFIG

logger = logging.getLogger(__name__)

# Overridable so self-contained stacks can point at their own warehouse
# database; production deployments default to the shared "public_data".
_TARGET_DATABASE = os.environ.get("PUBLIC_DATA_DB_NAME", "public_data")


def _get_pg_connection():
    """Get database connection."""
    import psycopg2

    details = PostgresConnectionFactory.auto(
        conn_id=getattr(CONFIG, "postgres_conn_id", None),
        prefix="POSTGRES_",
        database=_TARGET_DATABASE,
    )
    return psycopg2.connect(**details.psycopg_kwargs())


def get_laus_area_codes(geo_level: str, state_fips: Optional[str] = None) -> List[str]:
    """
    Get LAUS area codes for a given geographic level.

    LAUS area code format (15 characters):
    - State:   ST##00000000000  (ST + 2-digit state FIPS + 11 zeros)
    - County:  CN##CCC00000000  (CN + state FIPS + county FIPS + 8 zeros)
    - Metro:   MT#############   (MT + 13 digits)
    - City:    CI#############   (CI + 13 digits)

    This function queries raw_bls.bls_series metadata to get valid area codes
    that have been synced from BLS download.bls.gov metadata files.

    Args:
        geo_level: One of 'state', 'county', 'metro', 'city'
        state_fips: Required for county level, 2-digit state FIPS code

    Returns:
        List of 15-character LAUS area codes
    """
    if geo_level == "us":
        raise ValueError(
            "LAUS does not publish national series; use CPS/LN series for "
            "U.S. household labor statistics"
        )

    elif geo_level == "state":
        # Query all state-level area codes from metadata
        # State area codes begin ST + two-digit state FIPS.
        return _get_area_codes_from_db(prefix="ST")

    elif geo_level == "county":
        # Query county-level area codes for a specific state
        # County area codes begin CN + state FIPS + county FIPS.
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


def get_laus_series_ids(
    measure_codes: List[str],
    geo_level: str,
    state_fips: Optional[str] = None,
    seasonal: str = "U",
) -> List[str]:
    """Return complete, published LAUS series IDs from the local BLS catalog."""
    if geo_level == "us":
        raise ValueError(
            "LAUS does not publish national series; use CPS/LN series for "
            "U.S. household labor statistics"
        )
    if geo_level == "state":
        area_prefix = "ST"
    elif geo_level == "county":
        if not state_fips:
            raise ValueError("state_fips required for county-level LAUS series")
        state_fips_padded = str(state_fips).zfill(2)
        if len(state_fips_padded) != 2 or not state_fips_padded.isdigit():
            raise ValueError(f"state_fips must be one or two digits, got: {state_fips}")
        area_prefix = f"CN{state_fips_padded}"
    else:
        raise ValueError(f"Unsupported LAUS geo_level for ingestion: {geo_level}")

    if seasonal not in ("S", "U"):
        raise ValueError(f"seasonal must be 'S' or 'U', got: {seasonal}")
    if not measure_codes:
        return []
    invalid_measures = [
        code for code in measure_codes if len(code) != 2 or not code.isdigit()
    ]
    if invalid_measures:
        raise ValueError(f"measure codes must be two digits, got: {invalid_measures}")

    conn = _get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT series_id
                FROM raw_bls.bls_series
                WHERE program = 'la'
                  AND area_code LIKE %s
                  AND seasonal = %s
                  AND measure = ANY(%s)
                ORDER BY series_id;
                """,
                (f"{area_prefix}%", seasonal, measure_codes),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    series_ids = []
    for (series_id,) in rows:
        parsed = parse_laus_series_id(series_id)
        if parsed["geo_level"] != geo_level:
            logger.warning("Ignoring invalid LAUS metadata series_id: %s", series_id)
            continue
        if geo_level == "county" and parsed["state_fips"] != state_fips_padded:
            continue
        series_ids.append(series_id)

    logger.info(
        "Found %s published LAUS %s series for measures %s",
        len(series_ids),
        geo_level,
        measure_codes,
    )
    return series_ids


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

            logger.info(
                f"Found {len(area_codes)} LAUS area codes with prefix '{prefix}'"
            )
            return area_codes

    finally:
        conn.close()


def parse_laus_series_id(series_id: str) -> dict:
    """
    Parse a LAUS series ID into its components.

    LAUS series ID format: LA{S|U}{15-character area_code}{measure_code}
    - Positions 0-1:  'LA' (program identifier)
    - Position 2:     'S' (seasonally adjusted) or 'U' (not seasonally adjusted)
    - Positions 3-17: 15-character area code
    - Positions 18-19: 2-digit measure code

    Only state and county area patterns are supported by the geographic
    dimension used by this pipeline. LAUS has no national series.

    Returns:
        Parsed fields. Invalid IDs return the same keys populated with ``None``.
    """
    empty = {
        "program": None,
        "seasonal": None,
        "area_code": None,
        "measure_code": None,
        "geo_level": None,
        "geo_id": None,
        "state_fips": None,
        "county_fips": None,
    }
    if not isinstance(series_id, str) or len(series_id) != 20:
        return empty

    program = series_id[0:2]
    seasonal = series_id[2]
    area_code = series_id[3:18]
    measure_code = series_id[18:20]
    if program != "LA" or seasonal not in ("S", "U") or not measure_code.isdigit():
        return empty

    state_match = re.fullmatch(r"ST(\d{2})0{11}", area_code)
    county_match = re.fullmatch(r"CN(\d{2})(\d{3})0{8}", area_code)
    if state_match:
        geo_level = "state"
        state_fips = state_match.group(1)
        county_fips = None
        geo_id = f"state:{state_fips}"
    elif county_match:
        geo_level = "county"
        state_fips, county_fips = county_match.groups()
        geo_id = f"state:{state_fips}|county:{county_fips}"
    else:
        return empty

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


def build_laus_series_id(area_code: str, measure_code: str, seasonal: str = "U") -> str:
    """
    Build a LAUS series ID from components.

    Args:
        area_code: 15-character LAUS state or county area code
        measure_code: 2-digit measure code (03, 04, 05, 06, 07, 08, 09)
        seasonal: 'S' (seasonally adjusted) or 'U' (not seasonally adjusted)

    Returns:
        Full 20-character LAUS series ID
    """
    if len(area_code) != 15:
        raise ValueError(f"area_code must be 15 characters, got: {area_code}")
    if not (
        re.fullmatch(r"ST\d{2}0{11}", area_code)
        or re.fullmatch(r"CN\d{5}0{8}", area_code)
    ):
        raise ValueError(f"unsupported LAUS area_code pattern: {area_code}")
    if len(measure_code) != 2 or not measure_code.isdigit():
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
