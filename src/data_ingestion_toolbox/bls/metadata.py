from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import psycopg2
import polars as pl
import requests
from io import BytesIO
from data_ingestion_toolbox.utility.db_connection import (
    PostgresConnectionFactory,
    PostgresConnectionDetails,
)
from .config import CONFIG


# CONNECTION DETAILS #

# Which database inside the Postgres instance do you want to use?
# Change this if your metadata lives somewhere else.
# Overridable so self-contained stacks can point at their own warehouse
# database; production deployments default to the shared "public_data".
_TARGET_DATABASE = os.environ.get("PUBLIC_DATA_DB_NAME", "public_data")

# When running inside Airflow, you can let CONFIG.postgres_conn_id drive the
# connection. In local dev (no Airflow), this will be None and the factory
# will fall back to POSTGRES_* env vars.
_AIRFLOW_CONN_ID: Optional[str] = getattr(CONFIG, "postgres_conn_id", None)


def _get_pg_conn_details() -> PostgresConnectionDetails:
    """
    Get Postgres connection details from either:

    - Airflow connection (if _AIRFLOW_CONN_ID is set and Airflow is installed)
    - Environment variables POSTGRES_HOST, POSTGRES_PORT, etc. (local dev)
    """
    return PostgresConnectionFactory.auto(
        conn_id=_AIRFLOW_CONN_ID,
        prefix="POSTGRES_",
        database=_TARGET_DATABASE,
    )


def _get_pg_connection():
    """
    Open a psycopg2 connection using the shared connection factory.
    """
    details = _get_pg_conn_details()
    return psycopg2.connect(**details.psycopg_kwargs())


# BASE_URL for BLS metadata downloads
BASE_URL = "https://download.bls.gov/pub/time.series/"

# List of programs from config
programs = CONFIG.programs

# HELPER, download.bls.gov will occasionally answer 403 forbidden to bot-looking clients despite the url working well in a browser.
UA = "population_toolbox/1.0 (contact: your_email@example.com)"


def _safe_str(value) -> str:
    """Safely convert a value to string and strip whitespace."""
    if value is None:
        return ""
    return str(value).strip()


def read_bls_tsv(url: str) -> pl.DataFrame:
    r = requests.get(
        url,
        headers={"User-Agent": UA, "Accept": "text/plain,*/*"},
        timeout=120,
    )
    r.raise_for_status()
    # Read all columns as strings to avoid type inference issues
    return pl.read_csv(
        BytesIO(r.content),
        separator="\t",
        has_header=True,
        infer_schema=False,  # Published identifiers must remain exact strings.
    )


def fetch_bls_metadata(program: str) -> Tuple[Dict[str, any], Dict[str, any]]:
    """
    Fetch metadata for a given BLS program.

    Returns:
        Tuple of (series_data, dataset_data)
    """
    # Build URLs for the program
    series_url = f"{BASE_URL}{program}/{program}.series"
    area_type_url = (
        f"{BASE_URL}{program}/{program}.area_type" if program == "la" else None
    )
    area_url = f"{BASE_URL}{program}/{program}.area" if program == "la" else None

    # LOGGING
    print(series_url)
    print(area_type_url)
    print(area_url)

    series_df = read_bls_tsv(series_url)
    series_data = process_series_data(series_df, program)

    # For LAUS, also get area and area_type data.
    dataset_data = {}
    if program == "la":
        area_type_df = read_bls_tsv(area_type_url)
        area_type_data = process_area_type_data(area_type_df)
        area_df = read_bls_tsv(area_url)
        area_data = process_area_data(area_df)
        dataset_data = {"area_types": area_type_data, "areas": area_data}

    return series_data, dataset_data


def process_series_data(df: pl.DataFrame, program: str) -> List[Dict]:
    """
    Process the series data from BLS metadata TSV files.

    BLS metadata files have headers, so we don't need column_0/column_1 aliasing.
    This function normalizes the schema across different programs.
    """
    # Normalize column names (strip whitespace)
    df = df.rename({col: col.strip() for col in df.columns})

    # Convert to list of dicts for easier processing
    records = df.to_dicts()

    # Return the records with normalized structure
    return records


def process_area_type_data(df: pl.DataFrame) -> List[Dict]:
    """
    Process LAUS area type data.
    """
    # Normalize column names
    df = df.rename({col: col.strip() for col in df.columns})
    return df.to_dicts()


def process_area_data(df: pl.DataFrame) -> List[Dict]:
    """
    Process LAUS area data (US, states, counties, metros, cities).
    """
    # Normalize column names
    df = df.rename({col: col.strip() for col in df.columns})
    return df.to_dicts()


def sync_bls_series_metadata(program: str) -> int:
    """
    Fetch and sync series metadata from BLS download.bls.gov to raw_bls.bls_series.

    Returns the number of series records synced.

    This populates the series catalog that geography.py and ingest.py will reference
    when building LAUS series IDs or looking up series attributes.
    """
    print(f"Syncing series metadata for program: {program}")

    series_data, dataset_data = fetch_bls_metadata(program)

    if not series_data:
        raise RuntimeError(
            f"BLS returned no series metadata for configured program {program!r}"
        )

    conn = _get_pg_connection()
    count = 0

    try:
        with conn.cursor() as cur:
            for record in series_data:
                series_id = _safe_str(record.get("series_id", ""))
                if not series_id:
                    continue

                title = _safe_str(record.get("series_title") or record.get("title", ""))
                seasonal = (
                    _safe_str(record.get("seasonal", "")) if program == "la" else None
                )
                measure = _safe_str(
                    record.get("measure_code") or record.get("data_type_code", "")
                )
                area_code = (
                    _safe_str(record.get("area_code", "")) if program == "la" else None
                )
                area_text = (
                    _safe_str(record.get("area_text", "")) if program == "la" else None
                )

                # Store full record as JSONB
                raw_metadata = json.dumps(record)

                sql = """
                    INSERT INTO raw_bls.bls_series (
                        program, series_id, title, seasonal, measure,
                        area_code, area_text, raw_metadata,
                        first_seen_at, last_checked_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (program, series_id)
                    DO UPDATE SET
                        title = EXCLUDED.title,
                        seasonal = EXCLUDED.seasonal,
                        measure = EXCLUDED.measure,
                        area_code = EXCLUDED.area_code,
                        area_text = EXCLUDED.area_text,
                        raw_metadata = EXCLUDED.raw_metadata,
                        last_checked_at = EXCLUDED.last_checked_at;
                """

                now = datetime.now(timezone.utc)
                cur.execute(
                    sql,
                    (
                        program,
                        series_id,
                        title,
                        seasonal,
                        measure,
                        area_code,
                        area_text,
                        raw_metadata,
                        now,
                        now,
                    ),
                )
                count += 1

            if count <= 0:
                raise RuntimeError(
                    f"BLS metadata for {program!r} contained no usable series IDs"
                )

            conn.commit()
            print(f"Synced {count} series for program {program}")

    except Exception as e:
        conn.rollback()
        print(f"Error syncing series metadata for {program}: {e}")
        raise
    finally:
        conn.close()

    # Also sync area metadata if LAUS
    if program == "la" and dataset_data:
        _sync_laus_area_metadata(dataset_data)

    return count


def _sync_laus_area_metadata(dataset_data: Dict):
    """
    Internal helper: sync LAUS area and area_type metadata.

    This creates lookup tables for geography.py to use when generating
    series IDs for different geographic levels.
    """
    conn = _get_pg_connection()

    try:
        # Area rows are already represented by the synchronized series metadata.
        # Keep this diagnostic count until dedicated lookup tables are introduced.
        area_count = len(dataset_data.get("areas", []))
        print(f"Found {area_count} LAUS areas in metadata")

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error syncing LAUS area metadata: {e}")
    finally:
        conn.close()


def sync_bls_datasets_table() -> int:
    """
    Populate raw_bls.bls_datasets with available program/year combinations.

    BLS doesn't version datasets by year the same way Census does.
    Instead, we track which programs are available and their time ranges.

    This function creates records in bls_datasets to track metadata sync status.
    """
    conn = _get_pg_connection()
    count = 0

    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc)

            for program in CONFIG.programs:
                # For each program, create a record indicating availability
                # Use a nominal "year" (e.g., current year) to match schema
                year = datetime.now(timezone.utc).year
                title = f"BLS {program.upper()} Program"

                sql = """
                    INSERT INTO raw_bls.bls_datasets (
                        program, year, title, is_available,
                        first_seen_at, last_checked_at
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (program, year)
                    DO UPDATE SET
                        title = EXCLUDED.title,
                        is_available = EXCLUDED.is_available,
                        last_checked_at = EXCLUDED.last_checked_at;
                """

                cur.execute(sql, (program, year, title, True, now, now))
                count += 1

            conn.commit()
            print(f"Synced {count} dataset records to raw_bls.bls_datasets")

    except Exception as e:
        conn.rollback()
        print(f"Error syncing bls_datasets: {e}")
        raise
    finally:
        conn.close()

    return count


# Example usage / CLI interface
if __name__ == "__main__":
    print("=== BLS Metadata Sync ===\n")

    # Sync datasets table first
    sync_bls_datasets_table()

    # Then sync series metadata for each program
    for prog in CONFIG.programs:
        try:
            sync_bls_series_metadata(prog)
        except Exception as e:
            print(f"Failed to sync {prog}: {e}")
            continue

    print("\n=== BLS Metadata Sync Complete ===")
