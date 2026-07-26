# data_ingestion_toolbox/fred/metadata.py

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import httpx
import psycopg2

from data_ingestion_toolbox.utility.db_connection import (
    PostgresConnectionFactory,
    PostgresConnectionDetails,
)
from .config import CONFIG

logger = logging.getLogger(__name__)

# Target database
_TARGET_DATABASE = "public_data"

# FRED API base URL
FRED_API_BASE = "https://api.stlouisfed.org/fred"


def _get_pg_conn_details() -> PostgresConnectionDetails:
    """
    Get Postgres connection details from either:
    - Airflow connection (if CONFIG.postgres_conn_id is set)
    - Environment variables POSTGRES_* (local dev)
    """
    return PostgresConnectionFactory.auto(
        conn_id=getattr(CONFIG, "postgres_conn_id", None),
        prefix="POSTGRES_",
        database=_TARGET_DATABASE,
    )


def _get_pg_connection():
    """
    Open a psycopg2 connection using the shared connection factory.
    """
    details = _get_pg_conn_details()
    return psycopg2.connect(**details.psycopg_kwargs())


def fetch_fred_series_metadata(series_id: str) -> Dict:
    """
    Fetch metadata for a single FRED series using the FRED API /series endpoint.
    
    FRED API documentation:
    https://fred.stlouisfed.org/docs/api/fred/series.html
    
    Returns:
        Dict with series metadata including title, units, frequency, etc.
    """
    if not CONFIG.has_api_key:
        raise ValueError("FRED_API_KEY required for FRED metadata fetching")
    
    url = f"{FRED_API_BASE}/series"
    params = {
        "series_id": series_id,
        "api_key": CONFIG.fred_api_key,
        "file_type": "json",
    }
    
    logger.info(f"Fetching FRED metadata for series: {series_id}")
    
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()
        
        data = resp.json()
        
        # FRED API wraps response in "seriess" array
        series_list = data.get("seriess", [])
        if not series_list:
            logger.warning(f"No metadata found for series: {series_id}")
            return {}
        
        # Return first series (should only be one for a specific series_id query)
        return series_list[0]


def sync_fred_series_metadata(series_ids: Optional[List[str]] = None) -> int:
    """
    Fetch and sync series metadata from FRED API to raw_fred.fred_series.
    
    Args:
        series_ids: List of FRED series IDs. If None, uses CONFIG.curated_series_ids.
    
    Returns:
        Number of series records synced.
    """
    if series_ids is None:
        series_ids = CONFIG.curated_series_ids
    
    if not series_ids:
        logger.info("No series IDs to sync")
        return 0
    
    logger.info(f"Syncing metadata for {len(series_ids)} FRED series")
    
    conn = _get_pg_connection()
    count = 0
    
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc)
            
            for series_id in series_ids:
                try:
                    metadata = fetch_fred_series_metadata(series_id)
                    
                    if not metadata:
                        logger.warning(f"Skipping {series_id}: no metadata returned")
                        continue
                    
                    # Extract fields from FRED response
                    title = metadata.get("title", "")
                    units = metadata.get("units", "")
                    frequency = metadata.get("frequency", "")
                    seasonal_adjustment = metadata.get("seasonal_adjustment", "")
                    
                    # Parse observation dates
                    obs_start_str = metadata.get("observation_start")
                    obs_end_str = metadata.get("observation_end")
                    
                    obs_start = datetime.fromisoformat(obs_start_str).date() if obs_start_str else None
                    obs_end = datetime.fromisoformat(obs_end_str).date() if obs_end_str else None
                    
                    notes = metadata.get("notes", "")
                    raw_metadata_json = json.dumps(metadata)
                    
                    sql = """
                        INSERT INTO raw_fred.fred_series (
                            series_id, title, units, frequency, seasonal_adjustment,
                            observation_start, observation_end, notes, raw_metadata,
                            first_seen_at, last_checked_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (series_id)
                        DO UPDATE SET
                            title = EXCLUDED.title,
                            units = EXCLUDED.units,
                            frequency = EXCLUDED.frequency,
                            seasonal_adjustment = EXCLUDED.seasonal_adjustment,
                            observation_start = EXCLUDED.observation_start,
                            observation_end = EXCLUDED.observation_end,
                            notes = EXCLUDED.notes,
                            raw_metadata = EXCLUDED.raw_metadata,
                            last_checked_at = EXCLUDED.last_checked_at;
                    """
                    
                    cur.execute(sql, (
                        series_id, title, units, frequency, seasonal_adjustment,
                        obs_start, obs_end, notes, raw_metadata_json,
                        now, now
                    ))
                    
                    count += 1
                    logger.info(f"Synced metadata for {series_id}: {title}")
                
                except Exception as e:
                    logger.error(f"Error syncing metadata for {series_id}: {e}")
                    # Continue to next series rather than failing entirely
                    continue
            
            conn.commit()
            logger.info(f"Synced {count} FRED series metadata records")
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error syncing FRED series metadata: {e}")
        raise
    finally:
        conn.close()
    
    return count


def sync_fred_datasets_table() -> int:
    """
    Populate raw_fred.fred_datasets with available domain/series combinations.
    
    FRED doesn't have "datasets" in the Census sense. Instead, we track
    which series belong to which logical domains (labor_cycle, housing, etc.)
    
    This function creates records in fred_datasets to track sync status.
    """
    conn = _get_pg_connection()
    count = 0
    
    try:
        with conn.cursor() as cur:
            now = datetime.now(timezone.utc)
            
            # Group series by domain
            for domain, series_list in CONFIG.curated_by_domain.items():
                for series_id in series_list:
                    sql = """
                        INSERT INTO raw_fred.fred_datasets (
                            domain, series_id, is_available,
                            first_seen_at, last_checked_at
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (domain, series_id)
                        DO UPDATE SET
                            is_available = EXCLUDED.is_available,
                            last_checked_at = EXCLUDED.last_checked_at;
                    """
                    
                    cur.execute(sql, (domain, series_id, True, now, now))
                    count += 1
            
            conn.commit()
            logger.info(f"Synced {count} records to raw_fred.fred_datasets")
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error syncing fred_datasets: {e}")
        raise
    finally:
        conn.close()
    
    return count


def get_series_observation_range(series_id: str) -> tuple[Optional[str], Optional[str]]:
    """
    Get the observation date range for a FRED series.
    
    Returns:
        Tuple of (observation_start, observation_end) as ISO date strings, or (None, None) if unavailable.
    """
    try:
        metadata = fetch_fred_series_metadata(series_id)
        obs_start = metadata.get("observation_start")
        obs_end = metadata.get("observation_end")
        return obs_start, obs_end
    except Exception as e:
        logger.error(f"Error fetching observation range for {series_id}: {e}")
        return None, None


# CLI interface for testing
if __name__ == "__main__":
    import sys
    
    print("=== FRED Metadata Sync ===\n")
    
    # Sync datasets table first
    print("Syncing fred_datasets table...")
    sync_fred_datasets_table()
    
    # Then sync series metadata
    print("\nSyncing series metadata...")
    count = sync_fred_series_metadata()
    
    print(f"\n=== FRED Metadata Sync Complete ===")
    print(f"Total series synced: {count}")
