# data_ingestion_toolbox/fred/ingest.py

from __future__ import annotations

import io
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import List, Optional, Dict

import httpx
import polars as pl
import psycopg2
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from data_ingestion_toolbox.utility.db_connection import (
    PostgresConnectionDetails,
    PostgresConnectionFactory,
)
from data_ingestion_toolbox.utility.retry import retry_database_transaction
from data_ingestion_toolbox.normalization import NumericParseError, parse_decimal
from .config import CONFIG

logger = logging.getLogger(__name__)

# Target database
_TARGET_DATABASE = "public_data"

# FRED API base URL
FRED_API_BASE = "https://api.stlouisfed.org/fred"


# Exception classes for retry logic
class FredNoContent(Exception):
    """FRED API returned no data (not an error, just empty)."""

    pass


class FredRetryableHTTP(Exception):
    """Retry-worthy HTTP cases (429 / 5xx)."""

    pass


class FredPayloadError(ValueError):
    """The FRED payload shape cannot be normalized safely."""


def _get_pg_conn_details() -> PostgresConnectionDetails:
    """
    Get PostgresConnectionDetails from Airflow when running in Airflow,
    otherwise fall back to local env vars.
    """
    return PostgresConnectionFactory.auto(
        conn_id=getattr(CONFIG, "postgres_conn_id", None),
        prefix="POSTGRES_",
        database=_TARGET_DATABASE,
    )


def _get_pg_connection():
    """
    Open a psycopg2 connection using the factory's connection details.
    """
    details = _get_pg_conn_details()
    return psycopg2.connect(**details.psycopg_kwargs())


def get_curated_series_for_domain(domain: Optional[str] = None) -> List[str]:
    """
    Get the curated series list for a given domain from CONFIG.

    If domain is None, returns all curated series.
    """
    if domain is None:
        return CONFIG.curated_series_ids

    return CONFIG.curated_by_domain.get(domain, [])


def chunked(items: List[str], chunk_size: int) -> List[List[str]]:
    """Split list into chunks of specified size."""
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


@retry(
    reraise=True,
    stop=stop_after_attempt(8),
    wait=wait_exponential(multiplier=2, min=5, max=900),
    retry=retry_if_exception_type(
        (FredRetryableHTTP, httpx.TimeoutException, httpx.NetworkError)
    ),
)
def fetch_fred_observations(
    series_id: str,
    observation_start: str,
    observation_end: str,
    realtime_start: Optional[str] = None,
    realtime_end: Optional[str] = None,
) -> Dict:
    """
    Call the FRED API /series/observations endpoint and return the raw JSON response.

    FRED API documentation:
    https://fred.stlouisfed.org/docs/api/fred/series_observations.html

    Args:
        series_id: FRED series ID
        observation_start: Start date (YYYY-MM-DD)
        observation_end: End date (YYYY-MM-DD)
        realtime_start: Optional realtime start (default: today)
        realtime_end: Optional realtime end (default: today)

    Returns:
        Dict with structure: {"observations": [...]}
    """
    if not CONFIG.has_api_key:
        raise ValueError("FRED_API_KEY required for FRED ingestion")

    url = f"{FRED_API_BASE}/series/observations"

    params = {
        "series_id": series_id,
        "api_key": CONFIG.fred_api_key,
        "file_type": "json",
        "observation_start": observation_start,
        "observation_end": observation_end,
    }

    if realtime_start:
        params["realtime_start"] = realtime_start
    if realtime_end:
        params["realtime_end"] = realtime_end

    # Add jitter to avoid rhythmic bursts
    time.sleep(CONFIG.fred_api_min_spacing_seconds + random.random() * 0.3)

    logger.info(
        f"FRED API request: {series_id}, {observation_start} to {observation_end}"
    )

    with httpx.Client(timeout=httpx.Timeout(60.0)) as client:
        resp = client.get(url, params=params)

        # Handle rate limiting
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "300")
            try:
                delay = int(retry_after)
            except ValueError:
                delay = 300

            logger.warning(f"FRED 429 rate limit, sleeping {delay}s")
            time.sleep(delay + random.random() * 10)
            raise FredRetryableHTTP(f"429 rate limited: {url}")

        # Handle server errors
        if 500 <= resp.status_code <= 599:
            logger.warning(f"FRED {resp.status_code} server error, retrying")
            raise FredRetryableHTTP(f"{resp.status_code} server error: {url}")

        # Other errors are not retryable
        resp.raise_for_status()

        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise FredRetryableHTTP("FRED returned invalid JSON") from exc

        # Check if we got observations
        observations = data.get("observations", [])
        if not observations:
            logger.info(f"No observations returned for {series_id}")
            raise FredNoContent(f"No observations for {series_id}")

        return data


def parse_fred_response(
    response_data: Dict,
    series_id: str,
    domain: Optional[str],
    load_batch_id: uuid.UUID,
) -> pl.DataFrame:
    """
    Parse FRED API /series/observations response into a Polars DataFrame.

    FRED API response structure:
    {
        "realtime_start": "2024-01-31",
        "realtime_end": "2024-01-31",
        "observations": [
            {
                "realtime_start": "2024-01-31",
                "realtime_end": "2024-01-31",
                "date": "2020-01-01",
                "value": "152504.0"
            },
            ...
        ]
    }

    FRED uses "." to indicate missing values.
    """
    if not isinstance(response_data, dict):
        raise FredPayloadError("FRED response must be an object")

    records = []

    observations = response_data.get("observations", [])
    if not isinstance(observations, list):
        raise FredPayloadError("FRED observations must be a list")
    if not observations:
        raise FredNoContent(f"No observations for {series_id}")

    for obs in observations:
        if not isinstance(obs, dict):
            raise FredPayloadError("FRED observation must be an object")
        obs_date_str = obs.get("date")
        value_str = obs.get("value", "")
        realtime_start_str = obs.get("realtime_start")
        realtime_end_str = obs.get("realtime_end")

        # Parse date
        try:
            obs_date = (
                datetime.fromisoformat(obs_date_str).date() if obs_date_str else None
            )
        except (ValueError, TypeError):
            raise FredPayloadError(
                f"FRED observation has invalid date for series {series_id}"
            )
        if obs_date is None:
            raise FredPayloadError(
                f"FRED observation is missing date for series {series_id}"
            )

        # Parse realtime dates
        try:
            realtime_start = (
                datetime.fromisoformat(realtime_start_str).date()
                if realtime_start_str
                else None
            )
            realtime_end = (
                datetime.fromisoformat(realtime_end_str).date()
                if realtime_end_str
                else None
            )
        except (ValueError, TypeError):
            realtime_start = None
            realtime_end = None

        # Parse value (FRED uses "." for missing data)
        is_missing = False
        value = None

        try:
            parsed_value = parse_decimal(value_str)
        except NumericParseError:
            parsed_value = None
        is_missing = parsed_value is None
        value = float(parsed_value) if parsed_value is not None else None

        records.append(
            {
                "domain": domain,
                "series_id": series_id,
                "obs_date": obs_date,
                "value": value,
                "is_missing": is_missing,
                "realtime_start": realtime_start,
                "realtime_end": realtime_end,
                "load_batch_id": str(load_batch_id),
                "ingested_at": datetime.now(timezone.utc),
            }
        )

    if not records:
        raise FredNoContent(f"No observations for {series_id}")

    df = pl.DataFrame(records)
    return df


@retry_database_transaction
def load_df_to_fred_long(df: pl.DataFrame) -> int:
    """
    Bulk load a Polars DataFrame into raw_fred.fred_long using COPY.

    We delete existing rows for (series_id, obs_date, realtime_start, realtime_end)
    combinations present in this batch (idempotent upsert).
    """
    if df.is_empty():
        return 0
    if df.height > CONFIG.raw_load_max_rows:
        raise ValueError(
            f"FRED raw batch has {df.height} rows; configured maximum is "
            f"{CONFIG.raw_load_max_rows}"
        )

    conn = _get_pg_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        # Overlapping mapped tasks may replace the same natural key. Serialize
        # only writers for the same series so delete+COPY remains atomic while
        # unrelated series can still load concurrently.
        for series_id in sorted(df.get_column("series_id").unique().to_list()):
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"raw_fred.fred_long:{series_id}",),
            )

        # Get unique (series_id, obs_date, realtime_start, realtime_end) tuples for deletion
        delete_keys = df.select(
            ["series_id", "obs_date", "realtime_start", "realtime_end"]
        ).unique()

        for row in delete_keys.iter_rows():
            series_id, obs_date, realtime_start, realtime_end = row
            cur.execute(
                """
                DELETE FROM raw_fred.fred_long
                WHERE series_id = %s
                  AND obs_date = %s
                  AND realtime_start IS NOT DISTINCT FROM %s
                  AND realtime_end IS NOT DISTINCT FROM %s;
                """,
                (series_id, obs_date, realtime_start, realtime_end),
            )

        # Prepare CSV in-memory
        output = io.StringIO()
        df.select(
            [
                "domain",
                "series_id",
                "obs_date",
                "value",
                "is_missing",
                "realtime_start",
                "realtime_end",
                "load_batch_id",
                "ingested_at",
            ]
        ).write_csv(output, include_header=False)
        output.seek(0)

        # Copy into Postgres
        cur.copy_expert(
            """
            COPY raw_fred.fred_long (
                domain, series_id, obs_date, value, is_missing,
                realtime_start, realtime_end,
                load_batch_id, ingested_at
            )
            FROM STDIN WITH (FORMAT csv);
            """,
            output,
        )

        rowcount = cur.rowcount
        conn.commit()

        logger.info(f"Loaded {rowcount} rows to raw_fred.fred_long")
        return rowcount

    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading to fred_long: {e}")
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def ingest_slice(
    domain: str,
    series_ids: Optional[List[str]] = None,
    date_start: str = "2000-01-01",
    date_end: Optional[str] = None,
    realtime_start: Optional[str] = None,
    realtime_end: Optional[str] = None,
) -> int:
    """
    Ingest one slice of FRED data.

    Args:
        domain: Logical domain label (e.g., 'housing', 'labor_cycle', 'macro')
        series_ids: List of FRED series IDs. If None, uses curated series for domain.
        date_start: Start date for observations (YYYY-MM-DD)
        date_end: End date for observations (YYYY-MM-DD). If None, uses today.
        realtime_start: Optional realtime start (YYYY-MM-DD)
        realtime_end: Optional realtime end (YYYY-MM-DD)

    Returns:
        Number of rows inserted
    """
    # Default date_end to today if not provided
    if date_end is None:
        date_end = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Get series IDs
    if series_ids is None:
        series_ids = get_curated_series_for_domain(domain)

    if not series_ids:
        logger.info(f"No series IDs to ingest for domain {domain}")
        return 0

    logger.info(
        f"Ingesting FRED slice: domain={domain}, series_count={len(series_ids)}, "
        f"dates={date_start} to {date_end}"
    )

    batch_id = uuid.uuid4()
    frames: List[pl.DataFrame] = []

    # Process each series individually (FRED API is per-series)
    for series_id in series_ids:
        try:
            response_data = fetch_fred_observations(
                series_id=series_id,
                observation_start=date_start,
                observation_end=date_end,
                realtime_start=realtime_start,
                realtime_end=realtime_end,
            )

            df = parse_fred_response(
                response_data=response_data,
                series_id=series_id,
                domain=domain,
                load_batch_id=batch_id,
            )

            if not df.is_empty():
                frames.append(df)

        except FredNoContent:
            logger.info(f"No content for series {series_id}")
            continue

        # Rate limiting between series
        time.sleep(CONFIG.fred_api_min_spacing_seconds + random.random() * 0.2)

    if not frames:
        logger.info(f"No data retrieved for domain {domain}")
        return 0

    combined = pl.concat(frames, how="vertical_relaxed")
    return load_df_to_fred_long(combined)
