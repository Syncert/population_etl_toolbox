# data_ingestion_toolbox/bls/ingest.py

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
from data_ingestion_toolbox.normalization import NumericParseError, parse_decimal
from .config import CONFIG

logger = logging.getLogger(__name__)

# Target database
_TARGET_DATABASE = "public_data"


# Exception classes for retry logic
class BlsNoContent(Exception):
    """BLS API returned no data (not an error, just empty)."""

    pass


class BlsRetryableHTTP(Exception):
    """Retry-worthy HTTP cases (429 / 5xx)."""

    pass


class BlsDailyThresholdExceeded(Exception):
    """
    Raised when the BLS daily API request quota is exhausted
    (status=REQUEST_NOT_PROCESSED with a 'daily threshold' message).

    Intentionally does NOT inherit from BlsRetryableHTTP so tenacity's
    retry_if_exception_type guard passes it through immediately — no
    same-day retries are attempted.  Instead, Airflow handles the
    24-hour reschedule via the ingest_batch task's retries / retry_delay.
    """

    pass


class BlsPayloadError(ValueError):
    """The BLS payload shape cannot be normalized safely."""


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


def get_curated_series_for_program(program: str) -> List[str]:
    """
    Get the curated series list for a given program from CONFIG.

    For LAUS (program='la'), these are MEASURE CODES that need expansion.
    For CES/CPI/JOLTS, these are full series IDs.
    """
    return CONFIG.curated_by_program.get(program, [])


def expand_laus_series_ids(
    measure_codes: List[str],
    geo_level: str,
    state_fips: Optional[str] = None,
    seasonal: str = "U",  # U = not seasonally adjusted (typical for counties)
) -> List[str]:
    """
    Expand LAUS measure codes into full series IDs.

    LAUS series ID format: LA{seasonal}{area_code}{measure_code}

    - seasonal: 'S' (seasonally adjusted) or 'U' (not seasonally adjusted)
    - area_code: a published 15-character LAUS subnational area code
    - measure_code: 03, 04, 05, 06, 07, 08, 09

    This function returns only complete series IDs published in
    raw_bls.bls_series. It does not construct an area/measure cross product.
    """
    from .geography import get_laus_series_ids

    return get_laus_series_ids(
        measure_codes=measure_codes,
        geo_level=geo_level,
        state_fips=state_fips,
        seasonal=seasonal,
    )


def chunked(items: List[str], chunk_size: int) -> List[List[str]]:
    """Split list into chunks of specified size."""
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


@retry(
    reraise=True,
    stop=stop_after_attempt(8),
    wait=wait_exponential(multiplier=2, min=5, max=900),
    retry=retry_if_exception_type(
        (BlsRetryableHTTP, httpx.TimeoutException, httpx.NetworkError)
    ),
)
def fetch_bls_api(
    series_ids: List[str], start_year: int, end_year: int, api_version: str = "v2"
) -> Dict:
    """
    Call the BLS API v2 and return the raw JSON response.

    BLS API v2 limits:
    - 50 series per request (with API key)
    - 20 years of data per request
    - Rate limits apply (we handle with sleep + retry)

    Returns:
        Dict with structure: {"status": "REQUEST_SUCCEEDED", "Results": {"series": [...]}}
    """
    if not CONFIG.has_api_key:
        raise ValueError("BLS_API_KEY required for BLS ingestion")

    # BLS API endpoint
    url = f"https://api.bls.gov/publicAPI/{api_version}/timeseries/data/"

    # Build request payload
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": CONFIG.bls_api_key,
    }

    # Add jitter to avoid rhythmic bursts
    time.sleep(CONFIG.bls_api_min_spacing_seconds + random.random() * 0.3)

    # Verbose logging (sanitize API key)
    safe_payload = {
        "seriesid_count": len(series_ids),
        "seriesid_sample": series_ids[:5],
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": "***",
    }
    logger.info(
        "BLS API request: url=%s payload=%s",
        url,
        safe_payload,
    )

    with httpx.Client(timeout=httpx.Timeout(60.0)) as client:
        resp = client.post(url, json=payload)

        # Handle rate limiting
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "300")
            try:
                delay = int(retry_after)
            except ValueError:
                delay = 300

            logger.warning(f"BLS 429 rate limit, sleeping {delay}s")
            time.sleep(delay + random.random() * 10)
            raise BlsRetryableHTTP(f"429 rate limited: {url}")

        # Handle server errors
        if 500 <= resp.status_code <= 599:
            logger.warning(f"BLS {resp.status_code} server error, retrying")
            raise BlsRetryableHTTP(f"{resp.status_code} server error: {url}")

        # Other errors are not retryable
        resp.raise_for_status()

        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise BlsRetryableHTTP("BLS returned invalid JSON") from exc

        # Check BLS API response status
        status = data.get("status")
        if status != "REQUEST_SUCCEEDED":
            message = data.get("message", ["Unknown error"])
            logger.warning(f"BLS API status: {status}, message: {message}")

            # Empty results are OK (not an error)
            if "no data available" in str(message).lower():
                raise BlsNoContent("No data available for series")

            # Daily API quota exhausted — Airflow will retry in 24h; tenacity skips
            if (
                status == "REQUEST_NOT_PROCESSED"
                or "daily threshold" in str(message).lower()
            ):
                raise BlsDailyThresholdExceeded(
                    f"BLS daily quota exceeded: {status} - {message}"
                )

            # All other non-success statuses are transient (treat as retryable by tenacity)
            raise BlsRetryableHTTP(f"BLS API error: {status} - {message}")

        return data


def parse_bls_response(
    response_data: Dict,
    program: str,
    load_batch_id: uuid.UUID,
) -> pl.DataFrame:
    """
    Parse BLS API v2 response into a Polars DataFrame.

    BLS API response structure:
    {
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [
                {
                    "seriesID": "LAS00000003",
                    "data": [
                        {
                            "year": "2022",
                            "period": "M12",
                            "periodName": "December",
                            "value": "3.5",
                            "footnotes": [...]
                        },
                        ...
                    ]
                },
                ...
            ]
        }
    }
    """
    if not isinstance(response_data, dict):
        raise BlsPayloadError("BLS response must be an object")
    status = response_data.get("status")
    if status not in {None, "REQUEST_SUCCEEDED"}:
        raise BlsPayloadError(f"BLS response status was {status!r}")

    records = []

    results = response_data.get("Results", {})
    if not isinstance(results, dict):
        raise BlsPayloadError("BLS Results must be an object")
    series_list = results.get("series", [])
    if not isinstance(series_list, list):
        raise BlsPayloadError("BLS Results.series must be a list")
    if not series_list:
        raise BlsNoContent("BLS response contained no series")

    for series_obj in series_list:
        if not isinstance(series_obj, dict):
            raise BlsPayloadError("BLS series entry must be an object")
        series_id = series_obj.get("seriesID", "")
        data_points = series_obj.get("data", [])
        if not series_id or not isinstance(data_points, list):
            raise BlsPayloadError("BLS series is missing seriesID or data")

        for dp in data_points:
            if not isinstance(dp, dict):
                raise BlsPayloadError("BLS observation must be an object")
            year = dp.get("year")
            period = dp.get("period", "")
            period_name = dp.get("periodName", "")
            value_str = dp.get("value", "")
            footnotes = dp.get("footnotes", [])
            latest_raw = dp.get("latest", False)
            if isinstance(latest_raw, str):
                latest = latest_raw.strip().lower() == "true"
            else:
                latest = bool(latest_raw)

            # Parse value (BLS uses "-" for missing)
            try:
                parsed_value = parse_decimal(value_str)
            except NumericParseError:
                parsed_value = None
            value = float(parsed_value) if parsed_value is not None else None

            records.append(
                {
                    "program": program,
                    "series_id": series_id,
                    "year": int(year) if str(year).isdigit() else None,
                    "period": period,
                    "period_name": period_name,
                    "value": value,
                    "footnotes": json.dumps(footnotes) if footnotes else None,
                    "is_latest": latest,
                    "load_batch_id": str(load_batch_id),
                    "ingested_at": datetime.now(timezone.utc),
                }
            )

    if not records:
        raise BlsNoContent("BLS response contained no observations")

    df = pl.DataFrame(records)
    return df


def enrich_with_geography(df: pl.DataFrame, program: str) -> pl.DataFrame:
    """
    Optionally parse series IDs to extract geography information.

    For LAUS series IDs (LA{S|U}{area_code}{measure}):
    - Parse area_code to determine geo_level and FIPS codes

    For other programs, geography is typically not embedded in series IDs.
    """
    if df.is_empty():
        return df

    if program == "la":
        from .geography import parse_laus_series_id

        def parse_geography(series_id: str) -> tuple:
            parsed = parse_laus_series_id(series_id)
            return (
                parsed["geo_level"],
                parsed["geo_id"],
                parsed["state_fips"],
                parsed["county_fips"],
            )

        # Apply parsing
        parsed = (
            df.select("series_id")
            .to_series()
            .map_elements(parse_geography, return_dtype=pl.Object)
        )

        geo_info = pl.DataFrame(
            {
                "geo_level": [x[0] if x else None for x in parsed],
                "geo_id": [x[1] if x else None for x in parsed],
                "state_fips": [x[2] if x else None for x in parsed],
                "county_fips": [x[3] if x else None for x in parsed],
            }
        )

        df = pl.concat([df, geo_info], how="horizontal")
    else:
        # For non-LAUS programs, add null geography columns
        df = df.with_columns(
            [
                pl.lit(None, dtype=pl.Utf8).alias("geo_level"),
                pl.lit(None, dtype=pl.Utf8).alias("geo_id"),
                pl.lit(None, dtype=pl.Utf8).alias("state_fips"),
                pl.lit(None, dtype=pl.Utf8).alias("county_fips"),
            ]
        )

    return df


def load_df_to_bls_long(df: pl.DataFrame, program: str) -> int:
    """
    Bulk load a Polars DataFrame into raw_bls.bls_long using COPY.

    We delete existing rows for (program, series_id, year, period) combinations
    present in this batch (idempotent upsert).
    """
    if df.is_empty():
        return 0

    conn = _get_pg_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        # Get unique (series_id, year, period) tuples for deletion
        delete_keys = df.select(["series_id", "year", "period"]).unique()

        for row in delete_keys.iter_rows():
            series_id, year, period = row
            cur.execute(
                """
                DELETE FROM raw_bls.bls_long
                WHERE program = %s
                  AND series_id = %s
                  AND year = %s
                  AND period = %s;
                """,
                (program, series_id, year, period),
            )

        # Prepare CSV in-memory
        output = io.StringIO()
        df.select(
            [
                "program",
                "series_id",
                "year",
                "period",
                "period_name",
                "value",
                "footnotes",
                "is_latest",
                "geo_level",
                "geo_id",
                "state_fips",
                "county_fips",
                "load_batch_id",
                "ingested_at",
            ]
        ).write_csv(output, include_header=False)
        output.seek(0)

        # Copy into Postgres
        cur.copy_expert(
            """
            COPY raw_bls.bls_long (
                program, series_id, year, period, period_name,
                value, footnotes, is_latest,
                geo_level, geo_id, state_fips, county_fips,
                load_batch_id, ingested_at
            )
            FROM STDIN WITH (FORMAT csv);
            """,
            output,
        )

        rowcount = cur.rowcount
        conn.commit()

        logger.info(f"Loaded {rowcount} rows to raw_bls.bls_long for program {program}")
        return rowcount

    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading to bls_long: {e}")
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def ingest_slice(
    program: str,
    start_year: int,
    end_year: int,
    geo_level: Optional[str] = None,
    state_fips: Optional[str] = None,
) -> int:
    """
    Ingest one slice of BLS data.

    For LAUS (program='la'):
        - Requires a subnational geo_level ('state', 'county')
        - For 'county', requires state_fips
        - Expands measure codes to full series IDs

    For other programs (CES, CPI, JOLTS):
        - Uses full series IDs from config
        - geo_level and state_fips are ignored

    Args:
        program: BLS program code ('la', 'ce', 'cu', 'jt')
        start_year: Start year for data request
        end_year: End year for data request
        geo_level: Geographic level (for LAUS only)
        state_fips: State FIPS code (for LAUS county-level only)

    Returns:
        Number of rows inserted
    """
    logger.info(
        f"Ingesting BLS slice: program={program}, years={start_year}-{end_year}, "
        f"geo_level={geo_level}, state_fips={state_fips}"
    )

    # Get series IDs
    if program == "la":
        # LAUS requires expansion
        if not geo_level:
            raise ValueError("geo_level required for LAUS ingestion")

        measure_codes = get_curated_series_for_program(program)
        if not measure_codes:
            logger.info(f"No curated measure codes for program {program}")
            return 0

        series_ids = expand_laus_series_ids(
            measure_codes=measure_codes,
            geo_level=geo_level,
            state_fips=state_fips,
            seasonal="U",  # Counties are typically not seasonally adjusted
        )
    else:
        # Other programs use full series IDs
        series_ids = get_curated_series_for_program(program)
        if not series_ids:
            logger.info(f"No curated series for program {program}")
            return 0

    if not series_ids:
        logger.info(f"No series IDs generated for program {program}")
        return 0

    logger.info(f"Processing {len(series_ids)} series IDs for {program}")

    batch_id = uuid.uuid4()
    frames: List[pl.DataFrame] = []

    # Chunk by series (50 per request) and years (20 years per request)
    series_chunks = chunked(series_ids, CONFIG.bls_api_series_chunk_size)

    for series_chunk in series_chunks:
        # Also chunk by years if needed
        year_ranges = []
        current_start = start_year
        while current_start <= end_year:
            current_end = min(
                current_start + CONFIG.bls_api_year_chunk_size - 1, end_year
            )
            year_ranges.append((current_start, current_end))
            current_start = current_end + 1

        for yr_start, yr_end in year_ranges:
            try:
                response_data = fetch_bls_api(
                    series_ids=series_chunk,
                    start_year=yr_start,
                    end_year=yr_end,
                )

                df = parse_bls_response(
                    response_data=response_data,
                    program=program,
                    load_batch_id=batch_id,
                )

                if not df.is_empty():
                    # Enrich with geography parsing
                    df = enrich_with_geography(df, program)
                    frames.append(df)

            except BlsNoContent:
                logger.info(
                    f"No content for {program}, series chunk, years {yr_start}-{yr_end}"
                )
                continue

            # Rate limiting
            time.sleep(CONFIG.bls_api_min_spacing_seconds + random.random() * 0.2)

    if not frames:
        logger.info(f"No data retrieved for {program} slice")
        return 0

    combined = pl.concat(frames, how="vertical_relaxed")
    return load_df_to_bls_long(combined, program=program)
