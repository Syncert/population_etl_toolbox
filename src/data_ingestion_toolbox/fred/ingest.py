# data_ingestion_toolbox/fred/ingest.py

from __future__ import annotations

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
from data_ingestion_toolbox.capture import (
    ResponseCapture,
    persist_response_capture,
    request_fingerprint,
)
from data_ingestion_toolbox.normalization import (
    NumericParseError,
    parse_decimal,
    sanitize_error_message,
)
from data_ingestion_toolbox.fred.silver_fred.replay import replay_fred_capture
from .config import CONFIG

logger = logging.getLogger(__name__)
# FRED authenticates with a query parameter. httpx's INFO access log renders the
# complete request URL, so keep that third-party logger above INFO to prevent
# credentials from reaching scheduler, CI, or application logs.
logging.getLogger("httpx").setLevel(logging.WARNING)

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


class FredFetchedResponse(dict):
    """Decoded FRED document accompanied by the exact successful HTTP body."""

    def __init__(
        self,
        document: Dict,
        *,
        payload: bytes,
        response_headers: Dict[str, str],
        http_status: int,
    ) -> None:
        super().__init__(document)
        self.payload = payload
        self.response_headers = response_headers
        self.http_status = http_status


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

        return FredFetchedResponse(
            data,
            payload=resp.content,
            response_headers=dict(resp.headers),
            http_status=resp.status_code,
        )


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


def _execute_control(statement: str, parameters: tuple[object, ...]) -> None:
    """Commit one control-plane transition independently of capture parsing."""
    connection = _get_pg_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(statement, parameters)
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _start_capture_run(run_id: uuid.UUID) -> None:
    _execute_control(
        """
        INSERT INTO control.ingestion_run (
            run_id, source_code, status, started_at
        ) VALUES (%s, 'FRED', 'running', NOW())
        """,
        (str(run_id),),
    )


def _finish_capture_run(
    run_id: uuid.UUID, *, status: str, error: str | None = None
) -> None:
    _execute_control(
        """
        UPDATE control.ingestion_run
           SET status = %s, finished_at = NOW(), error_summary = %s,
               updated_at = NOW()
         WHERE run_id = %s AND source_code = 'FRED'
        """,
        (status, error, str(run_id)),
    )


def _start_capture_request(
    *,
    request_id: uuid.UUID,
    run_id: uuid.UUID,
    endpoint: str,
    parameters: dict[str, object],
    fingerprint: str,
) -> None:
    _execute_control(
        """
        INSERT INTO control.ingestion_request (
            request_id, run_id, source_code, endpoint,
            request_parameters, request_fingerprint, status,
            attempt_count, max_attempts, started_at
        ) VALUES (%s, %s, 'FRED', %s, %s::JSONB, %s, 'running', 1, 8, NOW())
        """,
        (
            str(request_id),
            str(run_id),
            endpoint,
            json.dumps(parameters, sort_keys=True),
            fingerprint,
        ),
    )


def _finish_capture_request(
    request_id: uuid.UUID,
    *,
    status: str,
    error: str | None = None,
) -> None:
    _execute_control(
        """
        UPDATE control.ingestion_request
           SET status = %s, finished_at = NOW(), last_error = %s,
               updated_at = NOW()
         WHERE request_id = %s AND source_code = 'FRED'
        """,
        (status, error, str(request_id)),
    )


def _quarantine_capture(
    *,
    capture_id: uuid.UUID,
    run_id: uuid.UUID,
    error: BaseException,
) -> None:
    _execute_control(
        """
        INSERT INTO control.capture_quarantine (
            quarantine_id, capture_id, run_id, source_code,
            parser_version, error_code, error_summary
        ) VALUES (%s, %s, %s, 'FRED', 'fred-observations-v1',
                  'INVALID_FRED_OBSERVATIONS', %s)
        ON CONFLICT (capture_id, parser_version, error_code) DO NOTHING
        """,
        (
            str(uuid.uuid4()),
            str(capture_id),
            str(run_id),
            sanitize_error_message(error),
        ),
    )


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

    run_id = uuid.uuid4()
    _start_capture_run(run_id)
    total_rows = 0
    endpoint = f"{FRED_API_BASE}/series/observations"

    try:
        # FRED is requested and captured one series at a time. Each successful
        # HTTP body commits before its silver replay transaction begins.
        for series_id in series_ids:
            request_id = uuid.uuid4()
            request_parameters: dict[str, object] = {
                "series_id": series_id,
                "file_type": "json",
                "observation_start": date_start,
                "observation_end": date_end,
                "domain": domain,
            }
            if realtime_start:
                request_parameters["realtime_start"] = realtime_start
            if realtime_end:
                request_parameters["realtime_end"] = realtime_end
            fingerprint = request_fingerprint("FRED", endpoint, request_parameters)
            _start_capture_request(
                request_id=request_id,
                run_id=run_id,
                endpoint=endpoint,
                parameters=request_parameters,
                fingerprint=fingerprint,
            )

            try:
                response_data = fetch_fred_observations(
                    series_id=series_id,
                    observation_start=date_start,
                    observation_end=date_end,
                    realtime_start=realtime_start,
                    realtime_end=realtime_end,
                )
                payload = getattr(response_data, "payload", None)
                if payload is None:
                    # Test doubles predating ARCH-007 may return only a decoded
                    # document. Production fetches always supply exact bytes.
                    payload = json.dumps(
                        response_data,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode("utf-8")
                capture_id = uuid.uuid4()
                capture = ResponseCapture(
                    capture_id=capture_id,
                    request_id=request_id,
                    run_id=run_id,
                    source_code="FRED",
                    endpoint=endpoint,
                    request_parameters=request_parameters,
                    retrieved_at=datetime.now(timezone.utc),
                    http_status=getattr(response_data, "http_status", 200),
                    response_headers=getattr(response_data, "response_headers", {}),
                    media_type="application/json",
                    payload=payload,
                    payload_schema_version="fred-series-observations-v1",
                    source_revision=(
                        realtime_end or _latest_source_revision(response_data)
                    ),
                )
                persist_response_capture(_get_pg_connection, capture)
            except BaseException as error:
                _finish_capture_request(
                    request_id,
                    status="failed",
                    error=sanitize_error_message(error),
                )
                raise
            try:
                rows = replay_fred_capture(
                    _get_pg_connection,
                    capture_id=capture_id,
                    series_id=series_id,
                    domain=domain,
                )
            except BaseException as error:
                sanitized = sanitize_error_message(error)
                _quarantine_capture(
                    capture_id=capture_id,
                    run_id=run_id,
                    error=error,
                )
                _finish_capture_request(
                    request_id, status="quarantined", error=sanitized
                )
                raise
            _finish_capture_request(
                request_id,
                status="captured" if rows else "empty",
            )
            total_rows += rows

            time.sleep(CONFIG.fred_api_min_spacing_seconds + random.random() * 0.2)
    except BaseException as error:
        _finish_capture_run(
            run_id,
            status="failed",
            error=sanitize_error_message(error),
        )
        raise

    _finish_capture_run(run_id, status="success")
    if not total_rows:
        logger.info("No data retrieved for domain %s", domain)
    return total_rows


def _latest_source_revision(response_data: Dict) -> str | None:
    observations = response_data.get("observations", [])
    if not isinstance(observations, list):
        return None
    revisions = [
        str(observation["realtime_end"])
        for observation in observations
        if isinstance(observation, dict) and observation.get("realtime_end")
    ]
    return max(revisions) if revisions else None
