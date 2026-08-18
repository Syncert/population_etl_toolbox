# data_ingestion_toolbox/census_acs/ingest.py

from __future__ import annotations

import io
import uuid
import random
import time
from datetime import datetime, timezone
from typing import Iterable, List, Dict, Optional

import httpx
import polars as pl
import psycopg2
from data_ingestion_toolbox.utility.db_connection import (
    PostgresConnectionDetails,
    PostgresConnectionFactory,
)
from data_ingestion_toolbox.utility.retry import retry_database_transaction
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import json
import logging

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.census_acs.silver_census.replay import (
    replay_census_capture,
)
from .config import CONFIG

CENSUS_NULL_SENTINELS = {
    "-222222222",
    "-333333333",
    "-555555555",
    "-666666666",
    "-888888888",
    "-999999999",
}


# classes for HTTP responses
class CensusNoContent(Exception):
    """HTTP 204 from Census API: treat as empty slice, not a failure."""

    pass


class CensusRetryableHTTP(Exception):
    """Retry-worthy HTTP cases (429 / 5xx)"""

    pass


class CensusPayloadError(ValueError):
    """The Census payload shape cannot be normalized safely."""


class CensusFetchedResponse(list):
    """Decoded Census array accompanied by exact successful response bytes."""

    def __init__(self, document: list, *, response: httpx.Response) -> None:
        super().__init__(document)
        self.payload = response.content
        self.response_headers = dict(response.headers)
        self.http_status = response.status_code


# initialize logger
logger = logging.getLogger(__name__)

# Which database inside the Postgres instance do you want?
# If you want this configurable, put it in CONFIG (recommended).
_TARGET_DATABASE = "public_data"


def _get_pg_conn_details() -> "PostgresConnectionDetails":
    """
    Get PostgresConnectionDetails from Airflow when running in Airflow,
    otherwise fall back to local env vars.
    """
    return PostgresConnectionFactory.auto(
        conn_id=getattr(CONFIG, "postgres_conn_id", None),
        prefix="POSTGRES_",
        database=getattr(CONFIG, "target_database", _TARGET_DATABASE),
    )


def _get_pg_connection():
    """
    Open a psycopg2 connection using the factory’s connection details.
    """
    details = _get_pg_conn_details()
    return psycopg2.connect(**details.psycopg_kwargs())


def get_curated_variables(year: int, dataset: str) -> List[str]:
    """
    Return the list of variable names (including E/M suffixes) for the given
    year+dataset, restricted to curated tables.
    """

    sql = """
        SELECT variable_name
        FROM raw_census.acs_variables
        WHERE dataset = %s
          AND year = %s
          AND table_id = ANY(%s)
        ORDER BY variable_name;
    """

    with _get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (dataset, year, CONFIG.curated_tables))
            rows = cur.fetchall()
    return [r[0] for r in rows]


def chunked(iterable: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(iterable), n):
        yield iterable[i : i + n]


def build_geo_params(
    geo_level: str, state_fips: Optional[str] = None
) -> Dict[str, str]:
    """
    Build the 'for' and 'in' query params for the ACS API, given geo_level.
    """
    if geo_level == "us":
        return {"for": "us:1"}
    elif geo_level == "state":
        return {"for": "state:*"}
    elif geo_level == "county":
        if not state_fips:
            raise ValueError("state_fips required for county-level requests")
        return {"for": "county:*", "in": f"state:{state_fips}"}
    else:
        raise ValueError(f"Unsupported geo_level: {geo_level}")


@retry(
    reraise=True,
    stop=stop_after_attempt(8),
    wait=wait_exponential(multiplier=2, min=5, max=900),  # up to 15 minutes
    retry=retry_if_exception_type(
        (
            CensusRetryableHTTP,
            httpx.TimeoutException,
            httpx.NetworkError,
        )
    ),
)
def fetch_acs_api(
    year: int,
    dataset: str,
    variables: List[str],
    geo_level: str,
    state_fips: Optional[str] = None,
) -> List[List[str]]:
    """
    Call the Census API and return the raw JSON (list-of-lists).

    """
    base_url = f"https://api.census.gov/data/{year}/acs/{dataset}"

    params: Dict[str, str] = {
        "get": ",".join(variables),
        "key": CONFIG.require_api_key(),
    }
    params.update(build_geo_params(geo_level, state_fips))

    # Small jitter even under lock to avoid rhythmic bursts on retries
    time.sleep(0.2 + random.random() * 0.4)

    with httpx.Client(
        timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
    ) as client:
        resp = client.get(base_url, params=params)

        # 204 = No Content (not an error; means "nothing for this query")
        if resp.status_code == 204:
            logger.info(
                "Census 204 No Content: year=%s dataset=%s geo_level=%s state_fips=%s vars_count=%s first_vars=%s url=%s",
                year,
                dataset,
                geo_level,
                state_fips,
                len(variables),
                variables[:5],
                str(resp.url),
            )
            return CensusFetchedResponse([], response=resp)

        # Sometimes APIs return 200 but still give an empty body (rare, but safe to handle)
        if resp.status_code == 200 and not resp.content:
            logger.info(
                "Census 200 but empty body (treat empty): url=%s", str(resp.url)
            )
            return CensusFetchedResponse([], response=resp)

        # 429 = rate limited -> retryable
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = int(retry_after)
                except ValueError:
                    delay = 300
            else:
                delay = 300

            time.sleep(delay + random.random() * 5)
            raise CensusRetryableHTTP(f"429 rate limited for {base_url}")

        # 5xx = server errors -> retryable
        if 500 <= resp.status_code <= 599:
            raise CensusRetryableHTTP(f"{resp.status_code} server error for {base_url}")

        # Other 4xx are NOT retryable; raise normally (you asked for something invalid)
        resp.raise_for_status()

        # At this point we expect JSON. If parsing fails, treat it as retryable once in case of transient weirdness.
        try:
            return CensusFetchedResponse(resp.json(), response=resp)
        except json.JSONDecodeError as e:
            # This is typically "empty body" or HTML error page. Make it retryable.
            raise CensusRetryableHTTP(f"Bad JSON response from Census API: {e}") from e


def rows_to_polars(
    raw: List[List[str]],
    dataset: str,
    year: int,
    geo_level: str,
    state_fips: Optional[str],
    load_batch_id: uuid.UUID,
) -> pl.DataFrame:
    if not raw:
        raise CensusNoContent("Census response contained no rows")

    header = raw[0]
    records = raw[1:]
    if not header or not records:
        raise CensusNoContent("Census response contained no data records")
    if len(set(header)) != len(header):
        raise CensusPayloadError("Census response contains duplicate headers")
    if any(len(record) != len(header) for record in records):
        raise CensusPayloadError("Census response row length does not match header")
    expected_geo_columns = {
        "us": {"us"},
        "state": {"state"},
        "county": {"state", "county"},
    }
    if geo_level not in expected_geo_columns:
        raise ValueError(f"Unsupported geo_level: {geo_level}")
    missing_geo = expected_geo_columns[geo_level] - set(header)
    if missing_geo:
        raise CensusPayloadError(
            f"Census response missing geography columns: {sorted(missing_geo)}"
        )

    df = pl.DataFrame(records, schema=[str(h) for h in header], orient="row")

    # Determine which columns are variables and which are geos
    geo_cols = [c for c in df.columns if c in ("us", "state", "county")]
    var_cols = [c for c in df.columns if c not in geo_cols]

    # For US-level, there will be 'us' as the geo; for state/county, there will be 'state', 'county'
    if geo_level == "us":
        df = df.with_columns(
            geo_id=pl.lit("us:1"),
            state_fips=pl.lit(None, dtype=pl.Utf8),
            county_fips=pl.lit(None, dtype=pl.Utf8),
        )
    elif geo_level == "state":
        df = df.with_columns(
            geo_id=pl.concat_str([pl.lit("state:"), pl.col("state")]),
            state_fips=pl.col("state"),
            county_fips=pl.lit(None, dtype=pl.Utf8),
        )
    elif geo_level == "county":
        df = df.with_columns(
            geo_id=pl.concat_str(
                [
                    pl.lit("state:"),
                    pl.col("state"),
                    pl.lit("|county:"),
                    pl.col("county"),
                ]
            ),
            state_fips=pl.col("state"),
            county_fips=pl.col("county"),
        )
    else:
        raise ValueError(f"Unsupported geo_level: {geo_level}")

    # Melt variable columns into long format
    long_df = df.unpivot(
        index=["geo_id", "state_fips", "county_fips"],
        on=var_cols,
        variable_name="variable_name",
        value_name="value_str",
    )

    # 1) Force value_str to Utf8 before any .str operations
    long_df = long_df.with_columns(pl.col("value_str").cast(pl.Utf8).alias("value_str"))

    # 2) Now safe to use string methods
    long_df = long_df.with_columns(
        pl.when(pl.col("value_str").str.strip_chars().eq(""))
        .then(None)
        .when(pl.col("value_str").is_in(CENSUS_NULL_SENTINELS))
        .then(None)
        .otherwise(pl.col("value_str"))
        .alias("value_str")
    )

    # 3) Convert to float, allowing failures to become null
    long_df = long_df.with_columns(
        pl.col("value_str").cast(pl.Float64, strict=False).alias("value")
    ).drop("value_str")

    # derive table_id from variable_name (e.g. 'B01001_001E' -> 'B01001')
    long_df = long_df.with_columns(
        pl.col("variable_name").str.split("_").list.get(0).alias("table_id")
    )

    # derive measure_type from variable_name (last char: E or M)
    long_df = long_df.with_columns(
        pl.col("variable_name").str.slice(-1, 1).alias("measure_type")
    )

    long_df = long_df.with_columns(
        dataset=pl.lit(dataset),
        year=pl.lit(year),
        geo_level=pl.lit(geo_level),
        load_batch_id=pl.lit(str(load_batch_id)),
        ingested_at=pl.lit(datetime.now(timezone.utc)),
    )

    # reorder columns
    long_df = long_df.select(
        [
            "dataset",
            "year",
            "geo_level",
            "geo_id",
            "state_fips",
            "county_fips",
            "table_id",
            "variable_name",
            "measure_type",
            "value",
            "load_batch_id",
            "ingested_at",
        ]
    )

    return long_df


@retry_database_transaction
def load_df_to_acs_long(
    df: pl.DataFrame, dataset: str, year: int, geo_level: str
) -> int:
    """
    Bulk load a Polars DataFrame into raw_census.acs_long using COPY.

    We first delete existing rows for (dataset, year, geo_level) for the
    subset of geo_ids present in this batch (idempotent per partition).
    """
    if df.is_empty():
        return 0
    if df.height > CONFIG.raw_load_max_rows:
        raise ValueError(
            f"Census raw batch has {df.height} rows; configured maximum is "
            f"{CONFIG.raw_load_max_rows}"
        )

    conn = _get_pg_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        geo_ids = df.select("geo_id").unique().to_series().to_list()
        for geo_id in sorted(geo_ids):
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"raw_census.acs_long:{dataset}:{year}:{geo_level}:{geo_id}",),
            )

        # Delete existing rows for this slice
        cur.execute(
            """
            DELETE FROM raw_census.acs_long
            WHERE dataset = %s
              AND year = %s
              AND geo_level = %s
              AND geo_id = ANY(%s);
            """,
            (dataset, year, geo_level, geo_ids),
        )

        # Prepare CSV in-memory
        output = io.StringIO()
        df.select(
            [
                "dataset",
                "year",
                "geo_level",
                "geo_id",
                "state_fips",
                "county_fips",
                "table_id",
                "variable_name",
                "measure_type",
                "value",
                "load_batch_id",
                "ingested_at",
            ]
        ).write_csv(output, include_header=False)
        output.seek(0)

        # Copy into Postgres
        cur.copy_expert(
            """
            COPY raw_census.acs_long (
                dataset, year, geo_level, geo_id,
                state_fips, county_fips, table_id,
                variable_name, measure_type,
                value, load_batch_id, ingested_at
            )
            FROM STDIN WITH (FORMAT csv);
            """,
            output,
        )

        rowcount = cur.rowcount  # COPY's rowcount is a bit weird, but good enough

        conn.commit()
        return rowcount

    finally:
        # Make sure we actually close this stuff even on error
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def ingest_slice(
    year: int,
    dataset: str,
    geo_level: str,
    state_fips: Optional[str] = None,
) -> int:
    """
    Ingest one slice: (year, dataset, geo_level[, state_fips]).

    For example:
        (2022, 'acs5', 'us')
        (2022, 'acs5', 'state')
        (2022, 'acs5', 'county', '55')  # WI counties
    """
    variables = get_curated_variables(year, dataset)
    if not variables:
        # nothing to ingest for this year+dataset
        return 0

    # We can also add 'NAME' if we want; here we keep purely numeric variables.
    control = CaptureControl(_get_pg_connection, source_code="CENSUS_ACS")
    run_id = control.start_run(
        watermark={
            "dataset": dataset,
            "year": year,
            "geo_level": geo_level,
            "state_fips": state_fips,
        }
    )
    endpoint = f"https://api.census.gov/data/{year}/acs/{dataset}"
    total_rows = 0

    return _ingest_capture_chunks(
        variables=variables,
        year=year,
        dataset=dataset,
        geo_level=geo_level,
        state_fips=state_fips,
        control=control,
        run_id=run_id,
        endpoint=endpoint,
        total_rows=total_rows,
    )

def _ingest_capture_chunks(
    *,
    variables: List[str],
    year: int,
    dataset: str,
    geo_level: str,
    state_fips: str | None,
    control: CaptureControl,
    run_id: uuid.UUID,
    endpoint: str,
    total_rows: int,
) -> int:
    try:
        for variable_chunk in chunked(variables, 50):
            parameters: dict[str, object] = {
                "get": ",".join(variable_chunk),
                "dataset": dataset,
                "year": year,
                "geo_level": geo_level,
            }
            parameters.update(build_geo_params(geo_level, state_fips))
            request = control.start_request(
                run_id=run_id,
                endpoint=endpoint,
                parameters=parameters,
                max_attempts=8,
            )
            try:
                raw = fetch_acs_api(
                    year=year,
                    dataset=dataset,
                    variables=variable_chunk,
                    geo_level=geo_level,
                    state_fips=state_fips,
                )
                payload = getattr(raw, "payload", None)
                if payload is None:
                    payload = json.dumps(
                        raw,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode("utf-8")
                capture_id = uuid.uuid4()
                persist_response_capture(
                    _get_pg_connection,
                    ResponseCapture(
                        capture_id=capture_id,
                        request_id=request.request_id,
                        run_id=run_id,
                        source_code="CENSUS_ACS",
                        endpoint=endpoint,
                        request_parameters=parameters,
                        retrieved_at=datetime.now(timezone.utc),
                        http_status=getattr(raw, "http_status", 200),
                        response_headers=getattr(raw, "response_headers", {}),
                        media_type="application/json",
                        payload=payload,
                        payload_schema_version="census-acs-array-v1",
                        source_revision=str(year),
                    ),
                )
            except BaseException as error:
                control.finish_request(
                    request.request_id, status="failed", error=error
                )
                raise

            if not raw:
                control.finish_request(request.request_id, status="empty")
                continue
            try:
                rows = replay_census_capture(
                    _get_pg_connection,
                    capture_id=capture_id,
                    dataset=dataset,
                    year=year,
                    geo_level=geo_level,
                )
            except BaseException as error:
                control.quarantine(
                    capture_id=capture_id,
                    run_id=run_id,
                    parser_version="census-acs-array-v1",
                    error_code="INVALID_CENSUS_ACS_ARRAY",
                    error=error,
                )
                control.finish_request(
                    request.request_id, status="quarantined", error=error
                )
                raise
            control.finish_request(
                request.request_id,
                status="captured" if rows else "empty",
            )
            total_rows += rows
            time.sleep(0.2 + random.random() * 0.3)
    except BaseException as error:
        control.finish_run(run_id, status="failed", error=error)
        raise

    control.finish_run(run_id, status="success")
    return total_rows
