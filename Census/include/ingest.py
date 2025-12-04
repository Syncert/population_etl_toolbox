# include/census_acs/ingest.py

from __future__ import annotations

import io
import math
import uuid
from datetime import datetime, timezone
from typing import Iterable, List, Dict, Optional

import httpx
import polars as pl
import psycopg2
from utilities.db_connection import PostgresConnectionFactory, PostgresConnectionDetails
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import CONFIG


# Which database inside the Postgres instance do you want?
_TARGET_DATABASE = "public_data"

# Optional: if/when you run this inside Airflow, set this to your conn_id (e.g. "public_datasets").
# For local dev, leaving it as None makes PostgresConnectionFactory.auto() fall back to env vars.
_AIRFLOW_CONN_ID: Optional[str] = None


def _get_pg_conn_details() -> "PostgresConnectionDetails":
    """
    Get PostgresConnectionDetails either from Airflow (if _AIRFLOW_CONN_ID is set
    and Airflow is available) or from local environment variables.
    """
    return PostgresConnectionFactory.auto(
        conn_id=_AIRFLOW_CONN_ID,     # None in local dev, "public_datasets" in Airflow
        prefix="POSTGRES_",           # POSTGRES_HOST, POSTGRES_PORT, etc. on dev box
        database=_TARGET_DATABASE,    # override database inside the Postgres instance
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
    conn = _get_pg_connection()
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


def build_geo_params(geo_level: str, state_fips: Optional[str] = None) -> Dict[str, str]:
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
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
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
    }
    params.update(build_geo_params(geo_level, state_fips))

    if CONFIG.has_api_key:
        params["key"] = CONFIG.census_api_key

    with httpx.Client(timeout=60.0) as client:
        resp = client.get(base_url, params=params)
        resp.raise_for_status()
        return resp.json()


def rows_to_polars(
    raw: List[List[str]],
    dataset: str,
    year: int,
    geo_level: str,
    state_fips: Optional[str],
    load_batch_id: uuid.UUID,
) -> pl.DataFrame:
    if not raw:
        return pl.DataFrame()

    header = raw[0]
    records = raw[1:]

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
                [pl.lit("state:"), pl.col("state"), pl.lit("|county:"), pl.col("county")]
            ),
            state_fips=pl.col("state"),
            county_fips=pl.col("county"),
        )
    else:
        raise ValueError(f"Unsupported geo_level: {geo_level}")

    # Melt variable columns into long format
    long_df = df.melt(
        id_vars=["geo_id", "state_fips", "county_fips"],
        value_vars=var_cols,
        variable_name="variable_name",
        value_name="value_str",
    )

    # Convert value to numeric (where possible)
    long_df = long_df.with_columns(
        pl.when(pl.col("value_str").str.strip_chars().eq(""))
        .then(None)
        .otherwise(pl.col("value_str"))
        .alias("value_str")
    )

    long_df = long_df.with_columns(
        pl.col("value_str").cast(pl.Float64, strict=False).alias("value")
    ).drop("value_str")

    #derive table_id from variable_name (e.g. 'B01001_001E' -> 'B01001')
    long_df = long_df.with_columns(
        pl.col("variable_name")
        .str.split("_")
        .list.get(0)
        .alias("table_id")
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
            "value",
            "load_batch_id",
            "ingested_at",
        ]
    )

    return long_df


def load_df_to_acs_long(df: pl.DataFrame, dataset: str, year: int, geo_level: str) -> int:
    """
    Bulk load a Polars DataFrame into raw_census.acs_long using COPY.

    We first delete existing rows for (dataset, year, geo_level) for the
    subset of geo_ids present in this batch (idempotent per partition).
    """
    if df.is_empty():
        return 0

    conn = _get_pg_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        geo_ids = df.select("geo_id").unique().to_series().to_list()

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
                variable_name, value, load_batch_id, ingested_at
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
    batch_id = uuid.uuid4()

    frames: List[pl.DataFrame] = []

    for chunk in chunked(variables, 50):  # API limit
        raw = fetch_acs_api(
            year=year,
            dataset=dataset,
            variables=chunk,
            geo_level=geo_level,
            state_fips=state_fips,
        )
        df = rows_to_polars(
            raw=raw,
            dataset=dataset,
            year=year,
            geo_level=geo_level,
            state_fips=state_fips,
            load_batch_id=batch_id,
        )
        if not df.is_empty():
            frames.append(df)

    if not frames:
        return 0

    combined = pl.concat(frames, how="vertical_relaxed")
    return load_df_to_acs_long(combined, dataset=dataset, year=year, geo_level=geo_level)