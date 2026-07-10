# census_acs/geography.py

from __future__ import annotations

import io
import logging
import time
from datetime import datetime, timezone
from typing import Optional
import zipfile

import httpx
import polars as pl
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_ingestion_toolbox.census_acs.config import CONFIG

logger = logging.getLogger(__name__)

GAZ_ROOT = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer"


# Gazetteer “national” files we need for your geo_dim.
# (We’re keeping this tight: US + states + counties.)
def _states_url(year: int) -> str:
    return f"{GAZ_ROOT}/{year}_Gazetteer/{year}_Gaz_state_national.zip"

def _counties_url(year: int) -> str:
    return f"{GAZ_ROOT}/{year}_Gazetteer/{year}_Gaz_counties_national.zip"


def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _url_exists(url: str, timeout_s: float = 10.0) -> bool:
    """
    Fast existence check. Uses HEAD first; falls back to a tiny GET if needed
    (some servers behave oddly for HEAD).
    """
    try:
        with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
            r = client.head(url)
            if r.status_code == 200:
                return True
            if r.status_code in (403, 405):  # HEAD not allowed sometimes
                r2 = client.get(url, headers={"Range": "bytes=0-50"})
                return r2.status_code == 200
            return False
    except Exception:
        return False


def resolve_latest_gazetteer_year(
    start_year: Optional[int] = None,
    min_year: int = 2010,
) -> int:
    """
    Pick the newest Gazetteer year that has BOTH required files (states + counties).
    """
    # Default search start: current year (UTC) — not ACS year.
    y0 = start_year or datetime.now(timezone.utc).year

    for y in range(y0, min_year - 1, -1):
        su = _states_url(y)
        cu = _counties_url(y)
        if _url_exists(su) and _url_exists(cu):
            logger.info("Resolved Gazetteer year=%s (states+counties present)", y)
            return y

    raise RuntimeError(
        f"Could not find Gazetteer states+counties files for any year {min_year}..{y0}. "
        f"Check network access or Census URL patterns."
    )


def _fetch_zipped_tsv(url: str, retries: int = 3) -> pl.DataFrame:
    last_exc: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            with httpx.Client(timeout=60, follow_redirects=True) as client:
                resp = client.get(url)
                resp.raise_for_status()
                zbytes = resp.content

            if len(zbytes) < 4 or not zbytes.startswith(b"PK"):
                preview = zbytes[:200].decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"Expected a zip payload from {url} but received non-zip content. "
                    f"Content-Type={resp.headers.get('content-type', 'unknown')!r}; "
                    f"preview={preview!r}"
                )

            with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
                txt_names = [n for n in zf.namelist() if n.lower().endswith(".txt")]
                if not txt_names:
                    raise RuntimeError(f"No .txt found inside zip: {url}")

                with zf.open(txt_names[0]) as f:
                    text = f.read().decode("utf-8", errors="replace")

            break

        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Gazetteer fetch attempt %d/%d failed for %s: %s",
                attempt,
                retries,
                url,
                exc,
            )
            if attempt < retries:
                time.sleep(2 ** attempt)

    else:
        raise RuntimeError(
            f"Failed to fetch Gazetteer zip from {url} after {retries} attempts"
        ) from last_exc

    # Try tab first (older), then pipe (newer), using GEOID as the “success” signal.
    for sep in ("\t", "|"):
        df = pl.read_csv(
            io.StringIO(text),
            separator=sep,
            infer_schema_length=2000,
            ignore_errors=True,
        )
        if "GEOID" in df.columns:
            return df

    # If we get here, show the header line to debug delimiter/schema issues.
    first_line = text.splitlines()[0] if text else ""
    raise RuntimeError(
        f"Could not parse Gazetteer file into expected columns. "
        f"First header line: {first_line[:200]}"
    )


def sync_geo_dim(source_year: Optional[int] = None, min_year: int = 2010) -> None:
    """
    Upsert US + states + counties into raw_census.geo_dim.

    If source_year is None, auto-resolve the latest Gazetteer year available.
    """
    hook = _get_hook()
    now = datetime.now(timezone.utc)

    y = source_year or resolve_latest_gazetteer_year(min_year=min_year)

    states_url = _states_url(y)
    counties_url = _counties_url(y)

    # 1) US row
    us_df = pl.DataFrame([{
        "geo_level": "us",
        "geo_id": "us:1",
        "state_fips": None,
        "county_fips": None,
        "name": "United States",
        "state_name": None,
        "county_name": None,
        "is_active": True,
        "source": "census_gazetteer",
        "source_year": y,
        "ingested_at": now,
    }])

    # 2) States
    st = _fetch_zipped_tsv(states_url)

    # Gazetteer state file includes GEOID + NAME
    st_df = (
        st.select([
            pl.col("GEOID").cast(pl.Utf8).alias("state_fips"),
            pl.col("NAME").cast(pl.Utf8).alias("state_name"),
        ])
        .with_columns([
            pl.lit("state").alias("geo_level"),
            pl.concat_str([pl.lit("state:"), pl.col("state_fips")]).alias("geo_id"),
            pl.lit(None, dtype=pl.Utf8).alias("county_fips"),
            pl.col("state_name").alias("name"),
            pl.lit(None, dtype=pl.Utf8).alias("county_name"),
            pl.lit(True).alias("is_active"),
            pl.lit("census_gazetteer").alias("source"),
            pl.lit(y).alias("source_year"),
            pl.lit(now).alias("ingested_at"),
        ])
    )

    # Ensure proper Typing
    st_df = st_df.with_columns([
        pl.col("state_fips").cast(pl.Utf8),
        pl.col("county_fips").cast(pl.Utf8),
        pl.col("is_active").cast(pl.Boolean),
        pl.col("source_year").cast(pl.Int32),
    ])

    # 3) Counties
    co = _fetch_zipped_tsv(counties_url)

    co_df = (
        co.select([
            pl.col("GEOID").cast(pl.Utf8).str.zfill(5).alias("geoid5"), # Guarantees length of 5 digits and 0 padded 
            pl.col("NAME").cast(pl.Utf8).alias("county_name"),
        ])
        .with_columns([
            pl.col("geoid5").str.slice(0, 2).alias("state_fips"),
            pl.col("geoid5").str.slice(2, 3).alias("county_fips"),
        ])
        .with_columns([
            pl.lit("county").alias("geo_level"),
            pl.concat_str([
                pl.lit("state:"), pl.col("state_fips"),
                pl.lit("|county:"), pl.col("county_fips"),
            ]).alias("geo_id"),
            pl.col("county_name").alias("county_name"),
            pl.col("county_name").alias("name"),
            pl.lit(True).alias("is_active"),
            pl.lit("census_gazetteer").alias("source"),
            pl.lit(y).alias("source_year"),
            pl.lit(now).alias("ingested_at"),
        ])
        .drop("geoid5")
    )
    
    # Ensure proper Typing
    co_df = co_df.with_columns([
        pl.col("state_fips").cast(pl.Utf8),
        pl.col("county_fips").cast(pl.Utf8),
        pl.col("is_active").cast(pl.Boolean),
        pl.col("source_year").cast(pl.Int32),
    ])

    # Attach state_name onto county rows (nice for display)
    df_states = st_df.select(["state_fips", "state_name"])
    co_df = co_df.join(df_states, on="state_fips", how="left")

    # --- Normalize schemas (Polars concat requires same column names) ---
    TARGET_COLS = [
        "geo_level",
        "geo_id",
        "state_fips",
        "county_fips",
        "name",
        "state_name",
        "county_name",
        "is_active",
        "source",
        "source_year",
        "ingested_at",
    ]

    def _ensure_cols(df: pl.DataFrame) -> pl.DataFrame:
        missing = [c for c in TARGET_COLS if c not in df.columns]
        if missing:
            df = df.with_columns([pl.lit(None).alias(c) for c in missing])
        return df.select(TARGET_COLS)

    us_df = _ensure_cols(us_df)
    st_df = _ensure_cols(st_df)
    co_df = _ensure_cols(co_df)

    df = pl.concat([us_df, st_df, co_df], how="vertical_relaxed")

    # Upsert
    sql = """
        INSERT INTO raw_census.geo_dim (
            geo_level, geo_id, state_fips, county_fips,
            name, state_name, county_name,
            is_active, source, source_year, ingested_at
        )
        VALUES (
            %(geo_level)s, %(geo_id)s, %(state_fips)s, %(county_fips)s,
            %(name)s, %(state_name)s, %(county_name)s,
            %(is_active)s, %(source)s, %(source_year)s, %(ingested_at)s
        )
        ON CONFLICT (geo_level, geo_id)
        DO UPDATE SET
            state_fips   = EXCLUDED.state_fips,
            county_fips  = EXCLUDED.county_fips,
            name         = EXCLUDED.name,
            state_name   = EXCLUDED.state_name,
            county_name  = EXCLUDED.county_name,
            is_active    = EXCLUDED.is_active,
            source       = EXCLUDED.source,
            source_year  = EXCLUDED.source_year,
            ingested_at  = EXCLUDED.ingested_at;
    """

    rows = df.to_dicts()
    with hook.get_conn() as conn, conn.cursor() as cur:
        for r in rows:
            cur.execute(sql, r)
        conn.commit()

    logger.info("geo_dim sync complete: %s rows upserted using Gazetteer year=%s", len(rows), y)