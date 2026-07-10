# silver_ref/geography.py

from __future__ import annotations

import io
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional
import zipfile

import httpx
import polars as pl
import shapefile as pyshp
from airflow.providers.postgres.hooks.postgres import PostgresHook

from data_ingestion_toolbox.silver_ref.config import CONFIG

logger = logging.getLogger(__name__)

GAZ_ROOT = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer"
GENZ_ROOT = "https://www2.census.gov/geo/tiger"


def _states_url(year: int) -> str:
    return f"{GAZ_ROOT}/{year}_Gazetteer/{year}_Gaz_state_national.zip"


def _counties_url(year: int) -> str:
    return f"{GAZ_ROOT}/{year}_Gazetteer/{year}_Gaz_counties_national.zip"


def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def _state_boundary_url(year: int) -> str:
    """Census cartographic boundary shapefile zip for states."""
    return f"{GENZ_ROOT}/GENZ{year}/shp/cb_{year}_us_state_500k.zip"


def _county_boundary_url(year: int) -> str:
    """Census cartographic boundary shapefile zip for counties."""
    return f"{GENZ_ROOT}/GENZ{year}/shp/cb_{year}_us_county_500k.zip"


def _url_exists(url: str, timeout_s: float = 10.0) -> bool:
    """Check if a URL exists. Logs all outcomes for diagnostics."""
    try:
        with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
            r = client.head(url)
            if r.status_code == 200:
                return True
            if r.status_code in (403, 405):
                r2 = client.get(url, headers={"Range": "bytes=0-50"})
                return r2.status_code == 200
            logger.debug("_url_exists: %s returned %s", url, r.status_code)
            return False
    except Exception as e:
        logger.debug("_url_exists: %s raised %s", url, type(e).__name__)
        return False


def resolve_latest_gazetteer_year(
    start_year: Optional[int] = None,
    min_year: int = 2010,
) -> int:
    y0 = start_year or datetime.now(timezone.utc).year

    for y in range(y0, min_year - 1, -1):
        if _url_exists(_states_url(y)) and _url_exists(_counties_url(y)):
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

    for sep in ("\t", "|"):
        df = pl.read_csv(
            io.StringIO(text),
            separator=sep,
            infer_schema_length=2000,
            ignore_errors=True,
        )
        if "GEOID" in df.columns:
            return df

    first_line = text.splitlines()[0] if text else ""
    raise RuntimeError(
        "Could not parse Gazetteer file into expected columns. "
        f"First header line: {first_line[:200]}"
    )


def _fetch_boundary_features(url: str, retries: int = 3) -> list[dict]:
    """Download a Census cartographic boundary shapefile zip and return a list
    of GeoJSON-style feature dicts ``{"properties": {...}, "geometry": {...}}``.

    Uses *pyshp* (``shapefile`` package) to parse the shapefile components
    directly from memory — no temp files needed.
    """
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            timeout = httpx.Timeout(connect=15.0, read=300.0, write=30.0, pool=15.0)
            with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                resp = client.get(url)
                resp.raise_for_status()

            zip_bytes = io.BytesIO(resp.content)
            with zipfile.ZipFile(zip_bytes) as zf:
                names = zf.namelist()
                shp_name = next((n for n in names if n.lower().endswith(".shp")), None)
                shx_name = next((n for n in names if n.lower().endswith(".shx")), None)
                dbf_name = next((n for n in names if n.lower().endswith(".dbf")), None)

                if not all([shp_name, shx_name, dbf_name]):
                    raise RuntimeError(
                        f"Shapefile zip missing .shp/.shx/.dbf in {url}. "
                        f"Contents: {names}"
                    )

                reader = pyshp.Reader(
                    shp=io.BytesIO(zf.read(shp_name)),
                    shx=io.BytesIO(zf.read(shx_name)),
                    dbf=io.BytesIO(zf.read(dbf_name)),
                )

            field_names = [f[0] for f in reader.fields[1:]]  # skip DeletionFlag
            features: list[dict] = []
            for sr in reader.shapeRecords():
                features.append(
                    {
                        "properties": dict(zip(field_names, sr.record)),
                        "geometry": sr.shape.__geo_interface__,
                    }
                )

            mb = len(resp.content) / 1_048_576
            logger.info(
                "Fetched shapefile: %s (%d features, %.1f MB, attempt %d)",
                url, len(features), mb, attempt,
            )
            return features

        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Shapefile fetch attempt %d/%d failed for %s: %s",
                attempt, retries, url, exc,
            )
            if attempt < retries:
                time.sleep(2 ** attempt)

    raise RuntimeError(
        f"Failed to fetch shapefile from {url} after {retries} attempts"
    ) from last_exc


def resolve_latest_genz_year(
    start_year: Optional[int] = None,
    min_year: int = 2013,
) -> int:
    """Probe Census TIGER/GENZ URLs to find the latest year with available
    cartographic boundary shapefiles (state + county).

    Falls back to county-only if no year has both.
    """
    y0 = start_year or datetime.now(timezone.utc).year
    county_only_year: Optional[int] = None

    for y in range(y0, min_year - 1, -1):
        county_ok = _url_exists(_county_boundary_url(y), timeout_s=15.0)
        if not county_ok:
            logger.debug("GENZ %d: county shapefile not available", y)
            continue

        state_ok = _url_exists(_state_boundary_url(y), timeout_s=15.0)
        if state_ok:
            logger.info(
                "Resolved GENZ year=%d (state + county shapefiles available)", y
            )
            return y

        if county_only_year is None:
            county_only_year = y
            logger.info(
                "GENZ %d: county shapefile available, state not "
                "(will use if no better year found)",
                y,
            )

    if county_only_year is not None:
        logger.info(
            "Resolved GENZ year=%d (county-only; no year has both state + county)",
            county_only_year,
        )
        return county_only_year

    raise RuntimeError(
        f"No GENZ cartographic boundary shapefiles found for any year {min_year}..{y0}. "
        f"Check network access to {GENZ_ROOT}."
    )


def _load_polygon_lookup(
    genz_year: Optional[int] = None,
) -> tuple[dict[str, str], dict[str, str]]:
    """Load state/county polygon geometry JSON keyed by FIPS from Census
    cartographic boundary GeoJSON.

    Resolves the latest available GENZ year when *genz_year* is ``None``.
    State and county downloads are independent — one can succeed without the
    other.
    """
    if genz_year is None:
        try:
            genz_year = resolve_latest_genz_year()
        except RuntimeError as exc:
            logger.error("Cannot resolve GENZ year for polygon data: %s", exc)
            return {}, {}

    state_polygons: dict[str, str] = {}
    county_polygons: dict[str, str] = {}

    # --- State polygons (independent) ------------------------------------
    state_url = _state_boundary_url(genz_year)
    try:
        state_features = _fetch_boundary_features(state_url)
        for feature in state_features:
            props = feature.get("properties") or {}
            geometry = feature.get("geometry")
            statefp = props.get("STATEFP")
            if statefp and geometry:
                state_polygons[str(statefp).zfill(2)] = json.dumps(
                    geometry, separators=(",", ":")
                )
        logger.info(
            "Loaded %d state polygons from GENZ %d", len(state_polygons), genz_year
        )
    except Exception as exc:
        logger.warning("State polygon load failed for GENZ %d: %s", genz_year, exc)

    # --- County polygons (independent) -----------------------------------
    county_url = _county_boundary_url(genz_year)
    try:
        county_features = _fetch_boundary_features(county_url)
        for feature in county_features:
            props = feature.get("properties") or {}
            geometry = feature.get("geometry")
            statefp = props.get("STATEFP")
            countyfp = props.get("COUNTYFP")
            if statefp and countyfp and geometry:
                fips5 = f"{str(statefp).zfill(2)}{str(countyfp).zfill(3)}"
                county_polygons[fips5] = json.dumps(
                    geometry, separators=(",", ":")
                )
        logger.info(
            "Loaded %d county polygons from GENZ %d",
            len(county_polygons), genz_year,
        )
    except Exception as exc:
        logger.warning("County polygon load failed for GENZ %d: %s", genz_year, exc)

    if not state_polygons and not county_polygons:
        logger.error(
            "NO polygons loaded for GENZ year %d. dim_geo.geom will remain NULL. "
            "Check network access to %s",
            genz_year, GENZ_ROOT,
        )

    return state_polygons, county_polygons


def sync_geo_dim(
    source_year: Optional[int] = None,
    min_year: int = 2010,
) -> int:
    """
    Upsert US + states + counties into silver_ref.dim_geo.

    If source_year is None, auto-resolve the latest Gazetteer year available.
    Returns number of rows upserted.
    """
    hook = _get_hook()
    now = datetime.now(timezone.utc)

    latest_year = source_year or resolve_latest_gazetteer_year(min_year=min_year)
    years = []
    years_checked = []
    
    for y in range(latest_year, min_year - 1, -1):
        years_checked.append(y)
        # Counties file is required; states file is optional (Census stopped publishing in 2023+)
        counties_ok = _url_exists(_counties_url(y))
        if counties_ok:
            years.append(y)
            logger.info("Gazetteer year=%s: FOUND (will load counties)", y)
        else:
            logger.info("Gazetteer year=%s: SKIPPED (counties file missing)", y)

    if not years:
        raise RuntimeError("No Gazetteer years available for geo_dim sync.")

    logger.info(
        "Loading Gazetteer years for dim_geo: %s..%s (%s total years available; checked %s). "
        "Note: Counties file required; states file optional (not available for 2023+).",
        min(years),
        max(years),
        len(years),
        len(years_checked),
    )
    if len(years) == 1:
        logger.warning(
            "Only one Gazetteer year available (%s). Historical coverage will be limited. "
            "Check network access or URL availability for earlier years.",
            years[0],
        )

    yearly_frames: list[pl.DataFrame] = []
    state_polygons, county_polygons = _load_polygon_lookup()
    state_poly_df = (
        pl.DataFrame(
            {
                "state_fips": list(state_polygons.keys()),
                "geom_geojson": list(state_polygons.values()),
            }
        )
        if state_polygons
        else pl.DataFrame(schema={"state_fips": pl.Utf8, "geom_geojson": pl.Utf8})
    )
    county_poly_df = (
        pl.DataFrame(
            {
                "geoid5": list(county_polygons.keys()),
                "geom_geojson": list(county_polygons.values()),
            }
        )
        if county_polygons
        else pl.DataFrame(schema={"geoid5": pl.Utf8, "geom_geojson": pl.Utf8})
    )

    def _coord_expr(frame: pl.DataFrame, source_col: str, out_col: str) -> pl.Expr:
        if source_col in frame.columns:
            return (
                pl.col(source_col)
                .cast(pl.Utf8)
                .str.strip_chars()
                .cast(pl.Float64, strict=False)
                .alias(out_col)
            )
        return pl.lit(None, dtype=pl.Float64).alias(out_col)

    for y in years:
        states_url = _states_url(y)
        counties_url = _counties_url(y)

        us_df = pl.DataFrame([
            {
                "geo_level": "us",
                "geo_id": "us:1",
                "state_fips": None,
                "county_fips": None,
                "name": "United States",
                "state_name": None,
                "county_name": None,
                "latitude": None,
                "longitude": None,
                "geom_geojson": None,
                "is_active": True,
                "source": "census_gazetteer",
                "source_year": y,
                "ingested_at": now,
            }
        ])

        # States file is optional (Census stopped publishing it in 2023+)
        st_df = None
        if _url_exists(states_url):
            try:
                st = _fetch_zipped_tsv(states_url)
                st_df = (
                    st.select([
                        pl.col("GEOID").cast(pl.Utf8).str.zfill(2).alias("state_fips"),
                        pl.col("NAME").cast(pl.Utf8).alias("state_name"),
                        _coord_expr(st, "INTPTLAT", "latitude"),
                        _coord_expr(st, "INTPTLONG", "longitude"),
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
                if state_poly_df.height > 0:
                    st_df = st_df.join(state_poly_df, on="state_fips", how="left")
                else:
                    st_df = st_df.with_columns(
                        pl.lit(None, dtype=pl.Utf8).alias("geom_geojson")
                    )
                st_df = st_df.with_columns([
                    pl.col("state_fips").cast(pl.Utf8),
                    pl.col("county_fips").cast(pl.Utf8),
                    pl.col("is_active").cast(pl.Boolean),
                    pl.col("source_year").cast(pl.Int32),
                ])
                yearly_frames.append(st_df)
            except Exception as e:
                logger.warning("Failed to load states for year=%s: %s", y, e)
        else:
            logger.debug("States file not available for year=%s (expected for 2023+)", y)

        # Counties file is required
        co = _fetch_zipped_tsv(counties_url)

        co_df = (
            co.select([
                pl.col("GEOID").cast(pl.Utf8).str.zfill(5).alias("geoid5"),
                pl.col("NAME").cast(pl.Utf8).alias("county_name"),
                _coord_expr(co, "INTPTLAT", "latitude"),
                _coord_expr(co, "INTPTLONG", "longitude"),
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
        )
        if county_poly_df.height > 0:
            co_df = co_df.join(county_poly_df, on="geoid5", how="left")
        else:
            co_df = co_df.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias("geom_geojson")
            )
        co_df = co_df.drop("geoid5")

        co_df = co_df.with_columns([
            pl.col("state_fips").cast(pl.Utf8),
            pl.col("county_fips").cast(pl.Utf8),
            pl.col("is_active").cast(pl.Boolean),
            pl.col("source_year").cast(pl.Int32),
        ])

        # Optionally attach state_name if state_df exists
        if st_df is not None:
            df_states = st_df.select(["state_fips", "state_name"])
            co_df = co_df.join(df_states, on="state_fips", how="left")

        yearly_frames.append(us_df)
        yearly_frames.append(co_df)

    target_cols = [
        "geo_level",
        "geo_id",
        "state_fips",
        "county_fips",
        "name",
        "state_name",
        "county_name",
        "latitude",
        "longitude",
        "geom_geojson",
        "is_active",
        "source",
        "source_year",
        "ingested_at",
    ]

    def _ensure_cols(df: pl.DataFrame) -> pl.DataFrame:
        missing = [c for c in target_cols if c not in df.columns]
        if missing:
            df = df.with_columns([pl.lit(None).alias(c) for c in missing])
        return df.select(target_cols)

    yearly_frames = [_ensure_cols(df) for df in yearly_frames]

    df_all = pl.concat(yearly_frames, how="vertical_relaxed")

    df_all = df_all.sort(["geo_level", "geo_id", "source_year"])
    df = df_all.group_by(["geo_level", "geo_id"]).agg([
        pl.col("state_fips").last().alias("state_fips"),
        pl.col("county_fips").last().alias("county_fips"),
        pl.col("name").last().alias("name"),
        pl.col("state_name").last().alias("state_name"),
        pl.col("county_name").last().alias("county_name"),
        pl.col("latitude").last().alias("latitude"),
        pl.col("longitude").last().alias("longitude"),
        pl.col("geom_geojson").last().alias("geom_geojson"),
        pl.col("is_active").last().alias("is_active"),
        pl.col("source").last().alias("source"),
        pl.col("source_year").last().alias("source_year"),
        pl.col("source_year").min().alias("first_seen_year"),
        pl.col("source_year").max().alias("last_seen_year"),
        pl.col("ingested_at").max().alias("ingested_at"),
    ])

    # Backfill county/state-equivalent state_name from canonical state rows.
    # This avoids null state_name when some years have counties but no states file.
    state_lookup = (
        df.filter(
            (pl.col("geo_level") == "state")
            & pl.col("state_fips").is_not_null()
            & pl.col("state_name").is_not_null()
        )
        .sort("source_year")
        .group_by("state_fips")
        .agg(pl.col("state_name").last().alias("state_name_lookup"))
    )

    df = (
        df.join(state_lookup, on="state_fips", how="left")
        .with_columns(
            pl.when(pl.col("geo_level").is_in(["county", "state"]))
            .then(pl.coalesce([pl.col("state_name"), pl.col("state_name_lookup")]))
            .otherwise(pl.col("state_name"))
            .alias("state_name")
        )
        .drop("state_name_lookup")
    )

    sql = """
        INSERT INTO silver_ref.dim_geo (
            geo_level, geo_id, state_fips, county_fips,
            name, state_name, county_name, latitude, longitude, geom,
            is_active, source, source_year,
            first_seen_year, last_seen_year,
            ingested_at
        )
        VALUES (
            %(geo_level)s, %(geo_id)s, %(state_fips)s, %(county_fips)s,
            %(name)s, %(state_name)s, %(county_name)s, %(latitude)s, %(longitude)s,
            ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%(geom_geojson)s), 4326)),
            %(is_active)s, %(source)s, %(source_year)s,
            %(first_seen_year)s, %(last_seen_year)s,
            %(ingested_at)s
        )
        ON CONFLICT (geo_level, geo_id)
        DO UPDATE SET
            state_fips   = EXCLUDED.state_fips,
            county_fips  = EXCLUDED.county_fips,
            name         = EXCLUDED.name,
            state_name   = EXCLUDED.state_name,
            county_name  = EXCLUDED.county_name,
            latitude     = EXCLUDED.latitude,
            longitude    = EXCLUDED.longitude,
            geom         = COALESCE(EXCLUDED.geom, silver_ref.dim_geo.geom),
            is_active    = EXCLUDED.is_active,
            source       = EXCLUDED.source,
            source_year  = EXCLUDED.source_year,
            first_seen_year = EXCLUDED.first_seen_year,
            last_seen_year  = EXCLUDED.last_seen_year,
            ingested_at  = EXCLUDED.ingested_at;
    """

    rows = df.to_dicts()
    with hook.get_conn() as conn, conn.cursor() as cur:
        for r in rows:
            cur.execute(sql, r)
        conn.commit()

    logger.info(
        "dim_geo sync complete: %s rows upserted (Gazetteer years %s..%s, includes counties%s)",
        len(rows),
        min(years),
        max(years),
        " + optional states where available" if max(years) >= 2024 else "",
    )
    return len(rows)
