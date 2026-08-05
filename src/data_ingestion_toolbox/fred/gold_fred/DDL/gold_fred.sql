-- fred/gold_fred/DDL/gold_fred.sql
-- REFACTORED: Source-First Architecture
-- Subject-scoped gold DDL for FRED objects — no unified wide table.
-- Per-source serving table with FRED-specific columns only.

CREATE SCHEMA IF NOT EXISTS gold_glossary;
CREATE SCHEMA IF NOT EXISTS gold_fred;
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE OR REPLACE VIEW gold_glossary.dim_geo AS
SELECT
    geo_sk,
    geo_level,
    geo_id,
    state_fips,
    county_fips,
    name,
    state_name,
    county_name,
    latitude,
    longitude,
    geom,
    ST_AsGeoJSON(geom)::TEXT AS geo_polygon_geojson,
    is_active,
    source,
    source_year,
    first_seen_year,
    last_seen_year,
    ingested_at
FROM silver_ref.dim_geo;

CREATE TABLE IF NOT EXISTS gold_glossary.dim_source_system (
    source_system_sk BIGSERIAL PRIMARY KEY,
    source_code      TEXT NOT NULL UNIQUE,
    source_name      TEXT NOT NULL,
    source_type      TEXT NOT NULL CHECK (source_type IN ('PRIMARY', 'REPUBLISHER', 'CURATED')),
    reference_url    TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO gold_glossary.dim_source_system (source_code, source_name, source_type, reference_url)
VALUES
    ('CENSUS_ACS', 'US Census ACS', 'PRIMARY', 'https://www.census.gov/programs-surveys/acs'),
    ('BLS', 'Bureau of Labor Statistics', 'PRIMARY', 'https://www.bls.gov/'),
    ('FRED', 'Federal Reserve Economic Data', 'REPUBLISHER', 'https://fred.stlouisfed.org/')
ON CONFLICT (source_code) DO UPDATE
SET source_name = EXCLUDED.source_name,
    source_type = EXCLUDED.source_type,
    reference_url = EXCLUDED.reference_url,
    updated_at = NOW();

CREATE TABLE IF NOT EXISTS gold_glossary.dim_metric_catalog (
    metric_catalog_sk      BIGSERIAL PRIMARY KEY,
    metric_code            TEXT NOT NULL UNIQUE,
    metric_display_name    TEXT NOT NULL,
    source_code            TEXT NOT NULL REFERENCES gold_glossary.dim_source_system(source_code),
    source_object_type     TEXT NOT NULL CHECK (source_object_type IN ('ACS_VARIABLE', 'BLS_SERIES', 'FRED_SERIES', 'COMPOSITE_VIEW')),
    business_definition    TEXT,
    caveats                TEXT,
    valid_geo_grains       TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    valid_time_grains      TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    dashboard_suitability  TEXT NOT NULL DEFAULT 'PUBLIC_SAFE'
        CHECK (dashboard_suitability IN ('PUBLIC_SAFE', 'INTERNAL_ONLY', 'EXPERIMENTAL')),
    comparability_group    TEXT,
    do_not_compare_with    TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    recommended_aggregation TEXT,
    owner_team             TEXT,
    is_active              BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold_glossary.serving_refresh_state (
    source_code                 TEXT PRIMARY KEY
        REFERENCES gold_glossary.dim_source_system(source_code),
    last_silver_ingested_at     TIMESTAMPTZ NOT NULL DEFAULT '-infinity'::TIMESTAMPTZ,
    last_refresh_started_at     TIMESTAMPTZ,
    last_refresh_completed_at   TIMESTAMPTZ,
    last_window_start           DATE,
    last_window_end             DATE,
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold_glossary.serving_refresh_chunk_state (
    source_code                         TEXT NOT NULL
        REFERENCES gold_glossary.dim_source_system(source_code),
    chunk_start                         DATE NOT NULL,
    chunk_end                           DATE NOT NULL,
    target_silver_ingested_at           TIMESTAMPTZ NOT NULL,
    completed_silver_ingested_at        TIMESTAMPTZ,
    status                              TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETE', 'FAILED')),
    attempt_count                       INTEGER NOT NULL DEFAULT 0,
    last_refresh_started_at             TIMESTAMPTZ,
    last_refresh_completed_at           TIMESTAMPTZ,
    last_error                          TEXT,
    updated_at                          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_code, chunk_start, chunk_end),
    CHECK (chunk_end >= chunk_start)
);

CREATE INDEX IF NOT EXISTS ix_serving_refresh_chunk_state_status
    ON gold_glossary.serving_refresh_chunk_state (source_code, status, chunk_start);

CREATE TABLE IF NOT EXISTS gold_glossary.bridge_metric_fred_series (
    metric_catalog_sk BIGINT NOT NULL REFERENCES gold_glossary.dim_metric_catalog(metric_catalog_sk),
    fred_series_sk    BIGINT NOT NULL,
    PRIMARY KEY (metric_catalog_sk, fred_series_sk)
);

CREATE TABLE IF NOT EXISTS gold_glossary.dim_geo_latest (
    geo_id       TEXT PRIMARY KEY,
    geo_level    TEXT,
    state_fips   TEXT,
    county_fips  TEXT,
    state_name   TEXT,
    county_name  TEXT,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    geo_geom     geometry(MultiPolygon, 4326),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_dim_geo_latest_geo_geom
    ON gold_glossary.dim_geo_latest USING GIST (geo_geom);

DROP PROCEDURE IF EXISTS gold_glossary.refresh_dim_geo_latest();
CREATE OR REPLACE PROCEDURE gold_glossary.refresh_dim_geo_latest()
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('gold_glossary.refresh_dim_geo_latest'));

    INSERT INTO gold_glossary.dim_geo_latest (
        geo_id,
        geo_level,
        state_fips,
        county_fips,
        state_name,
        county_name,
        latitude,
        longitude,
        geo_geom,
        refreshed_at
    )
    SELECT DISTINCT ON (g.geo_id)
        g.geo_id,
        CASE
            WHEN g.geo_level = 'us' THEN 'NATIONAL'
            WHEN g.geo_level = 'state' THEN 'STATE'
            WHEN g.geo_level = 'county' THEN 'COUNTY'
            ELSE UPPER(g.geo_level)
        END,
        CASE WHEN g.state_fips IS NOT NULL THEN LPAD(g.state_fips::TEXT, 2, '0') ELSE NULL END,
        CASE WHEN g.county_fips IS NOT NULL THEN LPAD(g.county_fips::TEXT, 3, '0') ELSE NULL END,
        g.state_name,
        g.county_name,
        g.latitude,
        g.longitude,
        g.geom,
        NOW()
    FROM gold_glossary.dim_geo g
    WHERE g.is_active = TRUE
    ORDER BY g.geo_id, g.source_year DESC NULLS LAST, g.ingested_at DESC
    ON CONFLICT (geo_id) DO UPDATE
    SET geo_level = EXCLUDED.geo_level,
        state_fips = EXCLUDED.state_fips,
        county_fips = EXCLUDED.county_fips,
        state_name = EXCLUDED.state_name,
        county_name = EXCLUDED.county_name,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        geo_geom = EXCLUDED.geo_geom,
        refreshed_at = NOW()
    WHERE (
        gold_glossary.dim_geo_latest.geo_level,
        gold_glossary.dim_geo_latest.state_fips,
        gold_glossary.dim_geo_latest.county_fips,
        gold_glossary.dim_geo_latest.state_name,
        gold_glossary.dim_geo_latest.county_name,
        gold_glossary.dim_geo_latest.latitude,
        gold_glossary.dim_geo_latest.longitude,
        gold_glossary.dim_geo_latest.geo_geom
    ) IS DISTINCT FROM (
        EXCLUDED.geo_level,
        EXCLUDED.state_fips,
        EXCLUDED.county_fips,
        EXCLUDED.state_name,
        EXCLUDED.county_name,
        EXCLUDED.latitude,
        EXCLUDED.longitude,
        EXCLUDED.geo_geom
    );

    DELETE FROM gold_glossary.dim_geo_latest d
    WHERE NOT EXISTS (
        SELECT 1
        FROM gold_glossary.dim_geo g
        WHERE g.is_active = TRUE
          AND g.geo_id = d.geo_id
    );
END;
$$;

CREATE TABLE IF NOT EXISTS gold_fred.dim_fred_series (
    fred_series_sk           BIGSERIAL PRIMARY KEY,
    series_id                TEXT NOT NULL UNIQUE,
    series_title             TEXT,
    source_provider          TEXT,
    original_source_name     TEXT,
    is_primary_source_series BOOLEAN NOT NULL DEFAULT FALSE,
    is_republished_series    BOOLEAN NOT NULL DEFAULT TRUE,
    frequency                TEXT,
    units                    TEXT,
    seasonal_adjustment      TEXT,
    transformation_method    TEXT,
    realtime_available       BOOLEAN,
    lineage_notes            TEXT,
    reference_url            TEXT,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- FRED FACT VIEW (source of truth)
-- ============================================================

CREATE OR REPLACE VIEW gold_fred.fact_fred_observation AS
SELECT
    'us:1'       AS geo_id,
    'NATIONAL'   AS geo_level,
    s.time_sk,
    s.observation_date,
    s.duration_start,
    s.duration_end,
    fs.fred_series_sk,
    s.value,
    NULL::DATE   AS realtime_start,
    NULL::DATE   AS realtime_end,
    s.frequency,
    s.unit_of_measure AS units,
    s.seasonal_adjustment,
    NULL::TEXT   AS transform_applied,
    'FRED'       AS source_provider,
    CURRENT_DATE AS as_of_date,
    s.ingested_at AS updated_at
FROM silver_fred.fact_economic_indicators s
JOIN gold_fred.dim_fred_series fs ON fs.series_id = s.series_id
WHERE s.is_missing = FALSE
  AND s.series_id IS NOT NULL
  AND s.series_id <> '';

-- ============================================================
-- FRED-SCOPED SERVING TABLE (Source-First: FRED-specific columns only)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_fred.rpt_fred_observations (
    source_code                TEXT NOT NULL DEFAULT 'FRED',
    observation_date           DATE NOT NULL,
    duration_start             DATE,
    duration_end               DATE,
    time_sk                    INTEGER,
    as_of_date                 DATE NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL,
    geo_id                     TEXT NOT NULL DEFAULT 'us:1',
    geo_level                  TEXT NOT NULL DEFAULT 'NATIONAL',
    state_fips                 TEXT,
    county_fips                TEXT,
    state_name                 TEXT,
    county_name                TEXT,
    geo_latitude               DOUBLE PRECISION,
    geo_longitude              DOUBLE PRECISION,
    -- FRED-specific columns (no NULLs for these)
    series_id                  TEXT NOT NULL,
    series_title               TEXT,
    value                      NUMERIC NOT NULL,
    value_type                 TEXT,
    units                      TEXT,
    frequency                  TEXT,
    seasonal_adjustment_status TEXT,
    source_provider            TEXT,
    original_source_name       TEXT,
    is_primary_source_series   BOOLEAN,
    is_republished_series      BOOLEAN,
    transformation_method      TEXT,
    realtime_start             DATE,
    realtime_end               DATE,
    -- Metric catalog association
    metric_code                TEXT,
    metric_display_name        TEXT,
    dashboard_suitability      TEXT,
    business_definition        TEXT,
    caveats                    TEXT,
    comparability_group        TEXT,
    do_not_compare_with        TEXT[],
    recommended_aggregation    TEXT,
    owner_team                 TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_fred_observations_nk
    ON gold_fred.rpt_fred_observations (
        observation_date,
        series_id,
        COALESCE(metric_code, ''),
        COALESCE(realtime_start, '0001-01-01'::DATE),
        COALESCE(realtime_end, '0001-01-01'::DATE)
    );

CREATE INDEX IF NOT EXISTS ix_rpt_fred_observations_metric_date
    ON gold_fred.rpt_fred_observations (metric_code, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_fred_observations_series_date
    ON gold_fred.rpt_fred_observations (series_id, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_fred_observations_metric_geo_date
    ON gold_fred.rpt_fred_observations (metric_code, geo_id, observation_date);

CREATE INDEX IF NOT EXISTS ix_rpt_fred_observations_updated_at
    ON gold_fred.rpt_fred_observations (updated_at DESC);

CREATE INDEX IF NOT EXISTS ix_rpt_fred_latest_selection
    ON gold_fred.rpt_fred_observations (
        series_id,
        metric_code,
        observation_date DESC,
        realtime_start DESC NULLS LAST,
        realtime_end DESC NULLS LAST,
        updated_at DESC
    );

-- ============================================================
-- FRED MATERIALIZED VIEW (Per-source latest)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_fred.mv_fred_latest
    (LIKE gold_fred.rpt_fred_observations INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMMENTS);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_fred_latest
    ON gold_fred.mv_fred_latest (
        series_id,
        COALESCE(metric_code, ''),
        COALESCE(realtime_start, '0001-01-01'::DATE),
        COALESCE(realtime_end, '0001-01-01'::DATE)
    );

CREATE INDEX IF NOT EXISTS ix_mv_fred_latest_source_metric
    ON gold_fred.mv_fred_latest (source_code, metric_code);

CREATE INDEX IF NOT EXISTS ix_mv_fred_latest_observation_date
    ON gold_fred.mv_fred_latest (observation_date);

CREATE INDEX IF NOT EXISTS ix_mv_fred_latest_metric_geo
    ON gold_fred.mv_fred_latest (metric_code, geo_id);

-- ============================================================
-- FRED REFRESH PROCEDURES
-- ============================================================

DROP PROCEDURE IF EXISTS gold_fred.refresh_rpt_fred_observations(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold_fred.refresh_rpt_fred_observations(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_started_at TIMESTAMPTZ := clock_timestamp();
    v_deleted_rows BIGINT;
    v_inserted_rows BIGINT;
    v_affected_keys BIGINT;
BEGIN
    RAISE NOTICE '[FRED RPT CHUNK] status=STARTED start=% end=%', p_start_date, p_end_date;

    DROP TABLE IF EXISTS pg_temp.gold_fred_affected_keys;
    CREATE TEMP TABLE gold_fred_affected_keys (
        series_id   TEXT NOT NULL,
        metric_code TEXT NOT NULL,
        PRIMARY KEY (series_id, metric_code)
    ) ON COMMIT DROP;

    INSERT INTO gold_fred_affected_keys (series_id, metric_code)
    SELECT DISTINCT d.series_id, d.metric_code
    FROM gold_fred.rpt_fred_observations d
    WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
      AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
    ON CONFLICT DO NOTHING;

    DELETE FROM gold_fred.rpt_fred_observations
    WHERE (p_start_date IS NULL OR observation_date >= p_start_date)
      AND (p_end_date IS NULL OR observation_date <= p_end_date);
    GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;

    INSERT INTO gold_fred.rpt_fred_observations (
        source_code,
        observation_date,
        duration_start,
        duration_end,
        time_sk,
        as_of_date,
        updated_at,
        geo_id,
        geo_level,
        state_fips,
        county_fips,
        state_name,
        county_name,
        geo_latitude,
        geo_longitude,
        metric_code,
        metric_display_name,
        dashboard_suitability,
        business_definition,
        caveats,
        comparability_group,
        do_not_compare_with,
        recommended_aggregation,
        owner_team,
        value,
        units,
        seasonal_adjustment_status,
        series_id,
        series_title,
        source_provider,
        original_source_name,
        is_primary_source_series,
        is_republished_series,
        frequency,
        transformation_method,
        realtime_start,
        realtime_end
    )
    SELECT
        'FRED',
        f.observation_date,
        f.duration_start,
        f.duration_end,
        f.time_sk,
        f.as_of_date,
        f.updated_at::TIMESTAMPTZ,
        'us:1',
        COALESCE(gl.geo_level, 'NATIONAL'),
        gl.state_fips,
        gl.county_fips,
        gl.state_name,
        gl.county_name,
        gl.latitude,
        gl.longitude,
        COALESCE(mc.metric_code,          'FRED:' || fs.series_id),
        COALESCE(mc.metric_display_name,  fs.series_title),
        COALESCE(mc.dashboard_suitability,'EXPERIMENTAL'),
        mc.business_definition,
        mc.caveats,
        mc.comparability_group,
        COALESCE(mc.do_not_compare_with, ARRAY[]::TEXT[]),
        mc.recommended_aggregation,
        mc.owner_team,
        f.value,
        COALESCE(f.units, fs.units),
        COALESCE(f.seasonal_adjustment, fs.seasonal_adjustment),
        fs.series_id,
        fs.series_title,
        COALESCE(f.source_provider, fs.source_provider),
        fs.original_source_name,
        fs.is_primary_source_series,
        fs.is_republished_series,
        COALESCE(f.frequency, fs.frequency),
        COALESCE(fs.transformation_method, f.transform_applied),
        f.realtime_start,
        f.realtime_end
    FROM gold_fred.fact_fred_observation f
    JOIN gold_fred.dim_fred_series fs ON fs.fred_series_sk = f.fred_series_sk
    LEFT JOIN gold_glossary.dim_geo_latest gl ON gl.geo_id = 'us:1'
    LEFT JOIN gold_glossary.bridge_metric_fred_series bmf ON bmf.fred_series_sk = f.fred_series_sk
    LEFT JOIN gold_glossary.dim_metric_catalog mc
        ON mc.metric_catalog_sk = bmf.metric_catalog_sk
       AND mc.is_active = TRUE
    WHERE (p_start_date IS NULL OR f.observation_date >= p_start_date)
      AND (p_end_date IS NULL OR f.observation_date <= p_end_date);
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    INSERT INTO gold_fred_affected_keys (series_id, metric_code)
    SELECT DISTINCT d.series_id, d.metric_code
    FROM gold_fred.rpt_fred_observations d
    WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
      AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
    ON CONFLICT DO NOTHING;

    SELECT COUNT(*) INTO v_affected_keys FROM gold_fred_affected_keys;
    RAISE NOTICE
        '[FRED RPT CHUNK] status=COMPLETE start=% end=% deleted_rows=% inserted_rows=% affected_keys=% duration_ms=%',
        p_start_date,
        p_end_date,
        v_deleted_rows,
        v_inserted_rows,
        v_affected_keys,
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;

DROP PROCEDURE IF EXISTS gold_fred.refresh_mv_fred_latest();
CREATE OR REPLACE PROCEDURE gold_fred.refresh_mv_fred_latest(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_started_at TIMESTAMPTZ := clock_timestamp();
    v_deleted_rows BIGINT;
    v_inserted_rows BIGINT;
BEGIN
    RAISE NOTICE '[FRED LATEST CHUNK] status=STARTED start=% end=%', p_start_date, p_end_date;

    -- Rebuild FRED slice — bounded by N_series (national only).
    IF to_regclass('pg_temp.gold_fred_affected_keys') IS NULL THEN
        CREATE TEMP TABLE gold_fred_affected_keys (
            series_id   TEXT NOT NULL,
            metric_code TEXT NOT NULL,
            PRIMARY KEY (series_id, metric_code)
        ) ON COMMIT DROP;

        INSERT INTO gold_fred_affected_keys (series_id, metric_code)
        SELECT DISTINCT d.series_id, d.metric_code
        FROM gold_fred.rpt_fred_observations d
        WHERE (p_start_date IS NULL OR d.observation_date >= p_start_date)
          AND (p_end_date IS NULL OR d.observation_date <= p_end_date)
        ON CONFLICT DO NOTHING;
    END IF;

    DELETE FROM gold_fred.mv_fred_latest m
    USING gold_fred_affected_keys k
    WHERE m.series_id = k.series_id
      AND m.metric_code = k.metric_code;
    GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;

    INSERT INTO gold_fred.mv_fred_latest
    SELECT DISTINCT ON (d.series_id, d.metric_code)
        d.*
    FROM gold_fred.rpt_fred_observations d
    JOIN gold_fred_affected_keys k
      ON k.series_id = d.series_id
     AND k.metric_code = d.metric_code
    ORDER BY
        d.series_id,
        d.metric_code,
        d.observation_date DESC,
        d.realtime_start DESC NULLS LAST,
        d.realtime_end DESC NULLS LAST,
        d.updated_at DESC;
    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    RAISE NOTICE
        '[FRED LATEST CHUNK] status=COMPLETE start=% end=% deleted_rows=% inserted_rows=% duration_ms=%',
        p_start_date,
        p_end_date,
        v_deleted_rows,
        v_inserted_rows,
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);

END;
$$;

DROP PROCEDURE IF EXISTS gold_fred.refresh_dashboard_serving_layer_fred(DATE, DATE);
DROP PROCEDURE IF EXISTS gold_fred.refresh_dashboard_serving_layer_fred(DATE, DATE, BOOLEAN);
CREATE OR REPLACE PROCEDURE gold_fred.refresh_dashboard_serving_layer_fred(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
    p_force_full BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_started_at TIMESTAMPTZ := clock_timestamp();
    v_step_started TIMESTAMPTZ;
    v_watermark TIMESTAMPTZ;
    v_high_watermark TIMESTAMPTZ;
    v_effective_start DATE;
    v_effective_end DATE;
BEGIN
    SET LOCAL statement_timeout = '30min';
    SET LOCAL lock_timeout = '30s';

    INSERT INTO gold_glossary.serving_refresh_state (
        source_code,
        last_silver_ingested_at,
        last_refresh_completed_at
    )
    SELECT
        'FRED',
        COALESCE(MAX(r.updated_at), '-infinity'::TIMESTAMPTZ),
        CASE WHEN COUNT(*) > 0 THEN NOW() ELSE NULL END
    FROM gold_fred.rpt_fred_observations r
    ON CONFLICT (source_code) DO NOTHING;

    SELECT last_silver_ingested_at
      INTO v_watermark
      FROM gold_glossary.serving_refresh_state
     WHERE source_code = 'FRED'
     FOR UPDATE;

    UPDATE gold_glossary.serving_refresh_state
       SET last_refresh_started_at = v_started_at,
           updated_at = NOW()
     WHERE source_code = 'FRED';

    SELECT MAX(s.ingested_at), MIN(s.observation_date), MAX(s.observation_date)
      INTO v_high_watermark, v_effective_start, v_effective_end
      FROM silver_fred.fact_economic_indicators s
     WHERE s.is_missing = FALSE
       AND (p_start_date IS NULL OR s.observation_date >= p_start_date)
       AND (p_end_date IS NULL OR s.observation_date <= p_end_date)
       AND (p_force_full OR s.ingested_at > v_watermark);

    IF v_effective_start IS NULL THEN
        UPDATE gold_glossary.serving_refresh_state
           SET last_refresh_completed_at = clock_timestamp(),
               updated_at = NOW()
         WHERE source_code = 'FRED';
        RAISE NOTICE '[FRED DASHBOARD REFRESH] no changed silver rows after watermark=%', v_watermark;
        RETURN;
    END IF;

    RAISE NOTICE '[FRED DASHBOARD REFRESH] start window_start=% window_end=% watermark=% force_full=%',
        v_effective_start, v_effective_end, v_watermark, p_force_full;

    v_step_started := clock_timestamp();
    CALL gold_fred.refresh_rpt_fred_observations(v_effective_start, v_effective_end);
    RAISE NOTICE
        '[FRED DASHBOARD REFRESH] step=refresh_rpt_fred_observations duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_step_started)) * 1000)::NUMERIC(18,2);

    v_step_started := clock_timestamp();
    CALL gold_fred.refresh_mv_fred_latest(v_effective_start, v_effective_end);
    RAISE NOTICE
        '[FRED DASHBOARD REFRESH] step=refresh_mv_fred_latest duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_step_started)) * 1000)::NUMERIC(18,2);

    UPDATE gold_glossary.serving_refresh_state
       SET last_silver_ingested_at = CASE
               WHEN p_force_full AND (p_start_date IS NOT NULL OR p_end_date IS NOT NULL)
                   THEN v_watermark
               ELSE GREATEST(v_watermark, v_high_watermark)
           END,
           last_refresh_completed_at = clock_timestamp(),
           last_window_start = v_effective_start,
           last_window_end = v_effective_end,
           updated_at = NOW()
     WHERE source_code = 'FRED';

    RAISE NOTICE
        '[FRED DASHBOARD REFRESH] completed total_duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;
