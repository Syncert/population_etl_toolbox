-- fred/gold_fred/DDL/gold_fred.sql
-- Subject-scoped gold DDL for FRED objects and serving refresh.

CREATE SCHEMA IF NOT EXISTS gold;
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE OR REPLACE VIEW gold.dim_geo AS
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
    is_active,
    source,
    source_year,
    first_seen_year,
    last_seen_year,
    ingested_at
FROM silver_ref.dim_geo;

CREATE TABLE IF NOT EXISTS gold.dim_source_system (
    source_system_sk BIGSERIAL PRIMARY KEY,
    source_code      TEXT NOT NULL UNIQUE,
    source_name      TEXT NOT NULL,
    source_type      TEXT NOT NULL CHECK (source_type IN ('PRIMARY', 'REPUBLISHER', 'CURATED')),
    reference_url    TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO gold.dim_source_system (source_code, source_name, source_type, reference_url)
VALUES
    ('CENSUS_ACS', 'US Census ACS', 'PRIMARY', 'https://www.census.gov/programs-surveys/acs'),
    ('BLS', 'Bureau of Labor Statistics', 'PRIMARY', 'https://www.bls.gov/'),
    ('FRED', 'Federal Reserve Economic Data', 'REPUBLISHER', 'https://fred.stlouisfed.org/')
ON CONFLICT (source_code) DO UPDATE
SET source_name = EXCLUDED.source_name,
    source_type = EXCLUDED.source_type,
    reference_url = EXCLUDED.reference_url,
    updated_at = NOW();

CREATE TABLE IF NOT EXISTS gold.dim_metric_catalog (
    metric_catalog_sk      BIGSERIAL PRIMARY KEY,
    metric_code            TEXT NOT NULL UNIQUE,
    metric_display_name    TEXT NOT NULL,
    source_code            TEXT NOT NULL REFERENCES gold.dim_source_system(source_code),
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

CREATE TABLE IF NOT EXISTS gold.bridge_metric_fred_series (
    metric_catalog_sk BIGINT NOT NULL REFERENCES gold.dim_metric_catalog(metric_catalog_sk),
    fred_series_sk    BIGINT NOT NULL,
    PRIMARY KEY (metric_catalog_sk, fred_series_sk)
);

CREATE TABLE IF NOT EXISTS gold.dim_geo_latest (
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

DROP PROCEDURE IF EXISTS gold.refresh_dim_geo_latest();
CREATE OR REPLACE PROCEDURE gold.refresh_dim_geo_latest()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE gold.dim_geo_latest;

    INSERT INTO gold.dim_geo_latest (
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
    FROM gold.dim_geo g
    WHERE g.is_active = TRUE
    ORDER BY g.geo_id, g.source_year DESC NULLS LAST, g.ingested_at DESC;
END;
$$;

CREATE TABLE IF NOT EXISTS gold.dim_fred_series (
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

CREATE TABLE IF NOT EXISTS gold.fact_fred_observation (
    fred_observation_sk BIGSERIAL PRIMARY KEY,
    geo_id              TEXT NOT NULL DEFAULT 'us:1',
    geo_level           TEXT NOT NULL DEFAULT 'NATIONAL' CHECK (geo_level = 'NATIONAL'),
    time_sk             INTEGER REFERENCES silver_ref.dim_time(time_sk),
    observation_date    DATE NOT NULL,
    duration_start      DATE,
    duration_end        DATE,
    fred_series_sk      BIGINT NOT NULL REFERENCES gold.dim_fred_series(fred_series_sk),
    value               NUMERIC,
    realtime_start      DATE,
    realtime_end        DATE,
    frequency           TEXT,
    units               TEXT,
    seasonal_adjustment TEXT,
    transform_applied   TEXT,
    source_provider     TEXT,
    as_of_date          DATE NOT NULL DEFAULT CURRENT_DATE,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (observation_date, fred_series_sk, realtime_start, realtime_end)
);

CREATE INDEX IF NOT EXISTS ix_fact_fred_obs_date ON gold.fact_fred_observation (observation_date);
CREATE INDEX IF NOT EXISTS ix_fact_fred_series_date ON gold.fact_fred_observation (fred_series_sk, observation_date);
CREATE INDEX IF NOT EXISTS ix_fact_fred_obs_brin ON gold.fact_fred_observation USING BRIN (observation_date);

CREATE TABLE IF NOT EXISTS gold.rpt_fred_observation_dashboard (
    source_code               TEXT NOT NULL DEFAULT 'FRED',
    observation_date          DATE NOT NULL,
    duration_start            DATE,
    duration_end              DATE,
    time_sk                   INTEGER,
    geo_id                    TEXT NOT NULL,
    geo_level                 TEXT NOT NULL,
    state_fips                TEXT,
    county_fips               TEXT,
    state_name                TEXT,
    county_name               TEXT,
    geo_latitude              DOUBLE PRECISION,
    geo_longitude             DOUBLE PRECISION,
    geo_geom                  geometry(MultiPolygon, 4326),
    as_of_date                DATE NOT NULL,
    updated_at                TIMESTAMPTZ NOT NULL,
    series_id                 TEXT NOT NULL,
    series_title              TEXT,
    source_provider           TEXT,
    original_source_name      TEXT,
    is_primary_source_series  BOOLEAN,
    is_republished_series     BOOLEAN,
    frequency                 TEXT,
    units                     TEXT,
    seasonal_adjustment       TEXT,
    transformation_method     TEXT,
    realtime_start            DATE,
    realtime_end              DATE,
    value                     NUMERIC,
    metric_code               TEXT,
    metric_display_name       TEXT,
    dashboard_suitability     TEXT,
    business_definition       TEXT,
    caveats                   TEXT,
    comparability_group       TEXT,
    do_not_compare_with       TEXT[],
    recommended_aggregation   TEXT,
    owner_team                TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_fred_dashboard_nk
    ON gold.rpt_fred_observation_dashboard (series_id, observation_date, realtime_start, realtime_end, metric_code);
CREATE INDEX IF NOT EXISTS ix_rpt_fred_dashboard_series_date
    ON gold.rpt_fred_observation_dashboard (series_id, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_fred_dashboard_metric_date
    ON gold.rpt_fred_observation_dashboard (metric_code, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_fred_dashboard_obs_brin
    ON gold.rpt_fred_observation_dashboard USING BRIN (observation_date);

CREATE TABLE IF NOT EXISTS gold.mv_fred_latest_dashboard
    (LIKE gold.rpt_fred_observation_dashboard INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_fred_latest_dashboard
    ON gold.mv_fred_latest_dashboard (geo_id, series_id, metric_code);
CREATE INDEX IF NOT EXISTS ix_mv_fred_latest_dashboard_metric_code
    ON gold.mv_fred_latest_dashboard (metric_code);
CREATE INDEX IF NOT EXISTS ix_mv_fred_latest_dashboard_observation_date
    ON gold.mv_fred_latest_dashboard (observation_date);

DROP PROCEDURE IF EXISTS gold.refresh_rpt_fred_observation_dashboard(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold.refresh_rpt_fred_observation_dashboard(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL gold.refresh_dim_geo_latest();

    IF p_start_date IS NULL OR p_end_date IS NULL THEN
        TRUNCATE TABLE gold.rpt_fred_observation_dashboard;

        INSERT INTO gold.rpt_fred_observation_dashboard
        SELECT
            'FRED',
            f.observation_date,
            f.duration_start,
            f.duration_end,
            f.time_sk,
            'us:1',
            COALESCE(gl.geo_level, f.geo_level, 'NATIONAL'),
            gl.state_fips,
            gl.county_fips,
            gl.state_name,
            gl.county_name,
            gl.latitude,
            gl.longitude,
            gl.geo_geom,
            f.as_of_date,
            f.updated_at,
            fs.series_id,
            fs.series_title,
            COALESCE(f.source_provider, fs.source_provider),
            fs.original_source_name,
            fs.is_primary_source_series,
            fs.is_republished_series,
            COALESCE(f.frequency, fs.frequency),
            COALESCE(f.units, fs.units),
            COALESCE(f.seasonal_adjustment, fs.seasonal_adjustment),
            COALESCE(fs.transformation_method, f.transform_applied),
            f.realtime_start,
            f.realtime_end,
            f.value,
            COALESCE(mc.metric_code, 'FRED:' || fs.series_id),
            COALESCE(mc.metric_display_name, fs.series_title),
            COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL'),
            mc.business_definition,
            mc.caveats,
            mc.comparability_group,
            COALESCE(mc.do_not_compare_with, ARRAY[]::TEXT[]),
            mc.recommended_aggregation,
            mc.owner_team
        FROM gold.fact_fred_observation f
        JOIN gold.dim_fred_series fs ON fs.fred_series_sk = f.fred_series_sk
        LEFT JOIN gold.dim_geo_latest gl ON gl.geo_id = 'us:1'
        LEFT JOIN gold.bridge_metric_fred_series bmf ON bmf.fred_series_sk = f.fred_series_sk
        LEFT JOIN gold.dim_metric_catalog mc
            ON mc.metric_catalog_sk = bmf.metric_catalog_sk
           AND mc.is_active = TRUE;
    ELSE
        DELETE FROM gold.rpt_fred_observation_dashboard
        WHERE observation_date BETWEEN p_start_date AND p_end_date;

        INSERT INTO gold.rpt_fred_observation_dashboard
        SELECT
            'FRED',
            f.observation_date,
            f.duration_start,
            f.duration_end,
            f.time_sk,
            'us:1',
            COALESCE(gl.geo_level, f.geo_level, 'NATIONAL'),
            gl.state_fips,
            gl.county_fips,
            gl.state_name,
            gl.county_name,
            gl.latitude,
            gl.longitude,
            gl.geo_geom,
            f.as_of_date,
            f.updated_at,
            fs.series_id,
            fs.series_title,
            COALESCE(f.source_provider, fs.source_provider),
            fs.original_source_name,
            fs.is_primary_source_series,
            fs.is_republished_series,
            COALESCE(f.frequency, fs.frequency),
            COALESCE(f.units, fs.units),
            COALESCE(f.seasonal_adjustment, fs.seasonal_adjustment),
            COALESCE(fs.transformation_method, f.transform_applied),
            f.realtime_start,
            f.realtime_end,
            f.value,
            COALESCE(mc.metric_code, 'FRED:' || fs.series_id),
            COALESCE(mc.metric_display_name, fs.series_title),
            COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL'),
            mc.business_definition,
            mc.caveats,
            mc.comparability_group,
            COALESCE(mc.do_not_compare_with, ARRAY[]::TEXT[]),
            mc.recommended_aggregation,
            mc.owner_team
        FROM gold.fact_fred_observation f
        JOIN gold.dim_fred_series fs ON fs.fred_series_sk = f.fred_series_sk
        LEFT JOIN gold.dim_geo_latest gl ON gl.geo_id = 'us:1'
        LEFT JOIN gold.bridge_metric_fred_series bmf ON bmf.fred_series_sk = f.fred_series_sk
        LEFT JOIN gold.dim_metric_catalog mc
            ON mc.metric_catalog_sk = bmf.metric_catalog_sk
           AND mc.is_active = TRUE
        WHERE f.observation_date BETWEEN p_start_date AND p_end_date;
    END IF;

    ANALYZE gold.rpt_fred_observation_dashboard;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_mv_fred_latest_dashboard(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold.refresh_mv_fred_latest_dashboard(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_start_date IS NULL OR p_end_date IS NULL THEN
        TRUNCATE TABLE gold.mv_fred_latest_dashboard;

        INSERT INTO gold.mv_fred_latest_dashboard
        SELECT DISTINCT ON (d.geo_id, d.series_id, d.metric_code)
            d.*
        FROM gold.rpt_fred_observation_dashboard d
        ORDER BY
            d.geo_id,
            d.series_id,
            d.metric_code,
            d.observation_date DESC,
            d.realtime_start DESC NULLS LAST,
            d.realtime_end DESC NULLS LAST,
            d.updated_at DESC;
    ELSE
        CREATE TEMP TABLE tmp_fred_touched_keys ON COMMIT DROP AS
        SELECT DISTINCT geo_id, series_id, metric_code
        FROM gold.rpt_fred_observation_dashboard
        WHERE observation_date BETWEEN p_start_date AND p_end_date;

        DELETE FROM gold.mv_fred_latest_dashboard mv
        USING tmp_fred_touched_keys t
        WHERE mv.geo_id = t.geo_id
          AND mv.series_id = t.series_id
          AND mv.metric_code = t.metric_code;

        INSERT INTO gold.mv_fred_latest_dashboard
        SELECT DISTINCT ON (d.geo_id, d.series_id, d.metric_code)
            d.*
        FROM gold.rpt_fred_observation_dashboard d
        JOIN tmp_fred_touched_keys t
          ON t.geo_id = d.geo_id
         AND t.series_id = d.series_id
         AND t.metric_code = d.metric_code
        ORDER BY
            d.geo_id,
            d.series_id,
            d.metric_code,
            d.observation_date DESC,
            d.realtime_start DESC NULLS LAST,
            d.realtime_end DESC NULLS LAST,
            d.updated_at DESC;
    END IF;

    ANALYZE gold.mv_fred_latest_dashboard;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_dashboard_serving_layer_fred(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold.refresh_dashboard_serving_layer_fred(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    SET LOCAL statement_timeout = 0;
    CALL gold.refresh_rpt_fred_observation_dashboard(p_start_date, p_end_date);
    CALL gold.refresh_mv_fred_latest_dashboard(p_start_date, p_end_date);
END;
$$;
