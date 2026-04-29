-- bls/gold_bls/DDL/gold_bls.sql
-- Subject-scoped gold DDL for BLS objects and serving refresh.

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
    ST_AsGeoJSON(geom)::TEXT AS geo_polygon_geojson,
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

CREATE TABLE IF NOT EXISTS gold.bridge_metric_bls_series (
    metric_catalog_sk BIGINT NOT NULL REFERENCES gold.dim_metric_catalog(metric_catalog_sk),
    bls_series_sk     BIGINT NOT NULL,
    PRIMARY KEY (metric_catalog_sk, bls_series_sk)
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

CREATE TABLE IF NOT EXISTS gold.dim_bls_survey (
    bls_survey_sk      BIGSERIAL PRIMARY KEY,
    program_code       TEXT NOT NULL UNIQUE,
    survey_name        TEXT NOT NULL,
    survey_universe    TEXT,
    observation_basis  TEXT NOT NULL CHECK (observation_basis IN ('PEOPLE', 'JOBS', 'PRICES', 'FLOWS')),
    primary_concept    TEXT,
    id_construction_type TEXT,
    comparison_warning TEXT,
    reference_url      TEXT,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.dim_bls_series (
    bls_series_sk              BIGSERIAL PRIMARY KEY,
    bls_survey_sk              BIGINT NOT NULL REFERENCES gold.dim_bls_survey(bls_survey_sk),
    program_code               TEXT NOT NULL,
    series_id                  TEXT NOT NULL UNIQUE,
    series_title               TEXT,
    measure_name               TEXT,
    measure_category           TEXT NOT NULL CHECK (
        measure_category IN (
            'EMPLOYMENT', 'UNEMPLOYMENT', 'LABOR_FORCE', 'PARTICIPATION', 'POPULATION',
            'EARNINGS', 'HOURS', 'PRICE_INDEX', 'OPENINGS', 'HIRES', 'QUITS', 'LAYOFFS', 'SEPARATIONS',
            'OTHER'
        )
    ),
    unit_of_measure            TEXT,
    value_type                 TEXT NOT NULL CHECK (value_type IN ('LEVEL', 'RATE', 'INDEX', 'PERCENT', 'CURRENCY', 'RATIO', 'OTHER')),
    seasonal_adjustment_status TEXT,
    geographic_level           TEXT,
    gold_metric_name           TEXT,
    analytic_role              TEXT,
    semantic_notes             TEXT,
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.fact_bls_observation (
    bls_observation_sk         BIGSERIAL PRIMARY KEY,
    geo_id                     TEXT NOT NULL,
    geo_level                  TEXT NOT NULL CHECK (geo_level IN ('NATIONAL', 'STATE', 'COUNTY')),
    state_id                   TEXT,
    state_name                 TEXT,
    county_id                  TEXT,
    county_name                TEXT,
    geo_latitude               DOUBLE PRECISION,
    geo_longitude              DOUBLE PRECISION,
    geo_geom                   geometry(MultiPolygon, 4326),
    time_sk                    INTEGER REFERENCES silver_ref.dim_time(time_sk),
    period_date                DATE NOT NULL,
    duration_start             DATE,
    duration_end               DATE,
    bls_survey_sk              BIGINT NOT NULL REFERENCES gold.dim_bls_survey(bls_survey_sk),
    bls_series_sk              BIGINT NOT NULL REFERENCES gold.dim_bls_series(bls_series_sk),
    program_code               TEXT NOT NULL,
    value                      NUMERIC,
    period_code                TEXT,
    seasonal_adjustment_status TEXT,
    observation_basis          TEXT NOT NULL CHECK (observation_basis IN ('PEOPLE', 'JOBS', 'PRICES', 'FLOWS')),
    measure_category           TEXT NOT NULL CHECK (
        measure_category IN (
            'EMPLOYMENT', 'UNEMPLOYMENT', 'LABOR_FORCE', 'PARTICIPATION', 'POPULATION',
            'EARNINGS', 'HOURS', 'PRICE_INDEX', 'OPENINGS', 'HIRES', 'QUITS', 'LAYOFFS', 'SEPARATIONS',
            'OTHER'
        )
    ),
    value_type                 TEXT NOT NULL CHECK (value_type IN ('LEVEL', 'RATE', 'INDEX', 'PERCENT', 'CURRENCY', 'RATIO', 'OTHER')),
    as_of_date                 DATE NOT NULL DEFAULT CURRENT_DATE,
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (geo_id, period_date, bls_series_sk)
);

CREATE INDEX IF NOT EXISTS ix_fact_bls_period_date ON gold.fact_bls_observation (period_date);
CREATE INDEX IF NOT EXISTS ix_fact_bls_geo_date ON gold.fact_bls_observation (geo_id, period_date);
CREATE INDEX IF NOT EXISTS ix_fact_bls_period_brin ON gold.fact_bls_observation USING BRIN (period_date);
CREATE INDEX IF NOT EXISTS ix_fact_bls_geo_geom ON gold.fact_bls_observation USING GIST (geo_geom);

CREATE TABLE IF NOT EXISTS gold.rpt_bls_observation_dashboard (
    source_code               TEXT NOT NULL DEFAULT 'BLS',
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
    program_code              TEXT NOT NULL,
    survey_name               TEXT,
    series_id                 TEXT NOT NULL,
    series_title              TEXT,
    gold_metric_name          TEXT,
    measure_name              TEXT,
    measure_category          TEXT,
    value_type                TEXT,
    unit_of_measure           TEXT,
    seasonal_adjustment_status TEXT,
    observation_basis         TEXT,
    value                     NUMERIC,
    metric_code               TEXT,
    metric_display_name       TEXT,
    dashboard_suitability     TEXT,
    business_definition       TEXT,
    metric_caveats            TEXT,
    comparison_warning        TEXT,
    comparability_group       TEXT,
    recommended_aggregation   TEXT,
    owner_team                TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_bls_dashboard_nk
    ON gold.rpt_bls_observation_dashboard (geo_id, observation_date, series_id, metric_code);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_geo_date
    ON gold.rpt_bls_observation_dashboard (geo_id, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_metric_date
    ON gold.rpt_bls_observation_dashboard (metric_code, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_obs_brin
    ON gold.rpt_bls_observation_dashboard USING BRIN (observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_geo_geom
    ON gold.rpt_bls_observation_dashboard USING GIST (geo_geom);

CREATE TABLE IF NOT EXISTS gold.mv_bls_latest_dashboard
    (LIKE gold.rpt_bls_observation_dashboard INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_bls_latest_dashboard
    ON gold.mv_bls_latest_dashboard (geo_id, series_id, metric_code);
CREATE INDEX IF NOT EXISTS ix_mv_bls_latest_dashboard_metric_code
    ON gold.mv_bls_latest_dashboard (metric_code);
CREATE INDEX IF NOT EXISTS ix_mv_bls_latest_dashboard_observation_date
    ON gold.mv_bls_latest_dashboard (observation_date);
CREATE INDEX IF NOT EXISTS ix_mv_bls_latest_dashboard_geo_geom
    ON gold.mv_bls_latest_dashboard USING GIST (geo_geom);

DROP PROCEDURE IF EXISTS gold.refresh_rpt_bls_observation_dashboard(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold.refresh_rpt_bls_observation_dashboard(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL gold.refresh_dim_geo_latest();

    IF p_start_date IS NULL OR p_end_date IS NULL THEN
        TRUNCATE TABLE gold.rpt_bls_observation_dashboard;

        INSERT INTO gold.rpt_bls_observation_dashboard
        SELECT
            'BLS',
            b.period_date,
            b.duration_start,
            b.duration_end,
            b.time_sk,
            b.geo_id,
            COALESCE(gl.geo_level, b.geo_level),
            COALESCE(gl.state_fips, b.state_id),
            COALESCE(gl.county_fips, RIGHT(b.county_id, 3)),
            COALESCE(gl.state_name, b.state_name),
            COALESCE(gl.county_name, b.county_name),
            COALESCE(gl.latitude, b.geo_latitude),
            COALESCE(gl.longitude, b.geo_longitude),
            COALESCE(gl.geo_geom, b.geo_geom),
            b.as_of_date,
            b.updated_at,
            b.program_code,
            s.survey_name,
            bs.series_id,
            bs.series_title,
            bs.gold_metric_name,
            bs.measure_name,
            b.measure_category,
            b.value_type,
            bs.unit_of_measure,
            COALESCE(b.seasonal_adjustment_status, bs.seasonal_adjustment_status),
            COALESCE(b.observation_basis, s.observation_basis),
            b.value,
            COALESCE(mc.metric_code, 'BLS:' || bs.series_id),
            COALESCE(mc.metric_display_name, bs.gold_metric_name, bs.series_title),
            COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL'),
            mc.business_definition,
            mc.caveats,
            s.comparison_warning,
            mc.comparability_group,
            mc.recommended_aggregation,
            mc.owner_team
        FROM gold.fact_bls_observation b
        JOIN gold.dim_bls_survey s ON s.bls_survey_sk = b.bls_survey_sk
        JOIN gold.dim_bls_series bs ON bs.bls_series_sk = b.bls_series_sk
        LEFT JOIN gold.dim_geo_latest gl ON gl.geo_id = b.geo_id
        LEFT JOIN gold.bridge_metric_bls_series bms ON bms.bls_series_sk = b.bls_series_sk
        LEFT JOIN gold.dim_metric_catalog mc
            ON mc.metric_catalog_sk = bms.metric_catalog_sk
           AND mc.is_active = TRUE;
    ELSE
        DELETE FROM gold.rpt_bls_observation_dashboard
        WHERE observation_date BETWEEN p_start_date AND p_end_date;

        INSERT INTO gold.rpt_bls_observation_dashboard
        SELECT
            'BLS',
            b.period_date,
            b.duration_start,
            b.duration_end,
            b.time_sk,
            b.geo_id,
            COALESCE(gl.geo_level, b.geo_level),
            COALESCE(gl.state_fips, b.state_id),
            COALESCE(gl.county_fips, RIGHT(b.county_id, 3)),
            COALESCE(gl.state_name, b.state_name),
            COALESCE(gl.county_name, b.county_name),
            COALESCE(gl.latitude, b.geo_latitude),
            COALESCE(gl.longitude, b.geo_longitude),
            COALESCE(gl.geo_geom, b.geo_geom),
            b.as_of_date,
            b.updated_at,
            b.program_code,
            s.survey_name,
            bs.series_id,
            bs.series_title,
            bs.gold_metric_name,
            bs.measure_name,
            b.measure_category,
            b.value_type,
            bs.unit_of_measure,
            COALESCE(b.seasonal_adjustment_status, bs.seasonal_adjustment_status),
            COALESCE(b.observation_basis, s.observation_basis),
            b.value,
            COALESCE(mc.metric_code, 'BLS:' || bs.series_id),
            COALESCE(mc.metric_display_name, bs.gold_metric_name, bs.series_title),
            COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL'),
            mc.business_definition,
            mc.caveats,
            s.comparison_warning,
            mc.comparability_group,
            mc.recommended_aggregation,
            mc.owner_team
        FROM gold.fact_bls_observation b
        JOIN gold.dim_bls_survey s ON s.bls_survey_sk = b.bls_survey_sk
        JOIN gold.dim_bls_series bs ON bs.bls_series_sk = b.bls_series_sk
        LEFT JOIN gold.dim_geo_latest gl ON gl.geo_id = b.geo_id
        LEFT JOIN gold.bridge_metric_bls_series bms ON bms.bls_series_sk = b.bls_series_sk
        LEFT JOIN gold.dim_metric_catalog mc
            ON mc.metric_catalog_sk = bms.metric_catalog_sk
           AND mc.is_active = TRUE
        WHERE b.period_date BETWEEN p_start_date AND p_end_date;
    END IF;

    ANALYZE gold.rpt_bls_observation_dashboard;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_mv_bls_latest_dashboard(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold.refresh_mv_bls_latest_dashboard(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_start_date IS NULL OR p_end_date IS NULL THEN
        TRUNCATE TABLE gold.mv_bls_latest_dashboard;

        INSERT INTO gold.mv_bls_latest_dashboard
        SELECT DISTINCT ON (d.geo_id, d.series_id, d.metric_code)
            d.*
        FROM gold.rpt_bls_observation_dashboard d
        ORDER BY
            d.geo_id,
            d.series_id,
            d.metric_code,
            d.observation_date DESC,
            d.updated_at DESC;
    ELSE
        CREATE TEMP TABLE tmp_bls_touched_keys ON COMMIT DROP AS
        SELECT DISTINCT geo_id, series_id, metric_code
        FROM gold.rpt_bls_observation_dashboard
        WHERE observation_date BETWEEN p_start_date AND p_end_date;

        DELETE FROM gold.mv_bls_latest_dashboard mv
        USING tmp_bls_touched_keys t
        WHERE mv.geo_id = t.geo_id
          AND mv.series_id = t.series_id
          AND mv.metric_code = t.metric_code;

        INSERT INTO gold.mv_bls_latest_dashboard
        SELECT DISTINCT ON (d.geo_id, d.series_id, d.metric_code)
            d.*
        FROM gold.rpt_bls_observation_dashboard d
        JOIN tmp_bls_touched_keys t
          ON t.geo_id = d.geo_id
         AND t.series_id = d.series_id
         AND t.metric_code = d.metric_code
        ORDER BY
            d.geo_id,
            d.series_id,
            d.metric_code,
            d.observation_date DESC,
            d.updated_at DESC;
    END IF;

    ANALYZE gold.mv_bls_latest_dashboard;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_dashboard_serving_layer_bls(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold.refresh_dashboard_serving_layer_bls(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    SET LOCAL statement_timeout = 0;
    CALL gold.refresh_rpt_bls_observation_dashboard(p_start_date, p_end_date);
    CALL gold.refresh_mv_bls_latest_dashboard(p_start_date, p_end_date);
END;
$$;
