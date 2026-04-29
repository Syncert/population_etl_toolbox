-- census_acs/gold_census/DDL/gold_acs.sql
-- Subject-scoped gold DDL for ACS objects and serving refresh.

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

CREATE OR REPLACE VIEW gold.dim_time AS
SELECT
    time_sk,
    date_key,
    year,
    quarter,
    month,
    day,
    day_of_week,
    day_name,
    month_name,
    week_of_year,
    is_weekend,
    is_month_start,
    is_month_end,
    is_quarter_start,
    is_quarter_end,
    is_year_start,
    is_year_end,
    ingested_at
FROM silver_ref.dim_time;

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

CREATE TABLE IF NOT EXISTS gold.bridge_metric_acs_variable (
    metric_catalog_sk BIGINT NOT NULL REFERENCES gold.dim_metric_catalog(metric_catalog_sk),
    acs_variable_sk   BIGINT NOT NULL,
    PRIMARY KEY (metric_catalog_sk, acs_variable_sk)
);

CREATE TABLE IF NOT EXISTS gold.bridge_metric_bls_series (
    metric_catalog_sk BIGINT NOT NULL REFERENCES gold.dim_metric_catalog(metric_catalog_sk),
    bls_series_sk     BIGINT NOT NULL,
    PRIMARY KEY (metric_catalog_sk, bls_series_sk)
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

CREATE INDEX IF NOT EXISTS ix_dim_geo_latest_geo_geom
    ON gold.dim_geo_latest USING GIST (geo_geom);

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

CREATE TABLE IF NOT EXISTS gold.dim_acs_table (
    acs_table_sk      BIGSERIAL PRIMARY KEY,
    dataset_code      TEXT NOT NULL CHECK (dataset_code IN ('acs1', 'acs5')),
    vintage_year      INTEGER NOT NULL,
    table_id          TEXT NOT NULL,
    table_title       TEXT,
    concept           TEXT,
    universe          TEXT,
    survey_span_years INTEGER NOT NULL CHECK (survey_span_years IN (1, 5)),
    reference_url     TEXT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_code, vintage_year, table_id)
);

CREATE TABLE IF NOT EXISTS gold.dim_acs_variable (
    acs_variable_sk        BIGSERIAL PRIMARY KEY,
    acs_table_sk           BIGINT NOT NULL REFERENCES gold.dim_acs_table(acs_table_sk),
    dataset_code           TEXT NOT NULL CHECK (dataset_code IN ('acs1', 'acs5')),
    vintage_year           INTEGER NOT NULL,
    variable_code          TEXT NOT NULL,
    variable_label         TEXT,
    concept                TEXT,
    universe               TEXT,
    value_role             TEXT NOT NULL CHECK (value_role IN ('ESTIMATE', 'MOE', 'ANNOTATION')),
    denominator_hint       TEXT,
    is_publishable_default BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_code, vintage_year, variable_code)
);

CREATE TABLE IF NOT EXISTS gold.fact_acs_observation (
    acs_observation_sk BIGSERIAL PRIMARY KEY,
    geo_id             TEXT NOT NULL,
    geo_level          TEXT NOT NULL CHECK (geo_level IN ('NATIONAL', 'STATE', 'COUNTY')),
    state_id           TEXT,
    state_name         TEXT,
    county_id          TEXT,
    county_name        TEXT,
    geo_latitude       DOUBLE PRECISION,
    geo_longitude      DOUBLE PRECISION,
    geo_geom           geometry(MultiPolygon, 4326),
    time_sk            INTEGER REFERENCES silver_ref.dim_time(time_sk),
    observation_date   DATE NOT NULL,
    duration_start     DATE,
    duration_end       DATE,
    acs_table_sk       BIGINT NOT NULL REFERENCES gold.dim_acs_table(acs_table_sk),
    acs_variable_sk    BIGINT NOT NULL REFERENCES gold.dim_acs_variable(acs_variable_sk),
    dataset_code       TEXT NOT NULL CHECK (dataset_code IN ('acs1', 'acs5')),
    vintage_year       INTEGER NOT NULL,
    estimate_value     NUMERIC,
    margin_of_error    NUMERIC,
    margin_of_error_pct NUMERIC,
    estimate_annotation TEXT,
    moe_annotation     TEXT,
    as_of_date         DATE NOT NULL DEFAULT CURRENT_DATE,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (geo_id, observation_date, acs_variable_sk, dataset_code)
);

CREATE INDEX IF NOT EXISTS ix_fact_acs_obs_date ON gold.fact_acs_observation (observation_date);
CREATE INDEX IF NOT EXISTS ix_fact_acs_geo_date ON gold.fact_acs_observation (geo_id, observation_date);
CREATE INDEX IF NOT EXISTS ix_fact_acs_obs_date_brin ON gold.fact_acs_observation USING BRIN (observation_date);
CREATE INDEX IF NOT EXISTS ix_fact_acs_geo_geom ON gold.fact_acs_observation USING GIST (geo_geom);

CREATE TABLE IF NOT EXISTS gold.rpt_acs_observation_dashboard (
    source_code             TEXT NOT NULL DEFAULT 'CENSUS_ACS',
    observation_date        DATE NOT NULL,
    duration_start          DATE,
    duration_end            DATE,
    time_sk                 INTEGER,
    geo_id                  TEXT NOT NULL,
    geo_level               TEXT NOT NULL,
    state_fips              TEXT,
    county_fips             TEXT,
    state_name              TEXT,
    county_name             TEXT,
    geo_latitude            DOUBLE PRECISION,
    geo_longitude           DOUBLE PRECISION,
    geo_geom                geometry(MultiPolygon, 4326),
    as_of_date              DATE NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL,
    dataset_code            TEXT NOT NULL,
    vintage_year            INTEGER NOT NULL,
    table_id                TEXT NOT NULL,
    table_title             TEXT,
    variable_code           TEXT NOT NULL,
    variable_label          TEXT,
    concept                 TEXT,
    universe                TEXT,
    denominator_hint        TEXT,
    is_publishable_default  BOOLEAN NOT NULL,
    estimate_value          NUMERIC,
    margin_of_error         NUMERIC,
    margin_of_error_pct     NUMERIC,
    estimate_annotation     TEXT,
    moe_annotation          TEXT,
    metric_code             TEXT,
    metric_display_name     TEXT,
    dashboard_suitability   TEXT,
    business_definition     TEXT,
    caveats                 TEXT,
    comparability_group     TEXT,
    do_not_compare_with     TEXT[],
    recommended_aggregation TEXT,
    owner_team              TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_acs_dashboard_nk
    ON gold.rpt_acs_observation_dashboard (geo_id, observation_date, dataset_code, vintage_year, variable_code, metric_code);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_geo_date
    ON gold.rpt_acs_observation_dashboard (geo_id, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_metric_date
    ON gold.rpt_acs_observation_dashboard (metric_code, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_obs_brin
    ON gold.rpt_acs_observation_dashboard USING BRIN (observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_geo_geom
    ON gold.rpt_acs_observation_dashboard USING GIST (geo_geom);

CREATE TABLE IF NOT EXISTS gold.mv_acs_latest_dashboard
    (LIKE gold.rpt_acs_observation_dashboard INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_acs_latest_dashboard
    ON gold.mv_acs_latest_dashboard (geo_id, variable_code, metric_code);
CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_dashboard_metric_code
    ON gold.mv_acs_latest_dashboard (metric_code);
CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_dashboard_observation_date
    ON gold.mv_acs_latest_dashboard (observation_date);
CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_dashboard_geo_geom
    ON gold.mv_acs_latest_dashboard USING GIST (geo_geom);

DROP PROCEDURE IF EXISTS gold.refresh_rpt_acs_observation_dashboard(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold.refresh_rpt_acs_observation_dashboard(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL gold.refresh_dim_geo_latest();

    IF p_start_date IS NULL OR p_end_date IS NULL THEN
        TRUNCATE TABLE gold.rpt_acs_observation_dashboard;

        INSERT INTO gold.rpt_acs_observation_dashboard (
            source_code,
            observation_date,
            duration_start,
            duration_end,
            time_sk,
            geo_id,
            geo_level,
            state_fips,
            county_fips,
            state_name,
            county_name,
            geo_latitude,
            geo_longitude,
            geo_geom,
            as_of_date,
            updated_at,
            dataset_code,
            vintage_year,
            table_id,
            table_title,
            variable_code,
            variable_label,
            concept,
            universe,
            denominator_hint,
            is_publishable_default,
            estimate_value,
            margin_of_error,
            margin_of_error_pct,
            estimate_annotation,
            moe_annotation,
            metric_code,
            metric_display_name,
            dashboard_suitability,
            business_definition,
            caveats,
            comparability_group,
            do_not_compare_with,
            recommended_aggregation,
            owner_team
        )
        SELECT
            'CENSUS_ACS',
            ao.observation_date,
            ao.duration_start,
            ao.duration_end,
            ao.time_sk,
            ao.geo_id,
            COALESCE(gl.geo_level, ao.geo_level),
            COALESCE(gl.state_fips, ao.state_id),
            COALESCE(gl.county_fips, RIGHT(ao.county_id, 3)),
            COALESCE(gl.state_name, ao.state_name),
            COALESCE(gl.county_name, ao.county_name),
            COALESCE(gl.latitude, ao.geo_latitude),
            COALESCE(gl.longitude, ao.geo_longitude),
            COALESCE(gl.geo_geom, ao.geo_geom),
            ao.as_of_date,
            ao.updated_at,
            ao.dataset_code,
            ao.vintage_year,
            t.table_id,
            t.table_title,
            v.variable_code,
            v.variable_label,
            COALESCE(v.concept, t.concept),
            COALESCE(v.universe, t.universe),
            v.denominator_hint,
            v.is_publishable_default,
            ao.estimate_value,
            ao.margin_of_error,
            ao.margin_of_error_pct,
            ao.estimate_annotation,
            ao.moe_annotation,
            COALESCE(mc.metric_code, 'ACS:' || ao.dataset_code || ':' || v.variable_code),
            COALESCE(mc.metric_display_name, v.variable_label),
            COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL'),
            mc.business_definition,
            mc.caveats,
            mc.comparability_group,
            COALESCE(mc.do_not_compare_with, ARRAY[]::TEXT[]),
            mc.recommended_aggregation,
            mc.owner_team
        FROM gold.fact_acs_observation ao
        JOIN gold.dim_acs_table t ON t.acs_table_sk = ao.acs_table_sk
        JOIN gold.dim_acs_variable v ON v.acs_variable_sk = ao.acs_variable_sk
        LEFT JOIN gold.dim_geo_latest gl ON gl.geo_id = ao.geo_id
        LEFT JOIN gold.bridge_metric_acs_variable bma ON bma.acs_variable_sk = ao.acs_variable_sk
        LEFT JOIN gold.dim_metric_catalog mc
            ON mc.metric_catalog_sk = bma.metric_catalog_sk
           AND mc.is_active = TRUE;
    ELSE
        DELETE FROM gold.rpt_acs_observation_dashboard
        WHERE observation_date BETWEEN p_start_date AND p_end_date;

        INSERT INTO gold.rpt_acs_observation_dashboard (
            source_code,
            observation_date,
            duration_start,
            duration_end,
            time_sk,
            geo_id,
            geo_level,
            state_fips,
            county_fips,
            state_name,
            county_name,
            geo_latitude,
            geo_longitude,
            geo_geom,
            as_of_date,
            updated_at,
            dataset_code,
            vintage_year,
            table_id,
            table_title,
            variable_code,
            variable_label,
            concept,
            universe,
            denominator_hint,
            is_publishable_default,
            estimate_value,
            margin_of_error,
            margin_of_error_pct,
            estimate_annotation,
            moe_annotation,
            metric_code,
            metric_display_name,
            dashboard_suitability,
            business_definition,
            caveats,
            comparability_group,
            do_not_compare_with,
            recommended_aggregation,
            owner_team
        )
        SELECT
            'CENSUS_ACS',
            ao.observation_date,
            ao.duration_start,
            ao.duration_end,
            ao.time_sk,
            ao.geo_id,
            COALESCE(gl.geo_level, ao.geo_level),
            COALESCE(gl.state_fips, ao.state_id),
            COALESCE(gl.county_fips, RIGHT(ao.county_id, 3)),
            COALESCE(gl.state_name, ao.state_name),
            COALESCE(gl.county_name, ao.county_name),
            COALESCE(gl.latitude, ao.geo_latitude),
            COALESCE(gl.longitude, ao.geo_longitude),
            COALESCE(gl.geo_geom, ao.geo_geom),
            ao.as_of_date,
            ao.updated_at,
            ao.dataset_code,
            ao.vintage_year,
            t.table_id,
            t.table_title,
            v.variable_code,
            v.variable_label,
            COALESCE(v.concept, t.concept),
            COALESCE(v.universe, t.universe),
            v.denominator_hint,
            v.is_publishable_default,
            ao.estimate_value,
            ao.margin_of_error,
            ao.margin_of_error_pct,
            ao.estimate_annotation,
            ao.moe_annotation,
            COALESCE(mc.metric_code, 'ACS:' || ao.dataset_code || ':' || v.variable_code),
            COALESCE(mc.metric_display_name, v.variable_label),
            COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL'),
            mc.business_definition,
            mc.caveats,
            mc.comparability_group,
            COALESCE(mc.do_not_compare_with, ARRAY[]::TEXT[]),
            mc.recommended_aggregation,
            mc.owner_team
        FROM gold.fact_acs_observation ao
        JOIN gold.dim_acs_table t ON t.acs_table_sk = ao.acs_table_sk
        JOIN gold.dim_acs_variable v ON v.acs_variable_sk = ao.acs_variable_sk
        LEFT JOIN gold.dim_geo_latest gl ON gl.geo_id = ao.geo_id
        LEFT JOIN gold.bridge_metric_acs_variable bma ON bma.acs_variable_sk = ao.acs_variable_sk
        LEFT JOIN gold.dim_metric_catalog mc
            ON mc.metric_catalog_sk = bma.metric_catalog_sk
           AND mc.is_active = TRUE
        WHERE ao.observation_date BETWEEN p_start_date AND p_end_date;
    END IF;

    ANALYZE gold.rpt_acs_observation_dashboard;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_mv_acs_latest_dashboard(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold.refresh_mv_acs_latest_dashboard(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_start_date IS NULL OR p_end_date IS NULL THEN
        TRUNCATE TABLE gold.mv_acs_latest_dashboard;

        INSERT INTO gold.mv_acs_latest_dashboard
        SELECT DISTINCT ON (d.geo_id, d.variable_code, d.metric_code)
            d.*
        FROM gold.rpt_acs_observation_dashboard d
        ORDER BY
            d.geo_id,
            d.variable_code,
            d.metric_code,
            d.observation_date DESC,
            d.updated_at DESC,
            CASE d.dataset_code WHEN 'acs1' THEN 1 WHEN 'acs5' THEN 2 ELSE 9 END,
            d.vintage_year DESC;
    ELSE
        CREATE TEMP TABLE tmp_acs_touched_keys ON COMMIT DROP AS
        SELECT DISTINCT geo_id, variable_code, metric_code
        FROM gold.rpt_acs_observation_dashboard
        WHERE observation_date BETWEEN p_start_date AND p_end_date;

        DELETE FROM gold.mv_acs_latest_dashboard mv
        USING tmp_acs_touched_keys t
        WHERE mv.geo_id = t.geo_id
          AND mv.variable_code = t.variable_code
          AND mv.metric_code = t.metric_code;

        INSERT INTO gold.mv_acs_latest_dashboard
        SELECT DISTINCT ON (d.geo_id, d.variable_code, d.metric_code)
            d.*
        FROM gold.rpt_acs_observation_dashboard d
        JOIN tmp_acs_touched_keys t
          ON t.geo_id = d.geo_id
         AND t.variable_code = d.variable_code
         AND t.metric_code = d.metric_code
        ORDER BY
            d.geo_id,
            d.variable_code,
            d.metric_code,
            d.observation_date DESC,
            d.updated_at DESC,
            CASE d.dataset_code WHEN 'acs1' THEN 1 WHEN 'acs5' THEN 2 ELSE 9 END,
            d.vintage_year DESC;
    END IF;

    ANALYZE gold.mv_acs_latest_dashboard;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_dashboard_serving_layer_acs(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold.refresh_dashboard_serving_layer_acs(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    SET LOCAL statement_timeout = 0;
    CALL gold.refresh_rpt_acs_observation_dashboard(p_start_date, p_end_date);
    CALL gold.refresh_mv_acs_latest_dashboard(p_start_date, p_end_date);
END;
$$;
