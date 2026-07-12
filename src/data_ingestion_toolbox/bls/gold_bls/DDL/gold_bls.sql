-- bls/gold_bls/DDL/gold_bls.sql
-- Subject-scoped gold DDL for BLS objects and serving refresh.

CREATE SCHEMA IF NOT EXISTS gold_glossary;
CREATE SCHEMA IF NOT EXISTS gold_bls;
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

CREATE TABLE IF NOT EXISTS gold_glossary.bridge_metric_bls_series (
    metric_catalog_sk BIGINT NOT NULL REFERENCES gold_glossary.dim_metric_catalog(metric_catalog_sk),
    bls_series_sk     BIGINT NOT NULL,
    PRIMARY KEY (metric_catalog_sk, bls_series_sk)
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

DROP PROCEDURE IF EXISTS gold_glossary.refresh_dim_geo_latest();
CREATE OR REPLACE PROCEDURE gold_glossary.refresh_dim_geo_latest()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE gold_glossary.dim_geo_latest;

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
    ORDER BY g.geo_id, g.source_year DESC NULLS LAST, g.ingested_at DESC;
END;
$$;

CREATE TABLE IF NOT EXISTS gold_bls.dim_bls_survey (
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

CREATE TABLE IF NOT EXISTS gold_bls.dim_bls_series (
    bls_series_sk              BIGSERIAL PRIMARY KEY,
    bls_survey_sk              BIGINT NOT NULL REFERENCES gold_bls.dim_bls_survey(bls_survey_sk),
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

-- fact_bls_observation: view over silver — no duplicate observation storage
CREATE OR REPLACE VIEW gold_bls.fact_bls_observation AS
SELECT
    s.geo_id,
    CASE
        WHEN LOWER(s.geo_level) = 'us'     THEN 'NATIONAL'
        WHEN LOWER(s.geo_level) = 'state'  THEN 'STATE'
        WHEN LOWER(s.geo_level) = 'county' THEN 'COUNTY'
        WHEN s.geo_id = 'us:1'             THEN 'NATIONAL'
        WHEN s.geo_id LIKE 'state:%|county:%' THEN 'COUNTY'
        WHEN s.geo_id LIKE 'state:%'       THEN 'STATE'
        ELSE 'NATIONAL'
    END AS geo_level,
    s.time_sk,
    s.period_date,
    s.duration_start,
    s.duration_end,
    sr.bls_survey_sk,
    sr.bls_series_sk,
    UPPER(s.program) AS program_code,
    s.value,
    s.period AS period_code,
    s.seasonal_adjustment AS seasonal_adjustment_status,
    sv.observation_basis,
    sr.measure_category,
    sr.value_type,
    CURRENT_DATE       AS as_of_date,
    s.ingested_at      AS updated_at
FROM silver_bls.fact_labor_statistics s
JOIN gold_bls.dim_bls_series sr ON sr.series_id = s.series_id
JOIN gold_bls.dim_bls_survey sv ON sv.bls_survey_sk = sr.bls_survey_sk
WHERE s.value IS NOT NULL
  AND s.series_id IS NOT NULL
  AND s.series_id <> '';

-- Unified serving tables (IF NOT EXISTS — idempotent regardless of DDL execution order)
CREATE TABLE IF NOT EXISTS gold_bls.rpt_observation_dashboard (
    source_code                TEXT NOT NULL,
    observation_date           DATE NOT NULL,
    duration_start             DATE,
    duration_end               DATE,
    time_sk                    INTEGER,
    as_of_date                 DATE NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL,
    geo_id                     TEXT NOT NULL,
    geo_level                  TEXT NOT NULL,
    state_fips                 TEXT,
    county_fips                TEXT,
    state_name                 TEXT,
    county_name                TEXT,
    geo_latitude               DOUBLE PRECISION,
    geo_longitude              DOUBLE PRECISION,
    metric_code                TEXT,
    metric_display_name        TEXT,
    dashboard_suitability      TEXT,
    business_definition        TEXT,
    caveats                    TEXT,
    comparability_group        TEXT,
    do_not_compare_with        TEXT[],
    recommended_aggregation    TEXT,
    owner_team                 TEXT,
    value                      NUMERIC,
    value_type                 TEXT,
    units                      TEXT,
    seasonal_adjustment_status TEXT,
    -- BLS-specific (NULL for other sources)
    series_id                  TEXT,
    program_code               TEXT,
    survey_name                TEXT,
    series_title               TEXT,
    measure_name               TEXT,
    measure_category           TEXT,
    observation_basis          TEXT,
    gold_metric_name           TEXT,
    comparison_warning         TEXT,
    -- ACS-specific (NULL for other sources)
    dataset_code               TEXT,
    vintage_year               INTEGER,
    table_id                   TEXT,
    table_title                TEXT,
    variable_code              TEXT,
    variable_label             TEXT,
    concept                    TEXT,
    universe                   TEXT,
    denominator_hint           TEXT,
    is_publishable_default     BOOLEAN,
    estimate_value             NUMERIC,
    margin_of_error            NUMERIC,
    margin_of_error_pct        NUMERIC,
    estimate_annotation        TEXT,
    moe_annotation             TEXT,
    -- FRED-specific (NULL for other sources)
    source_provider            TEXT,
    original_source_name       TEXT,
    is_primary_source_series   BOOLEAN,
    is_republished_series      BOOLEAN,
    frequency                  TEXT,
    transformation_method      TEXT,
    realtime_start             DATE,
    realtime_end               DATE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_observation_dashboard_nk
    ON gold_bls.rpt_observation_dashboard (
        source_code,
        geo_id,
        observation_date,
        COALESCE(series_id, ''),
        COALESCE(variable_code, ''),
        COALESCE(dataset_code, ''),
        COALESCE(metric_code, ''),
        COALESCE(realtime_start, '0001-01-01'::DATE),
        COALESCE(realtime_end, '0001-01-01'::DATE)
    );
CREATE INDEX IF NOT EXISTS ix_rpt_observation_dashboard_source_geo_date
    ON gold_bls.rpt_observation_dashboard (source_code, geo_id, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_observation_dashboard_metric_date
    ON gold_bls.rpt_observation_dashboard (metric_code, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_observation_dashboard_obs_brin
    ON gold_bls.rpt_observation_dashboard USING BRIN (observation_date);

CREATE TABLE IF NOT EXISTS gold_bls.mv_latest_dashboard
    (LIKE gold_bls.rpt_observation_dashboard INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMMENTS);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_latest_dashboard
    ON gold_bls.mv_latest_dashboard (
        source_code,
        geo_id,
        COALESCE(series_id, ''),
        COALESCE(variable_code, ''),
        COALESCE(dataset_code, ''),
        COALESCE(metric_code, '')
    );
CREATE INDEX IF NOT EXISTS ix_mv_latest_dashboard_source_metric
    ON gold_bls.mv_latest_dashboard (source_code, metric_code);
CREATE INDEX IF NOT EXISTS ix_mv_latest_dashboard_observation_date
    ON gold_bls.mv_latest_dashboard (observation_date);

DROP PROCEDURE IF EXISTS gold_bls.refresh_rpt_bls_observation_dashboard(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold_bls.refresh_rpt_bls_observation_dashboard(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL gold_glossary.refresh_dim_geo_latest();

    IF p_start_date IS NULL OR p_end_date IS NULL THEN
        DELETE FROM gold_bls.rpt_observation_dashboard WHERE source_code = 'BLS';
    ELSE
        DELETE FROM gold_bls.rpt_observation_dashboard
        WHERE source_code = 'BLS'
          AND observation_date BETWEEN p_start_date AND p_end_date;
    END IF;

    INSERT INTO gold_bls.rpt_observation_dashboard (
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
        value_type,
        units,
        seasonal_adjustment_status,
        series_id,
        program_code,
        survey_name,
        series_title,
        measure_name,
        measure_category,
        observation_basis,
        gold_metric_name,
        comparison_warning
    )
    SELECT
        'BLS',
        b.period_date,
        b.duration_start,
        b.duration_end,
        b.time_sk,
        b.as_of_date,
        b.updated_at,
        b.geo_id,
        COALESCE(gl.geo_level, b.geo_level),
        gl.state_fips,
        gl.county_fips,
        gl.state_name,
        gl.county_name,
        gl.latitude,
        gl.longitude,
        COALESCE(mc.metric_code,          'BLS:' || bs.series_id),
        COALESCE(mc.metric_display_name,  bs.gold_metric_name, bs.series_title),
        COALESCE(mc.dashboard_suitability,'EXPERIMENTAL'),
        mc.business_definition,
        mc.caveats,
        mc.comparability_group,
        COALESCE(mc.do_not_compare_with, ARRAY[]::TEXT[]),
        mc.recommended_aggregation,
        mc.owner_team,
        b.value,
        b.value_type,
        bs.unit_of_measure,
        COALESCE(b.seasonal_adjustment_status, bs.seasonal_adjustment_status),
        bs.series_id,
        b.program_code,
        s.survey_name,
        bs.series_title,
        bs.measure_name,
        b.measure_category,
        COALESCE(b.observation_basis, s.observation_basis),
        bs.gold_metric_name,
        s.comparison_warning
    FROM gold_bls.fact_bls_observation b
    JOIN gold_bls.dim_bls_survey s  ON s.bls_survey_sk  = b.bls_survey_sk
    JOIN gold_bls.dim_bls_series bs ON bs.bls_series_sk = b.bls_series_sk
    LEFT JOIN gold_glossary.dim_geo_latest gl ON gl.geo_id = b.geo_id
    LEFT JOIN gold_glossary.bridge_metric_bls_series bms ON bms.bls_series_sk = b.bls_series_sk
    LEFT JOIN gold_glossary.dim_metric_catalog mc
        ON mc.metric_catalog_sk = bms.metric_catalog_sk
       AND mc.is_active = TRUE
    WHERE (p_start_date IS NULL OR p_end_date IS NULL
           OR b.period_date BETWEEN p_start_date AND p_end_date);

    ANALYZE gold_bls.rpt_observation_dashboard;
END;
$$;

DROP PROCEDURE IF EXISTS gold_bls.refresh_mv_bls_latest_dashboard(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold_bls.refresh_mv_bls_latest_dashboard(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Always rebuild the BLS slice — the mv is bounded by N_series × N_geos, not N_observations.
    DELETE FROM gold_bls.mv_latest_dashboard WHERE source_code = 'BLS';

    INSERT INTO gold_bls.mv_latest_dashboard
    SELECT DISTINCT ON (d.geo_id, d.series_id, d.metric_code)
        d.*
    FROM gold_bls.rpt_observation_dashboard d
    WHERE d.source_code = 'BLS'
    ORDER BY
        d.geo_id,
        d.series_id,
        d.metric_code,
        d.observation_date DESC,
        d.updated_at DESC;

    ANALYZE gold_bls.mv_latest_dashboard;
END;
$$;

DROP PROCEDURE IF EXISTS gold_bls.refresh_dashboard_serving_layer_bls(DATE, DATE);
CREATE OR REPLACE PROCEDURE gold_bls.refresh_dashboard_serving_layer_bls(
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_started_at TIMESTAMPTZ := clock_timestamp();
    v_step_started TIMESTAMPTZ;
BEGIN
    SET LOCAL statement_timeout = 0;

    RAISE NOTICE '[BLS DASHBOARD REFRESH] start window_start=% window_end=%', p_start_date, p_end_date;

    v_step_started := clock_timestamp();
    CALL gold_bls.refresh_rpt_bls_observation_dashboard(p_start_date, p_end_date);
    RAISE NOTICE
        '[BLS DASHBOARD REFRESH] step=refresh_rpt_bls_observation_dashboard duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_step_started)) * 1000)::NUMERIC(18,2);

    v_step_started := clock_timestamp();
    CALL gold_bls.refresh_mv_bls_latest_dashboard(p_start_date, p_end_date);
    RAISE NOTICE
        '[BLS DASHBOARD REFRESH] step=refresh_mv_bls_latest_dashboard duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_step_started)) * 1000)::NUMERIC(18,2);

    RAISE NOTICE
        '[BLS DASHBOARD REFRESH] completed total_duration_ms=%',
        (EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::NUMERIC(18,2);
END;
$$;
