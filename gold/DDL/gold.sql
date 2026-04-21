-- gold/DDL/gold.sql
-- Gold analytics layer baseline schema (fresh install)

CREATE SCHEMA IF NOT EXISTS gold;
CREATE EXTENSION IF NOT EXISTS postgis;

-- ---------------------------------------------------------------------------
-- Conformed dimensions (read-only views over silver_ref)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS gold.dim_geo CASCADE;
CREATE VIEW gold.dim_geo AS
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

-- ---------------------------------------------------------------------------
-- Source-specific metadata dimensions
-- ---------------------------------------------------------------------------
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
    acs_variable_sk      BIGSERIAL PRIMARY KEY,
    acs_table_sk         BIGINT NOT NULL REFERENCES gold.dim_acs_table(acs_table_sk),
    dataset_code         TEXT NOT NULL CHECK (dataset_code IN ('acs1', 'acs5')),
    vintage_year         INTEGER NOT NULL,
    variable_code        TEXT NOT NULL,
    variable_label       TEXT,
    concept              TEXT,
    universe             TEXT,
    value_role           TEXT NOT NULL CHECK (value_role IN ('ESTIMATE', 'MOE', 'ANNOTATION')),
    denominator_hint     TEXT,
    is_publishable_default BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_code, vintage_year, variable_code)
);

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

-- ---------------------------------------------------------------------------
-- Source-specific fact tables
-- ---------------------------------------------------------------------------
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

CREATE TABLE IF NOT EXISTS gold.fact_bls_observation (
    bls_observation_sk        BIGSERIAL PRIMARY KEY,
    geo_id                    TEXT NOT NULL,
    geo_level                 TEXT NOT NULL CHECK (geo_level IN ('NATIONAL', 'STATE', 'COUNTY')),
    state_id                  TEXT,
    state_name                TEXT,
    county_id                 TEXT,
    county_name               TEXT,
    geo_latitude              DOUBLE PRECISION,
    geo_longitude             DOUBLE PRECISION,
    time_sk                   INTEGER REFERENCES silver_ref.dim_time(time_sk),
    period_date               DATE NOT NULL,
    duration_start            DATE,
    duration_end              DATE,
    bls_survey_sk             BIGINT NOT NULL REFERENCES gold.dim_bls_survey(bls_survey_sk),
    bls_series_sk             BIGINT NOT NULL REFERENCES gold.dim_bls_series(bls_series_sk),
    program_code              TEXT NOT NULL,
    value                     NUMERIC,
    period_code               TEXT,
    seasonal_adjustment_status TEXT,
    observation_basis         TEXT NOT NULL CHECK (observation_basis IN ('PEOPLE', 'JOBS', 'PRICES', 'FLOWS')),
    measure_category          TEXT NOT NULL CHECK (
        measure_category IN (
            'EMPLOYMENT', 'UNEMPLOYMENT', 'LABOR_FORCE', 'PARTICIPATION', 'POPULATION',
            'EARNINGS', 'HOURS', 'PRICE_INDEX', 'OPENINGS', 'HIRES', 'QUITS', 'LAYOFFS', 'SEPARATIONS',
            'OTHER'
        )
    ),
    value_type                TEXT NOT NULL CHECK (value_type IN ('LEVEL', 'RATE', 'INDEX', 'PERCENT', 'CURRENCY', 'RATIO', 'OTHER')),
    as_of_date                DATE NOT NULL DEFAULT CURRENT_DATE,
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (geo_id, period_date, bls_series_sk)
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

ALTER TABLE gold.fact_acs_observation
    ADD COLUMN IF NOT EXISTS geo_latitude DOUBLE PRECISION;

ALTER TABLE gold.fact_acs_observation
    ADD COLUMN IF NOT EXISTS geo_longitude DOUBLE PRECISION;

ALTER TABLE gold.fact_acs_observation
    ADD COLUMN IF NOT EXISTS geo_geom geometry(MultiPolygon, 4326);

ALTER TABLE gold.fact_bls_observation
    ADD COLUMN IF NOT EXISTS geo_latitude DOUBLE PRECISION;

ALTER TABLE gold.fact_bls_observation
    ADD COLUMN IF NOT EXISTS geo_longitude DOUBLE PRECISION;

ALTER TABLE gold.fact_bls_observation
    ADD COLUMN IF NOT EXISTS geo_geom geometry(MultiPolygon, 4326);

CREATE INDEX IF NOT EXISTS ix_fact_acs_geo_geom
    ON gold.fact_acs_observation USING GIST (geo_geom);

CREATE INDEX IF NOT EXISTS ix_fact_bls_geo_geom
    ON gold.fact_bls_observation USING GIST (geo_geom);

-- ---------------------------------------------------------------------------
-- Shared metric catalog + bridges
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_metric_catalog (
    metric_catalog_sk    BIGSERIAL PRIMARY KEY,
    metric_code          TEXT NOT NULL UNIQUE,
    metric_display_name  TEXT NOT NULL,
    source_code          TEXT NOT NULL REFERENCES gold.dim_source_system(source_code),
    source_object_type   TEXT NOT NULL CHECK (source_object_type IN ('ACS_VARIABLE', 'BLS_SERIES', 'FRED_SERIES', 'COMPOSITE_VIEW')),
    business_definition  TEXT,
    caveats              TEXT,
    valid_geo_grains     TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    valid_time_grains    TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    dashboard_suitability TEXT NOT NULL DEFAULT 'PUBLIC_SAFE'
        CHECK (dashboard_suitability IN ('PUBLIC_SAFE', 'INTERNAL_ONLY', 'EXPERIMENTAL')),
    comparability_group  TEXT,
    do_not_compare_with  TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    recommended_aggregation TEXT,
    owner_team           TEXT,
    is_active            BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.bridge_metric_acs_variable (
    metric_catalog_sk BIGINT NOT NULL REFERENCES gold.dim_metric_catalog(metric_catalog_sk),
    acs_variable_sk   BIGINT NOT NULL REFERENCES gold.dim_acs_variable(acs_variable_sk),
    PRIMARY KEY (metric_catalog_sk, acs_variable_sk)
);

CREATE TABLE IF NOT EXISTS gold.bridge_metric_bls_series (
    metric_catalog_sk BIGINT NOT NULL REFERENCES gold.dim_metric_catalog(metric_catalog_sk),
    bls_series_sk     BIGINT NOT NULL REFERENCES gold.dim_bls_series(bls_series_sk),
    PRIMARY KEY (metric_catalog_sk, bls_series_sk)
);

CREATE TABLE IF NOT EXISTS gold.bridge_metric_fred_series (
    metric_catalog_sk BIGINT NOT NULL REFERENCES gold.dim_metric_catalog(metric_catalog_sk),
    fred_series_sk    BIGINT NOT NULL REFERENCES gold.dim_fred_series(fred_series_sk),
    PRIMARY KEY (metric_catalog_sk, fred_series_sk)
);

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_fact_acs_obs_date ON gold.fact_acs_observation (observation_date);
CREATE INDEX IF NOT EXISTS ix_fact_acs_geo_date ON gold.fact_acs_observation (geo_id, observation_date);
CREATE INDEX IF NOT EXISTS ix_fact_bls_period_date ON gold.fact_bls_observation (period_date);
CREATE INDEX IF NOT EXISTS ix_fact_bls_geo_date ON gold.fact_bls_observation (geo_id, period_date);
CREATE INDEX IF NOT EXISTS ix_fact_bls_program ON gold.fact_bls_observation (program_code, period_date);
CREATE INDEX IF NOT EXISTS ix_fact_fred_obs_date ON gold.fact_fred_observation (observation_date);
CREATE INDEX IF NOT EXISTS ix_fact_fred_series_date ON gold.fact_fred_observation (fred_series_sk, observation_date);
CREATE INDEX IF NOT EXISTS ix_metric_catalog_source ON gold.dim_metric_catalog (source_code, is_active);
CREATE INDEX IF NOT EXISTS ix_metric_catalog_group ON gold.dim_metric_catalog (comparability_group);
CREATE INDEX IF NOT EXISTS ix_metric_catalog_geo_grains ON gold.dim_metric_catalog USING GIN (valid_geo_grains);
CREATE INDEX IF NOT EXISTS ix_metric_catalog_time_grains ON gold.dim_metric_catalog USING GIN (valid_time_grains);

-- ---------------------------------------------------------------------------
-- Dashboard-serving tables
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.rpt_acs_observation_dashboard (
    source_code               TEXT NOT NULL DEFAULT 'CENSUS_ACS',
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
    geo_polygon_geojson       TEXT,
    as_of_date                DATE NOT NULL,
    updated_at                TIMESTAMPTZ NOT NULL,
    dataset_code              TEXT NOT NULL,
    vintage_year              INTEGER NOT NULL,
    table_id                  TEXT NOT NULL,
    table_title               TEXT,
    variable_code             TEXT NOT NULL,
    variable_label            TEXT,
    concept                   TEXT,
    universe                  TEXT,
    denominator_hint          TEXT,
    is_publishable_default    BOOLEAN NOT NULL,
    estimate_value            NUMERIC,
    margin_of_error           NUMERIC,
    margin_of_error_pct       NUMERIC,
    estimate_annotation       TEXT,
    moe_annotation            TEXT,
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
    geo_polygon_geojson       TEXT,
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
    geo_polygon_geojson       TEXT,
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

-- ---------------------------------------------------------------------------
-- Dashboard-serving indexes
-- ---------------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_acs_dashboard_nk
    ON gold.rpt_acs_observation_dashboard (geo_id, observation_date, dataset_code, vintage_year, variable_code, metric_code);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_observation_date
    ON gold.rpt_acs_observation_dashboard (observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_geo_id
    ON gold.rpt_acs_observation_dashboard (geo_id);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_geo_level
    ON gold.rpt_acs_observation_dashboard (geo_level);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_dataset_code
    ON gold.rpt_acs_observation_dashboard (dataset_code);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_variable_code
    ON gold.rpt_acs_observation_dashboard (variable_code);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_metric_code
    ON gold.rpt_acs_observation_dashboard (metric_code);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_geo_date
    ON gold.rpt_acs_observation_dashboard (geo_id, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_variable_date
    ON gold.rpt_acs_observation_dashboard (variable_code, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_metric_date
    ON gold.rpt_acs_observation_dashboard (metric_code, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_observation_date_brin
    ON gold.rpt_acs_observation_dashboard USING BRIN (observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_acs_dashboard_geo_geom
    ON gold.rpt_acs_observation_dashboard USING GIST (geo_geom);

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_bls_dashboard_nk
    ON gold.rpt_bls_observation_dashboard (geo_id, observation_date, series_id, metric_code);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_observation_date
    ON gold.rpt_bls_observation_dashboard (observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_geo_id
    ON gold.rpt_bls_observation_dashboard (geo_id);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_geo_level
    ON gold.rpt_bls_observation_dashboard (geo_level);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_program_code
    ON gold.rpt_bls_observation_dashboard (program_code);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_series_id
    ON gold.rpt_bls_observation_dashboard (series_id);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_metric_code
    ON gold.rpt_bls_observation_dashboard (metric_code);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_measure_category
    ON gold.rpt_bls_observation_dashboard (measure_category);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_geo_date
    ON gold.rpt_bls_observation_dashboard (geo_id, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_series_date
    ON gold.rpt_bls_observation_dashboard (series_id, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_program_date
    ON gold.rpt_bls_observation_dashboard (program_code, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_observation_date_brin
    ON gold.rpt_bls_observation_dashboard USING BRIN (observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_bls_dashboard_geo_geom
    ON gold.rpt_bls_observation_dashboard USING GIST (geo_geom);

CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_fred_dashboard_nk
    ON gold.rpt_fred_observation_dashboard (series_id, observation_date, realtime_start, realtime_end, metric_code);
CREATE INDEX IF NOT EXISTS ix_rpt_fred_dashboard_observation_date
    ON gold.rpt_fred_observation_dashboard (observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_fred_dashboard_geo_id
    ON gold.rpt_fred_observation_dashboard (geo_id);
CREATE INDEX IF NOT EXISTS ix_rpt_fred_dashboard_geo_level
    ON gold.rpt_fred_observation_dashboard (geo_level);
CREATE INDEX IF NOT EXISTS ix_rpt_fred_dashboard_series_id
    ON gold.rpt_fred_observation_dashboard (series_id);
CREATE INDEX IF NOT EXISTS ix_rpt_fred_dashboard_metric_code
    ON gold.rpt_fred_observation_dashboard (metric_code);
CREATE INDEX IF NOT EXISTS ix_rpt_fred_dashboard_series_date
    ON gold.rpt_fred_observation_dashboard (series_id, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_fred_dashboard_metric_date
    ON gold.rpt_fred_observation_dashboard (metric_code, observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_fred_dashboard_observation_date_brin
    ON gold.rpt_fred_observation_dashboard USING BRIN (observation_date);
CREATE INDEX IF NOT EXISTS ix_rpt_fred_dashboard_geo_geom
    ON gold.rpt_fred_observation_dashboard USING GIST (geo_geom);

-- ---------------------------------------------------------------------------
-- Dashboard refresh procedures
-- ---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS gold.refresh_rpt_acs_observation_dashboard();
CREATE OR REPLACE PROCEDURE gold.refresh_rpt_acs_observation_dashboard()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS gold.rpt_acs_observation_dashboard__staging;
    DROP TABLE IF EXISTS gold.rpt_acs_observation_dashboard__old;

    CREATE TABLE gold.rpt_acs_observation_dashboard__staging
        (LIKE gold.rpt_acs_observation_dashboard INCLUDING ALL);

    INSERT INTO gold.rpt_acs_observation_dashboard__staging (
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
        geo_polygon_geojson,
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
    WITH geo_base AS (
        SELECT DISTINCT ON (g.geo_id)
            g.geo_id,
            g.geo_level,
            LPAD(g.state_fips::TEXT, 2, '0') AS state_fips,
            CASE
                WHEN g.county_fips IS NOT NULL THEN LPAD(g.county_fips::TEXT, 3, '0')
                ELSE NULL
            END AS county_fips,
            g.state_name,
            g.county_name,
            g.latitude,
            g.longitude,
            g.geom,
            g.geo_polygon_geojson
        FROM gold.dim_geo g
        WHERE g.is_active = TRUE
        ORDER BY g.geo_id, g.source_year DESC NULLS LAST, g.ingested_at DESC
    )
    SELECT
        'CENSUS_ACS' AS source_code,
        ao.observation_date,
        ao.duration_start,
        ao.duration_end,
        ao.time_sk,
        ao.geo_id,
        COALESCE(gb.geo_level, ao.geo_level) AS geo_level,
        COALESCE(gb.state_fips, ao.state_id) AS state_fips,
        COALESCE(gb.county_fips, RIGHT(ao.county_id, 3)) AS county_fips,
        COALESCE(gb.state_name, ao.state_name) AS state_name,
        COALESCE(gb.county_name, ao.county_name) AS county_name,
        COALESCE(gb.latitude, ao.geo_latitude) AS geo_latitude,
        COALESCE(gb.longitude, ao.geo_longitude) AS geo_longitude,
        COALESCE(gb.geom, ao.geo_geom) AS geo_geom,
        COALESCE(gb.geo_polygon_geojson, CASE WHEN ao.geo_geom IS NOT NULL THEN ST_AsGeoJSON(ao.geo_geom)::TEXT ELSE NULL END) AS geo_polygon_geojson,
        ao.as_of_date,
        ao.updated_at,
        ao.dataset_code,
        ao.vintage_year,
        t.table_id,
        t.table_title,
        v.variable_code,
        v.variable_label,
        COALESCE(v.concept, t.concept) AS concept,
        COALESCE(v.universe, t.universe) AS universe,
        v.denominator_hint,
        v.is_publishable_default,
        ao.estimate_value,
        ao.margin_of_error,
        ao.margin_of_error_pct,
        ao.estimate_annotation,
        ao.moe_annotation,
        COALESCE(mc.metric_code, 'ACS:' || ao.dataset_code || ':' || v.variable_code) AS metric_code,
        COALESCE(mc.metric_display_name, v.variable_label) AS metric_display_name,
        COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL') AS dashboard_suitability,
        mc.business_definition,
        mc.caveats,
        mc.comparability_group,
        COALESCE(mc.do_not_compare_with, ARRAY[]::TEXT[]) AS do_not_compare_with,
        mc.recommended_aggregation,
        mc.owner_team
    FROM gold.fact_acs_observation ao
    JOIN gold.dim_acs_table t
        ON t.acs_table_sk = ao.acs_table_sk
    JOIN gold.dim_acs_variable v
        ON v.acs_variable_sk = ao.acs_variable_sk
    LEFT JOIN geo_base gb
        ON gb.geo_id = ao.geo_id
    LEFT JOIN gold.bridge_metric_acs_variable bma
        ON bma.acs_variable_sk = ao.acs_variable_sk
    LEFT JOIN gold.dim_metric_catalog mc
        ON mc.metric_catalog_sk = bma.metric_catalog_sk
       AND mc.is_active = TRUE;

    ANALYZE gold.rpt_acs_observation_dashboard__staging;

    LOCK TABLE gold.rpt_acs_observation_dashboard IN ACCESS EXCLUSIVE MODE;

    ALTER TABLE gold.rpt_acs_observation_dashboard
        RENAME TO rpt_acs_observation_dashboard__old;

    ALTER TABLE gold.rpt_acs_observation_dashboard__staging
        RENAME TO rpt_acs_observation_dashboard;

    DROP TABLE gold.rpt_acs_observation_dashboard__old;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_rpt_bls_observation_dashboard();
CREATE OR REPLACE PROCEDURE gold.refresh_rpt_bls_observation_dashboard()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS gold.rpt_bls_observation_dashboard__staging;
    DROP TABLE IF EXISTS gold.rpt_bls_observation_dashboard__old;

    CREATE TABLE gold.rpt_bls_observation_dashboard__staging
        (LIKE gold.rpt_bls_observation_dashboard INCLUDING ALL);

    INSERT INTO gold.rpt_bls_observation_dashboard__staging (
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
        geo_polygon_geojson,
        as_of_date,
        updated_at,
        program_code,
        survey_name,
        series_id,
        series_title,
        gold_metric_name,
        measure_name,
        measure_category,
        value_type,
        unit_of_measure,
        seasonal_adjustment_status,
        observation_basis,
        value,
        metric_code,
        metric_display_name,
        dashboard_suitability,
        business_definition,
        metric_caveats,
        comparison_warning,
        comparability_group,
        recommended_aggregation,
        owner_team
    )
    WITH geo_base AS (
        SELECT DISTINCT ON (g.geo_id)
            g.geo_id,
            g.geo_level,
            LPAD(g.state_fips::TEXT, 2, '0') AS state_fips,
            CASE
                WHEN g.county_fips IS NOT NULL THEN LPAD(g.county_fips::TEXT, 3, '0')
                ELSE NULL
            END AS county_fips,
            g.state_name,
            g.county_name,
            g.latitude,
            g.longitude,
            g.geom,
            g.geo_polygon_geojson
        FROM gold.dim_geo g
        WHERE g.is_active = TRUE
        ORDER BY g.geo_id, g.source_year DESC NULLS LAST, g.ingested_at DESC
    )
    SELECT
        'BLS' AS source_code,
        b.period_date AS observation_date,
        b.duration_start,
        b.duration_end,
        b.time_sk,
        b.geo_id,
        COALESCE(gb.geo_level, b.geo_level) AS geo_level,
        COALESCE(gb.state_fips, b.state_id) AS state_fips,
        COALESCE(gb.county_fips, RIGHT(b.county_id, 3)) AS county_fips,
        COALESCE(gb.state_name, b.state_name) AS state_name,
        COALESCE(gb.county_name, b.county_name) AS county_name,
        COALESCE(gb.latitude, b.geo_latitude) AS geo_latitude,
        COALESCE(gb.longitude, b.geo_longitude) AS geo_longitude,
        COALESCE(gb.geom, b.geo_geom) AS geo_geom,
        COALESCE(gb.geo_polygon_geojson, CASE WHEN b.geo_geom IS NOT NULL THEN ST_AsGeoJSON(b.geo_geom)::TEXT ELSE NULL END) AS geo_polygon_geojson,
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
        COALESCE(b.seasonal_adjustment_status, bs.seasonal_adjustment_status) AS seasonal_adjustment_status,
        COALESCE(b.observation_basis, s.observation_basis) AS observation_basis,
        b.value,
        COALESCE(mc.metric_code, 'BLS:' || bs.series_id) AS metric_code,
        COALESCE(mc.metric_display_name, bs.gold_metric_name, bs.series_title) AS metric_display_name,
        COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL') AS dashboard_suitability,
        mc.business_definition,
        mc.caveats AS metric_caveats,
        s.comparison_warning,
        mc.comparability_group,
        mc.recommended_aggregation,
        mc.owner_team
    FROM gold.fact_bls_observation b
    JOIN gold.dim_bls_survey s
        ON s.bls_survey_sk = b.bls_survey_sk
    JOIN gold.dim_bls_series bs
        ON bs.bls_series_sk = b.bls_series_sk
    LEFT JOIN geo_base gb
        ON gb.geo_id = b.geo_id
    LEFT JOIN gold.bridge_metric_bls_series bms
        ON bms.bls_series_sk = b.bls_series_sk
    LEFT JOIN gold.dim_metric_catalog mc
        ON mc.metric_catalog_sk = bms.metric_catalog_sk
       AND mc.is_active = TRUE;

    ANALYZE gold.rpt_bls_observation_dashboard__staging;

    LOCK TABLE gold.rpt_bls_observation_dashboard IN ACCESS EXCLUSIVE MODE;

    ALTER TABLE gold.rpt_bls_observation_dashboard
        RENAME TO rpt_bls_observation_dashboard__old;

    ALTER TABLE gold.rpt_bls_observation_dashboard__staging
        RENAME TO rpt_bls_observation_dashboard;

    DROP TABLE gold.rpt_bls_observation_dashboard__old;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_rpt_fred_observation_dashboard();
CREATE OR REPLACE PROCEDURE gold.refresh_rpt_fred_observation_dashboard()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS gold.rpt_fred_observation_dashboard__staging;
    DROP TABLE IF EXISTS gold.rpt_fred_observation_dashboard__old;

    CREATE TABLE gold.rpt_fred_observation_dashboard__staging
        (LIKE gold.rpt_fred_observation_dashboard INCLUDING ALL);

    INSERT INTO gold.rpt_fred_observation_dashboard__staging (
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
        geo_polygon_geojson,
        as_of_date,
        updated_at,
        series_id,
        series_title,
        source_provider,
        original_source_name,
        is_primary_source_series,
        is_republished_series,
        frequency,
        units,
        seasonal_adjustment,
        transformation_method,
        realtime_start,
        realtime_end,
        value,
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
    WITH geo_base AS (
        SELECT DISTINCT ON (g.geo_id)
            g.geo_id,
            g.geo_level,
            LPAD(g.state_fips::TEXT, 2, '0') AS state_fips,
            CASE
                WHEN g.county_fips IS NOT NULL THEN LPAD(g.county_fips::TEXT, 3, '0')
                ELSE NULL
            END AS county_fips,
            g.state_name,
            g.county_name,
            g.latitude,
            g.longitude,
            g.geom,
            g.geo_polygon_geojson
        FROM gold.dim_geo g
        WHERE g.is_active = TRUE
        ORDER BY g.geo_id, g.source_year DESC NULLS LAST, g.ingested_at DESC
    )
    SELECT
        'FRED' AS source_code,
        f.observation_date,
        f.duration_start,
        f.duration_end,
        f.time_sk,
        'us:1' AS geo_id,
        COALESCE(gb.geo_level, f.geo_level, 'NATIONAL') AS geo_level,
        gb.state_fips,
        gb.county_fips,
        gb.state_name,
        gb.county_name,
        gb.latitude AS geo_latitude,
        gb.longitude AS geo_longitude,
        gb.geom AS geo_geom,
        gb.geo_polygon_geojson,
        f.as_of_date,
        f.updated_at,
        fs.series_id,
        fs.series_title,
        COALESCE(f.source_provider, fs.source_provider) AS source_provider,
        fs.original_source_name,
        fs.is_primary_source_series,
        fs.is_republished_series,
        COALESCE(f.frequency, fs.frequency) AS frequency,
        COALESCE(f.units, fs.units) AS units,
        COALESCE(f.seasonal_adjustment, fs.seasonal_adjustment) AS seasonal_adjustment,
        COALESCE(fs.transformation_method, f.transform_applied) AS transformation_method,
        f.realtime_start,
        f.realtime_end,
        f.value,
        COALESCE(mc.metric_code, 'FRED:' || fs.series_id) AS metric_code,
        COALESCE(mc.metric_display_name, fs.series_title) AS metric_display_name,
        COALESCE(mc.dashboard_suitability, 'EXPERIMENTAL') AS dashboard_suitability,
        mc.business_definition,
        mc.caveats,
        mc.comparability_group,
        COALESCE(mc.do_not_compare_with, ARRAY[]::TEXT[]) AS do_not_compare_with,
        mc.recommended_aggregation,
        mc.owner_team
    FROM gold.fact_fred_observation f
    JOIN gold.dim_fred_series fs
        ON fs.fred_series_sk = f.fred_series_sk
    LEFT JOIN geo_base gb
        ON gb.geo_id = 'us:1'
    LEFT JOIN gold.bridge_metric_fred_series bmf
        ON bmf.fred_series_sk = f.fred_series_sk
    LEFT JOIN gold.dim_metric_catalog mc
        ON mc.metric_catalog_sk = bmf.metric_catalog_sk
       AND mc.is_active = TRUE;

    ANALYZE gold.rpt_fred_observation_dashboard__staging;

    LOCK TABLE gold.rpt_fred_observation_dashboard IN ACCESS EXCLUSIVE MODE;

    ALTER TABLE gold.rpt_fred_observation_dashboard
        RENAME TO rpt_fred_observation_dashboard__old;

    ALTER TABLE gold.rpt_fred_observation_dashboard__staging
        RENAME TO rpt_fred_observation_dashboard;

    DROP TABLE gold.rpt_fred_observation_dashboard__old;
END;
$$;

-- ---------------------------------------------------------------------------
-- Latest dashboard snapshots
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n
            ON n.oid = c.relnamespace
        WHERE n.nspname = 'gold'
          AND c.relname = 'mv_acs_latest_dashboard'
          AND c.relkind = 'm'
    ) THEN
        EXECUTE 'DROP MATERIALIZED VIEW gold.mv_acs_latest_dashboard CASCADE';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n
            ON n.oid = c.relnamespace
        WHERE n.nspname = 'gold'
          AND c.relname = 'mv_bls_latest_dashboard'
          AND c.relkind = 'm'
    ) THEN
        EXECUTE 'DROP MATERIALIZED VIEW gold.mv_bls_latest_dashboard CASCADE';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n
            ON n.oid = c.relnamespace
        WHERE n.nspname = 'gold'
          AND c.relname = 'mv_fred_latest_dashboard'
          AND c.relkind = 'm'
    ) THEN
        EXECUTE 'DROP MATERIALIZED VIEW gold.mv_fred_latest_dashboard CASCADE';
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS gold.mv_acs_latest_dashboard
    (LIKE gold.rpt_acs_observation_dashboard INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS);

CREATE TABLE IF NOT EXISTS gold.mv_bls_latest_dashboard
    (LIKE gold.rpt_bls_observation_dashboard INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS);

CREATE TABLE IF NOT EXISTS gold.mv_fred_latest_dashboard
    (LIKE gold.rpt_fred_observation_dashboard INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_acs_latest_dashboard
    ON gold.mv_acs_latest_dashboard (geo_id, variable_code, metric_code);
CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_dashboard_metric_code
    ON gold.mv_acs_latest_dashboard (metric_code);
CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_dashboard_observation_date
    ON gold.mv_acs_latest_dashboard (observation_date);
CREATE INDEX IF NOT EXISTS ix_mv_acs_latest_dashboard_geo_geom
    ON gold.mv_acs_latest_dashboard USING GIST (geo_geom);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_bls_latest_dashboard
    ON gold.mv_bls_latest_dashboard (geo_id, series_id, metric_code);
CREATE INDEX IF NOT EXISTS ix_mv_bls_latest_dashboard_metric_code
    ON gold.mv_bls_latest_dashboard (metric_code);
CREATE INDEX IF NOT EXISTS ix_mv_bls_latest_dashboard_observation_date
    ON gold.mv_bls_latest_dashboard (observation_date);
CREATE INDEX IF NOT EXISTS ix_mv_bls_latest_dashboard_geo_geom
    ON gold.mv_bls_latest_dashboard USING GIST (geo_geom);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_fred_latest_dashboard
    ON gold.mv_fred_latest_dashboard (geo_id, series_id, metric_code);
CREATE INDEX IF NOT EXISTS ix_mv_fred_latest_dashboard_metric_code
    ON gold.mv_fred_latest_dashboard (metric_code);
CREATE INDEX IF NOT EXISTS ix_mv_fred_latest_dashboard_observation_date
    ON gold.mv_fred_latest_dashboard (observation_date);
CREATE INDEX IF NOT EXISTS ix_mv_fred_latest_dashboard_geo_geom
    ON gold.mv_fred_latest_dashboard USING GIST (geo_geom);

DROP PROCEDURE IF EXISTS gold.refresh_mv_acs_latest_dashboard();
CREATE OR REPLACE PROCEDURE gold.refresh_mv_acs_latest_dashboard()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS gold.mv_acs_latest_dashboard__staging;
    DROP TABLE IF EXISTS gold.mv_acs_latest_dashboard__old;

    CREATE TABLE gold.mv_acs_latest_dashboard__staging
        (LIKE gold.mv_acs_latest_dashboard INCLUDING ALL);

    INSERT INTO gold.mv_acs_latest_dashboard__staging (
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
        geo_polygon_geojson,
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
    SELECT DISTINCT ON (d.geo_id, d.variable_code, d.metric_code)
        d.source_code,
        d.observation_date,
        d.duration_start,
        d.duration_end,
        d.time_sk,
        d.geo_id,
        d.geo_level,
        d.state_fips,
        d.county_fips,
        d.state_name,
        d.county_name,
        d.geo_latitude,
        d.geo_longitude,
        d.geo_geom,
        d.geo_polygon_geojson,
        d.as_of_date,
        d.updated_at,
        d.dataset_code,
        d.vintage_year,
        d.table_id,
        d.table_title,
        d.variable_code,
        d.variable_label,
        d.concept,
        d.universe,
        d.denominator_hint,
        d.is_publishable_default,
        d.estimate_value,
        d.margin_of_error,
        d.margin_of_error_pct,
        d.estimate_annotation,
        d.moe_annotation,
        d.metric_code,
        d.metric_display_name,
        d.dashboard_suitability,
        d.business_definition,
        d.caveats,
        d.comparability_group,
        d.do_not_compare_with,
        d.recommended_aggregation,
        d.owner_team
    FROM gold.rpt_acs_observation_dashboard d
    ORDER BY
        d.geo_id,
        d.variable_code,
        d.metric_code,
        d.observation_date DESC,
        d.updated_at DESC,
        CASE d.dataset_code WHEN 'acs1' THEN 1 WHEN 'acs5' THEN 2 ELSE 9 END,
        d.vintage_year DESC;

    ANALYZE gold.mv_acs_latest_dashboard__staging;

    LOCK TABLE gold.mv_acs_latest_dashboard IN ACCESS EXCLUSIVE MODE;

    ALTER TABLE gold.mv_acs_latest_dashboard
        RENAME TO mv_acs_latest_dashboard__old;

    ALTER TABLE gold.mv_acs_latest_dashboard__staging
        RENAME TO mv_acs_latest_dashboard;

    DROP TABLE gold.mv_acs_latest_dashboard__old;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_mv_bls_latest_dashboard();
CREATE OR REPLACE PROCEDURE gold.refresh_mv_bls_latest_dashboard()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS gold.mv_bls_latest_dashboard__staging;
    DROP TABLE IF EXISTS gold.mv_bls_latest_dashboard__old;

    CREATE TABLE gold.mv_bls_latest_dashboard__staging
        (LIKE gold.mv_bls_latest_dashboard INCLUDING ALL);

    INSERT INTO gold.mv_bls_latest_dashboard__staging (
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
        geo_polygon_geojson,
        as_of_date,
        updated_at,
        program_code,
        survey_name,
        series_id,
        series_title,
        gold_metric_name,
        measure_name,
        measure_category,
        value_type,
        unit_of_measure,
        seasonal_adjustment_status,
        observation_basis,
        value,
        metric_code,
        metric_display_name,
        dashboard_suitability,
        business_definition,
        metric_caveats,
        comparison_warning,
        comparability_group,
        recommended_aggregation,
        owner_team
    )
    SELECT DISTINCT ON (d.geo_id, d.series_id, d.metric_code)
        d.source_code,
        d.observation_date,
        d.duration_start,
        d.duration_end,
        d.time_sk,
        d.geo_id,
        d.geo_level,
        d.state_fips,
        d.county_fips,
        d.state_name,
        d.county_name,
        d.geo_latitude,
        d.geo_longitude,
        d.geo_geom,
        d.geo_polygon_geojson,
        d.as_of_date,
        d.updated_at,
        d.program_code,
        d.survey_name,
        d.series_id,
        d.series_title,
        d.gold_metric_name,
        d.measure_name,
        d.measure_category,
        d.value_type,
        d.unit_of_measure,
        d.seasonal_adjustment_status,
        d.observation_basis,
        d.value,
        d.metric_code,
        d.metric_display_name,
        d.dashboard_suitability,
        d.business_definition,
        d.metric_caveats,
        d.comparison_warning,
        d.comparability_group,
        d.recommended_aggregation,
        d.owner_team
    FROM gold.rpt_bls_observation_dashboard d
    ORDER BY
        d.geo_id,
        d.series_id,
        d.metric_code,
        d.observation_date DESC,
        d.updated_at DESC;

    ANALYZE gold.mv_bls_latest_dashboard__staging;

    LOCK TABLE gold.mv_bls_latest_dashboard IN ACCESS EXCLUSIVE MODE;

    ALTER TABLE gold.mv_bls_latest_dashboard
        RENAME TO mv_bls_latest_dashboard__old;

    ALTER TABLE gold.mv_bls_latest_dashboard__staging
        RENAME TO mv_bls_latest_dashboard;

    DROP TABLE gold.mv_bls_latest_dashboard__old;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_mv_fred_latest_dashboard();
CREATE OR REPLACE PROCEDURE gold.refresh_mv_fred_latest_dashboard()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS gold.mv_fred_latest_dashboard__staging;
    DROP TABLE IF EXISTS gold.mv_fred_latest_dashboard__old;

    CREATE TABLE gold.mv_fred_latest_dashboard__staging
        (LIKE gold.mv_fred_latest_dashboard INCLUDING ALL);

    INSERT INTO gold.mv_fred_latest_dashboard__staging (
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
        geo_polygon_geojson,
        as_of_date,
        updated_at,
        series_id,
        series_title,
        source_provider,
        original_source_name,
        is_primary_source_series,
        is_republished_series,
        frequency,
        units,
        seasonal_adjustment,
        transformation_method,
        realtime_start,
        realtime_end,
        value,
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
    SELECT DISTINCT ON (d.geo_id, d.series_id, d.metric_code)
        d.source_code,
        d.observation_date,
        d.duration_start,
        d.duration_end,
        d.time_sk,
        d.geo_id,
        d.geo_level,
        d.state_fips,
        d.county_fips,
        d.state_name,
        d.county_name,
        d.geo_latitude,
        d.geo_longitude,
        d.geo_geom,
        d.geo_polygon_geojson,
        d.as_of_date,
        d.updated_at,
        d.series_id,
        d.series_title,
        d.source_provider,
        d.original_source_name,
        d.is_primary_source_series,
        d.is_republished_series,
        d.frequency,
        d.units,
        d.seasonal_adjustment,
        d.transformation_method,
        d.realtime_start,
        d.realtime_end,
        d.value,
        d.metric_code,
        d.metric_display_name,
        d.dashboard_suitability,
        d.business_definition,
        d.caveats,
        d.comparability_group,
        d.do_not_compare_with,
        d.recommended_aggregation,
        d.owner_team
    FROM gold.rpt_fred_observation_dashboard d
    ORDER BY
        d.geo_id,
        d.series_id,
        d.metric_code,
        d.observation_date DESC,
        d.realtime_start DESC NULLS LAST,
        d.realtime_end DESC NULLS LAST,
        d.updated_at DESC;

    ANALYZE gold.mv_fred_latest_dashboard__staging;

    LOCK TABLE gold.mv_fred_latest_dashboard IN ACCESS EXCLUSIVE MODE;

    ALTER TABLE gold.mv_fred_latest_dashboard
        RENAME TO mv_fred_latest_dashboard__old;

    ALTER TABLE gold.mv_fred_latest_dashboard__staging
        RENAME TO mv_fred_latest_dashboard;

    DROP TABLE gold.mv_fred_latest_dashboard__old;
END;
$$;

DROP PROCEDURE IF EXISTS gold.refresh_dashboard_serving_layer();
CREATE OR REPLACE PROCEDURE gold.refresh_dashboard_serving_layer()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL gold.refresh_rpt_acs_observation_dashboard();
    CALL gold.refresh_rpt_bls_observation_dashboard();
    CALL gold.refresh_rpt_fred_observation_dashboard();
    CALL gold.refresh_mv_acs_latest_dashboard();
    CALL gold.refresh_mv_bls_latest_dashboard();
    CALL gold.refresh_mv_fred_latest_dashboard();
END;
$$;

-- ---------------------------------------------------------------------------
-- Cleanup: retire slow dashboard views
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS gold.vw_metrics CASCADE;
DROP VIEW IF EXISTS gold.vw_fred_series CASCADE;
DROP VIEW IF EXISTS gold.vw_macro_headlines CASCADE;
DROP VIEW IF EXISTS gold.vw_bls_labor_market CASCADE;
DROP VIEW IF EXISTS gold.vw_acs_latest CASCADE;
DROP VIEW IF EXISTS gold.vw_acs_trends CASCADE;
DROP VIEW IF EXISTS gold.vw_geo_summary CASCADE;
