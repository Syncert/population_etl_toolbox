-- gold/DDL/gold.sql
-- Gold analytics layer baseline schema (fresh install)

CREATE SCHEMA IF NOT EXISTS gold;
CREATE EXTENSION IF NOT EXISTS postgis;

-- ---------------------------------------------------------------------------
-- Conformed dimensions (read-only views over silver_ref)
-- ---------------------------------------------------------------------------
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
    geo_geom                  geometry(MultiPolygon, 4326),
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
-- User-facing views
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_metric_catalog AS
SELECT
    c.metric_catalog_sk,
    c.metric_code,
    c.metric_display_name,
    c.source_code,
    c.source_object_type,
    c.business_definition,
    c.caveats,
    c.valid_geo_grains,
    c.valid_time_grains,
    c.dashboard_suitability,
    c.comparability_group,
    c.do_not_compare_with,
    c.recommended_aggregation,
    c.owner_team,
    c.is_active,
    c.updated_at
FROM gold.dim_metric_catalog c
WHERE c.is_active = TRUE;

CREATE OR REPLACE VIEW gold.vw_headline_macro_metrics AS
SELECT
    c.metric_code,
    c.metric_display_name,
    c.source_code,
    b.period_date AS observation_date,
    b.geo_id,
    b.value,
    c.caveats,
    c.comparability_group
FROM gold.dim_metric_catalog c
JOIN gold.bridge_metric_bls_series bm
    ON bm.metric_catalog_sk = c.metric_catalog_sk
JOIN gold.fact_bls_observation b
    ON b.bls_series_sk = bm.bls_series_sk
WHERE c.is_active = TRUE
UNION ALL
SELECT
    c.metric_code,
    c.metric_display_name,
    c.source_code,
    f.observation_date,
    f.geo_id,
    f.value,
    c.caveats,
    c.comparability_group
FROM gold.dim_metric_catalog c
JOIN gold.bridge_metric_fred_series fm
    ON fm.metric_catalog_sk = c.metric_catalog_sk
JOIN gold.fact_fred_observation f
    ON f.fred_series_sk = fm.fred_series_sk
WHERE c.is_active = TRUE;

CREATE OR REPLACE VIEW gold.vw_labor_market_overview AS
SELECT
    b.period_date AS observation_date,
    b.geo_id,
    b.geo_latitude,
    b.geo_longitude,
    b.geo_geom,
    ST_AsGeoJSON(b.geo_geom)::TEXT AS geo_polygon_geojson,
    b.program_code,
    s.survey_name,
    b.measure_category,
    b.value_type,
    b.value,
    s.comparison_warning
FROM gold.fact_bls_observation b
JOIN gold.dim_bls_survey s
    ON s.bls_survey_sk = b.bls_survey_sk
WHERE b.measure_category IN ('EMPLOYMENT', 'UNEMPLOYMENT', 'LABOR_FORCE', 'PARTICIPATION', 'OPENINGS', 'HIRES', 'QUITS', 'LAYOFFS', 'SEPARATIONS');

CREATE OR REPLACE VIEW gold.vw_acs_dashboard_metrics AS
WITH ranked AS (
    SELECT
        ao.observation_date,
        ao.duration_start,
        ao.duration_end,
        ao.geo_id,
        ao.geo_level,
        d.state_fips,
        d.county_fips,
        ao.state_name,
        ao.county_name,
        ao.geo_latitude,
        ao.geo_longitude,
        ao.geo_geom,
        ST_AsGeoJSON(ao.geo_geom)::TEXT AS geo_polygon_geojson,
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
        ('ACS:' || ao.dataset_code || ':' || v.variable_code) AS metric_code,
        ao.estimate_value,
        ao.margin_of_error,
        ao.margin_of_error_pct,
        ao.estimate_annotation,
        ao.moe_annotation,
        ao.as_of_date,
        ao.updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY ao.geo_id, v.variable_code
            ORDER BY ao.observation_date DESC,
                     ao.updated_at DESC,
                     CASE ao.dataset_code WHEN 'acs1' THEN 1 WHEN 'acs5' THEN 2 ELSE 3 END
        ) AS recency_rank
    FROM gold.fact_acs_observation ao
    JOIN gold.dim_acs_variable v
        ON v.acs_variable_sk = ao.acs_variable_sk
    JOIN gold.dim_acs_table t
        ON t.acs_table_sk = ao.acs_table_sk
    JOIN gold.dim_geo d
        ON d.geo_id = ao.geo_id
    WHERE d.is_active = TRUE
)
SELECT
    r.observation_date,
    r.duration_start,
    r.duration_end,
    r.geo_id,
    r.geo_level,
    r.state_fips,
    r.county_fips,
    r.state_name,
    r.county_name,
    r.geo_latitude,
    r.geo_longitude,
    r.geo_geom,
    r.geo_polygon_geojson,
    r.dataset_code,
    r.vintage_year,
    r.table_id,
    r.table_title,
    r.variable_code,
    r.variable_label,
    r.concept,
    r.universe,
    r.denominator_hint,
    r.is_publishable_default,
    r.metric_code,
    mc.metric_display_name,
    mc.dashboard_suitability,
    mc.business_definition,
    mc.caveats,
    mc.comparability_group,
    mc.do_not_compare_with,
    mc.recommended_aggregation,
    mc.owner_team,
    r.estimate_value,
    r.margin_of_error,
    r.margin_of_error_pct,
    r.estimate_annotation,
    r.moe_annotation,
    r.as_of_date,
    r.updated_at
FROM ranked r
LEFT JOIN gold.dim_metric_catalog mc
    ON mc.metric_code = r.metric_code
   AND mc.source_code = 'CENSUS_ACS'
   AND mc.is_active = TRUE
WHERE r.recency_rank = 1;
