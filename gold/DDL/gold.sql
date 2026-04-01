-- gold/DDL/gold.sql
-- Gold analytics layer baseline schema (fresh install)
-- NOTE: Existing environments should use versioned scripts in gold/DDL/migrations.

CREATE SCHEMA IF NOT EXISTS gold;

-- ---------------------------------------------------------------------------
-- dim_element: canonical element dictionary across all silver sources
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_element (
    element_sk      BIGSERIAL PRIMARY KEY,
    element_id      TEXT        NOT NULL,
    source_system   TEXT        NOT NULL,
    element_name    TEXT        NOT NULL,
    unit_of_measure TEXT,
    value_semantics TEXT,
    metric_family   TEXT,
    source_product  TEXT,
    survey_concept  TEXT,
    default_period_type TEXT,
    is_seasonally_adjusted_default BOOLEAN,
    is_saar_default BOOLEAN,
    notes           TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (element_id, source_system)
);

-- ---------------------------------------------------------------------------
-- fact_metrics: unified monthly metric grain
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_metrics (
    metric_sk           BIGSERIAL PRIMARY KEY,
    geo_id              TEXT        NOT NULL,
    geo_level           TEXT        NOT NULL,
    state_id            TEXT,
    state_name          TEXT,
    county_id           TEXT,
    county_name         TEXT,
    month_start         DATE        NOT NULL,
    period_type         TEXT        NOT NULL,
    year                INTEGER     NOT NULL,
    quarter             INTEGER     NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    source_system       TEXT        NOT NULL,
    element_id          TEXT        NOT NULL,
    element_name        TEXT        NOT NULL,
    value               NUMERIC,
    observation_date    DATE,
    observation_end     DATE,
    duration_start      DATE,
    duration_end        DATE,
    acs_dataset         TEXT,
    margin_of_error     NUMERIC,
    margin_of_error_pct NUMERIC,
    survey_concept      TEXT,
    unit_of_measure     TEXT,
    value_semantics     TEXT,
    is_seasonally_adjusted BOOLEAN,
    is_saar             BOOLEAN,
    seasonal_adjustment TEXT,
    source_published_date DATE,
    revision_date       DATE,
    data_vintage_date   DATE,
    as_of_date          DATE,
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fact_metrics_geo_level_chk
        CHECK (geo_level IN ('NATIONAL', 'STATE', 'COUNTY')),
    CONSTRAINT fact_metrics_period_type_chk
        CHECK (period_type IN ('MONTHLY', 'QUARTERLY', 'ANNUAL', 'ACS5')),
    CONSTRAINT fact_metrics_acs_dataset_chk
        CHECK (acs_dataset IN ('acs1', 'acs5') OR acs_dataset IS NULL),
    UNIQUE (geo_id, month_start, source_system, element_id)
);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_geo_id
    ON gold.fact_metrics (geo_id);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_geo_level
    ON gold.fact_metrics (geo_level);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_month_start
    ON gold.fact_metrics (month_start);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_source_system
    ON gold.fact_metrics (source_system);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_period_type
    ON gold.fact_metrics (period_type);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_element_id
    ON gold.fact_metrics (element_id);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_observation_end
    ON gold.fact_metrics (observation_end);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_geo_month
    ON gold.fact_metrics (geo_id, month_start);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_acs_dataset
    ON gold.fact_metrics (acs_dataset)
    WHERE source_system = 'CENSUS_ACS';

CREATE INDEX IF NOT EXISTS ix_fact_metrics_survey_concept
    ON gold.fact_metrics (survey_concept)
    WHERE source_system = 'BLS';

CREATE INDEX IF NOT EXISTS ix_fact_metrics_is_saar
    ON gold.fact_metrics (is_saar)
    WHERE source_system = 'FRED';

CREATE INDEX IF NOT EXISTS ix_fact_metrics_element_period
    ON gold.fact_metrics (source_system, element_id, month_start);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_state_id
    ON gold.fact_metrics (state_id);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_county_id
    ON gold.fact_metrics (county_id);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_year
    ON gold.fact_metrics (year);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_quarter
    ON gold.fact_metrics (quarter);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_state_period
    ON gold.fact_metrics (state_id, year, quarter, month_start);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_county_period
    ON gold.fact_metrics (county_id, year, quarter, month_start);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_state_name_period
    ON gold.fact_metrics (state_name, year, quarter, month_start);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_county_name_period
    ON gold.fact_metrics (county_name, year, quarter, month_start);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_state_name_ci_period
    ON gold.fact_metrics ((LOWER(state_name)), year, quarter, month_start);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_county_name_ci_period
    ON gold.fact_metrics ((LOWER(county_name)), year, quarter, month_start);

CREATE OR REPLACE VIEW gold.v_metrics_analytics AS
SELECT
    metric_sk,
    geo_id,
    geo_level,
    state_id,
    state_name,
    county_id,
    county_name,
    month_start,
    year,
    quarter,
    period_type,
    source_system,
    element_id,
    element_name,
    value,
    observation_date,
    observation_end,
    duration_start,
    duration_end,
    acs_dataset,
    margin_of_error,
    margin_of_error_pct,
    survey_concept,
    unit_of_measure,
    value_semantics,
    is_seasonally_adjusted,
    is_saar,
    source_published_date,
    revision_date,
    data_vintage_date,
    as_of_date,
    updated_at
FROM gold.fact_metrics;
