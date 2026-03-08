-- gold/DDL/gold.sql
-- Gold analytics layer schema

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
    state_id            TEXT,
    state_name          TEXT,
    county_id           TEXT,
    county_name         TEXT,
    month_start         DATE        NOT NULL,
    year                INTEGER     NOT NULL,
    quarter             INTEGER     NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    source_system       TEXT        NOT NULL,
    element_id          TEXT        NOT NULL,
    element_name        TEXT        NOT NULL,
    value               NUMERIC,
    observation_date    DATE,
    unit_of_measure     TEXT,
    seasonal_adjustment TEXT,
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (geo_id, month_start, source_system, element_id)
);

ALTER TABLE gold.fact_metrics
    ADD COLUMN IF NOT EXISTS state_id TEXT;

ALTER TABLE gold.fact_metrics
    ADD COLUMN IF NOT EXISTS state_name TEXT;

ALTER TABLE gold.fact_metrics
    ADD COLUMN IF NOT EXISTS county_id TEXT;

ALTER TABLE gold.fact_metrics
    ADD COLUMN IF NOT EXISTS county_name TEXT;

ALTER TABLE gold.fact_metrics
    ADD COLUMN IF NOT EXISTS year INTEGER;

ALTER TABLE gold.fact_metrics
    ADD COLUMN IF NOT EXISTS quarter INTEGER;

CREATE INDEX IF NOT EXISTS ix_fact_metrics_geo_id
    ON gold.fact_metrics (geo_id);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_month_start
    ON gold.fact_metrics (month_start);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_source_system
    ON gold.fact_metrics (source_system);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_element_id
    ON gold.fact_metrics (element_id);

CREATE INDEX IF NOT EXISTS ix_fact_metrics_geo_month
    ON gold.fact_metrics (geo_id, month_start);

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
