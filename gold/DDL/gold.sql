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
    month_start         DATE        NOT NULL,
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
