-- fred/DDL/fred_silver.sql
--
-- Silver layer for FRED: analytics-ready observations.
-- Housed in the fred subject folder alongside raw_fred.sql.
--
-- Geography is unified via silver_ref.dim_geo (geo_level + geo_id).
-- FRED is national-only; geo_level/geo_id default to 'us'/'us:1'.

CREATE SCHEMA IF NOT EXISTS silver_fred;

CREATE TABLE IF NOT EXISTS silver_fred.fred_observations (
    id              BIGSERIAL PRIMARY KEY,

    -- FRED identity
    domain          TEXT,                     -- logical grouping (e.g. 'housing', 'labor_cycle')
    series_id       TEXT NOT NULL,

    -- Geography (FRED is national-only; explicit for join compatibility)
    geo_level       TEXT NOT NULL DEFAULT 'us',
    geo_id          TEXT NOT NULL DEFAULT 'us:1',

    -- Time
    obs_date        DATE NOT NULL,
    year            INTEGER NOT NULL,
    month           INTEGER,
    quarter         INTEGER,

    -- Observation
    value           NUMERIC,
    is_missing      BOOLEAN NOT NULL DEFAULT FALSE,

    -- Lineage
    load_batch_id   UUID NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS fred_obs_uniq
    ON silver_fred.fred_observations (series_id, obs_date);

CREATE INDEX IF NOT EXISTS fred_obs_domain_idx
    ON silver_fred.fred_observations (domain)
    WHERE domain IS NOT NULL;

CREATE INDEX IF NOT EXISTS fred_obs_date_idx
    ON silver_fred.fred_observations (obs_date);

CREATE INDEX IF NOT EXISTS fred_obs_year_idx
    ON silver_fred.fred_observations (year);

CREATE INDEX IF NOT EXISTS fred_obs_series_idx
    ON silver_fred.fred_observations (series_id);
