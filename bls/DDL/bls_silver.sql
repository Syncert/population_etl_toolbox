-- bls/DDL/bls_silver.sql
--
-- Silver layer for BLS: analytics-ready observations.
-- Housed in the BLS subject folder alongside raw_bls.sql.
--
-- Geography is unified via silver_ref.dim_geo (geo_level + geo_id).

CREATE SCHEMA IF NOT EXISTS silver_bls;

CREATE TABLE IF NOT EXISTS silver_bls.bls_observations (
    id              BIGSERIAL PRIMARY KEY,

    -- BLS identity
    program         TEXT NOT NULL,            -- 'la', 'ln', 'ce', 'cu', 'jt'
    series_id       TEXT NOT NULL,

    -- Unified geography (matches silver_ref.dim_geo)
    geo_level       TEXT NOT NULL,            -- 'us', 'state', 'county'
    geo_id          TEXT NOT NULL,            -- 'us:1', 'state:06', 'state:06|county:037'
    state_fips      TEXT,
    county_fips     TEXT,

    -- Time (normalized from year + period)
    obs_date        DATE NOT NULL,            -- first of month/quarter/year
    year            INTEGER NOT NULL,
    month           INTEGER,                  -- NULL for annual (M13) or quarterly
    quarter         INTEGER,                  -- 1-4

    -- Observation
    value           NUMERIC,
    is_missing      BOOLEAN NOT NULL DEFAULT FALSE,

    -- Lineage
    load_batch_id   UUID NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS bls_obs_uniq
    ON silver_bls.bls_observations (series_id, geo_level, geo_id, obs_date);

CREATE INDEX IF NOT EXISTS bls_obs_program_idx
    ON silver_bls.bls_observations (program);

CREATE INDEX IF NOT EXISTS bls_obs_geo_idx
    ON silver_bls.bls_observations (geo_level, geo_id);

CREATE INDEX IF NOT EXISTS bls_obs_date_idx
    ON silver_bls.bls_observations (obs_date);

CREATE INDEX IF NOT EXISTS bls_obs_year_idx
    ON silver_bls.bls_observations (year);

CREATE INDEX IF NOT EXISTS bls_obs_series_idx
    ON silver_bls.bls_observations (series_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'bls_obs_geo_level_chk'
    ) THEN
        ALTER TABLE silver_bls.bls_observations
        ADD CONSTRAINT bls_obs_geo_level_chk
        CHECK (geo_level IN ('us', 'state', 'county'));
    END IF;
END $$;
