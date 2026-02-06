-- silver/DDL/silver.sql
--
-- Silver layer: analytics-ready tables consolidating BLS, FRED, and Census data.
-- Each row represents one observation for one geographic code at one time point.
-- Geography is unified via silver_ref.dim_geo (geo_level + geo_id).

CREATE SCHEMA IF NOT EXISTS silver;

-- -----------------------------------------------------------------
-- 1. Consolidated fact table: one row per source/series/geo/time
-- -----------------------------------------------------------------
-- Merges BLS (bls_long), Census ACS (acs_long), and FRED (fred_long)
-- into a single long-format fact table with unified geography keys.
--
-- Design:
--   - geo_level + geo_id foreign-key to silver_ref.dim_geo
--   - obs_date normalises BLS year+period and FRED obs_date to DATE
--   - FRED series (national-only) carry geo_level='us', geo_id='us:1'
--   - Census variables carry the E/M measure_type split
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.fact_observations (
    id              BIGSERIAL PRIMARY KEY,

    -- Source provenance
    source          TEXT NOT NULL,         -- 'bls', 'census', 'fred'
    program         TEXT,                  -- BLS program or Census dataset (e.g. 'la', 'acs5')
    domain          TEXT,                  -- FRED domain (e.g. 'housing', 'labor_cycle')

    -- Series / variable identity
    series_id       TEXT,                  -- BLS series_id or FRED series_id
    variable_name   TEXT,                  -- Census variable name (e.g. 'B01003_001E')
    table_id        TEXT,                  -- Census table_id (e.g. 'B01003')
    measure_type    TEXT,                  -- Census 'E'/'M', NULL for BLS/FRED

    -- Unified geography (matches silver_ref.dim_geo)
    geo_level       TEXT NOT NULL,         -- 'us', 'state', 'county'
    geo_id          TEXT NOT NULL,         -- 'us:1', 'state:06', 'state:06|county:037'
    state_fips      TEXT,
    county_fips     TEXT,

    -- Unified time
    obs_date        DATE NOT NULL,         -- Normalised observation date
    year            INTEGER NOT NULL,
    month           INTEGER,               -- NULL when period is annual
    quarter         INTEGER,               -- Derived quarter (1-4)

    -- Observation
    value           NUMERIC,
    is_missing      BOOLEAN NOT NULL DEFAULT FALSE,

    -- Lineage
    load_batch_id   UUID NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Uniqueness: one value per source/series-or-variable/geo/date
CREATE UNIQUE INDEX IF NOT EXISTS fact_obs_bls_uniq
    ON silver.fact_observations (source, series_id, geo_level, geo_id, obs_date)
    WHERE source = 'bls';

CREATE UNIQUE INDEX IF NOT EXISTS fact_obs_census_uniq
    ON silver.fact_observations (source, variable_name, geo_level, geo_id, obs_date, measure_type)
    WHERE source = 'census';

CREATE UNIQUE INDEX IF NOT EXISTS fact_obs_fred_uniq
    ON silver.fact_observations (source, series_id, obs_date)
    WHERE source = 'fred';

-- Query-path indexes
CREATE INDEX IF NOT EXISTS fact_obs_geo_idx
    ON silver.fact_observations (geo_level, geo_id);

CREATE INDEX IF NOT EXISTS fact_obs_date_idx
    ON silver.fact_observations (obs_date);

CREATE INDEX IF NOT EXISTS fact_obs_source_idx
    ON silver.fact_observations (source);

CREATE INDEX IF NOT EXISTS fact_obs_series_idx
    ON silver.fact_observations (series_id)
    WHERE series_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS fact_obs_variable_idx
    ON silver.fact_observations (variable_name)
    WHERE variable_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS fact_obs_year_idx
    ON silver.fact_observations (year);

-- Domain checks
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fact_obs_source_chk'
    ) THEN
        ALTER TABLE silver.fact_observations
        ADD CONSTRAINT fact_obs_source_chk
        CHECK (source IN ('bls', 'census', 'fred'));
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fact_obs_geo_level_chk'
    ) THEN
        ALTER TABLE silver.fact_observations
        ADD CONSTRAINT fact_obs_geo_level_chk
        CHECK (geo_level IN ('us', 'state', 'county'));
    END IF;
END $$;
