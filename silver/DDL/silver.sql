-- silver/DDL/silver.sql
--
-- Silver layer: per-source analytics-ready tables.
--
-- Architecture:
--   raw_bls  / raw_census  / raw_fred   – bronze (untouched API data)
--   silver_ref                           – shared dimensions (dim_geo, dim_time)
--   silver_bls / silver_census / silver_fred – clean, analytics-ready facts
--
-- Each silver table keeps only the columns natural to its source.
-- Geography is unified via silver_ref.dim_geo (geo_level + geo_id).
-- No columns are forced to NULL because another source doesn't have them.

-- =================================================================
-- silver_bls — BLS analytics-ready observations
-- =================================================================
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


-- =================================================================
-- silver_census — Census ACS analytics-ready observations
-- =================================================================
CREATE SCHEMA IF NOT EXISTS silver_census;

CREATE TABLE IF NOT EXISTS silver_census.census_observations (
    id              BIGSERIAL PRIMARY KEY,

    -- Census identity
    dataset         TEXT NOT NULL,            -- 'acs1', 'acs5'
    table_id        TEXT NOT NULL,            -- e.g. 'B01003'
    variable_name   TEXT NOT NULL,            -- e.g. 'B01003_001E'
    measure_type    TEXT NOT NULL,            -- 'E' (estimate) or 'M' (margin of error)

    -- Unified geography (matches silver_ref.dim_geo)
    geo_level       TEXT NOT NULL,
    geo_id          TEXT NOT NULL,
    state_fips      TEXT,
    county_fips     TEXT,

    -- Time (annual)
    obs_date        DATE NOT NULL,            -- Jan 1 of survey year
    year            INTEGER NOT NULL,

    -- Observation
    value           NUMERIC,
    is_missing      BOOLEAN NOT NULL DEFAULT FALSE,

    -- Lineage
    load_batch_id   UUID NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS census_obs_uniq
    ON silver_census.census_observations (variable_name, geo_level, geo_id, obs_date, measure_type);

CREATE INDEX IF NOT EXISTS census_obs_dataset_idx
    ON silver_census.census_observations (dataset);

CREATE INDEX IF NOT EXISTS census_obs_geo_idx
    ON silver_census.census_observations (geo_level, geo_id);

CREATE INDEX IF NOT EXISTS census_obs_date_idx
    ON silver_census.census_observations (obs_date);

CREATE INDEX IF NOT EXISTS census_obs_year_idx
    ON silver_census.census_observations (year);

CREATE INDEX IF NOT EXISTS census_obs_variable_idx
    ON silver_census.census_observations (variable_name);

CREATE INDEX IF NOT EXISTS census_obs_table_idx
    ON silver_census.census_observations (table_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'census_obs_geo_level_chk'
    ) THEN
        ALTER TABLE silver_census.census_observations
        ADD CONSTRAINT census_obs_geo_level_chk
        CHECK (geo_level IN ('us', 'state', 'county'));
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'census_obs_measure_type_chk'
    ) THEN
        ALTER TABLE silver_census.census_observations
        ADD CONSTRAINT census_obs_measure_type_chk
        CHECK (measure_type IN ('E', 'M'));
    END IF;
END $$;


-- =================================================================
-- silver_fred — FRED analytics-ready observations
-- =================================================================
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
