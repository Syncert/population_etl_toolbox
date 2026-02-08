-- census_acs/DDL/census_silver.sql
--
-- Silver layer for Census ACS: analytics-ready observations.
-- Housed in the census_acs subject folder alongside raw_census.sql.
--
-- Geography is unified via silver_ref.dim_geo (geo_level + geo_id).

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
