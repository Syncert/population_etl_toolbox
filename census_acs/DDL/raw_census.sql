CREATE SCHEMA IF NOT EXISTS raw_census;

CREATE TABLE IF NOT EXISTS raw_census.acs_long (
    id              BIGSERIAL PRIMARY KEY,
    dataset         TEXT NOT NULL,                 -- 'acs1' or 'acs5'
    year            INTEGER NOT NULL,
    geo_level       TEXT NOT NULL,                 -- 'us', 'state', 'county'
    geo_id          TEXT NOT NULL,                 -- e.g. '0100000US', or '0400000US55', etc. (from GEOID if we choose to add it later)
    state_fips      TEXT,
    county_fips     TEXT,
    table_id        TEXT NOT NULL,                 -- e.g. 'B01001'
    variable_name   TEXT NOT NULL,                 -- e.g. 'B01001_001E' (includes E/M suffix)
    measure_type    TEXT NOT NULL,                          -- 'E' (estimate) or 'M' (margin of error)
    value           NUMERIC,
    load_batch_id   UUID NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

--enforce E or M in measure_type
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'acs_long_measure_type_chk'
    ) THEN
        ALTER TABLE raw_census.acs_long
        ADD CONSTRAINT acs_long_measure_type_chk
        CHECK (measure_type IN ('E','M'));
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS acs_long_uniq
    ON raw_census.acs_long (dataset, year, geo_level, geo_id, variable_name);

CREATE INDEX IF NOT EXISTS acs_long_qc_slice
ON raw_census.acs_long (dataset, year, geo_level);

CREATE INDEX IF NOT EXISTS acs_long_qc_var
ON raw_census.acs_long (dataset, year, variable_name);

CREATE INDEX IF NOT EXISTS acs_long_qc_geo
ON raw_census.acs_long (geo_level, geo_id);    

CREATE INDEX IF NOT EXISTS acs_long_state_geo_idx
ON raw_census.acs_long (dataset, year, geo_level, geo_id)
WHERE geo_level = 'state';

-- Which ACS datasets exist and whether we’ve ingested them
CREATE TABLE IF NOT EXISTS raw_census.acs_datasets (
    dataset         TEXT NOT NULL,           -- 'acs1', 'acs5'
    year            INTEGER NOT NULL,
    census_id       TEXT,
    title           TEXT,
    is_available    BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_ingested_at TIMESTAMPTZ,
    PRIMARY KEY (dataset, year)
);

-- ACS table-level metadata
CREATE TABLE IF NOT EXISTS raw_census.acs_tables (
    dataset      TEXT NOT NULL,
    table_id     TEXT NOT NULL,
    concept      TEXT,
    product      TEXT,           -- 'acs1' or 'acs5'
    PRIMARY KEY (dataset, table_id)
);

-- Variable-level metadata (for curated tables only)
CREATE TABLE IF NOT EXISTS raw_census.acs_variables (
    dataset        TEXT NOT NULL,
    year           INTEGER NOT NULL,
    variable_name  TEXT NOT NULL,
    table_id       TEXT NOT NULL,
    label          TEXT,
    concept        TEXT,
    predicate_type TEXT,
    group_name     TEXT,
    PRIMARY KEY (dataset, year, variable_name)
);

-- Control table that tracks slice completion status for each dataset/year/geo_level combo
CREATE TABLE raw_census.acs_ingestion_slices (
  -- Optional surrogate id (handy for debugging / joins / admin UI)
  id BIGSERIAL PRIMARY KEY,

  dataset      TEXT NOT NULL,            -- 'acs1' / 'acs5'
  year         INTEGER NOT NULL,
  geo_level    TEXT NOT NULL,            -- 'us' / 'state' / 'county'

  -- NULL for geo_level in ('us','state'), required for 'county'
  state_fips   TEXT NULL,

  variables_hash TEXT,
  variables_count INTEGER,

  status       TEXT NOT NULL,            -- planned/running/success/empty/failed
  rows_loaded  BIGINT NOT NULL DEFAULT 0,

  started_at   TIMESTAMPTZ,
  finished_at  TIMESTAMPTZ,
  variables_hash_seen_at TIMESTAMPTZ,

  last_error   TEXT
);

-- Domain checks
ALTER TABLE raw_census.acs_ingestion_slices
  ADD CONSTRAINT chk_dataset CHECK (dataset IN ('acs1','acs5')),
  ADD CONSTRAINT chk_geo_level CHECK (geo_level IN ('us','state','county')),
  ADD CONSTRAINT chk_status CHECK (status IN ('planned','running','success','empty','failed')),
  ADD CONSTRAINT chk_year CHECK (year >= 2000 AND year <= EXTRACT(YEAR FROM CURRENT_DATE) + 1),
  ADD CONSTRAINT chk_rows_loaded_non_negative CHECK (rows_loaded >= 0);

-- Timestamps consistent (make precedence explicit)
ALTER TABLE raw_census.acs_ingestion_slices
  ADD CONSTRAINT chk_started_before_finished
  CHECK (finished_at IS NULL OR (started_at IS NOT NULL AND started_at <= finished_at));

-- Geo-level ↔ state_fips rule + formatting for county FIPS
ALTER TABLE raw_census.acs_ingestion_slices
  ADD CONSTRAINT chk_state_fips_by_geo_level
  CHECK (
    (geo_level IN ('us','state') AND state_fips IS NULL)
    OR
    (geo_level = 'county' AND state_fips ~ '^[0-9]{2}$')
  );

-- Uniqueness (your intended design)
CREATE UNIQUE INDEX acs_ingestion_slices_uniq_nostate
ON raw_census.acs_ingestion_slices (dataset, year, geo_level)
WHERE state_fips IS NULL;

CREATE UNIQUE INDEX acs_ingestion_slices_uniq_state
ON raw_census.acs_ingestion_slices (dataset, year, geo_level, state_fips)
WHERE state_fips IS NOT NULL;

-- Optional: speed up lookups used by the DAG
CREATE INDEX acs_ingestion_slices_status_idx
ON raw_census.acs_ingestion_slices (status);

CREATE INDEX acs_ingestion_slices_hash_idx
ON raw_census.acs_ingestion_slices (variables_hash);