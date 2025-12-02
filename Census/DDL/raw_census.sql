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
    value           NUMERIC,
    load_batch_id   UUID NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS acs_long_uniq
    ON raw_census.acs_long (dataset, year, geo_level, geo_id, variable_name);

-- Which ACS datasets exist and whether we’ve ingested them
CREATE TABLE IF NOT EXISTS raw_census.acs_datasets (
    dataset         TEXT NOT NULL,           -- 'acs1', 'acs5'
    year            INTEGER NOT NULL,
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
    universe     TEXT,
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