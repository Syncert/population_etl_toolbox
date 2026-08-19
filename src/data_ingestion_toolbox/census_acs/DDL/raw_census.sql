CREATE SCHEMA IF NOT EXISTS raw_census;

-- Census source metadata. Immutable responses live in raw_capture and parsed
-- observations live in silver_census.observation_revision.

-- Which ACS datasets exist and whether weâ€™ve ingested them
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

-- Table for tracking geography level geo_ids and availability
CREATE TABLE IF NOT EXISTS raw_census.geo_dim (
    geo_level      TEXT NOT NULL,                  -- 'us'|'state'|'county'
    geo_id         TEXT NOT NULL,                  -- 'us:1' | 'state:55' | 'state:55|county:025'
    state_fips     TEXT,
    county_fips    TEXT,
    name           TEXT,                           -- display name
    state_name     TEXT,
    county_name    TEXT,
    is_active      BOOLEAN NOT NULL DEFAULT TRUE,
    source         TEXT NOT NULL DEFAULT 'census_gazetteer',
    source_year    INTEGER,                        -- optional: year of gazetteer/tiger snapshot
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (geo_level, geo_id)
);

CREATE INDEX IF NOT EXISTS geo_dim_state_idx ON raw_census.geo_dim(state_fips);
CREATE INDEX IF NOT EXISTS geo_dim_county_idx ON raw_census.geo_dim(state_fips, county_fips);
