-- census_pep/DDL/silver_pep.sql
-- Silver analytics layer for Census PEP (Population Estimates).

CREATE SCHEMA IF NOT EXISTS silver_pep;

-- Observation revision: raw parsed rows from PEP API responses
CREATE TABLE IF NOT EXISTS silver_pep.observation_revision (
    capture_id          UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index    INTEGER NOT NULL CHECK (source_row_index >= 0),
    source_column_index INTEGER NOT NULL CHECK (source_column_index >= 0),
    source_header       TEXT NOT NULL,
    year                INTEGER NOT NULL,
    file_type           TEXT NOT NULL,
    state_fips_source   TEXT,
    county_fips_source  TEXT,
    place_fips_source   TEXT,
    name_source         TEXT,
    us_source           TEXT,
    variable_name       TEXT NOT NULL,
    value_source        TEXT,
    value               NUMERIC,
    value_status        TEXT NOT NULL
        CHECK (value_status IN ('valid', 'absent', 'blank', 'sentinel', 'invalid')),
    parsed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parser_version      TEXT NOT NULL DEFAULT 'census-pep-array-v1',
    PRIMARY KEY (capture_id, source_row_index, source_column_index),
    CHECK (value_status <> 'valid' OR value IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS pep_observation_revision_slice_idx
    ON silver_pep.observation_revision (
        year, file_type, state_fips_source, variable_name
    );

-- Fact population: transformed silver with dimension keys
CREATE TABLE IF NOT EXISTS silver_pep.fact_population (
    population_sk       BIGSERIAL PRIMARY KEY,
    time_sk             BIGINT REFERENCES silver_ref.dim_time(time_sk),
    geo_sk              BIGINT REFERENCES silver_ref.dim_geo_entity(geo_sk),
    duration_start      DATE NOT NULL,
    duration_end        DATE NOT NULL,
    estimate_year       INTEGER NOT NULL,
    dataset             TEXT NOT NULL,
    table_id            TEXT NOT NULL,
    variable_code       TEXT NOT NULL,
    geo_level           TEXT,
    geo_id              TEXT,
    state_fips          TEXT,
    county_fips         TEXT,
    estimate_value      BIGINT,
    margin_of_error     BIGINT,
    margin_of_error_pct DOUBLE PRECISION,
    variable_label      TEXT,
    variable_concept    TEXT,
    universe            TEXT,
    source_system       TEXT DEFAULT 'CENSUS_PEP',
    load_batch_id       UUID NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fact_population_uk UNIQUE (dataset, table_id, variable_code, geo_id, estimate_year)
);

CREATE INDEX IF NOT EXISTS idx_fact_pop_time_sk ON silver_pep.fact_population(time_sk);
CREATE INDEX IF NOT EXISTS idx_fact_pop_geo_sk ON silver_pep.fact_population(geo_sk);
CREATE INDEX IF NOT EXISTS idx_fact_pop_dataset ON silver_pep.fact_population(dataset);
CREATE INDEX IF NOT EXISTS idx_fact_pop_table_id ON silver_pep.fact_population(table_id);
CREATE INDEX IF NOT EXISTS idx_fact_pop_upsert_key ON silver_pep.fact_population(dataset, table_id, variable_code, geo_id, estimate_year);
CREATE INDEX IF NOT EXISTS idx_fact_pop_source_year ON silver_pep.fact_population(source_system, estimate_year);
CREATE INDEX IF NOT EXISTS idx_fact_pop_ingested_at ON silver_pep.fact_population(ingested_at);

-- Autovacuum for this high-update table
ALTER TABLE silver_pep.fact_population SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_cost_limit = 2000
);

-- Column metadata for PEP variables (API discovery)
CREATE TABLE IF NOT EXISTS silver_pep.pep_column_metadata (
    metadata_sk       BIGSERIAL PRIMARY KEY,
    variable_code     TEXT NOT NULL,
    variable_label    TEXT,
    concept           TEXT,
    universe          TEXT,
    data_type         TEXT,
    is_numeric        BOOLEAN,
    is_geometry       BOOLEAN,
    source_year       INTEGER NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (variable_code, source_year)
);

CREATE INDEX IF NOT EXISTS idx_pep_column_metadata_year ON silver_pep.pep_column_metadata(source_year);
CREATE INDEX IF NOT EXISTS idx_pep_column_metadata_code ON silver_pep.pep_column_metadata(variable_code);
