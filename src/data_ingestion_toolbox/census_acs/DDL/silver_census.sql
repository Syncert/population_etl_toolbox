CREATE SCHEMA IF NOT EXISTS silver_census;

CREATE TABLE IF NOT EXISTS silver_census.observation_revision (
    capture_id          UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index    INTEGER NOT NULL CHECK (source_row_index >= 0),
    source_column_index INTEGER NOT NULL CHECK (source_column_index >= 0),
    source_header       TEXT NOT NULL,
    dataset             TEXT NOT NULL,
    year                INTEGER NOT NULL,
    geo_level           TEXT NOT NULL CHECK (geo_level IN ('us', 'state', 'county')),
    us_source           TEXT,
    state_fips_source   TEXT,
    county_fips_source  TEXT,
    variable_name       TEXT NOT NULL,
    table_id            TEXT NOT NULL,
    measure_type        TEXT,
    value_source        TEXT,
    value               NUMERIC,
    value_status        TEXT NOT NULL
        CHECK (value_status IN ('valid', 'absent', 'blank', 'sentinel', 'invalid')),
    parsed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parser_version      TEXT NOT NULL DEFAULT 'census-acs-array-v1',
    PRIMARY KEY (capture_id, source_row_index, source_column_index),
    CHECK (value_status <> 'valid' OR value IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS census_observation_revision_slice_idx
    ON silver_census.observation_revision (
        dataset, year, geo_level, state_fips_source, county_fips_source
    );

CREATE TABLE IF NOT EXISTS silver_census.fact_demographics (
    demographic_sk BIGSERIAL PRIMARY KEY,
    time_sk INTEGER NOT NULL REFERENCES silver_ref.dim_time(time_sk),
    geo_sk BIGINT NOT NULL REFERENCES silver_ref.dim_geo_entity(geo_sk),
    duration_start DATE NOT NULL,
    duration_end DATE NOT NULL,
    estimate_year INTEGER NOT NULL,
    dataset VARCHAR(50) NOT NULL,
    table_id VARCHAR(50) NOT NULL,
    variable_code VARCHAR(100) NOT NULL,
    geo_level VARCHAR(50),
    geo_id VARCHAR(255),
    state_fips VARCHAR(2),
    county_fips VARCHAR(3),
    estimate_value NUMERIC,
    margin_of_error NUMERIC,
    margin_of_error_pct NUMERIC,
    variable_label TEXT,
    variable_concept TEXT,
    universe TEXT,
    source_system VARCHAR(50) DEFAULT 'CENSUS_ACS',
    load_batch_id UUID NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fact_demographics_uk UNIQUE (dataset, table_id, variable_code, geo_id, estimate_year)
);

CREATE INDEX IF NOT EXISTS idx_fact_demo_time_sk ON silver_census.fact_demographics(time_sk);
CREATE INDEX IF NOT EXISTS idx_fact_demo_geo_sk ON silver_census.fact_demographics(geo_sk);
CREATE INDEX IF NOT EXISTS idx_fact_demo_dataset ON silver_census.fact_demographics(dataset);
CREATE INDEX IF NOT EXISTS idx_fact_demo_table_id ON silver_census.fact_demographics(table_id);
CREATE INDEX IF NOT EXISTS idx_fact_demo_upsert_key ON silver_census.fact_demographics(dataset, table_id, variable_code, geo_id, estimate_year);
CREATE INDEX IF NOT EXISTS idx_fact_demo_source_year ON silver_census.fact_demographics(source_system, estimate_year);
CREATE INDEX IF NOT EXISTS idx_fact_demo_ingested_at ON silver_census.fact_demographics(ingested_at);

-- Autovacuum for this high-update table
ALTER TABLE silver_census.fact_demographics SET (
    autovacuum_vacuum_scale_factor = 0.05,  -- Vacuum when 5% of table updated (default 20%)
    autovacuum_analyze_scale_factor = 0.02, -- Analyze when 2% updated
    autovacuum_vacuum_cost_limit = 2000     -- Allow more aggressive vacuuming
);
