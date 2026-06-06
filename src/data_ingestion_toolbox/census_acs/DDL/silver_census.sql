CREATE SCHEMA IF NOT EXISTS silver_census;

CREATE TABLE IF NOT EXISTS silver_census.fact_demographics (
    demographic_sk BIGSERIAL PRIMARY KEY,
    time_sk INTEGER NOT NULL REFERENCES silver_ref.dim_time(time_sk),
    geo_sk INTEGER NOT NULL REFERENCES silver_ref.dim_geo(geo_sk),
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
-- idx_fact_demo_upsert_key removed: redundant with the fact_demographics_uk UNIQUE constraint
-- which already creates a B-tree index on (dataset, table_id, variable_code, geo_id, estimate_year).
CREATE INDEX IF NOT EXISTS idx_fact_demo_source_year ON silver_census.fact_demographics(source_system, estimate_year);

-- Autovacuum for this high-update table
ALTER TABLE silver_census.fact_demographics SET (
    autovacuum_vacuum_scale_factor = 0.05,  -- Vacuum when 5% of table updated (default 20%)
    autovacuum_analyze_scale_factor = 0.02, -- Analyze when 2% updated
    autovacuum_vacuum_cost_limit = 2000     -- Allow more aggressive vacuuming
);