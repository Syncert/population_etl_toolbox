CREATE SCHEMA IF NOT EXISTS silver_bls;

CREATE TABLE IF NOT EXISTS silver_bls.fact_labor_statistics (
    labor_stat_sk BIGSERIAL PRIMARY KEY,
    time_sk INTEGER NOT NULL REFERENCES silver_ref.dim_time(time_sk),
    geo_sk INTEGER NOT NULL REFERENCES silver_ref.dim_geo(geo_sk),
    duration_start DATE NOT NULL,
    duration_end DATE NOT NULL,
    period_date DATE NOT NULL,
    series_id VARCHAR(255) NOT NULL,
    program VARCHAR(50) NOT NULL,
    geo_level VARCHAR(50),
    geo_id VARCHAR(255),
    state_fips VARCHAR(2),
    county_fips VARCHAR(3),
    value NUMERIC,
    year INTEGER NOT NULL,
    period VARCHAR(10) NOT NULL,
    period_name VARCHAR(100),
    measure_code VARCHAR(10),
    measure_name TEXT,
    seasonal_adjustment VARCHAR(1) DEFAULT 'U',
    source_system VARCHAR(50) DEFAULT 'BLS',
    load_batch_id UUID NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fact_labor_stats_uk UNIQUE (series_id, period_date)
);

CREATE INDEX IF NOT EXISTS idx_fact_labor_time_sk ON silver_bls.fact_labor_statistics(time_sk);
CREATE INDEX IF NOT EXISTS idx_fact_labor_geo_sk ON silver_bls.fact_labor_statistics(geo_sk);
CREATE INDEX IF NOT EXISTS idx_fact_labor_series_id ON silver_bls.fact_labor_statistics(series_id);
CREATE INDEX IF NOT EXISTS idx_fact_labor_program ON silver_bls.fact_labor_statistics(program);
