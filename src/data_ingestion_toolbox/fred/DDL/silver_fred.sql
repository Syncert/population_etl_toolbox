CREATE SCHEMA IF NOT EXISTS silver_fred;

CREATE TABLE IF NOT EXISTS silver_fred.fact_economic_indicators (
    economic_indicator_sk BIGSERIAL PRIMARY KEY,
    time_sk INTEGER NOT NULL REFERENCES silver_ref.dim_time(time_sk),
    duration_start DATE NOT NULL,
    duration_end DATE NOT NULL,
    observation_date DATE NOT NULL,
    series_id VARCHAR(255) NOT NULL,
    domain VARCHAR(100),
    value NUMERIC,
    is_missing BOOLEAN DEFAULT FALSE,
    series_title TEXT,
    unit_of_measure VARCHAR(255),
    frequency VARCHAR(50),
    seasonal_adjustment VARCHAR(50),
    source_system VARCHAR(50) DEFAULT 'FRED',
    load_batch_id UUID NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fact_economic_indicators_uk UNIQUE (series_id, observation_date)
);

CREATE INDEX IF NOT EXISTS idx_fact_econ_time_sk ON silver_fred.fact_economic_indicators(time_sk);
CREATE INDEX IF NOT EXISTS idx_fact_econ_series_id ON silver_fred.fact_economic_indicators(series_id);
CREATE INDEX IF NOT EXISTS idx_fact_econ_domain ON silver_fred.fact_economic_indicators(domain);
CREATE INDEX IF NOT EXISTS idx_fact_econ_duration_start ON silver_fred.fact_economic_indicators(duration_start);
CREATE INDEX IF NOT EXISTS idx_fact_econ_ingested_at ON silver_fred.fact_economic_indicators(ingested_at);
