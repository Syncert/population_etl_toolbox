-- Control and silver relations for EIA retail gasoline prices
-- (grocery-and-gasoline-prices). Applied by the bootstrap manifest in the
-- `silver` phase and re-applied by the DAG's `ensure_eia_schema` task; every
-- statement is rerunnable.

CREATE SCHEMA IF NOT EXISTS silver_eia;

-- One row per read of a window of weeks.
CREATE TABLE IF NOT EXISTS control.eia_read (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    start_week DATE NOT NULL,
    end_week DATE,
    page_count INTEGER NOT NULL CHECK (page_count >= 1),
    row_total INTEGER NOT NULL CHECK (row_total >= 0),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    parsed_row_count INTEGER NOT NULL DEFAULT 0 CHECK (parsed_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT eia_read_window_ordered CHECK (end_week IS NULL OR end_week >= start_week),
    CONSTRAINT eia_read_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

-- Every captured page of a read, in order.
CREATE TABLE IF NOT EXISTS control.eia_page (
    run_id UUID NOT NULL REFERENCES control.eia_read(run_id),
    page_index INTEGER NOT NULL CHECK (page_index >= 0),
    capture_id UUID NOT NULL UNIQUE REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (run_id, page_index)
);

-- Every parsed row of every captured page.
CREATE TABLE IF NOT EXISTS silver_eia.price_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    row_index INTEGER NOT NULL CHECK (row_index >= 0),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    series_id TEXT NOT NULL,
    week_start DATE NOT NULL,
    duoarea TEXT NOT NULL,
    area_name TEXT,
    product TEXT NOT NULL,
    product_name TEXT,
    units TEXT NOT NULL CHECK (units = '$/GAL'),
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'provider_area')),
    geo_id TEXT,
    state_usps TEXT CHECK (state_usps IS NULL OR state_usps ~ '^[A-Z]{2}$'),
    value_source TEXT,
    value NUMERIC CHECK (value IS NULL OR value > 0),
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, row_index),
    CONSTRAINT eia_revision_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT eia_revision_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL),
    CONSTRAINT eia_revision_state_named CHECK ((geo_type = 'state') = (state_usps IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS eia_revision_run_idx ON silver_eia.price_revision (run_id);

CREATE TABLE IF NOT EXISTS silver_eia.observation_quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    row_index INTEGER NOT NULL CHECK (row_index >= -1),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, row_index, error_code)
);

-- The conformed weekly prices, keyed by what they describe and the capture
-- they came from: a week EIA revises is a second row, never an overwrite.
CREATE TABLE IF NOT EXISTS silver_eia.fact_retail_price (
    series_id TEXT NOT NULL,
    week_start DATE NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    retrieved_at TIMESTAMPTZ NOT NULL,
    product TEXT NOT NULL,
    duoarea TEXT NOT NULL,
    area_name TEXT,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'provider_area')),
    geo_id TEXT,
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value_source TEXT,
    value NUMERIC CHECK (value IS NULL OR value > 0),
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (series_id, week_start, capture_id),
    CONSTRAINT eia_fact_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT eia_fact_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL),
    CONSTRAINT eia_fact_resolved_has_geography CHECK (
        (geography_status = 'resolved') = (geo_sk IS NOT NULL AND geo_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS eia_fact_lookup_idx ON silver_eia.fact_retail_price (series_id, week_start);
CREATE INDEX IF NOT EXISTS eia_fact_run_idx ON silver_eia.fact_retail_price (run_id);
