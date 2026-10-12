-- Control and silver relations for the Census Building Permits Survey
-- (census-building-permits). Applied by the bootstrap manifest in the
-- `silver` phase and re-applied by the DAG's `ensure_census_bps_schema` task;
-- every statement is rerunnable.

CREATE SCHEMA IF NOT EXISTS silver_census_bps;

-- One row per captured file of one (frequency, year, month) run.
CREATE TABLE IF NOT EXISTS control.census_bps_slice (
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    slice_key TEXT NOT NULL CHECK (slice_key ~ '^(county|state|place:(midwest|northeast|south|west))$'),
    frequency TEXT NOT NULL CHECK (frequency IN ('monthly', 'annual')),
    year INTEGER NOT NULL CHECK (year BETWEEN 2000 AND 2100),
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    captured_row_count INTEGER NOT NULL DEFAULT 0 CHECK (captured_row_count >= 0),
    in_scope_row_count INTEGER NOT NULL DEFAULT 0 CHECK (in_scope_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN (
        'captured', 'empty', 'silver_ready', 'quarantined', 'published'
    )),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, slice_key),
    CONSTRAINT census_bps_slice_scope_within_rows CHECK (in_scope_row_count <= captured_row_count),
    CONSTRAINT census_bps_slice_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS census_bps_slice_capture_idx ON control.census_bps_slice (capture_id);

CREATE TABLE IF NOT EXISTS silver_census_bps.dim_measure (
    measure_id TEXT NOT NULL CHECK (measure_id IN ('buildings', 'units', 'valuation')),
    structure_type TEXT NOT NULL CHECK (structure_type IN ('1_unit', '2_units', '3_4_units', '5_plus_units')),
    measure_label TEXT NOT NULL,
    structure_label TEXT NOT NULL,
    unit TEXT NOT NULL,
    observation_basis TEXT NOT NULL,
    methodology_url TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (measure_id, structure_type)
);

-- Every parsed figure of every in-scope captured row, one row per measure
-- and structure type, with the Bureau's estimate and what the jurisdiction
-- reported itself.
CREATE TABLE IF NOT EXISTS silver_census_bps.observation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    measure_id TEXT NOT NULL,
    structure_type TEXT NOT NULL,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    slice_key TEXT NOT NULL,
    frequency TEXT NOT NULL CHECK (frequency IN ('monthly', 'annual')),
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county', 'place')),
    geo_source_code TEXT NOT NULL,
    geo_source_label TEXT,
    geo_id TEXT NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    value_source TEXT,
    value NUMERIC,
    reported_value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'not_reported', 'missing')),
    months_reported INTEGER CHECK (months_reported BETWEEN 0 AND 12),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index, measure_id, structure_type),
    FOREIGN KEY (measure_id, structure_type)
        REFERENCES silver_census_bps.dim_measure(measure_id, structure_type),
    CONSTRAINT bps_revision_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT bps_revision_unreported_value_absent CHECK (value_status = 'valid' OR value IS NULL),
    CONSTRAINT bps_revision_period_ordered CHECK (period_start <= period_end)
);

CREATE INDEX IF NOT EXISTS census_bps_revision_run_idx ON silver_census_bps.observation_revision (run_id);

CREATE TABLE IF NOT EXISTS silver_census_bps.observation_quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- The conformed observations, keyed by what they describe and the capture
-- they came from, so a revised file is kept beside the one it revised.
CREATE TABLE IF NOT EXISTS silver_census_bps.fact_observation (
    measure_id TEXT NOT NULL,
    structure_type TEXT NOT NULL,
    frequency TEXT NOT NULL CHECK (frequency IN ('monthly', 'annual')),
    geo_id TEXT NOT NULL,
    period_start DATE NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    retrieved_at TIMESTAMPTZ NOT NULL,
    period_end DATE NOT NULL,
    geo_sk BIGINT,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county', 'place')),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value_source TEXT,
    value NUMERIC,
    reported_value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'not_reported', 'missing')),
    months_reported INTEGER,
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (measure_id, structure_type, frequency, geo_id, period_start, capture_id),
    FOREIGN KEY (measure_id, structure_type)
        REFERENCES silver_census_bps.dim_measure(measure_id, structure_type),
    CONSTRAINT bps_fact_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT bps_fact_unreported_value_absent CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS census_bps_fact_lookup_idx
    ON silver_census_bps.fact_observation (measure_id, structure_type, frequency, geo_id, period_start);
CREATE INDEX IF NOT EXISTS census_bps_fact_run_idx ON silver_census_bps.fact_observation (run_id);
