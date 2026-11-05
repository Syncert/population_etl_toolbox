-- Control and silver relations for IRS SOI county and state income data
-- (irs-soi). Applied by the bootstrap manifest in the `silver` phase and
-- re-applied by the `irs_soi_ingest` DAG's `ensure_irs_soi_schema` task.
-- Every statement is rerun-safe.

CREATE SCHEMA IF NOT EXISTS silver_irs_soi;

CREATE SCHEMA IF NOT EXISTS gold_irs_soi;

-- One run per captured file: a product and a tax year.
CREATE TABLE IF NOT EXISTS control.irs_soi_file (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    file_kind TEXT NOT NULL CHECK (file_kind IN ('county_total', 'county_agi', 'state')),
    tax_year INTEGER NOT NULL CHECK (tax_year BETWEEN 1989 AND 2100),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    captured_row_count INTEGER NOT NULL DEFAULT 0 CHECK (captured_row_count >= 0),
    quarantined_row_count INTEGER NOT NULL DEFAULT 0 CHECK (quarantined_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT irs_soi_file_quarantine_within_rows CHECK (quarantined_row_count <= captured_row_count),
    CONSTRAINT irs_soi_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS irs_soi_file_capture_idx ON control.irs_soi_file (capture_id);
CREATE INDEX IF NOT EXISTS irs_soi_file_period_idx ON control.irs_soi_file (file_kind, tax_year, status);

-- The curated items and the AGI bands, written from the registry at replay.
CREATE TABLE IF NOT EXISTS silver_irs_soi.dim_measure (
    measure_id TEXT PRIMARY KEY CHECK (measure_id ~ '^[a-z0-9_]+$'),
    soi_column TEXT NOT NULL,
    measure_label TEXT NOT NULL,
    unit TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver_irs_soi.dim_agi_band (
    agi_band TEXT PRIMARY KEY CHECK (agi_band ~ '^[a-z0-9_]+$'),
    band_label TEXT NOT NULL,
    lower_dollars BIGINT,
    upper_dollars BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT irs_soi_band_ordered CHECK (
        lower_dollars IS NULL OR upper_dollars IS NULL OR lower_dollars < upper_dollars
    )
);

-- Every parsed value of every captured row, one row per measure, with SOI's
-- own text beside the number.
CREATE TABLE IF NOT EXISTS silver_irs_soi.observation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    measure_id TEXT NOT NULL REFERENCES silver_irs_soi.dim_measure(measure_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    file_kind TEXT NOT NULL CHECK (file_kind IN ('county_total', 'county_agi', 'state')),
    tax_year INTEGER NOT NULL,
    agi_stub INTEGER NOT NULL CHECK (agi_stub >= 0),
    agi_band TEXT NOT NULL REFERENCES silver_irs_soi.dim_agi_band(agi_band),
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county', 'other')),
    geo_source_code TEXT NOT NULL,
    geo_source_label TEXT,
    geo_id TEXT NOT NULL,
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index, measure_id),
    CONSTRAINT irs_soi_revision_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT irs_soi_revision_missing_value_absent CHECK (value_status <> 'missing' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS irs_soi_revision_run_idx ON silver_irs_soi.observation_revision (run_id);

-- Rows the parser could not read, and files it refused whole (row 0).
CREATE TABLE IF NOT EXISTS silver_irs_soi.observation_quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- The conformed values, keyed by what they describe and the capture they
-- came from, so a revised file is kept beside the one it revised. The
-- metric key is stored, not computed in the view, so a read pinned to one
-- metric and one place uses the lookup index.
CREATE TABLE IF NOT EXISTS silver_irs_soi.fact_observation (
    file_kind TEXT NOT NULL CHECK (file_kind IN ('county_total', 'county_agi', 'state')),
    metric_key TEXT NOT NULL,
    tax_year INTEGER NOT NULL,
    geo_id TEXT NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    measure_id TEXT NOT NULL REFERENCES silver_irs_soi.dim_measure(measure_id),
    agi_band TEXT NOT NULL REFERENCES silver_irs_soi.dim_agi_band(agi_band),
    agi_stub INTEGER NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL,
    geo_sk BIGINT REFERENCES silver_ref.dim_geo_entity(geo_sk),
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county', 'other')),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (file_kind, metric_key, tax_year, geo_id, capture_id),
    CONSTRAINT irs_soi_fact_metric_key CHECK (
        metric_key = measure_id || CASE WHEN agi_band = 'all' THEN '' ELSE ':' || agi_band END
    ),
    CONSTRAINT irs_soi_fact_resolved_has_key CHECK (geography_status <> 'resolved' OR geo_sk IS NOT NULL),
    CONSTRAINT irs_soi_fact_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT irs_soi_fact_missing_value_absent CHECK (value_status <> 'missing' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS irs_soi_fact_lookup_idx
    ON silver_irs_soi.fact_observation (metric_key, geo_id, tax_year);
CREATE INDEX IF NOT EXISTS irs_soi_fact_run_idx ON silver_irs_soi.fact_observation (run_id);
CREATE INDEX IF NOT EXISTS irs_soi_fact_capture_idx ON silver_irs_soi.fact_observation (capture_id);
