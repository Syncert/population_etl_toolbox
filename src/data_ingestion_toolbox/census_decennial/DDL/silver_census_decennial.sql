-- Capture-first 2020 Census control and silver contract
-- (group-quarters-by-type).
--
-- The source owns its relations: this file is applied by the bootstrap
-- manifest in the `silver` phase and re-applied by the `census_decennial_ingest`
-- DAG's `ensure_census_decennial_schema` task, so a warehouse a step behind the
-- DAG is repaired before the DAG writes to it. Every statement is rerun-safe.

CREATE SCHEMA IF NOT EXISTS silver_census_decennial;

CREATE SCHEMA IF NOT EXISTS gold_census_decennial;

-- One captured (dataset, grain, area) slice per row. An area the API answers
-- with 204 is recorded as `empty`, a published absence, not zero.
CREATE TABLE IF NOT EXISTS control.census_decennial_slice (
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    dataset_id TEXT NOT NULL CHECK (dataset_id IN ('pl')),
    geo_level TEXT NOT NULL CHECK (geo_level IN ('us', 'state', 'county', 'tract', 'block')),
    -- `1` for the nation, `*` for every state or county, a two-digit state
    -- for its tracts, a five-digit county for its blocks.
    scope TEXT NOT NULL CHECK (scope ~ '^(1|\*|[0-9]{2}|[0-9]{5})$'),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    captured_row_count INTEGER NOT NULL DEFAULT 0 CHECK (captured_row_count >= 0),
    status TEXT NOT NULL CONSTRAINT census_decennial_slice_status_check CHECK (status IN (
        'captured', 'empty', 'unchanged', 'quarantined', 'silver_ready', 'published'
    )),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, geo_level, scope),
    CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS census_decennial_slice_latest_idx
    ON control.census_decennial_slice (dataset_id, geo_level, scope, created_at DESC);

CREATE INDEX IF NOT EXISTS census_decennial_slice_capture_idx
    ON control.census_decennial_slice (capture_id);

CREATE TABLE IF NOT EXISTS silver_census_decennial.dim_variable (
    dataset_id TEXT NOT NULL CHECK (dataset_id IN ('pl')),
    variable_id TEXT NOT NULL CHECK (variable_id ~ '^[A-Z][0-9A-Z]*_[0-9]{3}N$'),
    variable_label TEXT NOT NULL,
    universe TEXT NOT NULL,
    unit TEXT NOT NULL,
    parent_variable TEXT,
    methodology_url TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset_id, variable_id)
);

-- Every parsed count of every captured row, one row per variable. The
-- provider's own text is kept beside each number.
CREATE TABLE IF NOT EXISTS silver_census_decennial.observation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    variable_id TEXT NOT NULL,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    dataset_id TEXT NOT NULL CHECK (dataset_id IN ('pl')),
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county', 'tract', 'block')),
    geo_source_code TEXT NOT NULL,
    geo_source_label TEXT,
    geo_id TEXT NOT NULL,
    value_source TEXT,
    value BIGINT,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    source_record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index, variable_id),
    FOREIGN KEY (dataset_id, variable_id)
        REFERENCES silver_census_decennial.dim_variable(dataset_id, variable_id),
    CONSTRAINT decennial_revision_valid_value_present
        CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT decennial_revision_missing_value_absent
        CHECK (value_status <> 'missing' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS census_decennial_revision_run_idx
    ON silver_census_decennial.observation_revision (run_id);

CREATE TABLE IF NOT EXISTS silver_census_decennial.observation_quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- The conformed counts, keyed by what they describe and the capture they came
-- from. A block has no canonical geography yet, so it is `unsupported` and
-- never served (outbreak-model-inputs: block data waits on Nick's decision).
CREATE TABLE IF NOT EXISTS silver_census_decennial.fact_count (
    dataset_id TEXT NOT NULL,
    variable_id TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    retrieved_at TIMESTAMPTZ NOT NULL,
    geo_sk BIGINT,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county', 'tract', 'block')),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped', 'unsupported')),
    value_source TEXT,
    value BIGINT,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset_id, variable_id, geo_id, capture_id),
    FOREIGN KEY (dataset_id, variable_id)
        REFERENCES silver_census_decennial.dim_variable(dataset_id, variable_id),
    CONSTRAINT decennial_fact_valid_value_present
        CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT decennial_fact_missing_value_absent
        CHECK (value_status <> 'missing' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS census_decennial_fact_lookup_idx
    ON silver_census_decennial.fact_count (dataset_id, variable_id, geo_id);

CREATE INDEX IF NOT EXISTS census_decennial_fact_geo_idx
    ON silver_census_decennial.fact_count (geo_id, variable_id);

CREATE INDEX IF NOT EXISTS census_decennial_fact_run_idx
    ON silver_census_decennial.fact_count (run_id);
