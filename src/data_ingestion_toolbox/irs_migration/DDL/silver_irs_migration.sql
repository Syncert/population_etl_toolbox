-- Control and silver relations for IRS SOI county migration flows
-- (irs-county-migration). The flow fact carries two geography keys, origin
-- and destination; ADR-0008 records that shape.

CREATE SCHEMA IF NOT EXISTS silver_irs_migration;

-- One run per captured file: a direction and a pair of filing years.
CREATE TABLE IF NOT EXISTS control.irs_migration_file (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    direction TEXT NOT NULL CHECK (direction IN ('inflow', 'outflow')),
    year_pair TEXT NOT NULL CHECK (year_pair ~ '^[0-9]{4}-[0-9]{4}$'),
    year1 INTEGER NOT NULL,
    year2 INTEGER NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    captured_row_count INTEGER NOT NULL DEFAULT 0 CHECK (captured_row_count >= 0),
    parsed_row_count INTEGER NOT NULL DEFAULT 0 CHECK (parsed_row_count >= 0),
    refused_row_count INTEGER NOT NULL DEFAULT 0 CHECK (refused_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT irs_file_year_pair_consecutive CHECK (
        year2 = year1 + 1 AND year_pair = year1::TEXT || '-' || year2::TEXT
    ),
    CONSTRAINT irs_file_counts_within_rows CHECK (parsed_row_count <= captured_row_count),
    CONSTRAINT irs_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS irs_migration_file_capture_idx ON control.irs_migration_file (capture_id);

-- Every parsed row of every captured file, as the file states it.
CREATE TABLE IF NOT EXISTS silver_irs_migration.flow_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    direction TEXT NOT NULL CHECK (direction IN ('inflow', 'outflow')),
    year_pair TEXT NOT NULL,
    subject_geo_id TEXT NOT NULL,
    category TEXT NOT NULL,
    counterpart_code TEXT NOT NULL CHECK (counterpart_code ~ '^[0-9]{2}:[0-9]{3}$'),
    counterpart_state_abbr TEXT,
    counterpart_label TEXT,
    counterpart_geo_id TEXT,
    origin_geo_id TEXT,
    destination_geo_id TEXT,
    returns BIGINT,
    individuals BIGINT,
    agi NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'withheld')),
    value_source TEXT NOT NULL,
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index),
    CONSTRAINT irs_revision_withheld_value_absent CHECK (
        value_status <> 'withheld' OR (returns IS NULL AND individuals IS NULL AND agi IS NULL)
    ),
    CONSTRAINT irs_revision_valid_value_present CHECK (
        value_status <> 'valid' OR (returns IS NOT NULL AND individuals IS NOT NULL AND agi IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS irs_flow_revision_run_idx ON silver_irs_migration.flow_revision (run_id);

-- Rows the parser could not read and rows conformance refused.
CREATE TABLE IF NOT EXISTS silver_irs_migration.flow_quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- The conformed flows. A county-to-county flow and a county's non-migrants
-- name both counties, and both resolve; SOI's own categories name neither,
-- and keep their label instead of a county.
CREATE TABLE IF NOT EXISTS silver_irs_migration.fact_flow (
    direction TEXT NOT NULL CHECK (direction IN ('inflow', 'outflow')),
    year_pair TEXT NOT NULL,
    subject_geo_id TEXT NOT NULL,
    counterpart_code TEXT NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    year1 INTEGER NOT NULL,
    year2 INTEGER NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL,
    category TEXT NOT NULL,
    counterpart_label TEXT,
    subject_geo_sk BIGINT NOT NULL REFERENCES silver_ref.dim_geo_entity(geo_sk),
    origin_geo_id TEXT,
    origin_geo_sk BIGINT REFERENCES silver_ref.dim_geo_entity(geo_sk),
    destination_geo_id TEXT,
    destination_geo_sk BIGINT REFERENCES silver_ref.dim_geo_entity(geo_sk),
    returns BIGINT,
    individuals BIGINT,
    agi NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'withheld')),
    value_source TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (direction, year_pair, subject_geo_id, counterpart_code, capture_id),
    CONSTRAINT irs_flow_endpoints_resolved CHECK (
        category NOT IN ('county', 'non_migrants')
        OR (origin_geo_id IS NOT NULL AND origin_geo_sk IS NOT NULL
            AND destination_geo_id IS NOT NULL AND destination_geo_sk IS NOT NULL)
    ),
    CONSTRAINT irs_flow_category_names_no_county CHECK (
        category IN ('county', 'non_migrants')
        OR (origin_geo_id IS NULL AND origin_geo_sk IS NULL
            AND destination_geo_id IS NULL AND destination_geo_sk IS NULL)
    ),
    CONSTRAINT irs_flow_withheld_value_absent CHECK (
        value_status <> 'withheld' OR (returns IS NULL AND individuals IS NULL AND agi IS NULL)
    ),
    CONSTRAINT irs_flow_valid_value_present CHECK (value_status <> 'valid' OR returns IS NOT NULL),
    CONSTRAINT irs_flow_valid_measures_present CHECK (
        value_status <> 'valid' OR (individuals IS NOT NULL AND agi IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS irs_fact_flow_subject_idx
    ON silver_irs_migration.fact_flow (subject_geo_id, direction, year_pair);
CREATE INDEX IF NOT EXISTS irs_fact_flow_run_idx ON silver_irs_migration.fact_flow (run_id);
