-- Control, silver and lineage tables for vehicle registrations by type
-- (vehicle-fleet-tables): WisDOT Report 28 and FHWA MV-1. Applied by the
-- bootstrap manifest in the `silver` phase and re-applied by the DAG's
-- `ensure_vehicle_registrations_schema` task.

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS silver_vehicle_registrations;

-- One read of both publishers: WisDOT's two index pages, every Report 28 they
-- link, and every MV-1 year from the configured first year. A release whose
-- documents all repeat the published bytes is `unchanged` and writes nothing.
CREATE TABLE IF NOT EXISTS control.vehicle_registrations_release (
    release_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    document_count INTEGER NOT NULL DEFAULT 0 CHECK (document_count >= 0),
    changed_value_count INTEGER NOT NULL DEFAULT 0 CHECK (changed_value_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('capturing', 'failed', 'captured', 'unchanged', 'silver_ready', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT vehicle_registrations_release_published_has_time
        CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS vehicle_registrations_release_status_idx
    ON control.vehicle_registrations_release (status, created_at);

-- One captured page or file per release. `document_key` is
-- `wisdot_index:<path>`, `wisdot_rpt28:cal:<year>` or `fhwa_mv1:<year>`.
-- `unchanged` repeats the newest published bytes and is not parsed again;
-- `set_aside` is a file whose layout is not the registered one.
CREATE TABLE IF NOT EXISTS control.vehicle_registrations_document (
    release_id UUID NOT NULL REFERENCES control.vehicle_registrations_release(release_id),
    document_key TEXT NOT NULL,
    document_kind TEXT NOT NULL CHECK (document_kind IN ('index', 'rpt28', 'mv1')),
    data_year SMALLINT CHECK (data_year IS NULL OR data_year BETWEEN 1990 AND 2100),
    source_url TEXT NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL CHECK (status IN ('captured', 'unchanged', 'parsed', 'set_aside')),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    kept_row_count INTEGER NOT NULL DEFAULT 0 CHECK (kept_row_count >= 0),
    repeated_row_count INTEGER NOT NULL DEFAULT 0 CHECK (repeated_row_count >= 0),
    -- Printed totals that are not the sum of what they total, kept as
    -- published: [{check, county_code, column, printed, summed}].
    disagreements JSONB NOT NULL DEFAULT '[]'::JSONB,
    -- MV-1's footnotes and notes under the table: [{mark, text}].
    footnotes JSONB NOT NULL DEFAULT '[]'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (release_id, document_key),
    CONSTRAINT vehicle_registrations_document_year_of_kind
        CHECK ((document_kind = 'index') = (data_year IS NULL))
);

CREATE INDEX IF NOT EXISTS vehicle_registrations_document_capture_idx
    ON control.vehicle_registrations_document (capture_id);
CREATE INDEX IF NOT EXISTS vehicle_registrations_document_key_idx
    ON control.vehicle_registrations_document (document_key, release_id);

-- WisDOT's county codes (its own, alphabetical with Menominee 73), written
-- from `vehicle_registrations.registry`.
CREATE TABLE IF NOT EXISTS silver_vehicle_registrations.wisdot_county (
    county_code TEXT PRIMARY KEY CHECK (county_code ~ '^[0-9]{2}$'),
    county_name TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The served figures, written from `vehicle_registrations.registry`: one
-- per Report 28 column and one per MV-1 column.
CREATE TABLE IF NOT EXISTS silver_vehicle_registrations.dim_measure (
    measure_id TEXT PRIMARY KEY,
    publisher TEXT NOT NULL CHECK (publisher IN ('wisdot_rpt28', 'fhwa_mv1')),
    source_column TEXT NOT NULL,
    measure_label TEXT NOT NULL,
    unit TEXT NOT NULL,
    basis TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver_vehicle_registrations.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    release_id UUID NOT NULL REFERENCES control.vehicle_registrations_release(release_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- Report 28, one row per printed line and column, for each release in which
-- WisDOT's text for it first appeared or changed (CHURN RULE). `table_part`
-- `cvt` is Table 1 (a county's cities, villages and towns, its UNKNOWN line
-- and its Sum line), `county` is Table 2 (one line per county, UNKNOWN and
-- the statewide Sum). `county_code` is '' for vehicles with no county and
-- for the statewide Sum. `vehicle_type` is WisDOT's column (AUCY, AUTO,
-- CYCL, TRLR, TRUK) or SUM, the line's printed total. A blank cell is
-- `blank` with no number; a column a county's table does not print has no row.
CREATE TABLE IF NOT EXISTS silver_vehicle_registrations.wisdot_count (
    data_year SMALLINT NOT NULL CHECK (data_year BETWEEN 1990 AND 2100),
    table_part TEXT NOT NULL CHECK (table_part IN ('cvt', 'county')),
    county_code TEXT NOT NULL CHECK (county_code ~ '^([0-9]{2})?$'),
    line_key TEXT NOT NULL,
    vehicle_type TEXT NOT NULL CHECK (vehicle_type ~ '^[A-Z]{3,4}$'),
    release_id UUID NOT NULL REFERENCES control.vehicle_registrations_release(release_id),
    row_kind TEXT NOT NULL,
    county_name TEXT NOT NULL,
    cvt_name TEXT NOT NULL,
    cvt_code TEXT NOT NULL,
    cvt_kind TEXT NOT NULL CHECK (cvt_kind IN ('', 'T', 'V', 'C')),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    page_number INTEGER NOT NULL CHECK (page_number > 0),
    source_row_index INTEGER NOT NULL CHECK (source_row_index > 0),
    value_source TEXT NOT NULL,
    value BIGINT CHECK (value >= 0),
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'blank')),
    PRIMARY KEY (data_year, table_part, county_code, line_key, vehicle_type, release_id),
    CONSTRAINT vehicle_registrations_wisdot_valid_value_present
        CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT vehicle_registrations_wisdot_other_value_absent
        CHECK (value_status = 'valid' OR value IS NULL)
);

-- Named and re-declared so a warehouse built before 2020's out-of-state
-- lines were read takes the wider list on the next manifest apply.
ALTER TABLE silver_vehicle_registrations.wisdot_count
    DROP CONSTRAINT IF EXISTS wisdot_count_row_kind_check;
ALTER TABLE silver_vehicle_registrations.wisdot_count
    ADD CONSTRAINT wisdot_count_row_kind_check
    CHECK (row_kind IN ('cvt', 'county', 'unknown', 'out_of_state', 'sum'));

CREATE INDEX IF NOT EXISTS vehicle_registrations_wisdot_count_capture_idx
    ON silver_vehicle_registrations.wisdot_count (capture_id);
CREATE INDEX IF NOT EXISTS vehicle_registrations_wisdot_count_release_idx
    ON silver_vehicle_registrations.wisdot_count (release_id);

-- MV-1, one row per area (state, District or Total), year, vehicle class and
-- ownership column, for each release in which FHWA's text for it first
-- appeared or changed (CHURN RULE). The value is the workbook's stored text,
-- decimals kept where FHWA stored an estimate with them; an empty or dashed
-- cell is `not_reported` with no number.
CREATE TABLE IF NOT EXISTS silver_vehicle_registrations.fhwa_mv1_figure (
    data_year SMALLINT NOT NULL CHECK (data_year BETWEEN 1990 AND 2100),
    area_name TEXT NOT NULL,
    vehicle_class TEXT NOT NULL CHECK (vehicle_class IN ('automobiles', 'buses', 'trucks', 'motorcycles', 'all_motor_vehicles')),
    ownership TEXT NOT NULL CHECK (ownership IN ('private_commercial', 'publicly_owned', 'total')),
    release_id UUID NOT NULL REFERENCES control.vehicle_registrations_release(release_id),
    source_label TEXT NOT NULL,
    state_fips TEXT CHECK (state_fips IS NULL OR state_fips ~ '^[0-9]{2}$'),
    footnote_marks TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index > 0),
    value_source TEXT NOT NULL,
    value NUMERIC CHECK (value >= 0),
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'not_reported')),
    PRIMARY KEY (data_year, area_name, vehicle_class, ownership, release_id),
    CONSTRAINT vehicle_registrations_mv1_valid_value_present
        CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT vehicle_registrations_mv1_other_value_absent
        CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS vehicle_registrations_mv1_capture_idx
    ON silver_vehicle_registrations.fhwa_mv1_figure (capture_id);
CREATE INDEX IF NOT EXISTS vehicle_registrations_mv1_release_idx
    ON silver_vehicle_registrations.fhwa_mv1_figure (release_id);

COMMENT ON TABLE silver_vehicle_registrations.wisdot_count IS
    'WisDOT DMV Report 28: registered vehicles by county and city/village/town by WisDOT vehicle type, calendar year, as printed.';
COMMENT ON TABLE silver_vehicle_registrations.fhwa_mv1_figure IS
    'FHWA Highway Statistics MV-1: state motor-vehicle registrations by vehicle class, private and commercial and publicly owned, as published.';
