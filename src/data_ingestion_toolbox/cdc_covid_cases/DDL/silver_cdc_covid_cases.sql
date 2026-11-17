-- Control, silver and lineage tables for the CDC COVID-19 case surveillance
-- record counts (outbreak-calibration-sets). Applied by the bootstrap
-- manifest in the `silver` phase and re-applied by the DAG's
-- `ensure_cdc_covid_cases_schema` task.

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS silver_cdc_covid_cases;

-- One complete read of n8mc-b4w4: its view document, the slice index (CDC's
-- records per state and month) and one grouped request per slice. The
-- dataset is frozen, so a complete snapshot is never read again unless a run
-- asks for history; a history snapshot that repeats the published bytes is
-- recorded `unchanged` and replays nothing.
CREATE TABLE IF NOT EXISTS control.cdc_covid_cases_snapshot (
    snapshot_id UUID PRIMARY KEY,
    metadata_run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    metadata_capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    index_run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    index_capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    index_payload_checksum TEXT NOT NULL CHECK (index_payload_checksum ~ '^[0-9a-f]{64}$'),
    rows_updated_at BIGINT,
    group_limit INTEGER NOT NULL CHECK (group_limit > 0),
    slice_count INTEGER NOT NULL CHECK (slice_count >= 0),
    index_record_count BIGINT NOT NULL CHECK (index_record_count >= 0),
    group_count BIGINT NOT NULL DEFAULT 0 CHECK (group_count >= 0),
    captured_record_count BIGINT NOT NULL DEFAULT 0 CHECK (captured_record_count >= 0),
    kept_record_count BIGINT NOT NULL DEFAULT 0 CHECK (kept_record_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('capturing', 'captured', 'unchanged', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT cdc_covid_snapshot_kept_within_captured
        CHECK (kept_record_count <= captured_record_count),
    CONSTRAINT cdc_covid_snapshot_published_has_time
        CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS cdc_covid_snapshot_status_idx
    ON control.cdc_covid_cases_snapshot (status, created_at);

-- One row per state and month the index names; the slice's own run, request
-- and capture once it is read. `record_count` is what its groups add to,
-- accepted only when it equals `index_record_count`.
CREATE TABLE IF NOT EXISTS control.cdc_covid_cases_slice (
    snapshot_id UUID NOT NULL REFERENCES control.cdc_covid_cases_snapshot(snapshot_id),
    slice_index INTEGER NOT NULL CHECK (slice_index >= 0),
    res_state TEXT,
    case_month TEXT,
    index_record_count BIGINT NOT NULL CHECK (index_record_count >= 0),
    run_id UUID REFERENCES control.ingestion_run(run_id),
    capture_id UUID REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    group_count INTEGER CHECK (group_count >= 0),
    record_count BIGINT CHECK (record_count >= 0),
    captured_at TIMESTAMPTZ,
    PRIMARY KEY (snapshot_id, slice_index),
    CONSTRAINT cdc_covid_slice_captured_together CHECK (
        (run_id IS NULL) = (capture_id IS NULL)
        AND (run_id IS NULL) = (payload_checksum IS NULL)
        AND (run_id IS NULL) = (group_count IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS cdc_covid_slice_capture_idx
    ON control.cdc_covid_cases_slice (capture_id);
CREATE INDEX IF NOT EXISTS cdc_covid_slice_pending_idx
    ON control.cdc_covid_cases_slice (snapshot_id, slice_index) WHERE run_id IS NULL;

-- The registered counts, written from `cdc_covid_cases.registry`.
CREATE TABLE IF NOT EXISTS silver_cdc_covid_cases.dim_measure (
    measure_id TEXT PRIMARY KEY,
    measure_label TEXT NOT NULL,
    basis TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver_cdc_covid_cases.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    snapshot_id UUID NOT NULL REFERENCES control.cdc_covid_cases_snapshot(snapshot_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    record_count BIGINT CHECK (record_count >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- Every group CDC's API returned for a slice, as returned: the county code,
-- age group and the hospitalisation and death text, with the count of CDC's
-- published records in it. Race and ethnicity are never requested.
CREATE TABLE IF NOT EXISTS silver_cdc_covid_cases.slice_group (
    snapshot_id UUID NOT NULL REFERENCES control.cdc_covid_cases_snapshot(snapshot_id),
    slice_index INTEGER NOT NULL CHECK (slice_index >= 0),
    source_row_index INTEGER NOT NULL CHECK (source_row_index > 0),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    res_state TEXT,
    state_fips TEXT CHECK (state_fips ~ '^[0-9]{2}$'),
    case_month DATE NOT NULL,
    county_fips_code TEXT,
    age_group TEXT,
    age_key TEXT NOT NULL,
    hosp_yn TEXT,
    death_yn TEXT,
    record_count BIGINT NOT NULL CHECK (record_count > 0),
    value_status TEXT NOT NULL DEFAULT 'valid' CHECK (value_status = 'valid'),
    PRIMARY KEY (snapshot_id, slice_index, source_row_index),
    CONSTRAINT cdc_covid_group_month_first CHECK (EXTRACT(DAY FROM case_month) = 1)
);

CREATE INDEX IF NOT EXISTS cdc_covid_slice_group_capture_idx
    ON silver_cdc_covid_cases.slice_group (capture_id);

-- Each snapshot's counts by county, state and nation for every case month,
-- age group (and all ages) and registered count. A record on no county (CDC
-- withheld it, or the code is not a county of the shared geography in the
-- record's state) counts in its state and the nation only
-- (`unplaced_record_count`). An area, month and age group with no published
-- record has no row: absence is never written as zero.
CREATE TABLE IF NOT EXISTS silver_cdc_covid_cases.area_month (
    snapshot_id UUID NOT NULL REFERENCES control.cdc_covid_cases_snapshot(snapshot_id),
    measure_id TEXT NOT NULL REFERENCES silver_cdc_covid_cases.dim_measure(measure_id),
    age_key TEXT NOT NULL,
    case_month DATE NOT NULL,
    geo_id TEXT NOT NULL,
    geo_sk BIGINT,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county')),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value BIGINT NOT NULL CHECK (value >= 0),
    value_status TEXT NOT NULL CHECK (value_status = 'valid'),
    case_record_count BIGINT NOT NULL CHECK (case_record_count > 0),
    unplaced_record_count BIGINT NOT NULL CHECK (unplaced_record_count >= 0),
    source_record_id TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, measure_id, age_key, geo_id, case_month),
    CONSTRAINT cdc_covid_area_within_records CHECK (value <= case_record_count)
);

CREATE INDEX IF NOT EXISTS cdc_covid_area_month_geo_idx
    ON silver_cdc_covid_cases.area_month (geo_id, measure_id, age_key, case_month);

COMMENT ON TABLE silver_cdc_covid_cases.slice_group IS
    'CDC COVID-19 Case Surveillance Public Use Data with Geography (n8mc-b4w4): counts of published case records per state, month, county, age group and hospitalisation and death field, as the Socrata API grouped them.';
