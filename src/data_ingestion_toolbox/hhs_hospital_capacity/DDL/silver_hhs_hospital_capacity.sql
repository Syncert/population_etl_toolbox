-- Control, silver and lineage tables for the HHS COVID-19 hospital capacity
-- by facility file (hospital-capacity-and-locations). Applied by the bootstrap
-- manifest in the `silver` phase and re-applied by the DAG's
-- `ensure_hhs_hospital_capacity_schema` task.

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS silver_hhs_hospital_capacity;

-- One complete read of the dataset: its view document, then every page in
-- order. The dataset is frozen, so a complete snapshot is never read again
-- unless a run asks for history; a history snapshot whose every page repeats
-- the published one is recorded `unchanged` and replays nothing.
CREATE TABLE IF NOT EXISTS control.hhs_hospital_capacity_snapshot (
    snapshot_id UUID PRIMARY KEY,
    metadata_run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    metadata_capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    rows_updated_at BIGINT,
    page_size INTEGER NOT NULL CHECK (page_size > 0),
    page_count INTEGER NOT NULL DEFAULT 0 CHECK (page_count >= 0),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    kept_row_count INTEGER NOT NULL DEFAULT 0 CHECK (kept_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('capturing', 'captured', 'unchanged', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT hhs_snapshot_kept_within_rows CHECK (kept_row_count <= row_count),
    CONSTRAINT hhs_snapshot_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS hhs_snapshot_status_idx
    ON control.hhs_hospital_capacity_snapshot (status, created_at);

-- One row per captured page: its own run, request and capture.
CREATE TABLE IF NOT EXISTS control.hhs_hospital_capacity_page (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    snapshot_id UUID NOT NULL REFERENCES control.hhs_hospital_capacity_snapshot(snapshot_id),
    page_index INTEGER NOT NULL CHECK (page_index >= 0),
    row_offset INTEGER NOT NULL CHECK (row_offset >= 0),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT hhs_page_once_per_snapshot UNIQUE (snapshot_id, page_index)
);

CREATE INDEX IF NOT EXISTS hhs_page_capture_idx
    ON control.hhs_hospital_capacity_page (capture_id);

-- The registered figures, written from `hhs_hospital_capacity.registry`.
CREATE TABLE IF NOT EXISTS silver_hhs_hospital_capacity.dim_measure (
    measure_id TEXT PRIMARY KEY,
    source_column TEXT NOT NULL,
    coverage_column TEXT NOT NULL,
    measure_label TEXT NOT NULL,
    unit TEXT NOT NULL,
    basis TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver_hhs_hospital_capacity.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    snapshot_id UUID NOT NULL REFERENCES control.hhs_hospital_capacity_snapshot(snapshot_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- Each hospital of a snapshot, as its newest published week describes it,
-- placed on the county its published FIPS code names when that county is in
-- the shared geography and of the hospital's own state; its point is HHS's
-- published geocode, never a third-party geocode.
CREATE TABLE IF NOT EXISTS silver_hhs_hospital_capacity.facility (
    snapshot_id UUID NOT NULL REFERENCES control.hhs_hospital_capacity_snapshot(snapshot_id),
    hospital_pk TEXT NOT NULL,
    ccn TEXT,
    hospital_name TEXT NOT NULL,
    street_address TEXT,
    city TEXT,
    zip_code TEXT,
    state_code TEXT NOT NULL,
    state_fips TEXT CHECK (state_fips ~ '^[0-9]{2}$'),
    hospital_subtype TEXT,
    fips_code TEXT,
    is_metro_micro BOOLEAN,
    longitude DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
    latitude DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
    county_geo_id TEXT,
    county_geo_sk BIGINT,
    county_status TEXT NOT NULL CHECK (county_status IN ('resolved', 'unmapped', 'not_listed')),
    first_week DATE NOT NULL,
    last_week DATE NOT NULL,
    week_count INTEGER NOT NULL CHECK (week_count > 0),
    PRIMARY KEY (snapshot_id, hospital_pk),
    CONSTRAINT hhs_facility_week_order CHECK (first_week <= last_week),
    CONSTRAINT hhs_facility_resolved_has_county
        CHECK ((county_status = 'resolved') = (county_geo_id IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS hhs_facility_county_idx
    ON silver_hhs_hospital_capacity.facility (county_geo_id, snapshot_id);

-- One row per hospital, collection week and registered figure, as HHS
-- published it: `valid` with its value, `suppressed` where HHS wrote -999999
-- (a weekly sum between 1 and 3), `missing` where the row omits it; HHS's
-- own text beside it, and the days of the week the facility reported it.
CREATE TABLE IF NOT EXISTS silver_hhs_hospital_capacity.facility_week_value (
    snapshot_id UUID NOT NULL REFERENCES control.hhs_hospital_capacity_snapshot(snapshot_id),
    hospital_pk TEXT NOT NULL,
    collection_week DATE NOT NULL,
    measure_id TEXT NOT NULL REFERENCES silver_hhs_hospital_capacity.dim_measure(measure_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index > 0),
    value_source TEXT,
    value NUMERIC CHECK (value >= 0),
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'suppressed', 'missing')),
    coverage_days SMALLINT CHECK (coverage_days BETWEEN 0 AND 7),
    PRIMARY KEY (snapshot_id, hospital_pk, collection_week, measure_id),
    CONSTRAINT hhs_value_valid_value_present
        CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT hhs_value_other_value_absent
        CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS hhs_facility_week_value_capture_idx
    ON silver_hhs_hospital_capacity.facility_week_value (capture_id);

-- Each snapshot's sums by county, state and nation for every week and
-- figure: the published values of the hospitals placed in the area added
-- together, with how many hospitals reported a value, how many cells HHS
-- suppressed and how many the file omits beside each sum. A hospital on no
-- county counts in its state and the nation (`unplaced_facility_count`).
-- An area week whose every cell is suppressed or missing has no value.
CREATE TABLE IF NOT EXISTS silver_hhs_hospital_capacity.area_week (
    snapshot_id UUID NOT NULL REFERENCES control.hhs_hospital_capacity_snapshot(snapshot_id),
    measure_id TEXT NOT NULL REFERENCES silver_hhs_hospital_capacity.dim_measure(measure_id),
    collection_week DATE NOT NULL,
    geo_id TEXT NOT NULL,
    geo_sk BIGINT,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county')),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'suppressed', 'missing')),
    facility_count INTEGER NOT NULL CHECK (facility_count > 0),
    reporting_facility_count INTEGER NOT NULL CHECK (reporting_facility_count >= 0),
    suppressed_count INTEGER NOT NULL CHECK (suppressed_count >= 0),
    missing_count INTEGER NOT NULL CHECK (missing_count >= 0),
    unplaced_facility_count INTEGER NOT NULL CHECK (unplaced_facility_count >= 0),
    source_record_id TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, measure_id, geo_id, collection_week),
    CONSTRAINT hhs_area_counts_add_up
        CHECK (reporting_facility_count + suppressed_count + missing_count = facility_count),
    CONSTRAINT hhs_area_value_when_reported
        CHECK ((value IS NOT NULL) = (reporting_facility_count > 0))
);

CREATE INDEX IF NOT EXISTS hhs_area_week_geo_idx
    ON silver_hhs_hospital_capacity.area_week (geo_id, measure_id, collection_week);

COMMENT ON TABLE silver_hhs_hospital_capacity.facility_week_value IS
    'HHS COVID-19 Reported Patient Impact and Hospital Capacity by Facility (anag-cw7u), one row per hospital, collection week and figure, as published.';
