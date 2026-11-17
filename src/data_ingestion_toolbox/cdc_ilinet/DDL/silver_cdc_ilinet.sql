-- Control, silver and lineage tables for CDC FluView ILINet
-- (outbreak-calibration-sets). Applied by the bootstrap manifest in the
-- `silver` phase and re-applied by the DAG's `ensure_cdc_ilinet_schema` task.

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS silver_cdc_ilinet;

-- One read of FluView: its init document and one download per region type.
-- A release whose three ILINet.csv members repeat the published release's
-- bytes is `unchanged` and replays nothing.
CREATE TABLE IF NOT EXISTS control.cdc_ilinet_release (
    release_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    init_capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    kept_row_count INTEGER NOT NULL DEFAULT 0 CHECK (kept_row_count >= 0),
    changed_value_count INTEGER NOT NULL DEFAULT 0 CHECK (changed_value_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('capturing', 'failed', 'captured', 'unchanged', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT cdc_ilinet_release_kept_within_rows CHECK (kept_row_count <= row_count),
    CONSTRAINT cdc_ilinet_release_published_has_time
        CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS cdc_ilinet_release_status_idx
    ON control.cdc_ilinet_release (status, created_at);

-- One download per release and region type: its capture and the checksum of
-- its ILINet.csv member (the zip's own bytes can differ for the same file).
CREATE TABLE IF NOT EXISTS control.cdc_ilinet_download (
    release_id UUID NOT NULL REFERENCES control.cdc_ilinet_release(release_id),
    region_type TEXT NOT NULL CHECK (region_type IN ('national', 'hhs_region', 'state')),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    csv_checksum TEXT NOT NULL CHECK (csv_checksum ~ '^[0-9a-f]{64}$'),
    season_count INTEGER NOT NULL CHECK (season_count > 0),
    text_encoding TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (release_id, region_type)
);

CREATE INDEX IF NOT EXISTS cdc_ilinet_download_capture_idx
    ON control.cdc_ilinet_download (capture_id);

-- The registered figures, written from `cdc_ilinet.registry`.
CREATE TABLE IF NOT EXISTS silver_cdc_ilinet.dim_measure (
    measure_id TEXT PRIMARY KEY,
    source_column TEXT NOT NULL,
    measure_label TEXT NOT NULL,
    unit TEXT NOT NULL,
    basis TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver_cdc_ilinet.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    release_id UUID NOT NULL REFERENCES control.cdc_ilinet_release(release_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- Each ILINet reporting region and how it is served: the nation, a state of
-- the shared geography by CDC's own state FIPS code, or `unsupported` (an HHS
-- region, or a jurisdiction CDC reports beside its state, such as New York
-- City), kept here and not served on a geography.
CREATE TABLE IF NOT EXISTS silver_cdc_ilinet.region (
    region_type TEXT NOT NULL CHECK (region_type IN ('national', 'hhs_region', 'state')),
    region TEXT NOT NULL,
    geo_id TEXT,
    geo_type TEXT CHECK (geo_type IN ('nation', 'state')),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped', 'unsupported')),
    jurisdiction_note TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (region_type, region),
    CONSTRAINT cdc_ilinet_region_served_has_geo
        CHECK ((geography_status = 'unsupported') = (geo_id IS NULL))
);

-- One row per region, MMWR week and figure for each release in which CDC's
-- text for it first appeared or changed (CHURN RULE: an unchanged figure is
-- never written again). `valid` with its value; `not_published` where CDC
-- wrote X; `missing` where the cell is empty.
CREATE TABLE IF NOT EXISTS silver_cdc_ilinet.week_value (
    region_type TEXT NOT NULL,
    region TEXT NOT NULL,
    mmwr_year SMALLINT NOT NULL,
    mmwr_week SMALLINT NOT NULL CHECK (mmwr_week BETWEEN 1 AND 53),
    measure_id TEXT NOT NULL REFERENCES silver_cdc_ilinet.dim_measure(measure_id),
    release_id UUID NOT NULL REFERENCES control.cdc_ilinet_release(release_id),
    week_start DATE NOT NULL CHECK (EXTRACT(DOW FROM week_start) = 0),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index > 0),
    value_source TEXT,
    value NUMERIC CHECK (value >= 0),
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'not_published', 'missing')),
    PRIMARY KEY (region_type, region, mmwr_year, mmwr_week, measure_id, release_id),
    FOREIGN KEY (region_type, region) REFERENCES silver_cdc_ilinet.region(region_type, region),
    CONSTRAINT cdc_ilinet_value_valid_value_present
        CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT cdc_ilinet_value_other_value_absent
        CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS cdc_ilinet_week_value_capture_idx
    ON silver_cdc_ilinet.week_value (capture_id);
CREATE INDEX IF NOT EXISTS cdc_ilinet_week_value_region_idx
    ON silver_cdc_ilinet.week_value (region, measure_id, week_start);

COMMENT ON TABLE silver_cdc_ilinet.week_value IS
    'CDC FluView ILINet: weekly outpatient influenza-like illness by national, HHS region and state, one row per figure per release in which it first appeared or changed, as published.';
