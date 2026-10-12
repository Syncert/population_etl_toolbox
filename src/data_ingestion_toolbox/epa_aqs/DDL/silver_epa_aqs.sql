-- Control and silver relations for EPA air quality (AirData annual monitor
-- files, epa-aqs).

CREATE SCHEMA IF NOT EXISTS silver_epa_aqs;

-- One run per read of a year's file. A read whose bytes equal that year's
-- last published capture is `unchanged` and replays nothing.
CREATE TABLE IF NOT EXISTS control.epa_aqs_file (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    year INTEGER NOT NULL CHECK (year BETWEEN 1980 AND 2100),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    in_scope_row_count INTEGER NOT NULL DEFAULT 0 CHECK (in_scope_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'unchanged', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT epa_aqs_file_scope_within_rows CHECK (in_scope_row_count <= row_count),
    CONSTRAINT epa_aqs_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS epa_aqs_file_capture_idx ON control.epa_aqs_file (capture_id);

CREATE TABLE IF NOT EXISTS silver_epa_aqs.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- Every in-scope monitor-year row of every replayed file, with its event
-- type, completeness and certification. A county resolves by its FIPS codes.
CREATE TABLE IF NOT EXISTS silver_epa_aqs.monitor_fact (
    run_id UUID NOT NULL REFERENCES control.epa_aqs_file(run_id),
    monitor_id TEXT NOT NULL CHECK (monitor_id ~ '^[0-9]{5}-[0-9]{4}-[0-9]{5}-[0-9]+$'),
    sample_duration TEXT NOT NULL,
    pollutant_standard TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (
        event_type IN ('No Events', 'Events Included', 'Events Excluded', 'Concurred Events Excluded')
    ),
    year INTEGER NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    measure TEXT NOT NULL CHECK (measure IN ('pm25_annual_mean', 'ozone_8hour_4th_max')),
    parameter_code TEXT NOT NULL,
    poc INTEGER NOT NULL,
    site_number TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    completeness TEXT NOT NULL CHECK (completeness IN ('Y', 'N')),
    certification TEXT NOT NULL,
    observation_count INTEGER NOT NULL CHECK (observation_count >= 0),
    units TEXT NOT NULL,
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    date_of_last_change DATE,
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, monitor_id, sample_duration, pollutant_standard, event_type),
    CONSTRAINT epa_aqs_monitor_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT epa_aqs_monitor_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS epa_aqs_monitor_county_idx ON silver_epa_aqs.monitor_fact (geo_id, year, measure);
