-- Control and silver relations for NOAA nClimDiv county monthly temperature
-- and precipitation (noaa-climdiv).

CREATE SCHEMA IF NOT EXISTS silver_noaa_climdiv;

-- One row per run and registered element: the county file the directory
-- listed. A file whose name was already published is `already_loaded` and
-- has no capture; one whose bytes equal the element's last published
-- capture is `unchanged` and replays nothing.
CREATE TABLE IF NOT EXISTS control.noaa_climdiv_file (
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    file_element TEXT NOT NULL CHECK (file_element IN ('pcpn', 'tmpc', 'tmax', 'tmin', 'hddc', 'cddc')),
    file_name TEXT NOT NULL CHECK (file_name ~ '^climdiv-[a-z]{4}cy-v[0-9]+\.[0-9]+\.[0-9]+-[0-9]{8}$'),
    version TEXT NOT NULL,
    release_stamp TEXT NOT NULL CHECK (release_stamp ~ '^[0-9]{8}$'),
    capture_id UUID REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT CHECK (payload_checksum IS NULL OR payload_checksum ~ '^[0-9a-f]{64}$'),
    line_count INTEGER NOT NULL DEFAULT 0 CHECK (line_count >= 0),
    value_count INTEGER NOT NULL DEFAULT 0 CHECK (value_count >= 0),
    -- Monthly values this read changed or added; an unchanged month is
    -- counted, never rewritten.
    written_count INTEGER NOT NULL DEFAULT 0 CHECK (written_count >= 0),
    status TEXT NOT NULL CHECK (
        status IN ('already_loaded', 'captured', 'unchanged', 'silver_ready', 'quarantined', 'published')
    ),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, file_element),
    CONSTRAINT noaa_climdiv_file_capture_unless_already_loaded CHECK (
        (status = 'already_loaded') = (capture_id IS NULL)
    ),
    CONSTRAINT noaa_climdiv_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS noaa_climdiv_file_name_idx ON control.noaa_climdiv_file (file_name, status);
CREATE INDEX IF NOT EXISTS noaa_climdiv_file_capture_idx ON control.noaa_climdiv_file (capture_id);

CREATE TABLE IF NOT EXISTS silver_noaa_climdiv.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    -- The line number in the file; 0 when the whole file is refused.
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- The current published value of every county, element, year and month.
-- A read writes a row only when NCEI's published text for that month
-- changed (the superseded row moves to `county_month_revision` first), or
-- when the month is new. The county is NCEI's state code crosswalked to
-- the state FIPS code with the county FIPS code beside it; a code the
-- shared geography does not hold is kept `unmapped`. About 30 million rows.
-- `capture_id` references the capture it was parsed from (six captures per
-- release, so the key check reads a small index); `run_id` carries no
-- foreign key, and DQ-CLIMDIV-001 checks every row names a published file.
CREATE TABLE IF NOT EXISTS silver_noaa_climdiv.county_month (
    ncei_state_code TEXT NOT NULL CHECK (ncei_state_code ~ '^[0-9]{2}$'),
    county_fips TEXT NOT NULL CHECK (county_fips ~ '^[0-9]{3}$'),
    measure TEXT NOT NULL CHECK (
        measure IN (
            'monthly_precipitation_total', 'monthly_average_temperature', 'monthly_maximum_temperature',
            'monthly_minimum_temperature', 'monthly_heating_degree_days', 'monthly_cooling_degree_days'
        )
    ),
    year INTEGER NOT NULL CHECK (year >= 1895),
    month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    state_fips TEXT NOT NULL CHECK (state_fips ~ '^[0-9]{2}$'),
    geo_id TEXT,
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    missing_reason TEXT CHECK (missing_reason IS NULL OR missing_reason IN ('not_yet_published')),
    run_id UUID NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_line INTEGER NOT NULL CHECK (source_line >= 1),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{32}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ncei_state_code, county_fips, measure, year, month),
    CONSTRAINT noaa_climdiv_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT noaa_climdiv_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL),
    CONSTRAINT noaa_climdiv_missing_has_reason CHECK ((value_status = 'valid') = (missing_reason IS NULL)),
    CONSTRAINT noaa_climdiv_resolved_has_geo CHECK (
        (geography_status = 'resolved') = (geo_id IS NOT NULL AND geo_sk IS NOT NULL)
    )
);

-- A place page reads one county; a map reads one measure and month.
CREATE INDEX IF NOT EXISTS noaa_climdiv_county_month_geo_idx
    ON silver_noaa_climdiv.county_month (geo_id, measure, year, month);
CREATE INDEX IF NOT EXISTS noaa_climdiv_county_month_period_idx
    ON silver_noaa_climdiv.county_month (measure, year, month);
CREATE INDEX IF NOT EXISTS noaa_climdiv_county_month_run_idx
    ON silver_noaa_climdiv.county_month (run_id);

-- Every value a later read replaced, as it was published, with the run
-- that replaced it.
CREATE TABLE IF NOT EXISTS silver_noaa_climdiv.county_month_revision (
    ncei_state_code TEXT NOT NULL,
    county_fips TEXT NOT NULL,
    measure TEXT NOT NULL,
    year INTEGER NOT NULL,
    month SMALLINT NOT NULL,
    state_fips TEXT NOT NULL,
    geo_id TEXT,
    geo_sk BIGINT,
    geography_status TEXT NOT NULL,
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL,
    missing_reason TEXT,
    run_id UUID NOT NULL,
    capture_id UUID NOT NULL,
    source_line INTEGER NOT NULL,
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    superseded_by_run_id UUID NOT NULL,
    superseded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ncei_state_code, county_fips, measure, year, month, run_id)
);

CREATE INDEX IF NOT EXISTS noaa_climdiv_revision_geo_idx
    ON silver_noaa_climdiv.county_month_revision (geo_id, measure, year, month);
