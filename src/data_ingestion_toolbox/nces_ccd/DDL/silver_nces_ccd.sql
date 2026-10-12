-- Control and silver relations for NCES Common Core of Data public schools
-- (CCD school-universe files and EDGE school geocodes, nces-ccd).

CREATE SCHEMA IF NOT EXISTS silver_nces_ccd;

-- One run per read of a registered file. A read whose bytes equal the same
-- file's last published capture is `unchanged` and replays nothing. NCES
-- names a new release in the file name; `version_rank` orders releases of one
-- component and school year (1a < 1b < 2a).
CREATE TABLE IF NOT EXISTS control.nces_ccd_file (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    component TEXT NOT NULL CHECK (component IN ('geocode', 'directory', 'membership', 'staff', 'lunch')),
    school_year TEXT NOT NULL CHECK (school_year ~ '^[0-9]{4}-[0-9]{4}$'),
    file_stem TEXT NOT NULL,
    release_version TEXT NOT NULL,
    version_rank INTEGER NOT NULL CHECK (version_rank >= 0),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    kept_row_count INTEGER NOT NULL DEFAULT 0 CHECK (kept_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'unchanged', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT nces_ccd_file_kept_within_rows CHECK (kept_row_count <= row_count),
    CONSTRAINT nces_ccd_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS nces_ccd_file_capture_idx ON control.nces_ccd_file (capture_id);
CREATE INDEX IF NOT EXISTS nces_ccd_file_component_idx ON control.nces_ccd_file (component, school_year, status);

CREATE TABLE IF NOT EXISTS silver_nces_ccd.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- Every school of every replayed EDGE geocode file: its physical state and
-- county by NCES's own codes, resolved to the shared geography by code. The
-- operating state (`OPSTFIPS`, 59 for BIE and 63 for DoDEA schools) is kept
-- and never resolved as a state.
CREATE TABLE IF NOT EXISTS silver_nces_ccd.school_location (
    run_id UUID NOT NULL REFERENCES control.nces_ccd_file(run_id),
    ncessch TEXT NOT NULL CHECK (ncessch ~ '^[0-9]{12}$'),
    leaid TEXT NOT NULL CHECK (leaid ~ '^[0-9]{7}$'),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    operating_state_fips TEXT NOT NULL CHECK (operating_state_fips ~ '^[0-9]{2}$'),
    state_fips TEXT NOT NULL CHECK (state_fips ~ '^[0-9]{2}$'),
    county_fips TEXT NOT NULL CHECK (county_fips ~ '^[0-9]{5}$'),
    latitude NUMERIC,
    longitude NUMERIC,
    geo_id TEXT NOT NULL,
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, ncessch),
    CONSTRAINT nces_ccd_location_county_in_state CHECK (LEFT(county_fips, 2) = state_fips),
    CONSTRAINT nces_ccd_location_resolved_has_key CHECK ((geography_status = 'resolved') = (geo_sk IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS nces_ccd_location_school_idx ON silver_nces_ccd.school_location (ncessch);

-- Every school of every replayed directory file, with its status that year.
CREATE TABLE IF NOT EXISTS silver_nces_ccd.school_directory (
    run_id UUID NOT NULL REFERENCES control.nces_ccd_file(run_id),
    ncessch TEXT NOT NULL CHECK (ncessch ~ '^[0-9]{12}$'),
    leaid TEXT NOT NULL CHECK (leaid ~ '^[0-9]{7}$'),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    operating_state_fips TEXT NOT NULL CHECK (operating_state_fips ~ '^[0-9]{2}$'),
    school_status TEXT NOT NULL,
    school_type TEXT NOT NULL,
    charter TEXT NOT NULL,
    school_level TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, ncessch)
);

-- Every registered school count of every replayed membership, staff and
-- lunch file. Only a `Reported` count carries a number.
CREATE TABLE IF NOT EXISTS silver_nces_ccd.school_count (
    run_id UUID NOT NULL REFERENCES control.nces_ccd_file(run_id),
    ncessch TEXT NOT NULL CHECK (ncessch ~ '^[0-9]{12}$'),
    measure TEXT NOT NULL CHECK (
        measure IN (
            'student_membership', 'teacher_fte', 'frpl_eligible', 'free_lunch_eligible',
            'reduced_price_lunch_eligible', 'direct_certification'
        )
    ),
    leaid TEXT NOT NULL CHECK (leaid ~ '^[0-9]{7}$'),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    operating_state_fips TEXT NOT NULL CHECK (operating_state_fips ~ '^[0-9]{2}$'),
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing', 'suppressed')),
    missing_reason TEXT CHECK (missing_reason IS NULL OR missing_reason IN ('not_reported', 'missing', 'suppressed')),
    dms_flag TEXT NOT NULL CHECK (dms_flag IN ('Reported', 'Not reported', 'Missing', 'Suppressed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, ncessch, measure),
    CONSTRAINT nces_ccd_count_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT nces_ccd_count_withheld_value_absent CHECK (value_status = 'valid' OR value IS NULL),
    CONSTRAINT nces_ccd_count_not_negative CHECK (value IS NULL OR value >= 0)
);

CREATE INDEX IF NOT EXISTS nces_ccd_count_school_idx ON silver_nces_ccd.school_count (ncessch, measure);
