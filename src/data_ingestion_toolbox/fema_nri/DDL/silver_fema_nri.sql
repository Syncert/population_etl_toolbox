-- Control and silver relations for the FEMA National Risk Index and disaster
-- declarations (fema-nri-declarations).

CREATE SCHEMA IF NOT EXISTS silver_fema_nri;

-- One run per read of a stream. An NRI read whose pages are byte-for-byte the
-- last published read's is `unchanged` and replays nothing.
CREATE TABLE IF NOT EXISTS control.fema_nri_run (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    stream TEXT NOT NULL CHECK (stream IN ('nri', 'declarations')),
    status TEXT NOT NULL CHECK (
        status IN ('capturing', 'captured', 'unchanged', 'silver_ready', 'quarantined', 'published')
    ),
    run_checksum TEXT CHECK (run_checksum IS NULL OR run_checksum ~ '^[0-9a-f]{64}$'),
    nri_version TEXT,
    page_count INTEGER NOT NULL DEFAULT 0 CHECK (page_count >= 0),
    record_count INTEGER NOT NULL DEFAULT 0 CHECK (record_count >= 0),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fema_nri_run_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL),
    CONSTRAINT fema_nri_run_nri_has_version CHECK (
        stream <> 'nri' OR status IN ('capturing', 'quarantined') OR nri_version IS NOT NULL
    )
);

-- One row per committed page of a run.
CREATE TABLE IF NOT EXISTS control.fema_nri_page (
    run_id UUID NOT NULL REFERENCES control.fema_nri_run(run_id),
    page_index INTEGER NOT NULL CHECK (page_index >= 0),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    record_count INTEGER NOT NULL CHECK (record_count >= 0),
    PRIMARY KEY (run_id, page_index)
);

CREATE TABLE IF NOT EXISTS silver_fema_nri.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    record_index INTEGER NOT NULL CHECK (record_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, record_index, error_code)
);

-- Every registered NRI field of every county of a replayed read. A field
-- whose rating says it is not a measurement carries that status and no
-- number.
CREATE TABLE IF NOT EXISTS silver_fema_nri.nri_fact (
    run_id UUID NOT NULL REFERENCES control.fema_nri_run(run_id),
    geo_id TEXT NOT NULL,
    field TEXT NOT NULL,
    measure TEXT NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    stcofips TEXT NOT NULL CHECK (stcofips ~ '^[0-9]{5}$'),
    county_type TEXT,
    nri_version TEXT NOT NULL,
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing', 'not_applicable')),
    missing_reason TEXT CHECK (
        missing_reason IS NULL
        OR missing_reason IN ('hazard_not_applicable', 'insufficient_data', 'data_unavailable', 'blank')
    ),
    rating TEXT,
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, geo_id, field),
    CONSTRAINT fema_nri_fact_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT fema_nri_fact_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS fema_nri_fact_measure_idx ON silver_fema_nri.nri_fact (measure, geo_id);

-- Every revision of every declaration area row: a new `hash` for an `id` is
-- a new revision beside the old one. A `000` county code is a statewide or
-- non-county area (`area`), kept and never counted as a county.
CREATE TABLE IF NOT EXISTS silver_fema_nri.declaration_revision (
    declaration_id TEXT NOT NULL,
    revision_hash TEXT NOT NULL CHECK (revision_hash ~ '^[0-9a-f]{40}$'),
    run_id UUID NOT NULL REFERENCES control.fema_nri_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    record_index INTEGER NOT NULL CHECK (record_index >= 1),
    declaration_string TEXT NOT NULL,
    disaster_number INTEGER NOT NULL,
    declaration_type TEXT NOT NULL CHECK (declaration_type IN ('DR', 'EM', 'FM')),
    declaration_date DATE NOT NULL,
    incident_type TEXT NOT NULL,
    state_fips TEXT NOT NULL CHECK (state_fips ~ '^[0-9]{2}$'),
    county_fips TEXT NOT NULL CHECK (county_fips ~ '^[0-9]{3}$'),
    place_code TEXT NOT NULL,
    designated_area TEXT NOT NULL,
    last_refresh TIMESTAMPTZ NOT NULL,
    geo_id TEXT,
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped', 'area')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (declaration_id, revision_hash),
    CONSTRAINT fema_declaration_area_has_no_county CHECK ((county_fips = '000') = (geography_status = 'area')),
    CONSTRAINT fema_declaration_county_has_geo CHECK (geography_status = 'area' OR geo_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS fema_declaration_geo_idx ON silver_fema_nri.declaration_revision (geo_id, declaration_date);
