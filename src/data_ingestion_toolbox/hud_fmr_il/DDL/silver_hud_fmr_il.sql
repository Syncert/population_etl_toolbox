-- Control and silver relations for HUD Fair Market Rents and income limits
-- (hud-fair-market-rents-and-income-limits).

CREATE SCHEMA IF NOT EXISTS silver_hud_fmr_il;

-- One run per read of a registered edition: a dataset, a fiscal year and an
-- original or revised workbook. A read whose bytes equal that edition's last
-- published file is `unchanged` and replays nothing.
CREATE TABLE IF NOT EXISTS control.hud_fmr_il_file (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    dataset TEXT NOT NULL CHECK (dataset IN ('fmr', 'il')),
    fiscal_year INTEGER NOT NULL CHECK (fiscal_year BETWEEN 1983 AND 2100),
    edition TEXT NOT NULL CHECK (edition IN ('original', 'revised')),
    effective_date DATE NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    county_row_count INTEGER NOT NULL DEFAULT 0 CHECK (county_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'unchanged', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT hud_fmr_il_file_counties_within_rows CHECK (county_row_count <= row_count),
    CONSTRAINT hud_fmr_il_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS hud_fmr_il_file_capture_idx ON control.hud_fmr_il_file (capture_id);

-- Every row of every replayed workbook, one row per measure, as the workbook
-- stores it. A town row (a `fips` not ending 99999) is a county subdivision.
CREATE TABLE IF NOT EXISTS silver_hud_fmr_il.observation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    measure TEXT NOT NULL CHECK (measure ~ '^(fmr_[0-4]br|median_family_income|income_limit_(30|50|80)_[1-8]p)$'),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    fips_code TEXT NOT NULL CHECK (fips_code ~ '^[0-9]{10}$'),
    geo_type TEXT NOT NULL CHECK (geo_type IN ('county', 'county_subdivision')),
    geo_id TEXT NOT NULL,
    hud_area_code TEXT NOT NULL,
    hud_area_name TEXT NOT NULL,
    metro BOOLEAN NOT NULL,
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    missing_reason TEXT CHECK (missing_reason IS NULL OR missing_reason = 'provider_missing'),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index, measure),
    CONSTRAINT hud_fmr_il_revision_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT hud_fmr_il_revision_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS hud_fmr_il_revision_run_idx ON silver_hud_fmr_il.observation_revision (run_id);

CREATE TABLE IF NOT EXISTS silver_hud_fmr_il.observation_quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- The conformed observations, keyed by what they describe and the capture
-- they came from, so an original and a revised edition are kept side by
-- side. A town row is `unsupported`: held, never published as its county.
CREATE TABLE IF NOT EXISTS silver_hud_fmr_il.fact_observation (
    measure TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    dataset TEXT NOT NULL,
    edition TEXT NOT NULL,
    effective_date DATE NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL,
    fips_code TEXT NOT NULL,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('county', 'county_subdivision')),
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped', 'unsupported')),
    hud_area_code TEXT NOT NULL,
    hud_area_name TEXT NOT NULL,
    metro BOOLEAN NOT NULL,
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    missing_reason TEXT,
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (measure, geo_id, fiscal_year, capture_id),
    CONSTRAINT hud_fmr_il_fact_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT hud_fmr_il_fact_missing_value_absent CHECK (value_status = 'valid' OR value IS NULL),
    CONSTRAINT hud_fmr_il_fact_town_unsupported CHECK (geo_type = 'county' OR geography_status = 'unsupported')
);

CREATE INDEX IF NOT EXISTS hud_fmr_il_fact_lookup_idx
    ON silver_hud_fmr_il.fact_observation (measure, geo_id, fiscal_year);
CREATE INDEX IF NOT EXISTS hud_fmr_il_fact_run_idx ON silver_hud_fmr_il.fact_observation (run_id);
