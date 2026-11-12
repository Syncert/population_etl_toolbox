-- Control and silver relations for USAspending federal award obligations by
-- place of performance (where-public-money-goes slice 4).

CREATE SCHEMA IF NOT EXISTS silver_usaspending;

-- One row per run and request: a fiscal year cut by everything, one award
-- type group or one awarding agency, for one layer; or one page of the
-- year's awarding-agency list (layer `nation`, cut `agency_list`).
CREATE TABLE IF NOT EXISTS control.usaspending_request (
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    fiscal_year INTEGER NOT NULL CHECK (fiscal_year >= 2008),
    geo_layer TEXT NOT NULL CHECK (geo_layer IN ('state', 'county', 'nation')),
    cut_kind TEXT NOT NULL CHECK (cut_kind IN ('all', 'award_type', 'agency', 'agency_list')),
    -- '' for `all`; the award group key; the agency's code; the page number.
    cut_key TEXT NOT NULL,
    agency_name TEXT,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    -- Rows this answer changed, added or withdrew; an unchanged row is
    -- counted, never rewritten.
    written_count INTEGER NOT NULL DEFAULT 0 CHECK (written_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, fiscal_year, geo_layer, cut_kind, cut_key),
    CONSTRAINT usaspending_request_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS usaspending_request_key_idx
    ON control.usaspending_request (fiscal_year, geo_layer, cut_kind, cut_key, status);
CREATE INDEX IF NOT EXISTS usaspending_request_capture_idx
    ON control.usaspending_request (capture_id);

CREATE TABLE IF NOT EXISTS silver_usaspending.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    -- The row's position in the answer's results; 0 when the answer is refused.
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- The awarding agencies USAspending names for a fiscal year, as published.
CREATE TABLE IF NOT EXISTS silver_usaspending.agency_year (
    fiscal_year INTEGER NOT NULL,
    agency_code TEXT NOT NULL,
    agency_name TEXT NOT NULL,
    agency_id INTEGER,
    value_source TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fiscal_year, agency_code)
);

-- The current published amount of every request key and place: obligations
-- in dollars and cents beside the text USAspending wrote. An area with no
-- recorded place (an empty shape code, or one that names no state or county)
-- is kept `unsupported` and never served. `geo_id` follows from the shape code
-- alone; gold resolves it against the shared geography when read, so a
-- boundary load never rewrites a published row.
CREATE TABLE IF NOT EXISTS silver_usaspending.geography_amount (
    fiscal_year INTEGER NOT NULL CHECK (fiscal_year >= 2008),
    geo_layer TEXT NOT NULL CHECK (geo_layer IN ('state', 'county')),
    cut_kind TEXT NOT NULL CHECK (cut_kind IN ('all', 'award_type', 'agency')),
    cut_key TEXT NOT NULL,
    shape_code TEXT NOT NULL,
    display_name TEXT,
    -- The amount as USAspending wrote it; `value_status` reads it.
    value_source TEXT NOT NULL,
    amount NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    population_source TEXT,
    geo_id TEXT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unsupported')),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{32}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fiscal_year, geo_layer, cut_kind, cut_key, shape_code),
    CONSTRAINT usaspending_valid_value_present CHECK (value_status <> 'valid' OR amount IS NOT NULL),
    CONSTRAINT usaspending_missing_value_absent CHECK (value_status = 'valid' OR amount IS NULL)
);

-- A place's whole history in one index range.
CREATE INDEX IF NOT EXISTS usaspending_geography_amount_geo_idx
    ON silver_usaspending.geography_amount (geo_id, fiscal_year, cut_kind);
CREATE INDEX IF NOT EXISTS usaspending_geography_amount_run_idx
    ON silver_usaspending.geography_amount (run_id);

-- Every row a later answer replaced or no longer listed, as it was published.
CREATE TABLE IF NOT EXISTS silver_usaspending.geography_amount_revision (
    fiscal_year INTEGER NOT NULL,
    geo_layer TEXT NOT NULL,
    cut_kind TEXT NOT NULL,
    cut_key TEXT NOT NULL,
    shape_code TEXT NOT NULL,
    display_name TEXT,
    value_source TEXT NOT NULL,
    amount NUMERIC,
    value_status TEXT NOT NULL,
    population_source TEXT,
    geo_id TEXT,
    geography_status TEXT NOT NULL,
    run_id UUID NOT NULL,
    capture_id UUID NOT NULL,
    source_row_index INTEGER NOT NULL,
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    change_kind TEXT NOT NULL CHECK (change_kind IN ('replaced', 'withdrawn')),
    superseded_by_run_id UUID NOT NULL,
    superseded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fiscal_year, geo_layer, cut_kind, cut_key, shape_code, run_id)
);

CREATE INDEX IF NOT EXISTS usaspending_geography_amount_revision_geo_idx
    ON silver_usaspending.geography_amount_revision (geo_id, fiscal_year);
