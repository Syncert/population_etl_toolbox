-- Control and silver relations for the Treasury Monthly Treasury Statement,
-- Table 9: receipts by source and outlays by budget function
-- (where-public-money-goes slice 3).

CREATE SCHEMA IF NOT EXISTS silver_treasury_mts;

-- One row per run and page of the Fiscal Data API read.
CREATE TABLE IF NOT EXISTS control.treasury_mts_page (
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    page_number INTEGER NOT NULL CHECK (page_number >= 1),
    since_record_date DATE NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    -- Statement lines this read changed or added; an unchanged line is
    -- counted, never rewritten.
    written_count INTEGER NOT NULL DEFAULT 0 CHECK (written_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, page_number),
    CONSTRAINT treasury_mts_page_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS treasury_mts_page_capture_idx ON control.treasury_mts_page (capture_id);

CREATE TABLE IF NOT EXISTS silver_treasury_mts.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    -- The row's position in the page's data list; 0 when the page is refused.
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- The current published row of every statement month and line, as Treasury
-- writes it: amounts in dollars and cents, each beside its published text.
-- A heading line's `null` is `missing` with no number, never zero. A read
-- writes a row only when the published row changed (the superseded row moves
-- to `statement_line_revision` first) or is new.
CREATE TABLE IF NOT EXISTS silver_treasury_mts.statement_line (
    record_date DATE NOT NULL,
    line_code TEXT NOT NULL CHECK (line_code ~ '^[0-9]+$'),
    classification_id TEXT NOT NULL,
    parent_id TEXT,
    classification_desc TEXT NOT NULL,
    sequence_level INTEGER,
    sequence_number_cd TEXT,
    src_line_nbr INTEGER,
    print_order_nbr TEXT,
    data_type_cd TEXT,
    record_type_cd TEXT,
    fiscal_year INTEGER NOT NULL CHECK (fiscal_year >= 2015),
    fiscal_quarter INTEGER CHECK (fiscal_quarter BETWEEN 1 AND 4),
    calendar_month INTEGER NOT NULL CHECK (calendar_month BETWEEN 1 AND 12),
    current_month_source TEXT NOT NULL,
    current_month_amount NUMERIC,
    -- The fiscal-year-to-date text as Treasury wrote it; `value_status` reads it.
    value_source TEXT NOT NULL,
    current_fytd_amount NUMERIC,
    prior_fytd_source TEXT NOT NULL,
    prior_fytd_amount NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{32}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (record_date, line_code),
    -- A valid row carries its fiscal-year-to-date amount; a missing one none.
    CONSTRAINT treasury_mts_valid_value_present CHECK (value_status <> 'valid' OR current_fytd_amount IS NOT NULL),
    CONSTRAINT treasury_mts_missing_value_absent CHECK (value_status = 'valid' OR current_fytd_amount IS NULL)
);

CREATE INDEX IF NOT EXISTS treasury_mts_statement_line_fiscal_idx
    ON silver_treasury_mts.statement_line (line_code, fiscal_year, calendar_month);
CREATE INDEX IF NOT EXISTS treasury_mts_statement_line_run_idx
    ON silver_treasury_mts.statement_line (run_id);

-- Every row a later read replaced, as it was published.
CREATE TABLE IF NOT EXISTS silver_treasury_mts.statement_line_revision (
    record_date DATE NOT NULL,
    line_code TEXT NOT NULL,
    classification_id TEXT NOT NULL,
    parent_id TEXT,
    classification_desc TEXT NOT NULL,
    sequence_level INTEGER,
    sequence_number_cd TEXT,
    src_line_nbr INTEGER,
    print_order_nbr TEXT,
    data_type_cd TEXT,
    record_type_cd TEXT,
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER,
    calendar_month INTEGER NOT NULL,
    current_month_source TEXT NOT NULL,
    current_month_amount NUMERIC,
    -- The fiscal-year-to-date text as Treasury wrote it; `value_status` reads it.
    value_source TEXT NOT NULL,
    current_fytd_amount NUMERIC,
    prior_fytd_source TEXT NOT NULL,
    prior_fytd_amount NUMERIC,
    value_status TEXT NOT NULL,
    run_id UUID NOT NULL,
    capture_id UUID NOT NULL,
    source_row_index INTEGER NOT NULL,
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    superseded_by_run_id UUID NOT NULL,
    superseded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (record_date, line_code, run_id)
);
