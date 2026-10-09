-- Control and silver relations for BEA regional accounts (bea-regional-accounts).
-- Applied by the bootstrap manifest in the `silver` phase and re-applied by
-- the DAG's `ensure_bea_schema` task; every statement is rerunnable.

CREATE SCHEMA IF NOT EXISTS silver_bea;

-- One row per captured table zip.
CREATE TABLE IF NOT EXISTS control.bea_table_capture (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    table_code TEXT NOT NULL CONSTRAINT bea_table_capture_table_code_check
        CHECK (table_code ~ '^(CA|SA|MA|PA)[A-Z0-9]+$'),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    member_name TEXT,
    release_date DATE,
    captured_row_count INTEGER NOT NULL DEFAULT 0 CHECK (captured_row_count >= 0),
    in_scope_row_count INTEGER NOT NULL DEFAULT 0 CHECK (in_scope_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('captured', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT bea_capture_scope_within_rows CHECK (in_scope_row_count <= captured_row_count),
    CONSTRAINT bea_capture_published_has_release CHECK (
        status <> 'published' OR (published_at IS NOT NULL AND release_date IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS bea_table_capture_capture_idx ON control.bea_table_capture (capture_id);

CREATE TABLE IF NOT EXISTS silver_bea.dim_line (
    table_code TEXT NOT NULL,
    line_code TEXT NOT NULL CHECK (line_code ~ '^[0-9]+$'),
    table_title TEXT NOT NULL,
    description TEXT NOT NULL,
    unit TEXT NOT NULL,
    dollar_basis TEXT NOT NULL CONSTRAINT dim_line_dollar_basis_check CHECK (dollar_basis IN (
        'current_dollars', 'chained_dollars', 'per_capita_current_dollars', 'persons',
        'price_level_us_100'
    )),
    observation_basis TEXT NOT NULL,
    methodology_url TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_code, line_code)
);

-- Every parsed cell of every in-scope captured row, one row per year.
CREATE TABLE IF NOT EXISTS silver_bea.observation_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    year INTEGER NOT NULL CHECK (year BETWEEN 1929 AND 2100),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    table_code TEXT NOT NULL,
    line_code TEXT NOT NULL,
    geo_type TEXT NOT NULL CONSTRAINT observation_revision_geo_type_check CHECK (geo_type IN (
        'nation', 'state', 'county', 'metro', 'provider_area'
    )),
    geo_source_code TEXT NOT NULL,
    geo_source_label TEXT,
    geo_id TEXT NOT NULL,
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN (
        'valid', 'withheld', 'not_available', 'not_meaningful', 'below_threshold', 'missing'
    )),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index, year),
    FOREIGN KEY (table_code, line_code) REFERENCES silver_bea.dim_line(table_code, line_code),
    CONSTRAINT bea_revision_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT bea_revision_code_value_absent CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS bea_revision_run_idx ON silver_bea.observation_revision (run_id);

CREATE TABLE IF NOT EXISTS silver_bea.observation_quarantine (
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
-- they came from, so each release is kept beside the one it revised.
CREATE TABLE IF NOT EXISTS silver_bea.fact_observation (
    table_code TEXT NOT NULL,
    line_code TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    release_date DATE NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL,
    geo_sk BIGINT,
    geo_type TEXT NOT NULL CONSTRAINT fact_observation_geo_type_check CHECK (geo_type IN (
        'nation', 'state', 'county', 'metro', 'provider_area'
    )),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN (
        'valid', 'withheld', 'not_available', 'not_meaningful', 'below_threshold', 'missing'
    )),
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_code, line_code, geo_id, year, capture_id),
    FOREIGN KEY (table_code, line_code) REFERENCES silver_bea.dim_line(table_code, line_code),
    CONSTRAINT bea_fact_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT bea_fact_code_value_absent CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS bea_fact_lookup_idx ON silver_bea.fact_observation (table_code, line_code, geo_id, year);
CREATE INDEX IF NOT EXISTS bea_fact_run_idx ON silver_bea.fact_observation (run_id);

-- Regional price parities (grocery-and-gasoline-prices) add a basis -- a
-- price level with the nation at 100 -- and two geography types: CBSAs and
-- BEA's own state portions. A warehouse created before them holds the three
-- CHECKs under the names Postgres gave them; each is replaced only while its
-- definition does not yet name the new value, and every existing row
-- satisfies both definitions.
DO $$
BEGIN
    -- The price tables are state (SA), metro (MA) and portion (PA) tables.
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'control.bea_table_capture'::regclass
          AND conname = 'bea_table_capture_table_code_check'
          AND pg_get_constraintdef(oid) NOT LIKE '%MA|PA%'
    ) THEN
        ALTER TABLE control.bea_table_capture DROP CONSTRAINT bea_table_capture_table_code_check;
        ALTER TABLE control.bea_table_capture ADD CONSTRAINT bea_table_capture_table_code_check
            CHECK (table_code ~ '^(CA|SA|MA|PA)[A-Z0-9]+$');
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'silver_bea.dim_line'::regclass
          AND conname = 'dim_line_dollar_basis_check'
          AND pg_get_constraintdef(oid) NOT LIKE '%price_level_us_100%'
    ) THEN
        ALTER TABLE silver_bea.dim_line DROP CONSTRAINT dim_line_dollar_basis_check;
        ALTER TABLE silver_bea.dim_line ADD CONSTRAINT dim_line_dollar_basis_check
            CHECK (dollar_basis IN (
                'current_dollars', 'chained_dollars', 'per_capita_current_dollars', 'persons',
                'price_level_us_100'
            ));
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'silver_bea.observation_revision'::regclass
          AND conname = 'observation_revision_geo_type_check'
          AND pg_get_constraintdef(oid) NOT LIKE '%provider_area%'
    ) THEN
        ALTER TABLE silver_bea.observation_revision DROP CONSTRAINT observation_revision_geo_type_check;
        ALTER TABLE silver_bea.observation_revision ADD CONSTRAINT observation_revision_geo_type_check
            CHECK (geo_type IN ('nation', 'state', 'county', 'metro', 'provider_area'));
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'silver_bea.fact_observation'::regclass
          AND conname = 'fact_observation_geo_type_check'
          AND pg_get_constraintdef(oid) NOT LIKE '%provider_area%'
    ) THEN
        ALTER TABLE silver_bea.fact_observation DROP CONSTRAINT fact_observation_geo_type_check;
        ALTER TABLE silver_bea.fact_observation ADD CONSTRAINT fact_observation_geo_type_check
            CHECK (geo_type IN ('nation', 'state', 'county', 'metro', 'provider_area'));
    END IF;
END $$;
