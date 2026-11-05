-- Capture-first Census government finance control and silver contract
-- (taxes-and-public-money T-5).
--
-- The source owns its relations: this file is applied by the bootstrap
-- manifest in the `silver` phase and re-applied by the `census_govfin_ingest`
-- DAG's `ensure_census_govfin_schema` task, so a warehouse a step behind the
-- DAG is repaired before the DAG writes to it. Every statement is rerun-safe.

CREATE SCHEMA IF NOT EXISTS silver_census_govfin;

CREATE SCHEMA IF NOT EXISTS gold_census_govfin;

-- One captured slice per row: a Census Data API request (one grain of one
-- year of `timeseries/govs`, one year of QTAX) or one zip of a Census of
-- Governments edition. A year the API does not publish answers 204 and is
-- recorded as `empty`, a published absence, not a failure and not zero.
CREATE TABLE IF NOT EXISTS control.census_govfin_slice (
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    dataset_id TEXT NOT NULL CHECK (dataset_id IN ('cog_units', 'asslgf', 'qtax')),
    period INTEGER NOT NULL CHECK (period BETWEEN 1950 AND 2100),
    slice_key TEXT NOT NULL CHECK (slice_key ~ '^[a-z_]+$'),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    captured_row_count INTEGER NOT NULL DEFAULT 0 CHECK (captured_row_count >= 0),
    quarantined_row_count INTEGER NOT NULL DEFAULT 0 CHECK (quarantined_row_count >= 0),
    status TEXT NOT NULL CHECK (status IN (
        'captured', 'empty', 'quarantined', 'silver_ready', 'published'
    )),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, slice_key),
    CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS census_govfin_slice_period_idx
    ON control.census_govfin_slice (dataset_id, period, slice_key, created_at DESC);

CREATE INDEX IF NOT EXISTS census_govfin_slice_capture_idx
    ON control.census_govfin_slice (capture_id);

CREATE TABLE IF NOT EXISTS silver_census_govfin.dim_measure (
    dataset_id TEXT NOT NULL CHECK (dataset_id IN ('cog_units', 'asslgf', 'qtax')),
    measure_id TEXT NOT NULL CHECK (measure_id ~ '^[a-z_]+$'),
    measure_label TEXT NOT NULL,
    definition TEXT NOT NULL,
    unit TEXT NOT NULL,
    cadence TEXT NOT NULL CHECK (cadence IN ('annual', 'quarterly')),
    methodology_url TEXT NOT NULL,
    parser_contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset_id, measure_id)
);

-- The item codes each measure sums, as the replay found them in a unit file.
CREATE TABLE IF NOT EXISTS silver_census_govfin.measure_item (
    measure_id TEXT NOT NULL CHECK (measure_id ~ '^[a-z_]+$'),
    item_code TEXT NOT NULL CHECK (item_code ~ '^[0-9A-Z]{3}$'),
    PRIMARY KEY (measure_id, item_code)
);

CREATE INDEX IF NOT EXISTS census_govfin_measure_item_item_idx
    ON silver_census_govfin.measure_item (item_code);

-- Every government of a Census of Governments directory (PID) file, with
-- the FIPS state and county Census places it in. A government spanning
-- counties has one home county. Codes are 12 digits from 2017 (FIPS state,
-- type, FIPS county, unit) and 14 before (Census alphabetical state).
CREATE TABLE IF NOT EXISTS silver_census_govfin.unit_government (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    census_year INTEGER NOT NULL,
    government_code TEXT NOT NULL CHECK (government_code ~ '^([0-9]{12}|[0-9]{14})$'),
    type_code TEXT NOT NULL CHECK (type_code IN ('0', '1', '2', '3', '4', '5')),
    government_type TEXT NOT NULL,
    government_name TEXT NOT NULL,
    county_name TEXT,
    fips_state TEXT NOT NULL CHECK (fips_state ~ '^[0-9]{2}$'),
    fips_county TEXT CHECK (fips_county ~ '^[0-9]{3}$'),
    population BIGINT CHECK (population >= 0),
    fiscal_year_ending TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index)
);

CREATE INDEX IF NOT EXISTS census_govfin_unit_government_run_idx
    ON silver_census_govfin.unit_government (run_id, government_code);

-- Every (government, item, amount) record of a unit file, in thousands of
-- dollars, with the imputation flag Census gives it (R reported, I imputed,
-- A analyst correction, S alternative source). Census writes no record for
-- an item a government did not report, so an absent item is a zero in a
-- roll-up; an amount Census wrote that is not a number is kept `missing`,
-- never zero. `fips_county` is read from the code where the edition's codes
-- carry it.
CREATE TABLE IF NOT EXISTS silver_census_govfin.unit_amount (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    census_year INTEGER NOT NULL,
    government_code TEXT NOT NULL CHECK (government_code ~ '^([0-9]{12}|[0-9]{14})$'),
    type_code TEXT NOT NULL CHECK (type_code IN ('0', '1', '2', '3', '4', '5')),
    fips_state TEXT NOT NULL CHECK (fips_state ~ '^[0-9]{2}$'),
    fips_county TEXT CHECK (fips_county ~ '^[0-9]{3}$'),
    item_code TEXT NOT NULL CHECK (item_code ~ '^[0-9A-Z]{3}$'),
    amount_source TEXT NOT NULL,
    amount NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    data_year INTEGER,
    imputation_flag TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index),
    CONSTRAINT unit_amount_valid_value_present
        CHECK (value_status <> 'valid' OR amount IS NOT NULL),
    CONSTRAINT unit_amount_missing_value_absent
        CHECK (value_status <> 'missing' OR amount IS NULL)
);

CREATE INDEX IF NOT EXISTS census_govfin_unit_amount_run_idx
    ON silver_census_govfin.unit_amount (run_id, government_code);

-- Census's own estimates by state (`00` the nation) and level of government
-- for each item, from the edition's `statetypepu` file: what the county-area
-- roll-up is checked against (DQ-GOVFIN-003).
CREATE TABLE IF NOT EXISTS silver_census_govfin.state_type_total (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    census_year INTEGER NOT NULL,
    fips_state TEXT NOT NULL CHECK (fips_state ~ '^[0-9]{2}$'),
    level_code TEXT NOT NULL CHECK (level_code IN ('1', '2', '3')),
    item_code TEXT NOT NULL CHECK (item_code ~ '^[0-9A-Z]{3}$'),
    amount NUMERIC,
    coefficient_of_variation NUMERIC,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index)
);

CREATE INDEX IF NOT EXISTS census_govfin_state_type_total_run_idx
    ON silver_census_govfin.state_type_total (run_id, fips_state, level_code);

-- Every parsed row of a Census Data API slice that names a registered
-- measure, with the provider's own record beside it.
CREATE TABLE IF NOT EXISTS silver_census_govfin.api_revision (
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    dataset_id TEXT NOT NULL CHECK (dataset_id IN ('asslgf', 'qtax')),
    measure_id TEXT NOT NULL,
    government_level TEXT NOT NULL
        CHECK (government_level IN ('state_and_local', 'state', 'local')),
    fiscal_year INTEGER NOT NULL,
    quarter SMALLINT NOT NULL CHECK (quarter BETWEEN 0 AND 4),
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state')),
    geo_source_code TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{64}$'),
    source_record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index),
    FOREIGN KEY (dataset_id, measure_id)
        REFERENCES silver_census_govfin.dim_measure(dataset_id, measure_id),
    CONSTRAINT api_revision_valid_value_present
        CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT api_revision_missing_value_absent
        CHECK (value_status <> 'missing' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS census_govfin_api_revision_run_idx
    ON silver_census_govfin.api_revision (run_id);

CREATE TABLE IF NOT EXISTS silver_census_govfin.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    member TEXT NOT NULL DEFAULT '',
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, member, source_row_index, error_code)
);

-- The conformed observations: Census's published API figures, and the unit
-- files rolled up (`derivation`): local governments to their home county
-- area (`county_area_rollup`) and to the state (`state_local_rollup`), and
-- the state government's own records (`state_government_units`). Keyed by
-- what they describe and the capture they came from, so a revised
-- publication is kept beside the one it revised. `quarter` is 0 for a
-- fiscal year.
CREATE TABLE IF NOT EXISTS silver_census_govfin.fact_observation (
    dataset_id TEXT NOT NULL,
    measure_id TEXT NOT NULL,
    government_level TEXT NOT NULL
        CHECK (government_level IN ('state_and_local', 'state', 'local')),
    fiscal_year INTEGER NOT NULL,
    quarter SMALLINT NOT NULL CHECK (quarter BETWEEN 0 AND 4),
    geo_id TEXT NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    retrieved_at TIMESTAMPTZ NOT NULL,
    geo_sk BIGINT,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('nation', 'state', 'county')),
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    value_source TEXT,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing')),
    government_count INTEGER CHECK (government_count >= 0),
    derivation TEXT CHECK (derivation IN (
        'county_area_rollup', 'state_local_rollup', 'state_government_units'
    )),
    -- A roll-up's unit file: `reported_and_imputed` (2012 onward) or
    -- `reported_only` (2002, whose file dropped imputed records).
    record_coverage TEXT CHECK (record_coverage IN ('reported_and_imputed', 'reported_only')),
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset_id, measure_id, government_level, fiscal_year, quarter, geo_id, capture_id),
    FOREIGN KEY (dataset_id, measure_id)
        REFERENCES silver_census_govfin.dim_measure(dataset_id, measure_id),
    CONSTRAINT govfin_fact_valid_value_present
        CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT govfin_fact_missing_value_absent
        CHECK (value_status <> 'missing' OR value IS NULL),
    CONSTRAINT govfin_fact_rollup_is_unit_file
        CHECK (derivation IS NULL OR dataset_id = 'cog_units'),
    CONSTRAINT govfin_fact_rollup_states_coverage
        CHECK ((derivation IS NULL) = (record_coverage IS NULL))
);

CREATE INDEX IF NOT EXISTS census_govfin_fact_run_idx
    ON silver_census_govfin.fact_observation (run_id);

-- Served reads pin one measure and one place (prevention list).
CREATE INDEX IF NOT EXISTS census_govfin_fact_metric_geo_idx
    ON silver_census_govfin.fact_observation (measure_id, geo_id, fiscal_year);
