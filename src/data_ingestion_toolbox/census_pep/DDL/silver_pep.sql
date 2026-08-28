-- PEP-002: capture-scoped replay of registered Census PEP bulk CSV releases.

CREATE SCHEMA IF NOT EXISTS silver_pep;

CREATE TABLE IF NOT EXISTS silver_pep.observation_revision (
    capture_id                  UUID NOT NULL
        REFERENCES raw_capture.response_capture(capture_id),
    source_row_index            INTEGER NOT NULL
        CHECK (source_row_index >= 0),
    source_column_index         INTEGER NOT NULL
        CHECK (source_column_index >= 0),
    source_header               TEXT NOT NULL,
    dataset_code                TEXT NOT NULL,
    release_vintage             SMALLINT NOT NULL,
    product_code                TEXT NOT NULL,
    observation_year            SMALLINT NOT NULL,
    metric_code                 TEXT NOT NULL,
    unit                        TEXT NOT NULL,
    summary_level               TEXT NOT NULL,
    region_code_source          TEXT,
    division_code_source        TEXT,
    state_fips_source           TEXT,
    county_fips_source          TEXT,
    place_fips_source           TEXT,
    county_subdivision_source   TEXT,
    consolidated_city_source    TEXT,
    functional_status_source    TEXT,
    name_source                 TEXT,
    state_name_source           TEXT,
    value_source                TEXT,
    value                       NUMERIC,
    value_status                TEXT NOT NULL,
    parser_version              TEXT NOT NULL DEFAULT 'census-pep-bulk-csv-v1',
    parsed_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index, source_column_index),
    FOREIGN KEY (dataset_code, release_vintage, product_code)
        REFERENCES silver_pep.pep_release (
            dataset_code, vintage_year, product_code
        ),
    CHECK (observation_year BETWEEN 2020 AND release_vintage),
    CHECK (unit IN ('persons', 'per_1000_population')),
    CHECK (value_status IN ('valid', 'blank', 'sentinel', 'invalid')),
    CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CHECK (value_status = 'valid' OR value IS NULL)
);

CREATE INDEX IF NOT EXISTS pep_observation_revision_release_idx
    ON silver_pep.observation_revision (
        dataset_code,
        release_vintage,
        observation_year,
        metric_code
    );

CREATE INDEX IF NOT EXISTS pep_observation_revision_geography_idx
    ON silver_pep.observation_revision (
        summary_level,
        state_fips_source,
        county_fips_source,
        place_fips_source
    );

CREATE TABLE IF NOT EXISTS silver_pep.dim_measure (
    metric_code TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    unit TEXT NOT NULL CHECK (unit IN ('persons', 'per_1000_population')),
    value_type TEXT NOT NULL DEFAULT 'numeric',
    is_component BOOLEAN NOT NULL,
    allows_negative BOOLEAN NOT NULL,
    population_universe TEXT NOT NULL DEFAULT 'resident_population',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver_pep.release_load (
    capture_id UUID PRIMARY KEY REFERENCES raw_capture.response_capture(capture_id),
    dataset_code TEXT NOT NULL,
    release_vintage SMALLINT NOT NULL,
    product_code TEXT NOT NULL,
    source_record_count INTEGER NOT NULL CHECK (source_record_count > 0),
    observation_count INTEGER NOT NULL CHECK (observation_count > 0),
    completeness_status TEXT NOT NULL CHECK (completeness_status IN ('complete', 'incomplete')),
    completeness_reason TEXT,
    validated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (dataset_code, release_vintage, product_code)
        REFERENCES silver_pep.pep_release(dataset_code, vintage_year, product_code),
    CHECK (
        (completeness_status = 'complete' AND completeness_reason IS NULL)
        OR (completeness_status = 'incomplete' AND completeness_reason IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS silver_pep.fact_population_estimate (
    capture_id UUID NOT NULL,
    source_row_index INTEGER NOT NULL,
    source_column_index INTEGER NOT NULL,
    dataset_code TEXT NOT NULL,
    release_vintage SMALLINT NOT NULL,
    product_code TEXT NOT NULL,
    metric_code TEXT NOT NULL REFERENCES silver_pep.dim_measure(metric_code),
    observation_year SMALLINT NOT NULL,
    estimate_date DATE NOT NULL,
    geo_id TEXT,
    geo_sk BIGINT REFERENCES silver_ref.dim_geo_entity(geo_sk),
    geo_type TEXT NOT NULL,
    geography_basis_date DATE NOT NULL,
    resolution_status TEXT NOT NULL CHECK (resolution_status IN ('resolved', 'unmapped', 'unsupported')),
    summary_level TEXT NOT NULL,
    source_geo_code TEXT NOT NULL,
    source_name TEXT,
    functional_status_source TEXT,
    value_source TEXT NOT NULL,
    value NUMERIC NOT NULL,
    unit TEXT NOT NULL CHECK (unit IN ('persons', 'per_1000_population')),
    transformed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (capture_id, source_row_index, source_column_index),
    FOREIGN KEY (capture_id, source_row_index, source_column_index)
        REFERENCES silver_pep.observation_revision(capture_id, source_row_index, source_column_index),
    CHECK (observation_year <= release_vintage),
    CHECK (estimate_date = MAKE_DATE(observation_year, 7, 1)),
    CHECK (
        (resolution_status = 'resolved' AND geo_id IS NOT NULL AND geo_sk IS NOT NULL)
        OR (resolution_status <> 'resolved' AND geo_sk IS NULL)
    ),
    CHECK (metric_code NOT IN ('ESTIMATESBASE', 'POPESTIMATE') OR value >= 0)
);

CREATE INDEX IF NOT EXISTS pep_fact_natural_key_idx
    ON silver_pep.fact_population_estimate (
        dataset_code, release_vintage, metric_code, geo_id, observation_year
    );
