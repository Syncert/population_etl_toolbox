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
