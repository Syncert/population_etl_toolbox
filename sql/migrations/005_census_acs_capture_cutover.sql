-- ARCH-005 beta cutover: complete Census response arrays are parsed in silver.
CREATE SCHEMA IF NOT EXISTS silver_census;

CREATE TABLE IF NOT EXISTS silver_census.observation_revision (
    capture_id          UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index    INTEGER NOT NULL CHECK (source_row_index >= 0),
    source_column_index INTEGER NOT NULL CHECK (source_column_index >= 0),
    source_header       TEXT NOT NULL,
    dataset             TEXT NOT NULL,
    year                INTEGER NOT NULL,
    geo_level           TEXT NOT NULL CHECK (geo_level IN ('us', 'state', 'county')),
    us_source           TEXT,
    state_fips_source   TEXT,
    county_fips_source  TEXT,
    variable_name       TEXT NOT NULL,
    table_id            TEXT NOT NULL,
    measure_type        TEXT,
    value_source        TEXT,
    value               NUMERIC,
    value_status        TEXT NOT NULL
        CHECK (value_status IN ('valid', 'absent', 'blank', 'sentinel', 'invalid')),
    parsed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parser_version      TEXT NOT NULL DEFAULT 'census-acs-array-v1',
    PRIMARY KEY (capture_id, source_row_index, source_column_index),
    CHECK (value_status <> 'valid' OR value IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS census_observation_revision_slice_idx
    ON silver_census.observation_revision (
        dataset, year, geo_level, state_fips_source, county_fips_source
    );
