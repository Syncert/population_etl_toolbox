-- PEP-001 census population estimates/prices registry tables.
-- Safe on bootstrap and clean beta reset/re-ingestion.

CREATE SCHEMA IF NOT EXISTS silver_pep;

CREATE TABLE IF NOT EXISTS silver_pep.pep_datasets (
    dataset_id         TEXT NOT NULL,
    dataset_name       TEXT NOT NULL,
    description        TEXT,
    granularity        TEXT NOT NULL,
    is_curated         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset_id),
    CHECK (granularity IN ('national', 'state', 'county', 'place', 'division', 'region', 'msa', 'cd')),
    CHECK (is_curated IN (TRUE, FALSE))
);

CREATE TABLE IF NOT EXISTS silver_pep.pep_vintages (
    vintage_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    dataset_id         TEXT NOT NULL REFERENCES silver_pep.pep_datasets(dataset_id),
    vintage_year       SMALLINT NOT NULL,
    status             TEXT NOT NULL DEFAULT 'published',
    effective_date     DATE,
    notes              TEXT,
    captured_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (vintage_id),
    UNIQUE (dataset_id, vintage_year),
    CHECK (status IN ('published', 'preliminary', 'suppressed'))
);

CREATE TABLE IF NOT EXISTS silver_pep.pep_release_series (
    release_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    dataset_id         TEXT NOT NULL REFERENCES silver_pep.pep_datasets(dataset_id),
    vintage_year       SMALLINT NOT NULL,
    series_version     TEXT NOT NULL,
    series_uri         TEXT NOT NULL,
    file_name          TEXT,
    file_sha256        TEXT,
    file_size_bytes    BIGINT,
    published_at       TIMESTAMPTZ,
    notes              TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (release_id),
    UNIQUE (dataset_id, vintage_year, series_uri),
    CHECK (series_version ~ '^\d+(\.\d+)*$')
);

-- Seed curated dataset definitions (PEP-001.3)
INSERT INTO silver_pep.pep_datasets (dataset_id, dataset_name, description, granularity, is_curated)
VALUES
    ('pep_annual_estimates', 'Annual Population Estimates', 'Annual resident population totals and components by geography', 'national', TRUE),
    ('pep_annual_estimates_state', 'Annual State Population Estimates', 'Annual resident population totals and components by state', 'state', TRUE),
    ('pep_annual_estimates_county', 'Annual County Population Estimates', 'Annual resident population totals and components by county', 'county', TRUE),
    ('pep_annual_estimates_place', 'Annual Place Population Estimates', 'Annual resident population totals by incorporated place', 'place', TRUE),
    ('pep_interim_estimates', 'Interim Population Estimates', 'Interim (monthly/quarterly) population estimates', 'national', TRUE),
    ('pep_aging_estimates', 'Aging Population Estimates', 'Population by age and sex cohorts', 'national', TRUE)
ON CONFLICT (dataset_id) DO NOTHING;
