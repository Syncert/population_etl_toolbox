-- PEP-001: versioned Census Population Estimates Program source registry.
-- This registry describes provider releases; it does not store observations.

CREATE SCHEMA IF NOT EXISTS silver_pep;

CREATE TABLE IF NOT EXISTS silver_pep.pep_dataset (
    dataset_code       TEXT PRIMARY KEY,
    title              TEXT NOT NULL,
    transport          TEXT NOT NULL CHECK (transport IN ('bulk_csv')),
    geography_levels   TEXT[] NOT NULL,
    summary_levels     TEXT[] NOT NULL,
    variable_families  TEXT[] NOT NULL,
    parser_version     TEXT NOT NULL,
    release_page_url   TEXT NOT NULL,
    decennial_base     SMALLINT NOT NULL,
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (cardinality(geography_levels) > 0),
    CHECK (cardinality(summary_levels) > 0),
    CHECK (cardinality(variable_families) > 0),
    CHECK (release_page_url LIKE 'https://www.census.gov/%')
);

CREATE TABLE IF NOT EXISTS silver_pep.pep_release (
    dataset_code           TEXT NOT NULL
        REFERENCES silver_pep.pep_dataset(dataset_code),
    vintage_year           SMALLINT NOT NULL,
    product_code           TEXT NOT NULL,
    data_url               TEXT NOT NULL,
    layout_url             TEXT NOT NULL,
    release_date           DATE NOT NULL,
    observation_start_year SMALLINT NOT NULL,
    observation_end_year   SMALLINT NOT NULL,
    geography_basis_date   DATE NOT NULL,
    schema_version         TEXT NOT NULL,
    status                 TEXT NOT NULL,
    media_type             TEXT NOT NULL DEFAULT 'text/csv',
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset_code, vintage_year),
    UNIQUE (product_code),
    UNIQUE (dataset_code, vintage_year, product_code),
    CHECK (status IN ('published', 'archived')),
    CHECK (observation_start_year <= observation_end_year),
    CHECK (observation_end_year = vintage_year),
    CHECK (data_url LIKE 'https://www2.census.gov/%'),
    CHECK (layout_url LIKE 'https://www2.census.gov/%'),
    CHECK (media_type = 'text/csv')
);

INSERT INTO silver_pep.pep_dataset (
    dataset_code,
    title,
    transport,
    geography_levels,
    summary_levels,
    variable_families,
    parser_version,
    release_page_url,
    decennial_base,
    is_active
)
VALUES
    (
        'pep_nst_alldata',
        'National and State Population Estimates and Components of Change',
        'bulk_csv',
        ARRAY['national', 'region', 'division', 'state'],
        ARRAY['010', '020', '030', '040'],
        ARRAY[
            'ESTIMATESBASE', 'POPESTIMATE', 'NPOPCHG', 'BIRTHS', 'DEATHS',
            'NATURALCHG', 'INTERNATIONALMIG', 'DOMESTICMIG', 'NETMIG',
            'RESIDUAL', 'RBIRTH', 'RDEATH', 'RNATURALCHG',
            'RINTERNATIONALMIG', 'RDOMESTICMIG', 'RNETMIG'
        ],
        'census-pep-bulk-csv-v1',
        'https://www.census.gov/data/tables/time-series/demo/popest/2020s-national-total.html',
        2020,
        TRUE
    ),
    (
        'pep_county_alldata',
        'State and County Population Estimates and Components of Change',
        'bulk_csv',
        ARRAY['state', 'county'],
        ARRAY['040', '050'],
        ARRAY[
            'ESTIMATESBASE', 'POPESTIMATE', 'NPOPCHG', 'BIRTHS', 'DEATHS',
            'NATURALCHG', 'INTERNATIONALMIG', 'DOMESTICMIG', 'NETMIG',
            'RESIDUAL', 'RBIRTH', 'RDEATH', 'RNATURALCHG',
            'RINTERNATIONALMIG', 'RDOMESTICMIG', 'RNETMIG'
        ],
        'census-pep-bulk-csv-v1',
        'https://www.census.gov/data/datasets/time-series/demo/popest/2020s-counties-total.html',
        2020,
        TRUE
    ),
    (
        'pep_subcounty',
        'Subcounty Resident Population Estimates',
        'bulk_csv',
        ARRAY['state', 'county', 'county_subdivision', 'place', 'consolidated_city'],
        ARRAY['040', '050', '061', '071', '157', '162', '170', '172'],
        ARRAY['ESTIMATESBASE', 'POPESTIMATE'],
        'census-pep-bulk-csv-v1',
        'https://www.census.gov/data/tables/time-series/demo/popest/2020s-total-cities-and-towns.html',
        2020,
        TRUE
    )
ON CONFLICT (dataset_code) DO UPDATE SET
    title = EXCLUDED.title,
    transport = EXCLUDED.transport,
    geography_levels = EXCLUDED.geography_levels,
    summary_levels = EXCLUDED.summary_levels,
    variable_families = EXCLUDED.variable_families,
    parser_version = EXCLUDED.parser_version,
    release_page_url = EXCLUDED.release_page_url,
    decennial_base = EXCLUDED.decennial_base,
    is_active = EXCLUDED.is_active,
    updated_at = NOW();

INSERT INTO silver_pep.pep_release (
    dataset_code,
    vintage_year,
    product_code,
    data_url,
    layout_url,
    release_date,
    observation_start_year,
    observation_end_year,
    geography_basis_date,
    schema_version,
    status,
    media_type
)
VALUES
    (
        'pep_nst_alldata', 2024, 'NST-EST2024-ALLDATA',
        'https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/state/totals/NST-EST2024-ALLDATA.csv',
        'https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-2024/NST-EST2024-ALLDATA.pdf',
        DATE '2024-12-19', 2020, 2024, DATE '2024-01-01',
        'nst-est2024-alldata', 'archived', 'text/csv'
    ),
    (
        'pep_nst_alldata', 2025, 'NST-EST2025-ALLDATA',
        'https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/state/totals/NST-EST2025-ALLDATA.csv',
        'https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-2025/NST-EST2025-ALLDATA.pdf',
        DATE '2026-01-27', 2020, 2025, DATE '2025-01-01',
        'nst-est2025-alldata', 'published', 'text/csv'
    ),
    (
        'pep_county_alldata', 2024, 'CO-EST2024-ALLDATA',
        'https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/totals/co-est2024-alldata.csv',
        'https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-2024/CO-EST2024-ALLDATA.pdf',
        DATE '2025-03-13', 2020, 2024, DATE '2024-01-01',
        'co-est2024-alldata', 'archived', 'text/csv'
    ),
    (
        'pep_county_alldata', 2025, 'CO-EST2025-ALLDATA',
        'https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/counties/totals/co-est2025-alldata.csv',
        'https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-2025/CO-EST2025-ALLDATA.pdf',
        DATE '2026-03-26', 2020, 2025, DATE '2025-01-01',
        'co-est2025-alldata', 'published', 'text/csv'
    ),
    (
        'pep_subcounty', 2024, 'SUB-EST2024',
        'https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/cities/totals/sub-est2024.csv',
        'https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-2024/SUB-EST2024.pdf',
        DATE '2025-05-15', 2020, 2024, DATE '2024-01-01',
        'sub-est2024', 'archived', 'text/csv'
    ),
    (
        'pep_subcounty', 2025, 'SUB-EST2025',
        'https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/cities/totals/sub-est2025.csv',
        'https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-2025/SUB-EST2025.pdf',
        DATE '2026-05-14', 2020, 2025, DATE '2025-01-01',
        'sub-est2025', 'published', 'text/csv'
    )
ON CONFLICT (dataset_code, vintage_year) DO UPDATE SET
    product_code = EXCLUDED.product_code,
    data_url = EXCLUDED.data_url,
    layout_url = EXCLUDED.layout_url,
    release_date = EXCLUDED.release_date,
    observation_start_year = EXCLUDED.observation_start_year,
    observation_end_year = EXCLUDED.observation_end_year,
    geography_basis_date = EXCLUDED.geography_basis_date,
    schema_version = EXCLUDED.schema_version,
    status = EXCLUDED.status,
    media_type = EXCLUDED.media_type,
    updated_at = NOW();
