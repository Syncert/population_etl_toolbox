-- PEH-001/PEH-002: Census PEP historical series.
--
-- The PEP registry was frozen on the 2020s decade: every release began at the
-- 2020 estimates base and ended at its own vintage, every product shipped as a
-- plain CSV, and every observation date was a July estimate. The Bureau
-- publishes a separate series per decade, so this migration widens those
-- contracts to what it actually publishes and registers the closed decades.
--
-- Nothing here changes a value already served. It relaxes constraints that
-- excluded earlier decades, adds the columns that distinguish one decade's
-- publication from another's, and inserts the additional products.

-- ---------------------------------------------------------------------------
-- 1. Dataset descriptor: which decade, which series, which principal grain
-- ---------------------------------------------------------------------------

ALTER TABLE silver_pep.pep_dataset
    ADD COLUMN IF NOT EXISTS series_kind TEXT NOT NULL DEFAULT 'postcensal',
    ADD COLUMN IF NOT EXISTS era TEXT NOT NULL DEFAULT '',
    -- The finest summary level the file publishes in its own right. A state
    -- row in the county file is a rollup of that file's principal rows; the
    -- same state row in the state file is the publisher's own. Release
    -- precedence reads this to prefer the publisher's own grain.
    ADD COLUMN IF NOT EXISTS native_grain TEXT NOT NULL DEFAULT '',
    -- Recorded when a product's totals are summed from characteristic cells
    -- rather than published as totals, so a derived value is never presented
    -- as one the Bureau printed.
    ADD COLUMN IF NOT EXISTS derivation TEXT,
    ADD COLUMN IF NOT EXISTS archive_member TEXT;

DO $$
DECLARE
    doomed TEXT;
BEGIN
    -- The transport check named only 'bulk_csv'; one archival product ships
    -- as a zip. Dropped by definition rather than by generated name.
    FOR doomed IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'silver_pep.pep_dataset'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%transport%'
    LOOP
        EXECUTE format(
            'ALTER TABLE silver_pep.pep_dataset DROP CONSTRAINT %I', doomed
        );
    END LOOP;
END $$;

ALTER TABLE silver_pep.pep_dataset
    DROP CONSTRAINT IF EXISTS pep_dataset_transport_check,
    ADD CONSTRAINT pep_dataset_transport_check
        CHECK (transport IN ('bulk_csv', 'bulk_zip'));

ALTER TABLE silver_pep.pep_dataset
    DROP CONSTRAINT IF EXISTS pep_dataset_series_kind_check,
    ADD CONSTRAINT pep_dataset_series_kind_check
        CHECK (series_kind IN ('postcensal', 'intercensal'));

-- ---------------------------------------------------------------------------
-- 2. Release contract: an observation range that need not end at its vintage
-- ---------------------------------------------------------------------------

ALTER TABLE silver_pep.pep_release
    ADD COLUMN IF NOT EXISTS series_kind TEXT NOT NULL DEFAULT 'postcensal',
    ADD COLUMN IF NOT EXISTS archive_member TEXT;

DO $$
DECLARE
    doomed TEXT;
BEGIN
    -- Two checks assumed the 2020s shape: the observation range always ended
    -- at the vintage, and every product was a CSV.
    FOR doomed IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'silver_pep.pep_release'::regclass
          AND contype = 'c'
          AND (
              pg_get_constraintdef(oid)
                  LIKE '%observation_end_year = vintage_year%'
              OR pg_get_constraintdef(oid) LIKE '%media_type%'
          )
    LOOP
        EXECUTE format(
            'ALTER TABLE silver_pep.pep_release DROP CONSTRAINT %I', doomed
        );
    END LOOP;
END $$;

ALTER TABLE silver_pep.pep_release
    DROP CONSTRAINT IF EXISTS pep_release_series_kind_check,
    ADD CONSTRAINT pep_release_series_kind_check
        CHECK (series_kind IN ('postcensal', 'intercensal'));

-- A postcensal release is published during the decade it estimates, so its
-- last observation year is its vintage. An intercensal release closes a
-- decade against the following census and is published later, so it ends
-- before its vintage -- but never after it.
ALTER TABLE silver_pep.pep_release
    DROP CONSTRAINT IF EXISTS pep_release_observation_range_check,
    ADD CONSTRAINT pep_release_observation_range_check
        CHECK (
            observation_end_year <= vintage_year
            AND (
                series_kind <> 'postcensal'
                OR observation_end_year = vintage_year
            )
        );

ALTER TABLE silver_pep.pep_release
    DROP CONSTRAINT IF EXISTS pep_release_media_type_check,
    ADD CONSTRAINT pep_release_media_type_check
        CHECK (media_type IN ('text/csv', 'text/plain', 'application/zip'));

-- ---------------------------------------------------------------------------
-- 3. Silver revisions and facts: dates that are not all July estimates
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    doomed TEXT;
BEGIN
    -- The revision table floored the observation year at 2020.
    FOR doomed IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'silver_pep.observation_revision'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%observation_year >= 2020%'
    LOOP
        EXECUTE format(
            'ALTER TABLE silver_pep.observation_revision DROP CONSTRAINT %I',
            doomed
        );
    END LOOP;

    -- The fact table required every observation to fall on 1 July, which the
    -- decennial count column does not.
    FOR doomed IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'silver_pep.fact_population_estimate'::regclass
          AND contype = 'c'
          AND (
              pg_get_constraintdef(oid) LIKE '%make_date%'
              OR pg_get_constraintdef(oid) LIKE '%MAKE_DATE%'
          )
    LOOP
        EXECUTE format(
            'ALTER TABLE silver_pep.fact_population_estimate DROP CONSTRAINT %I',
            doomed
        );
    END LOOP;

    -- The non-negative guard named the two measures that existed then.
    FOR doomed IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'silver_pep.fact_population_estimate'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%ESTIMATESBASE%'
    LOOP
        EXECUTE format(
            'ALTER TABLE silver_pep.fact_population_estimate DROP CONSTRAINT %I',
            doomed
        );
    END LOOP;
END $$;

-- 1900 is the first year the Bureau publishes county population tables for,
-- so it is the domain floor rather than a decade this pipeline happens to
-- register. The exact range a given file covers is enforced per release by
-- the parser, which rejects a metric column outside it.
ALTER TABLE silver_pep.observation_revision
    DROP CONSTRAINT IF EXISTS observation_revision_observation_year_check,
    ADD CONSTRAINT observation_revision_observation_year_check
        CHECK (observation_year BETWEEN 1900 AND release_vintage);

-- A decennial count is an April enumeration; every estimate is dated 1 July.
ALTER TABLE silver_pep.fact_population_estimate
    DROP CONSTRAINT IF EXISTS fact_population_estimate_date_check,
    ADD CONSTRAINT fact_population_estimate_date_check
        CHECK (
            estimate_date = MAKE_DATE(
                observation_year,
                CASE WHEN metric_code = 'CENSUSPOP' THEN 4 ELSE 7 END,
                1
            )
        );

ALTER TABLE silver_pep.fact_population_estimate
    DROP CONSTRAINT IF EXISTS fact_population_estimate_stock_sign_check,
    ADD CONSTRAINT fact_population_estimate_stock_sign_check
        CHECK (
            metric_code NOT IN ('ESTIMATESBASE', 'POPESTIMATE', 'CENSUSPOP')
            OR value >= 0
        );

-- ---------------------------------------------------------------------------
-- 4. The closed decades
-- ---------------------------------------------------------------------------

INSERT INTO silver_pep.pep_dataset (
    dataset_code,
    title,
    transport,
    geography_levels,
    summary_levels,
    variable_families,
    parser_version,
    text_encoding,
    release_page_url,
    decennial_base,
    is_active,
    series_kind,
    era,
    native_grain
)
VALUES
    (
        'pep_county_alldata_2010s',
        'State and County Population Estimates and Components of Change, 2010-2020 (Vintage 2020)',
        'bulk_csv',
        ARRAY['state', 'county'],
        ARRAY['040', '050'],
        ARRAY[
            'CENSUSPOP', 'ESTIMATESBASE', 'POPESTIMATE', 'NPOPCHG', 'BIRTHS',
            'DEATHS', 'NATURALINC', 'INTERNATIONALMIG', 'DOMESTICMIG',
            'NETMIG', 'RESIDUAL', 'RBIRTH', 'RDEATH', 'RNATURALINC',
            'RINTERNATIONALMIG', 'RDOMESTICMIG', 'RNETMIG'
        ],
        'census-pep-bulk-csv-v1',
        'cp1252',
        'https://www.census.gov/programs-surveys/popest/data/tables.html',
        2010,
        TRUE,
        'postcensal',
        '2010s',
        '050'
    ),
    (
        'pep_nst_alldata_2010s',
        'National and State Population Estimates and Components of Change, 2010-2020 (Vintage 2020)',
        'bulk_csv',
        ARRAY['national', 'region', 'division', 'state'],
        ARRAY['010', '020', '030', '040'],
        ARRAY[
            'CENSUSPOP', 'ESTIMATESBASE', 'POPESTIMATE', 'NPOPCHG', 'BIRTHS',
            'DEATHS', 'NATURALINC', 'INTERNATIONALMIG', 'DOMESTICMIG',
            'NETMIG', 'RESIDUAL', 'RBIRTH', 'RDEATH', 'RNATURALINC',
            'RINTERNATIONALMIG', 'RDOMESTICMIG', 'RNETMIG'
        ],
        'census-pep-bulk-csv-v1',
        'utf-8-sig',
        'https://www.census.gov/programs-surveys/popest/data/tables.html',
        2010,
        TRUE,
        'postcensal',
        '2010s',
        '040'
    ),
    (
        'pep_county_alldata_2000s',
        'State and County Population Estimates and Components of Change, 2000-2009 (Vintage 2009)',
        'bulk_csv',
        ARRAY['state', 'county'],
        ARRAY['040', '050'],
        ARRAY[
            'CENSUSPOP', 'ESTIMATESBASE', 'POPESTIMATE', 'NPOPCHG', 'BIRTHS',
            'DEATHS', 'NATURALINC', 'INTERNATIONALMIG', 'DOMESTICMIG',
            'NETMIG', 'RESIDUAL', 'RBIRTH', 'RDEATH', 'RNATURALINC',
            'RINTERNATIONALMIG', 'RDOMESTICMIG', 'RNETMIG'
        ],
        'census-pep-bulk-csv-v1',
        'cp1252',
        'https://www.census.gov/programs-surveys/popest/data/tables.html',
        2000,
        TRUE,
        'postcensal',
        '2000s',
        '050'
    )
ON CONFLICT (dataset_code) DO UPDATE SET
    title = EXCLUDED.title,
    transport = EXCLUDED.transport,
    geography_levels = EXCLUDED.geography_levels,
    summary_levels = EXCLUDED.summary_levels,
    variable_families = EXCLUDED.variable_families,
    parser_version = EXCLUDED.parser_version,
    text_encoding = EXCLUDED.text_encoding,
    release_page_url = EXCLUDED.release_page_url,
    decennial_base = EXCLUDED.decennial_base,
    is_active = EXCLUDED.is_active,
    series_kind = EXCLUDED.series_kind,
    era = EXCLUDED.era,
    native_grain = EXCLUDED.native_grain,
    updated_at = NOW();

-- The decennial count column exists in every "all data" file and was never
-- read. Publishing it as its own measure is additive: no existing measure
-- changes, and the April count stops being a candidate for the July slot.
UPDATE silver_pep.pep_dataset
SET variable_families = ARRAY(
        SELECT DISTINCT unnest(variable_families || ARRAY['CENSUSPOP'])
    ),
    updated_at = NOW()
WHERE dataset_code IN ('pep_nst_alldata', 'pep_county_alldata')
  AND NOT ('CENSUSPOP' = ANY (variable_families));

UPDATE silver_pep.pep_dataset
SET series_kind = 'postcensal',
    era = '2020s',
    native_grain = CASE dataset_code
        WHEN 'pep_nst_alldata' THEN '040'
        WHEN 'pep_county_alldata' THEN '050'
        WHEN 'pep_subcounty' THEN '162'
    END,
    updated_at = NOW()
WHERE dataset_code IN ('pep_nst_alldata', 'pep_county_alldata', 'pep_subcounty');

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
    media_type,
    series_kind
)
VALUES
    (
        'pep_county_alldata_2010s', 2020, 'CO-EST2020-ALLDATA',
        'https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv',
        'https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2010-2020/co-est2020-alldata.pdf',
        DATE '2021-05-04', 2010, 2020, DATE '2020-01-01',
        'co-est2020-alldata', 'published', 'text/csv', 'postcensal'
    ),
    (
        'pep_nst_alldata_2010s', 2020, 'NST-EST2020-ALLDATA',
        'https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/state/totals/nst-est2020-alldata.csv',
        'https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2010-2020/nst-est2020-alldata.pdf',
        DATE '2021-05-04', 2010, 2020, DATE '2020-01-01',
        'nst-est2020-alldata', 'published', 'text/csv', 'postcensal'
    ),
    (
        'pep_county_alldata_2000s', 2009, 'CO-EST2009-ALLDATA',
        'https://www2.census.gov/programs-surveys/popest/datasets/2000-2009/counties/totals/co-est2009-alldata.csv',
        'https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2000-2009/co-est2009-alldata.pdf',
        DATE '2016-07-19', 2000, 2009, DATE '2009-01-01',
        'co-est2009-alldata', 'published', 'text/csv', 'postcensal'
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
    series_kind = EXCLUDED.series_kind,
    updated_at = NOW();
