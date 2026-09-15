-- Seed for the frontend live-stack smoke tier: one measure per served source.
--
-- What the tier does with this seed. `live-stack.smoke.test.js` discovers the
-- deployment's sources from `/catalog/capabilities`, asks each one's catalog
-- for its current metrics, and requires every one of them to answer rows
-- through whichever access shape the explorer's own
-- `buildLatestObservationRequest` selects. Against this seed every source
-- resolves to the neutral `/observations` resource, because every source
-- declares it; a source falling back to its source-scoped `latest` pair is
-- legitimate but is now the exception, and a silent drift back to it is one
-- of the things the tier watches for. What it is checking is the client's
-- reading of a real deployment, which no fixture can establish.
--
-- Why this file grew. It used to publish exactly one ACS measure for one
-- county, so that loop iterated one metric of one source and the tier's own
-- summary line -- "every active catalog metric answers" -- described a check
-- over a seventh of the surface it names. Six of the seven registered sources
-- could have stopped answering entirely with this tier green: the neutral
-- dispatch's three identity strategies, the stratified envelopes, and the
-- grain vocabularies of CDC, FBI UCR and USDA NASS were all unexercised.
--
-- Three rules this file keeps, each of which has a way of going wrong:
--
-- 1. THE CATALOG ROWS ARE NOT WRITTEN HERE. Every one is selected from the
--    source's own `gold_<source>.metric_publisher` view at the bottom of this
--    file, composed exactly as `glossary/harvest.py` composes it
--    (`source_code || ':' || source_object_key`). A hand-written catalog row
--    carries a hand-written `physical_lineage`, and that is the field the
--    neutral resource resolves a metric's serving rows through: a fixture
--    spelling it independently of the publisher keeps testing whatever shape
--    was true the day it was written, which is the class of drift ARC-005 and
--    DB-034 both ended. Seed the data; let the publisher say what it means.
--
-- 2. NO MEASURE MAY DECLARE THE `STATE` GRAIN. `spatialGrains` returns
--    ['STATE', 'COUNTY'] in that order -- the tile layer publishes both
--    attribution fields -- and the tier's tile-join test walks those grains
--    and picks the first source publishing any metric at one. The Martin seed
--    draws a single county polygon (Dane County, `state:55|county:025`) and no
--    state, so a measure advertising STATE would be chosen, decode zero
--    features, and fail a test about geography joins for a reason that is
--    purely about this seed. County-grain measures here all use that county's
--    `geo_id` for the same reason: the join must be on a real shared
--    geography, not on two fixtures agreeing.
--
-- 3. EVERY VALUE IS DETERMINISTIC. Fixed uuids, fixed far-future dates, no
--    `NOW()`. DB-039 records why: a `NOW()` in this file made the seed encode
--    a release date unrelated to the row's ingestion, a state the refresh can
--    no longer produce, and made two runs of the same seed differ.
--
-- The far-future period (2094-2098) keeps these rows sorting after anything a
-- real ingestion could publish into the same relations.

-- ---------------------------------------------------------------------------
-- Shared: the geography identity, and one capture graph per source.
-- ---------------------------------------------------------------------------

-- The Martin seed publishes Dane County into `gold_glossary.dim_geo_latest`
-- but not into the silver identity, and Census PEP's fact table requires a
-- resolved row to carry a real `geo_sk`. Seeded here so the one county this
-- deployment can draw is a geography every layer agrees exists.
INSERT INTO silver_ref.dim_geo_entity (
    geo_id, geo_type, census_geoid, state_fips, county_fips,
    first_seen_version, last_seen_version
) VALUES (
    'state:55|county:025', 'county', '55025', '55', '025', 2098, 2098
) ON CONFLICT (geo_id) DO NOTHING;

-- One run/request/capture chain per source that records provenance on its
-- facts. The uuids are fixed and obviously synthetic: every capture-first
-- relation below carries a foreign key into this graph, and a fixture that
-- invented a run id would be citing an ingestion that never happened.
INSERT INTO control.ingestion_run (run_id, source_code, status) VALUES
    ('00000000-0000-4000-8000-000000000cdc', 'CDC', 'success'),
    ('00000000-0000-4000-8000-000000000fb1', 'FBI_UCR', 'success'),
    ('00000000-0000-4000-8000-000000000a55', 'USDA_NASS', 'success'),
    ('00000000-0000-4000-8000-000000000e70', 'CENSUS_PEP', 'success')
ON CONFLICT (run_id) DO NOTHING;

INSERT INTO raw_capture.payload_blob (payload_checksum, payload, payload_size)
VALUES ('44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a', '\x7b7d'::BYTEA, 2)
ON CONFLICT (payload_checksum) DO NOTHING;

INSERT INTO control.ingestion_request (
    request_id, run_id, source_code, endpoint, request_parameters,
    request_fingerprint, status
) VALUES
    ('00000000-0000-4000-9000-000000000cdc', '00000000-0000-4000-8000-000000000cdc',
     'CDC', 'smoke://seed', '{}'::JSONB, 'a563afccf114b55950d2e24f6b196d55d0e63fb8937625f6f87c4e29e07a1d24', 'captured'),
    ('00000000-0000-4000-9000-000000000fb1', '00000000-0000-4000-8000-000000000fb1',
     'FBI_UCR', 'smoke://seed', '{}'::JSONB, '07735be126dc5d37c8091f510a48748544218be74b194484d721a4869d23654c', 'captured'),
    ('00000000-0000-4000-9000-000000000a55', '00000000-0000-4000-8000-000000000a55',
     'USDA_NASS', 'smoke://seed', '{}'::JSONB, 'd3210f3ccecd2dd1a0473c07b25b34fb0715c0654b7fc12a96768d3017719090', 'captured'),
    ('00000000-0000-4000-9000-000000000e70', '00000000-0000-4000-8000-000000000e70',
     'CENSUS_PEP', 'smoke://seed', '{}'::JSONB, '3f45d5d8b3eb1261ea67453de9821d7207c2c93db3965bb54a9f853a0073015a', 'captured')
ON CONFLICT (request_id) DO NOTHING;

INSERT INTO raw_capture.response_capture (
    capture_id, request_id, run_id, source_code, endpoint, request_parameters,
    request_fingerprint, retrieved_at, http_status, response_headers,
    media_type, payload_checksum
) VALUES
    ('00000000-0000-4000-a000-000000000cdc', '00000000-0000-4000-9000-000000000cdc',
     '00000000-0000-4000-8000-000000000cdc', 'CDC', 'smoke://seed', '{}'::JSONB,
     'a563afccf114b55950d2e24f6b196d55d0e63fb8937625f6f87c4e29e07a1d24', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000fb1', '00000000-0000-4000-9000-000000000fb1',
     '00000000-0000-4000-8000-000000000fb1', 'FBI_UCR', 'smoke://seed', '{}'::JSONB,
     '07735be126dc5d37c8091f510a48748544218be74b194484d721a4869d23654c', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000a55', '00000000-0000-4000-9000-000000000a55',
     '00000000-0000-4000-8000-000000000a55', 'USDA_NASS', 'smoke://seed', '{}'::JSONB,
     'd3210f3ccecd2dd1a0473c07b25b34fb0715c0654b7fc12a96768d3017719090', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
    ('00000000-0000-4000-a000-000000000e70', '00000000-0000-4000-9000-000000000e70',
     '00000000-0000-4000-8000-000000000e70', 'CENSUS_PEP', 'smoke://seed', '{}'::JSONB,
     '3f45d5d8b3eb1261ea67453de9821d7207c2c93db3965bb54a9f853a0073015a', '2098-12-31 00:00:00+00', 200, '{}'::JSONB, 'application/json',
     '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a')
ON CONFLICT (capture_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Census ACS -- the survey family: dataset/vintage identity and a margin of error.
-- ---------------------------------------------------------------------------

INSERT INTO gold_census.dim_acs_table (
    dataset_code, vintage_year, table_id, table_title, survey_span_years
) VALUES (
    'acs5', 2098, 'B01003', 'Total population (smoke fixture)', 5
) ON CONFLICT DO NOTHING;

INSERT INTO gold_census.dim_acs_variable (
    acs_table_sk, dataset_code, vintage_year, variable_code, variable_label,
    value_role
)
SELECT acs_table_sk, 'acs5', 2098, 'B01003_001_SMOKE',
       'Total population (smoke fixture)', 'ESTIMATE'
FROM gold_census.dim_acs_table
WHERE dataset_code = 'acs5' AND vintage_year = 2098 AND table_id = 'B01003'
ON CONFLICT DO NOTHING;

-- The serving row, keyed on the code the publisher composes for the variable
-- above. The geography is the county the Martin seed draws, so a discovered
-- tile layer and a served observation join on a real shared geo_id.
INSERT INTO gold_census.rpt_acs_observations (
    source_code, observation_date, duration_start, duration_end, time_sk,
    as_of_date, updated_at, geo_id, geo_level, state_fips, county_fips,
    state_name, county_name, geo_latitude, geo_longitude, value,
    dataset_code, vintage_year, table_id, variable_code, estimate_value,
    value_type, units, metric_code, metric_display_name
) VALUES (
    'CENSUS_ACS', '2098-01-01', '2094-01-01', '2098-12-31', 20980101,
    -- `as_of_date` and `updated_at` are one fact about this row, not
    -- two: the serving refresh derives the release date from the silver
    -- row's `ingested_at`, which is what `updated_at` publishes (DB-039).
    '2098-12-31', '2098-12-31 00:00:00+00', 'state:55|county:025', 'COUNTY', '55', '025',
    'Wisconsin', 'Dane County', 43.0667, -89.4000, 561504,
    'acs5', 2098, 'B01003', 'B01003_001_SMOKE', 561504,
    'ESTIMATE', 'people', 'CENSUS_ACS:acs5:B01003_001_SMOKE',
    'Total population (smoke fixture)'
) ON CONFLICT DO NOTHING;

INSERT INTO gold_census.mv_acs_latest
SELECT * FROM gold_census.rpt_acs_observations
WHERE metric_code = 'CENSUS_ACS:acs5:B01003_001_SMOKE'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- FRED -- the national macro family: one series, no geography dimension.
-- ---------------------------------------------------------------------------

INSERT INTO gold_fred.dim_fred_series (
    series_id, series_title, units, frequency, seasonal_adjustment,
    reference_url, updated_at
) VALUES (
    'SMOKE_UNRATE', 'Unemployment rate (smoke fixture)', 'Percent', 'Monthly',
    'Seasonally Adjusted', 'https://fred.stlouisfed.org/series/SMOKE_UNRATE',
    '2098-12-31 00:00:00+00'
) ON CONFLICT (series_id) DO NOTHING;

INSERT INTO gold_fred.rpt_fred_observations (
    source_code, observation_date, duration_start, duration_end, time_sk,
    as_of_date, updated_at, geo_id, geo_level, series_id, series_title,
    value, units, frequency, seasonal_adjustment_status, metric_code,
    metric_display_name
) VALUES (
    'FRED', '2098-01-01', '2098-01-01', '2098-01-31', 20980101,
    '2098-12-31', '2098-12-31 00:00:00+00', 'us:1', 'NATIONAL',
    'SMOKE_UNRATE', 'Unemployment rate (smoke fixture)', 4.2, 'Percent',
    'Monthly', 'Seasonally Adjusted', 'FRED:SMOKE_UNRATE',
    'Unemployment rate (smoke fixture)'
) ON CONFLICT DO NOTHING;

INSERT INTO gold_fred.mv_fred_latest
SELECT * FROM gold_fred.rpt_fred_observations
WHERE metric_code = 'FRED:SMOKE_UNRATE'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- BLS -- the labour family: a survey/series identity carrying seasonal adjustment.
--
-- `gold_bls.metric_publisher` publishes a series only where no measure
-- identity claims its program (the LAUS measure-identity migration, ETL-047).
-- `gold_bls.dim_bls_measure` is empty in this deployment, so the series
-- branch is the one that publishes here -- which is also the branch the
-- source-scoped `/bls/observations/latest` pair reads.
-- ---------------------------------------------------------------------------

INSERT INTO gold_bls.dim_bls_survey (
    program_code, survey_name, observation_basis
) VALUES (
    'SM', 'Smoke survey (fixture)', 'JOBS'
) ON CONFLICT DO NOTHING;

INSERT INTO gold_bls.dim_bls_series (
    bls_survey_sk, program_code, series_id, series_title, measure_category,
    value_type, unit_of_measure, seasonal_adjustment_status
)
SELECT bls_survey_sk, 'SM', 'SMOKE_SERIES_0001',
       'Smoke employment level (fixture)', 'EMPLOYMENT', 'LEVEL', 'persons',
       'Seasonally Adjusted'
FROM gold_bls.dim_bls_survey WHERE program_code = 'SM'
ON CONFLICT DO NOTHING;

INSERT INTO gold_bls.rpt_bls_observations (
    source_code, observation_date, duration_start, duration_end, time_sk,
    as_of_date, updated_at, geo_id, geo_level, state_fips, county_fips,
    state_name, county_name, series_id, program_code, series_title, value,
    units, seasonal_adjustment_status, metric_code, metric_display_name
) VALUES (
    'BLS', '2098-01-01', '2098-01-01', '2098-01-31', 20980101,
    '2098-12-31', '2098-12-31 00:00:00+00', 'state:55|county:025', 'COUNTY',
    '55', '025', 'Wisconsin', 'Dane County', 'SMOKE_SERIES_0001', 'SM',
    'Smoke employment level (fixture)', 375000, 'persons',
    'Seasonally Adjusted', 'BLS:SMOKE_SERIES_0001',
    'Smoke employment level (fixture)'
) ON CONFLICT DO NOTHING;

INSERT INTO gold_bls.mv_bls_latest
SELECT * FROM gold_bls.rpt_bls_observations
WHERE metric_code = 'BLS:SMOKE_SERIES_0001'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Census PEP -- the vintage family, and the `lineage_key_column` identity
-- strategy: the serving revision keys rows by the bare measure code while the
-- catalog publishes a composed one. DB-034 is the defect that gets exercised
-- here -- matching only the lineage key made the route refuse the identity it
-- had just published.
-- ---------------------------------------------------------------------------

INSERT INTO silver_pep.dim_measure (
    metric_code, display_name, unit, is_component, allows_negative
) VALUES (
    'SMOKEPOP', 'Resident population estimate (smoke fixture)', 'persons',
    FALSE, FALSE
) ON CONFLICT (metric_code) DO NOTHING;

INSERT INTO silver_pep.pep_dataset (
    dataset_code, title, transport, geography_levels, summary_levels,
    variable_families, parser_version, release_page_url, decennial_base
) VALUES (
    'pep_smoke', 'Population estimates (smoke fixture)', 'bulk_csv',
    ARRAY['county'], ARRAY['050'], ARRAY['POP'], '1',
    'https://www.census.gov/programs-surveys/popest.html', 2090
) ON CONFLICT (dataset_code) DO NOTHING;

INSERT INTO silver_pep.pep_release (
    dataset_code, vintage_year, product_code, data_url, layout_url,
    release_date, observation_start_year, observation_end_year,
    geography_basis_date, schema_version, status
) VALUES (
    'pep_smoke', 2098, 'alldata',
    'https://www2.census.gov/programs-surveys/popest/smoke/alldata.csv',
    'https://www2.census.gov/programs-surveys/popest/smoke/layout.txt',
    '2098-12-31', 2098, 2098, '2098-01-01', '1', 'published'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_pep.release_load (
    capture_id, dataset_code, release_vintage, product_code,
    source_record_count, observation_count, completeness_status
) VALUES (
    '00000000-0000-4000-a000-000000000e70', 'pep_smoke', 2098, 'alldata',
    1, 1, 'complete'
) ON CONFLICT DO NOTHING;

-- The parsed revision the fact row is a resolved projection of: PEP's
-- as-released surface is the revision, and `gold_pep.population_estimate_revision`
-- reads it, so the fact table keys into it rather than standing alone.
INSERT INTO silver_pep.observation_revision (
    capture_id, source_row_index, source_column_index, source_header,
    dataset_code, release_vintage, product_code, observation_year,
    metric_code, unit, summary_level, state_fips_source, county_fips_source,
    name_source, state_name_source, value_source, value, value_status
) VALUES (
    '00000000-0000-4000-a000-000000000e70', 0, 0, 'POPESTIMATE2098',
    'pep_smoke', 2098, 'alldata', 2098, 'SMOKEPOP', 'persons', '050',
    '55', '025', 'Dane County', 'Wisconsin', '561504', 561504, 'valid'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_pep.fact_population_estimate (
    capture_id, source_row_index, source_column_index, dataset_code,
    release_vintage, product_code, metric_code, observation_year,
    estimate_date, geo_id, geo_sk, geo_type, geography_basis_date,
    resolution_status, summary_level, source_geo_code, value_source, value,
    unit
)
SELECT '00000000-0000-4000-a000-000000000e70', 0, 0, 'pep_smoke', 2098,
       'alldata', 'SMOKEPOP', 2098, '2098-07-01', 'state:55|county:025',
       geo_sk, 'county', '2098-01-01', 'resolved', '050', '55025',
       '561504', 561504, 'persons'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- CDC -- the `identity_columns` strategy (asset/measure/value-type), and the
-- first of the stratified envelopes: rows carry a stratum and an adjustment
-- status that an aligned single-value analysis would collapse.
-- ---------------------------------------------------------------------------

INSERT INTO silver_cdc.dim_dataset_release (
    asset_id, release_watermark, socrata_id, title, methodology_url,
    geography_basis, parser_contract_version, estimate_method,
    population_basis, metadata_capture_id, source_run_id, source_record_count,
    quarantine_count, status, reconciled_at, published_at
) VALUES (
    'cdi', '20980101', 'smok-0001', 'Chronic disease indicators (smoke fixture)',
    'https://www.cdc.gov/cdi', 'county', '1', 'model-based', 'adults',
    '00000000-0000-4000-a000-000000000cdc', '00000000-0000-4000-8000-000000000cdc',
    1, 0, 'published', '2098-12-31 00:00:00+00', '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_cdc.dim_measure (
    asset_id, measure_id, value_type_id, measure_label, topic,
    value_type_label, unit, adjustment_status, estimate_method,
    population_basis
) VALUES (
    'cdi', 'SMOKE_CDI_01', 'crude', 'Smoke health indicator (fixture)',
    'Smoke topic', 'Crude prevalence', 'percent', 'crude', 'model-based',
    'adults'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_cdc.dim_stratum (stratum_id, strata) VALUES (
    '23deed5745968020bb282f7742ddab6a9bed872684e14f8983d89b02dc203ad7', '[["OVERALL", "Overall", "OVR", "Overall"]]'::JSONB
) ON CONFLICT (stratum_id) DO NOTHING;

INSERT INTO silver_cdc.fact_health_observation (
    asset_id, release_watermark, source_record_id, source_run_id, capture_id,
    source_row_index, measure_id, value_type_id, stratum_id, period_start,
    period_end, geo_id, geo_sk, geo_type, geography_status, value_source,
    value, value_status, unit, adjustment_status, estimate_method,
    population_basis, transformation_version
)
SELECT 'cdi', '20980101', 'e542fa20911eb14e6a74ce7bc00bc84ee2834abff9d2ccf4ef0a5d99e6aa1069',
       '00000000-0000-4000-8000-000000000cdc',
       '00000000-0000-4000-a000-000000000cdc', 0, 'SMOKE_CDI_01', 'crude',
       '23deed5745968020bb282f7742ddab6a9bed872684e14f8983d89b02dc203ad7', 2098, 2098, 'state:55|county:025',
       geo_sk, 'county', 'resolved', '12.5', 12.5, 'valid', 'percent',
       'crude', 'model-based', 'adults', '1'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- FBI UCR -- the participation family. Its rows carry a reporting basis this
-- API deliberately does not flatten into the source-scoped row shape, so the
-- neutral `/observations` resource is its only observation surface: exactly
-- the access-shape choice this tier exists to check the client makes.
--
-- `subject_type` is the grain expression (`UPPER(subject_type)`), so `county`
-- here publishes the COUNTY grain and joins the drawn boundary.
-- ---------------------------------------------------------------------------

INSERT INTO silver_fbi.dim_ucr_dataset_release (
    product_id, release_key, refresh_date, max_data_month, ucr_program,
    offense_code, offense_label, period_start, period_end, documentation_url,
    methodology_url, parser_contract_version, reported_status,
    counted_entity_note, release_capture_id, source_run_id,
    source_record_count, quarantine_count, status, reconciled_at, published_at
) VALUES (
    'estimates', '2098-12-31', '2098-12-31', '2098-12', 'summary',
    'V', 'Violent crime (smoke fixture)', '2098-01-01', '2098-12-31',
    'https://cde.ucr.cjis.gov/', 'https://cde.ucr.cjis.gov/methodology', '1',
    'reported', 'Counted offences, smoke fixture',
    '00000000-0000-4000-a000-000000000fb1',
    '00000000-0000-4000-8000-000000000fb1', 1, 0, 'published',
    '2098-12-31 00:00:00+00', '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_fbi.dim_offense_measure (
    product_id, measure_id, ucr_program, offense_code, offense_label,
    measure_form, counted_entity_basis, unit, reported_status
) VALUES (
    'estimates', 'SMOKE_V_COUNT', 'summary', 'V',
    'Violent crime (smoke fixture)', 'count', 'offense', 'offenses',
    'reported'
) ON CONFLICT DO NOTHING;

-- The participation basis every crime observation is keyed to. FBI UCR
-- counts what reporting agencies submitted, so a value without its coverage
-- is not a fact this warehouse will store: the fact table's foreign key says
-- so, and the neutral envelope publishes the coverage beside the value.
INSERT INTO silver_fbi.fact_reporting_participation (
    product_id, release_key, ucr_program, subject_type, subject_code,
    subject_label, source_geo_level, period, period_start, period_end,
    geo_id, geo_sk, geography_status, population, participated_population,
    coverage_percent, coverage_basis, participation_status, source_run_id,
    capture_id, source_row_index, transformation_version
)
SELECT 'estimates', '2098-12-31', 'summary', 'county', '55025',
       'Dane County', 'county', '2098', '2098-01-01', '2098-12-31',
       'state:55|county:025', geo_sk, 'provider_geo_exact', 561504, 561504,
       100.0, 'agency_reported_months', 'full_year',
       '00000000-0000-4000-8000-000000000fb1',
       '00000000-0000-4000-a000-000000000fb1', 0, '1'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

INSERT INTO silver_fbi.fact_crime_observation (
    product_id, release_key, source_record_id, measure_id, subject_type,
    subject_code, subject_label, source_geo_level, geo_id, geo_sk, period,
    period_start, period_end, geography_status, value_source, value,
    value_status, source_run_id, capture_id, source_row_index,
    transformation_version
)
SELECT 'estimates', '2098-12-31', 'ccf93d8d198bb4d68f2264e0624fefe83b6511b82169556899eb68b7eb049690', 'SMOKE_V_COUNT',
       'county', '55025', 'Dane County', 'county', 'state:55|county:025',
       geo_sk, '2098', '2098-01-01', '2098-12-31', 'provider_geo_exact',
       '1234', 1234, 'reported', '00000000-0000-4000-8000-000000000fb1',
       '00000000-0000-4000-a000-000000000fb1', 0, '1'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- USDA NASS -- the widest `identity_columns` tuple and its own grain
-- vocabulary (`UPPER(agg_level_desc)`), which published NATION from one column
-- while filtering on another whose word is NATIONAL (DB-028). COUNTY here, so
-- the grain the catalog advertises is one the tier can send straight back.
-- ---------------------------------------------------------------------------

INSERT INTO silver_nass.dim_dataset_release (
    product_id, release_watermark, label, source_desc, slice_mode,
    methodology_url, parser_contract_version, incremental_field,
    release_expectation, registered_years, source_run_id, source_record_count,
    quarantine_count, slice_count, status, reconciled_at, published_at
) VALUES (
    'crops', '2098', 'Crops (smoke fixture)', 'SURVEY', 'full',
    'https://www.nass.usda.gov/', '1', 'year', 'annual',
    '[2098]'::JSONB, '00000000-0000-4000-8000-000000000a55', 1, 0, 1,
    'published', '2098-12-31 00:00:00+00', '2098-12-31 00:00:00+00'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_nass.dim_commodity (
    commodity_sk, sector_desc, group_desc, commodity_desc, class_desc,
    prodn_practice_desc, util_practice_desc
) VALUES (
    'c5084f7c89d9d1128b54fd91f0d8ac2b0aca74ff2cb2c41c56e4fc7609a15cc4', 'CROPS', 'FIELD CROPS', 'CORN', 'ALL CLASSES',
    'ALL PRODUCTION PRACTICES', 'ALL UTILIZATION PRACTICES'
) ON CONFLICT DO NOTHING;

INSERT INTO silver_nass.dim_domain (domain_sk, domain_desc, domaincat_desc)
VALUES ('f2912ead6f4ce0d029af5662376d793d691cdf6906a4161a103e4a4a983cfd26', 'TOTAL', 'NOT SPECIFIED')
ON CONFLICT DO NOTHING;

INSERT INTO silver_nass.dim_statistic (
    statistic_sk, source_desc, statisticcat_desc, short_desc, unit_desc,
    freq_desc, value_kind, calculation_basis, additive_behavior,
    additive_behavior_known
) VALUES (
    'f41a8794e3844f67adf4c587312401ece4d8538b8f791fba5bdfc2b7fbf0ebeb', 'SURVEY', 'YIELD',
    'CORN, GRAIN - YIELD, MEASURED IN BU / ACRE (SMOKE FIXTURE)',
    'BU / ACRE', 'ANNUAL', 'ratio', 'per-acre', 'non_additive', TRUE
) ON CONFLICT DO NOTHING;

INSERT INTO silver_nass.fact_crop_observation (
    product_id, release_watermark, source_record_id, source_run_id,
    capture_id, source_row_index, slice_key, commodity_sk, statistic_sk,
    domain_sk, geo_id, geo_sk, geo_type, geography_status, geo_source_code,
    agg_level_desc, location_desc, year, freq_desc, begin_code, end_code,
    reference_period_desc, value_source, value, value_status, unit_desc,
    cv_source, cv_status, source_desc, transformation_version
)
SELECT 'crops', '2098', 'f65bd325f32b0a4b12726509ce34ce28cdaeb93e0c92d57bba9318c57d1be559',
       '00000000-0000-4000-8000-000000000a55',
       '00000000-0000-4000-a000-000000000a55', 0, '2098', 'c5084f7c89d9d1128b54fd91f0d8ac2b0aca74ff2cb2c41c56e4fc7609a15cc4',
       'f41a8794e3844f67adf4c587312401ece4d8538b8f791fba5bdfc2b7fbf0ebeb', 'f2912ead6f4ce0d029af5662376d793d691cdf6906a4161a103e4a4a983cfd26', 'state:55|county:025', geo_sk, 'county',
       'resolved', '55025', 'COUNTY', 'DANE', 2098, 'ANNUAL', '00', '00',
       'YEAR', '185.4', 185.4, 'valid', 'BU / ACRE', '', 'not_available',
       'SURVEY', '1'
FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:55|county:025'
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Publish the catalog, exactly as the glossary harvest would.
--
-- `glossary/harvest.py` reads each `gold_<source>.metric_publisher` view and
-- writes `source_code || ':' || source_object_key` as the metric code. That
-- rule, and the publisher's own `physical_lineage`, are what the neutral
-- resource resolves a metric's serving rows through -- so they are read from
-- the publishers here rather than restated. A source whose publisher stops
-- yielding a row for the data above publishes no catalog row, and the tier
-- then reports a source with no metrics instead of passing on a metric whose
-- lineage this file invented.
-- ---------------------------------------------------------------------------

CREATE TEMP VIEW smoke_published AS
SELECT * FROM gold_census.metric_publisher
UNION ALL SELECT * FROM gold_fred.metric_publisher
UNION ALL SELECT * FROM gold_bls.metric_publisher
UNION ALL SELECT * FROM gold_pep.metric_publisher
UNION ALL SELECT * FROM gold_cdc.metric_publisher
UNION ALL SELECT * FROM gold_fbi.metric_publisher
UNION ALL SELECT * FROM gold_nass.metric_publisher;

INSERT INTO gold_glossary.dim_source_system (
    source_code, source_name, source_type, reference_url
)
SELECT DISTINCT ON (source_code)
       source_code, source_name, source_type, reference_url
FROM smoke_published
ORDER BY source_code
ON CONFLICT (source_code) DO UPDATE SET
    source_name = EXCLUDED.source_name,
    source_type = EXCLUDED.source_type,
    reference_url = EXCLUDED.reference_url;

INSERT INTO gold_glossary.dim_metric_catalog (
    metric_code, source_code, source_object_type, source_object_key,
    metric_display_name, units, measure_kind, valid_geo_grains,
    valid_time_grains, aggregation_characteristic, physical_lineage,
    publisher_contract_version, source_watermark, source_run_id,
    publication_time
)
SELECT source_code || ':' || source_object_key, source_code,
       source_object_type, source_object_key, metric_display_name, units,
       measure_kind, valid_geo_grains, valid_time_grains,
       aggregation_characteristic, physical_lineage,
       publisher_contract_version, source_watermark, source_run_id,
       publication_time
FROM smoke_published
ON CONFLICT (metric_code) DO UPDATE SET
    source_object_type = EXCLUDED.source_object_type,
    source_object_key = EXCLUDED.source_object_key,
    metric_display_name = EXCLUDED.metric_display_name,
    units = EXCLUDED.units,
    measure_kind = EXCLUDED.measure_kind,
    valid_geo_grains = EXCLUDED.valid_geo_grains,
    valid_time_grains = EXCLUDED.valid_time_grains,
    aggregation_characteristic = EXCLUDED.aggregation_characteristic,
    physical_lineage = EXCLUDED.physical_lineage;

DROP VIEW smoke_published;
