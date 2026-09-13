-- Seed for the frontend live-stack smoke tier.
--
-- This seed used to keep two spellings of one ACS metric apart on purpose: the
-- glossary published 'CENSUS_ACS:<dataset>:<variable>' while the serving
-- relations stored 'ACS:<dataset>:<variable>', so a catalog code sent to the
-- legacy /census/observations/latest pair matched nothing and answered an
-- empty page that looked exactly like a geography with no published values.
--
-- ARC-005 removed the disagreement at its source rather than bridging it: the
-- ACS refresh now composes the served metric_code from the same source_code
-- gold_census.metric_publisher publishes, so both surfaces spell one identity.
-- This seed spells it once, as the warehouse now does. What the tier still
-- proves is the client's own reading of the deployment — that it selects an
-- access shape the API declares and reaches real rows through it — which no
-- fixture can establish.

-- The catalog identity, as the glossary publishes it: CENSUS_ACS-prefixed,
-- with a physical_lineage whose schema/relation agree with the reviewed
-- dispatch and whose `key` is the publisher's own lineage key.
INSERT INTO gold_glossary.dim_metric_catalog (
    metric_code, metric_display_name, source_code, source_object_type,
    source_object_key, valid_geo_grains, valid_time_grains, units,
    measure_kind, aggregation_characteristic, physical_lineage
) VALUES (
    'CENSUS_ACS:acs5:B01003_001_SMOKE', 'Total population (smoke fixture)',
    'CENSUS_ACS', 'ACS_VARIABLE', 'acs5:B01003_001_SMOKE',
    ARRAY['COUNTY'], ARRAY['ANNUAL'], 'people', 'estimate', 'non-additive',
    '{"schema":"gold_census","relation":"fact_acs_observation",'
    '"key":"acs5:B01003_001_SMOKE"}'::JSONB
) ON CONFLICT (metric_code) DO UPDATE SET
    physical_lineage = EXCLUDED.physical_lineage;

-- The serving row, keyed on the same code the catalog publishes above, as
-- gold_census.refresh_rpt_acs_observations now composes it.
--
-- The geography is the county the Martin seed publishes a polygon for
-- (state:55|county:025), so a discovered tile layer and a served observation
-- join on a real shared geo_id rather than on two fixtures agreeing.
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
    -- row's `ingested_at`, which is what `updated_at` publishes
    -- (DB-039). `NOW()` here made the fixture encode a state the
    -- refresh can no longer produce -- a release date unrelated to the
    -- row's ingestion -- and made the seed non-reproducible besides.
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
