-- Seed for the frontend live-stack smoke tier.
--
-- Its whole purpose is to reproduce one production fact the browser fixtures
-- could not: a metric's glossary identity and its serving identity are spelled
-- differently for Census ACS.
--
-- apps.api.registry declares CENSUS_ACS with lineage_key_prefix 'ACS:' — the
-- glossary publishes 'CENSUS_ACS:<dataset>:<variable>' while the serving
-- relations spell the same identity 'ACS:<dataset>:<variable>'. The neutral
-- /observations resource bridges the two through the published lineage key.
-- The legacy /census/observations/latest pair does not: it filters the serving
-- relation on the requested string, so a glossary code sent there matches
-- nothing and answers an empty page that looks exactly like a geography with
-- no published values.
--
-- Every other fixture in this repository spells both identities the same way,
-- which is why a client that read observations through the legacy pair stayed
-- green everywhere and returned nothing against a real deployment. This seed
-- keeps the two spellings apart on purpose, so that regression is expressible.

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

-- The serving identity, as the gold relations spell it: ACS-prefixed. This is
-- the row the neutral resource reaches by composing the lineage key, and the
-- row the legacy pair cannot reach from the catalog's own metric code.
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
    '2098-12-31', NOW(), 'state:55|county:025', 'COUNTY', '55', '025',
    'Wisconsin', 'Dane County', 43.0667, -89.4000, 561504,
    'acs5', 2098, 'B01003', 'B01003_001_SMOKE', 561504,
    'ESTIMATE', 'people', 'ACS:acs5:B01003_001_SMOKE',
    'Total population (smoke fixture)'
) ON CONFLICT DO NOTHING;

INSERT INTO gold_census.mv_acs_latest
SELECT * FROM gold_census.rpt_acs_observations
WHERE metric_code = 'ACS:acs5:B01003_001_SMOKE'
ON CONFLICT DO NOTHING;
