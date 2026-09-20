-- Deterministic PostGIS/Martin/API seed owned by the integration suite.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'martin_test') THEN
        CREATE ROLE martin_test LOGIN PASSWORD 'martin-test-readonly';
    END IF;
END
$$;

ALTER ROLE martin_test SET default_transaction_read_only = on;
REVOKE CREATE ON SCHEMA public FROM martin_test;
REVOKE ALL ON ALL TABLES IN SCHEMA gold, gold_glossary FROM martin_test;
GRANT CONNECT ON DATABASE population_etl_test TO martin_test;
GRANT USAGE ON SCHEMA gold, gold_glossary TO martin_test;
-- Grant access to view and underlying table
GRANT SELECT ON gold.dim_geo_latest TO martin_test;
-- The relation Martin actually publishes (DB-053). `dim_geo_latest` stays
-- granted because the catalog tests read it; this is what the tile layer
-- selects from, and the stack's healthcheck gates on it for that reason.
GRANT SELECT ON gold.tile_boundary TO martin_test;
GRANT SELECT ON gold_glossary.dim_geo_latest TO martin_test;

INSERT INTO gold_glossary.dim_geo_latest (
    geo_id, geo_level, state_fips, county_fips, state_name, county_name,
    latitude, longitude, geo_geom
) VALUES (
    'state:55|county:025', 'COUNTY', '55', '025', 'Wisconsin', 'Dane County',
    43.0667, -89.4000,
    ST_Multi(ST_GeomFromText(
        'POLYGON((-89.55 42.98,-89.25 42.98,-89.25 43.16,-89.55 43.16,-89.55 42.98))',
        4326
    ))
) ON CONFLICT (geo_id) DO UPDATE SET
    geo_level = EXCLUDED.geo_level,
    state_fips = EXCLUDED.state_fips,
    county_fips = EXCLUDED.county_fips,
    state_name = EXCLUDED.state_name,
    county_name = EXCLUDED.county_name,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    geo_geom = EXCLUDED.geo_geom;

INSERT INTO gold_glossary.dim_metric_catalog (
    metric_code, metric_display_name, source_code, source_object_type,
    source_object_key, valid_geo_grains, valid_time_grains, units,
    measure_kind, aggregation_characteristic, physical_lineage
) VALUES (
    'CENSUS_ACS:acs5:B01003_001_MARTIN_TEST', 'Martin county population fixture',
    'CENSUS_ACS', 'ACS_VARIABLE', 'acs5:B01003_001_MARTIN_TEST',
    ARRAY['COUNTY'], ARRAY['ANNUAL'], 'people', 'estimate', 'non-additive',
    -- The lineage the reviewed dispatch declares for CENSUS_ACS
    -- (apps.api.registry.OBSERVATION_DISPATCH). It named
    -- silver_census.fact_demographics, which the neutral resource rejects as a
    -- publication/registry disagreement, so this metric was reachable only
    -- through the legacy route. `key` remains the publisher's lineage key;
    -- since ARC-005 the serving relations carry the catalog's own composed
    -- code, so both surfaces below spell one identity.
    '{"schema":"gold_census","relation":"fact_acs_observation",'
    '"key":"acs5:B01003_001_MARTIN_TEST"}'::JSONB
) ON CONFLICT (metric_code) DO UPDATE SET
    physical_lineage = EXCLUDED.physical_lineage;

INSERT INTO gold_census.rpt_acs_observations (
    source_code, observation_date, duration_start, duration_end, time_sk,
    as_of_date, updated_at, geo_id, geo_level, state_fips, county_fips,
    state_name, county_name, geo_latitude, geo_longitude, value,
    dataset_code, vintage_year, table_id, variable_code, estimate_value,
    value_type, units, metric_code, metric_display_name
) VALUES (
    'CENSUS_ACS', '2099-01-01', '2095-01-01', '2099-12-31', 20990101,
    -- One fact, not two: see the note in frontend_smoke_seed.sql (DB-039).
    '2099-12-31', '2099-12-31 00:00:00+00', 'state:55|county:025', 'COUNTY', '55', '025',
    'Wisconsin', 'Dane County', 43.0667, -89.4000, 600000,
    'acs5', 2099, 'B01003', 'B01003_001', 600000,
    'ESTIMATE', 'people', 'CENSUS_ACS:acs5:B01003_001_MARTIN_TEST',
    'Martin county population fixture'
) ON CONFLICT DO NOTHING;

INSERT INTO gold_census.mv_acs_latest
SELECT * FROM gold_census.rpt_acs_observations
WHERE metric_code = 'CENSUS_ACS:acs5:B01003_001_MARTIN_TEST'
ON CONFLICT DO NOTHING;
