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
    valid_geo_grains, valid_time_grains, dashboard_suitability,
    do_not_compare_with, recommended_aggregation, owner_team, is_active
) VALUES (
    'ACS:acs5:B01003_001_MARTIN_TEST', 'Martin county population fixture',
    'CENSUS_ACS', 'ACS_VARIABLE', ARRAY['COUNTY'], ARRAY['ANNUAL'],
    'PUBLIC_SAFE', ARRAY[]::TEXT[], 'LAST', 'test', TRUE
) ON CONFLICT (metric_code) DO NOTHING;

INSERT INTO gold_census.rpt_acs_observations (
    source_code, observation_date, duration_start, duration_end, time_sk,
    as_of_date, updated_at, geo_id, geo_level, state_fips, county_fips,
    state_name, county_name, geo_latitude, geo_longitude, value,
    dataset_code, vintage_year, table_id, variable_code, estimate_value,
    value_type, units, metric_code, metric_display_name, dashboard_suitability
) VALUES (
    'CENSUS_ACS', '2099-01-01', '2095-01-01', '2099-12-31', 20990101,
    '2099-12-31', NOW(), 'state:55|county:025', 'COUNTY', '55', '025',
    'Wisconsin', 'Dane County', 43.0667, -89.4000, 600000,
    'acs5', 2099, 'B01003', 'B01003_001', 600000,
    'ESTIMATE', 'people', 'ACS:acs5:B01003_001_MARTIN_TEST',
    'Martin county population fixture', 'PUBLIC_SAFE'
) ON CONFLICT DO NOTHING;

INSERT INTO gold_census.mv_acs_latest
SELECT * FROM gold_census.rpt_acs_observations
WHERE metric_code = 'ACS:acs5:B01003_001_MARTIN_TEST'
ON CONFLICT DO NOTHING;
