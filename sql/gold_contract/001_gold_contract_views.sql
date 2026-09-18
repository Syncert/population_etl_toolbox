-- API-facing contracts for the source-first gold schemas.
-- Apply after the reference, silver, source gold, and gold glossary DDL.

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS gold_glossary;
CREATE SCHEMA IF NOT EXISTS gold_bls;
CREATE SCHEMA IF NOT EXISTS gold_census;
CREATE SCHEMA IF NOT EXISTS gold_fred;

-- Shared catalog contracts.


-- BLS observation contracts.
-- `gold_glossary.dim_metric` and `gold_glossary.dim_geography` are not
-- defined here. They were, and this file runs last in manifest order, so its
-- copies silently won over the ones `002_gold_glossary_schema.sql` and
-- migration `003` define -- three bodies for one view, and whichever ran last
-- decided what the warehouse held. The bodies were identical, which is the
-- only reason nothing broke and the only reason removing them is safe;
-- `tests/integration/database/test_schema_snapshot.py` is what proves the
-- surviving definition is unchanged, and `test_every_contract_view_has_exactly_one_body`
-- is what stops a second copy coming back.

CREATE OR REPLACE VIEW gold_bls.fact_observation AS
SELECT
    source_code,
    source_code AS source,
    observation_date,
    observation_date::TEXT AS period,
    duration_start,
    duration_end,
    time_sk,
    as_of_date,
    as_of_date AS release_date,
    updated_at,
    geo_id,
    geo_level,
    gold_glossary.geo_name(place_name, county_name, state_name, geo_id) AS geo_name,
    state_fips,
    county_fips,
    state_name,
    county_name,
    geo_latitude,
    geo_longitude,
    metric_code,
    metric_display_name,
    value,
    value_type,
    units,
    units AS unit,
    seasonal_adjustment_status,
    program_code AS dataset_code,
    program_code AS dataset,
    NULL::INTEGER AS vintage_year,
    NULL::TEXT AS vintage,
    NULL::NUMERIC AS margin_of_error,
    NULL::NUMERIC AS margin_of_error_pct
FROM gold_bls.rpt_bls_observations;

CREATE OR REPLACE VIEW gold_bls.v_metric_latest_by_geo AS
SELECT
    source_code,
    source_code AS source,
    observation_date,
    observation_date::TEXT AS period,
    duration_start,
    duration_end,
    time_sk,
    as_of_date,
    as_of_date AS release_date,
    updated_at,
    geo_id,
    geo_level,
    gold_glossary.geo_name(place_name, county_name, state_name, geo_id) AS geo_name,
    state_fips,
    county_fips,
    state_name,
    county_name,
    geo_latitude,
    geo_longitude,
    metric_code,
    metric_display_name,
    value,
    value_type,
    units,
    units AS unit,
    seasonal_adjustment_status,
    program_code AS dataset_code,
    program_code AS dataset,
    NULL::INTEGER AS vintage_year,
    NULL::TEXT AS vintage,
    NULL::NUMERIC AS margin_of_error,
    NULL::NUMERIC AS margin_of_error_pct
FROM gold_bls.mv_bls_latest;

CREATE OR REPLACE VIEW gold_bls.v_metric_timeseries_by_geo AS
SELECT * FROM gold_bls.fact_observation;

-- Census ACS observation contracts.
CREATE OR REPLACE VIEW gold_census.fact_observation AS
SELECT
    source_code,
    source_code AS source,
    observation_date,
    observation_date::TEXT AS period,
    duration_start,
    duration_end,
    time_sk,
    as_of_date,
    as_of_date AS release_date,
    updated_at,
    geo_id,
    geo_level,
    gold_glossary.geo_name(place_name, county_name, state_name, geo_id) AS geo_name,
    state_fips,
    county_fips,
    state_name,
    county_name,
    geo_latitude,
    geo_longitude,
    metric_code,
    metric_display_name,
    value,
    value_type,
    units,
    units AS unit,
    NULL::TEXT AS seasonal_adjustment_status,
    dataset_code,
    dataset_code AS dataset,
    vintage_year,
    vintage_year::TEXT AS vintage,
    margin_of_error,
    margin_of_error_pct
FROM gold_census.rpt_acs_observations;

CREATE OR REPLACE VIEW gold_census.v_metric_latest_by_geo AS
SELECT
    source_code,
    source_code AS source,
    observation_date,
    observation_date::TEXT AS period,
    duration_start,
    duration_end,
    time_sk,
    as_of_date,
    as_of_date AS release_date,
    updated_at,
    geo_id,
    geo_level,
    gold_glossary.geo_name(place_name, county_name, state_name, geo_id) AS geo_name,
    state_fips,
    county_fips,
    state_name,
    county_name,
    geo_latitude,
    geo_longitude,
    metric_code,
    metric_display_name,
    value,
    value_type,
    units,
    units AS unit,
    NULL::TEXT AS seasonal_adjustment_status,
    dataset_code,
    dataset_code AS dataset,
    vintage_year,
    vintage_year::TEXT AS vintage,
    margin_of_error,
    margin_of_error_pct
FROM gold_census.mv_acs_latest;

CREATE OR REPLACE VIEW gold_census.v_metric_timeseries_by_geo AS
SELECT * FROM gold_census.fact_observation;

-- FRED observation contracts.
CREATE OR REPLACE VIEW gold_fred.fact_observation AS
SELECT
    source_code,
    source_code AS source,
    observation_date,
    observation_date::TEXT AS period,
    duration_start,
    duration_end,
    time_sk,
    as_of_date,
    as_of_date AS release_date,
    updated_at,
    geo_id,
    geo_level,
    gold_glossary.geo_name(place_name, county_name, state_name, geo_id) AS geo_name,
    state_fips,
    county_fips,
    state_name,
    county_name,
    geo_latitude,
    geo_longitude,
    metric_code,
    metric_display_name,
    value,
    value_type,
    units,
    units AS unit,
    seasonal_adjustment_status,
    'fred'::TEXT AS dataset_code,
    'fred'::TEXT AS dataset,
    NULL::INTEGER AS vintage_year,
    NULL::TEXT AS vintage,
    NULL::NUMERIC AS margin_of_error,
    NULL::NUMERIC AS margin_of_error_pct
FROM gold_fred.rpt_fred_observations;

CREATE OR REPLACE VIEW gold_fred.v_metric_latest_by_geo AS
SELECT
    source_code,
    source_code AS source,
    observation_date,
    observation_date::TEXT AS period,
    duration_start,
    duration_end,
    time_sk,
    as_of_date,
    as_of_date AS release_date,
    updated_at,
    geo_id,
    geo_level,
    gold_glossary.geo_name(place_name, county_name, state_name, geo_id) AS geo_name,
    state_fips,
    county_fips,
    state_name,
    county_name,
    geo_latitude,
    geo_longitude,
    metric_code,
    metric_display_name,
    value,
    value_type,
    units,
    units AS unit,
    seasonal_adjustment_status,
    'fred'::TEXT AS dataset_code,
    'fred'::TEXT AS dataset,
    NULL::INTEGER AS vintage_year,
    NULL::TEXT AS vintage,
    NULL::NUMERIC AS margin_of_error,
    NULL::NUMERIC AS margin_of_error_pct
FROM gold_fred.mv_fred_latest;

CREATE OR REPLACE VIEW gold_fred.v_metric_timeseries_by_geo AS
SELECT * FROM gold_fred.fact_observation;

-- Backward-compatible cross-source catalog contracts.
CREATE OR REPLACE VIEW gold.dim_source_system AS
SELECT * FROM gold_glossary.dim_source_system;

CREATE OR REPLACE VIEW gold.dim_metric_catalog AS
SELECT * FROM gold_glossary.dim_metric_catalog;

CREATE OR REPLACE VIEW gold.dim_geo_latest AS
SELECT * FROM gold_glossary.dim_geo_latest;

-- What the tile server publishes, which is not the same as what the geography
-- catalog holds (DB-053).
--
-- `dim_geo_latest` carries every level the reference loads. Measured on a
-- loaded warehouse that is 32,629 places against 3,235 counties and 56 states,
-- and the boundary cannot draw a place: `apps/web/lib/tileGrains.ts` declares
-- STATE and COUNTY as the drawable grains and names PLACE as one it refuses
-- before any observation is read. So every place polygon was serialised into
-- the tile, sent, decoded, and hidden by the client's own layer filter -- 90%
-- of a 2 MB tile at zoom 4.
--
-- This filters to the grains the boundary draws, so the payload is what the
-- map uses. It is a view rather than a `WHERE` in `martin.yml` because Martin
-- table sources do not take one, and keeping the filter in SQL puts it beside
-- the relation it filters.
--
-- Retired rows are excluded for the same reason: a boundary that no longer
-- exists is not one a reader can select.
CREATE OR REPLACE VIEW gold.tile_boundary AS
SELECT geo_id, geo_level, state_fips, county_fips, state_name, county_name,
       latitude, longitude, geo_geom, boundary_vintage
FROM gold_glossary.dim_geo_latest
--
-- The two grains are named through `gold_glossary.geo_grain` rather than by
-- their published spellings. The vocabulary has one source (DB-037), and a
-- literal here would be a second copy of its output that a change to the
-- function would not reach.
WHERE geo_level IN (
        gold_glossary.geo_grain('state'),
        gold_glossary.geo_grain('county')
      )
  AND geography_state = 'current'
  AND geo_geom IS NOT NULL;

CREATE OR REPLACE VIEW gold.dim_metric AS
SELECT * FROM gold_glossary.dim_metric;

CREATE OR REPLACE VIEW gold.dim_geography AS
SELECT * FROM gold_glossary.dim_geography;

-- Cross-source observation contracts use durable source reporting tables for
-- history and the independently refreshed latest tables for current values.
CREATE OR REPLACE VIEW gold.v_metric_timeseries_by_geo AS
SELECT * FROM gold_census.v_metric_timeseries_by_geo
UNION ALL
SELECT * FROM gold_bls.v_metric_timeseries_by_geo
UNION ALL
SELECT * FROM gold_fred.v_metric_timeseries_by_geo;

CREATE OR REPLACE VIEW gold.v_metric_latest_by_geo AS
SELECT * FROM gold_census.v_metric_latest_by_geo
UNION ALL
SELECT * FROM gold_bls.v_metric_latest_by_geo
UNION ALL
SELECT * FROM gold_fred.v_metric_latest_by_geo;

CREATE OR REPLACE VIEW gold.fact_observation AS
SELECT * FROM gold.v_metric_timeseries_by_geo;

CREATE OR REPLACE VIEW gold.rpt_observation_dashboard AS
SELECT * FROM gold.v_metric_timeseries_by_geo;

CREATE OR REPLACE VIEW gold.mv_latest_dashboard AS
SELECT * FROM gold.v_metric_latest_by_geo;
