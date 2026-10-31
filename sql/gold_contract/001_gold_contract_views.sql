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

-- Census tracts, as their own tile layer (sub-county-geography). Tracts are
-- about 85,000 small polygons, so they are not in `tile_boundary`: the state
-- and county layer stays the size DB-053 measured it at, and the client asks
-- for tracts only when a page draws inside one county. `county_name` is the
-- tract's county; `area_name` is the tract's own name.
CREATE OR REPLACE VIEW gold.tile_tract AS
SELECT geo_id, geo_level, state_fips, county_fips, county_name, area_name,
       latitude, longitude, geo_geom, boundary_vintage
FROM gold_glossary.dim_geo_latest
WHERE geo_level = gold_glossary.geo_grain('tract')
  AND geography_state = 'current'
  AND geo_geom IS NOT NULL;

-- Cities and towns (Census places) and metros, as tile layers versioned by
-- boundary vintage (tiles-places-and-metros). Martin publishes each as a
-- function source, `places` and `metros`, because a table source cannot take
-- a parameter and these layers must: `?vintage=2012` draws the boundaries the
-- reference loaded for 2012, and without it the layer draws the newest
-- vintage it holds. A vintage the layer does not hold, or one that is not a
-- year, draws nothing. It never falls back to another vintage, because a
-- figure drawn on another vintage's boundary is a figure drawn on the wrong
-- shape. `gold.tile_layer_vintage` says which vintages exist.
--
-- Features carry identity and the vintage, never a value: a map colours them
-- by joining published values on `geo_id`, so a geography with no published
-- value is uncoloured rather than drawn as zero.

-- What a layer holds per grain and vintage, and which vintage a request that
-- names none is served.
CREATE OR REPLACE VIEW gold.tile_layer_vintage AS
SELECT boundary.geo_level,
       boundary.boundary_vintage,
       COUNT(*)::INTEGER AS feature_count,
       boundary.boundary_vintage = MAX(boundary.boundary_vintage)
           OVER (PARTITION BY boundary.geo_level) AS is_default
FROM gold_glossary.tile_boundary_version AS boundary
GROUP BY boundary.geo_level, boundary.boundary_vintage;

-- A boundary simplified to one tile pixel at the zoom it is drawn at. A
-- 4096-unit tile cannot show a vertex finer than that, so dropping it changes
-- no drawn pixel; it is what keeps a dense low-zoom tile (thousands of
-- Northeast places at z6 measured 514 KB against a 512 KiB budget) inside the
-- budget as vintages add vertices. `preserveCollapsed` keeps a place smaller
-- than a pixel as a dot rather than dropping it: a place left out of a tile
-- would read as missing, not as uncoloured.
CREATE OR REPLACE FUNCTION gold.tile_simplified(tile_geom GEOMETRY, z INTEGER)
RETURNS GEOMETRY
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT ST_Simplify(
        tile_geom,
        40075016.685578488 / (POWER(2, z) * 4096.0),
        TRUE
    )
$$;

-- The vintage a tile request is served: the one it names when that is a year,
-- the newest the grain holds when it names none, and NULL (nothing drawn)
-- otherwise.
CREATE OR REPLACE FUNCTION gold.tile_requested_vintage(
    p_geo_level TEXT,
    query_params JSON
)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
PARALLEL SAFE
AS $$
DECLARE
    requested TEXT := NULLIF(TRIM(COALESCE(query_params ->> 'vintage', '')), '');
BEGIN
    IF requested IS NULL THEN
        RETURN (
            SELECT MAX(boundary.boundary_vintage)
            FROM gold_glossary.tile_boundary_version AS boundary
            WHERE boundary.geo_level = p_geo_level
        );
    END IF;
    IF requested !~ '^[0-9]{4}$' THEN
        RETURN NULL;
    END IF;
    RETURN requested::INTEGER;
END;
$$;

-- Census places. Drawn from zoom 6, as tracts are: 32,000 polygons at a
-- national zoom are a tile no browser should be sent.
CREATE OR REPLACE FUNCTION gold.tile_places(
    z INTEGER,
    x INTEGER,
    y INTEGER,
    query_params JSON
)
RETURNS BYTEA
LANGUAGE plpgsql
STABLE
PARALLEL SAFE
AS $$
DECLARE
    wanted INTEGER := gold.tile_requested_vintage(
        gold_glossary.geo_grain('place'), query_params
    );
    envelope GEOMETRY := ST_TileEnvelope(z, x, y);
    tile BYTEA;
BEGIN
    SELECT ST_AsMVT(feature, 'places', 4096, 'geom')
    INTO tile
    FROM (
        SELECT ST_AsMVTGeom(
                   gold.tile_simplified(boundary.tile_geom, z), envelope, 4096, 64, TRUE
               ) AS geom,
               boundary.geo_id,
               boundary.geo_level,
               boundary.state_fips,
               boundary.place_fips,
               boundary.state_name,
               boundary.geo_name AS place_name,
               boundary.latitude,
               boundary.longitude,
               boundary.boundary_vintage
        FROM gold_glossary.tile_boundary_version AS boundary
        WHERE boundary.geo_level = gold_glossary.geo_grain('place')
          AND boundary.boundary_vintage = wanted
          AND boundary.tile_geom && envelope
    ) AS feature
    WHERE feature.geom IS NOT NULL;
    RETURN tile;
END;
$$;

COMMENT ON FUNCTION gold.tile_places(INTEGER, INTEGER, INTEGER, JSON) IS
    '{"description":"Census places (cities and towns) by boundary vintage. '
    '?vintage=YYYY selects a vintage; without it, the newest. Features carry '
    'no values: a place with no published value is uncoloured, not zero.",'
    '"vector_layers":[{"id":"places","fields":{"geo_id":"String",'
    '"geo_level":"String","state_fips":"String","place_fips":"String",'
    '"state_name":"String","place_name":"String","latitude":"Number",'
    '"longitude":"Number","boundary_vintage":"Number"}}]}';

-- Metropolitan and micropolitan statistical areas (CBSAs), from the Census
-- cartographic boundary of each OMB delineation. About 925 polygons, so they
-- draw from zoom 0 like counties.
CREATE OR REPLACE FUNCTION gold.tile_metros(
    z INTEGER,
    x INTEGER,
    y INTEGER,
    query_params JSON
)
RETURNS BYTEA
LANGUAGE plpgsql
STABLE
PARALLEL SAFE
AS $$
DECLARE
    wanted INTEGER := gold.tile_requested_vintage(
        gold_glossary.geo_grain('metro'), query_params
    );
    envelope GEOMETRY := ST_TileEnvelope(z, x, y);
    tile BYTEA;
BEGIN
    SELECT ST_AsMVT(feature, 'metros', 4096, 'geom')
    INTO tile
    FROM (
        SELECT ST_AsMVTGeom(
                   gold.tile_simplified(boundary.tile_geom, z), envelope, 4096, 64, TRUE
               ) AS geom,
               boundary.geo_id,
               boundary.geo_level,
               boundary.area_code AS cbsa_code,
               boundary.geo_name AS metro_name,
               boundary.lsad,
               boundary.latitude,
               boundary.longitude,
               boundary.boundary_vintage
        FROM gold_glossary.tile_boundary_version AS boundary
        WHERE boundary.geo_level = gold_glossary.geo_grain('metro')
          AND boundary.boundary_vintage = wanted
          AND boundary.tile_geom && envelope
    ) AS feature
    WHERE feature.geom IS NOT NULL;
    RETURN tile;
END;
$$;

COMMENT ON FUNCTION gold.tile_metros(INTEGER, INTEGER, INTEGER, JSON) IS
    '{"description":"Metropolitan and micropolitan statistical areas by OMB '
    'delineation vintage. ?vintage=YYYY selects a delineation; without it, the '
    'newest. Features carry no values: a metro with no published value is '
    'uncoloured, not zero.",'
    '"vector_layers":[{"id":"metros","fields":{"geo_id":"String",'
    '"geo_level":"String","cbsa_code":"String","metro_name":"String",'
    '"lsad":"String","latitude":"Number","longitude":"Number",'
    '"boundary_vintage":"Number"}}]}';

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
