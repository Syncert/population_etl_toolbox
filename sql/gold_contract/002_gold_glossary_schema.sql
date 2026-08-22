-- sql/gold_contract/002_gold_glossary_schema.sql
-- gold_glossary schema: shared multi-source metadata tables and catalog views.
--
-- This schema is the single source of truth for cross-source metadata.
-- Source-specific schemas (gold_bls, gold_census, gold_fred) reference these objects.

CREATE SCHEMA IF NOT EXISTS gold_glossary;

-- ─────────────────────────────────────────────────────────────────────────────
-- Source system registry (shared across all sources)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_glossary.dim_source_system (
    source_system_sk BIGSERIAL PRIMARY KEY,
    source_code      TEXT NOT NULL UNIQUE,
    source_name      TEXT NOT NULL,
    source_type      TEXT NOT NULL CHECK (source_type IN ('PRIMARY', 'REPUBLISHER', 'CURATED')),
    reference_url    TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO gold_glossary.dim_source_system (source_code, source_name, source_type, reference_url)
VALUES
    ('CENSUS_ACS', 'US Census ACS',                 'PRIMARY',     'https://www.census.gov/programs-surveys/acs'),
    ('BLS',        'Bureau of Labor Statistics',    'PRIMARY',     'https://www.bls.gov/'),
    ('FRED',       'Federal Reserve Economic Data', 'REPUBLISHER', 'https://fred.stlouisfed.org/')
ON CONFLICT (source_code) DO UPDATE
SET source_name   = EXCLUDED.source_name,
    source_type   = EXCLUDED.source_type,
    reference_url = EXCLUDED.reference_url,
    updated_at    = NOW();

-- ─────────────────────────────────────────────────────────────────────────────
-- Metric catalog (shared across all sources)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_glossary.dim_metric_catalog (
    metric_catalog_sk      BIGSERIAL PRIMARY KEY,
    metric_code            TEXT NOT NULL UNIQUE,
    metric_display_name    TEXT NOT NULL,
    source_code            TEXT NOT NULL REFERENCES gold_glossary.dim_source_system(source_code),
    source_object_type     TEXT NOT NULL,
    source_object_key      TEXT,
    valid_geo_grains       TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    valid_time_grains      TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    units                  TEXT,
    measure_kind           TEXT,
    aggregation_characteristic TEXT,
    physical_lineage       JSONB NOT NULL DEFAULT '{}'::JSONB,
    publisher_contract_version TEXT,
    source_watermark       TEXT,
    source_run_id          UUID,
    publication_time       TIMESTAMPTZ,
    harvested_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    freshness_state        TEXT NOT NULL DEFAULT 'current'
        CHECK (freshness_state IN ('current', 'stale', 'retired')),
    missing_harvest_count  INTEGER NOT NULL DEFAULT 0
        CHECK (missing_harvest_count >= 0),
    UNIQUE (source_code, source_object_type, source_object_key)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Bridge tables (metric ↔ source-specific series/variables)
-- ─────────────────────────────────────────────────────────────────────────────
-- ─────────────────────────────────────────────────────────────────────────────
-- Geography dimension (shared across all sources)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_glossary.dim_geo_latest (
    geo_id       TEXT PRIMARY KEY,
    geo_level    TEXT,
    state_fips   TEXT,
    county_fips  TEXT,
    place_fips   TEXT,
    state_name   TEXT,
    county_name  TEXT,
    place_name   TEXT,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    geo_geom     geometry(MultiPolygon, 4326),
    boundary_vintage INTEGER,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE gold_glossary.dim_geo_latest
    ADD COLUMN IF NOT EXISTS place_fips TEXT,
    ADD COLUMN IF NOT EXISTS place_name TEXT,
    ADD COLUMN IF NOT EXISTS boundary_vintage INTEGER;

CREATE INDEX IF NOT EXISTS ix_gold_glossary_dim_geo_latest_geom
    ON gold_glossary.dim_geo_latest USING GIST (geo_geom);

DROP PROCEDURE IF EXISTS gold_glossary.refresh_dim_geo_latest();
CREATE OR REPLACE PROCEDURE gold_glossary.refresh_dim_geo_latest()
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('gold_glossary.refresh_dim_geo_latest'));

    INSERT INTO gold_glossary.dim_geo_latest (
        geo_id,
        geo_level,
        state_fips,
        county_fips,
        place_fips,
        state_name,
        county_name,
        place_name,
        latitude,
        longitude,
        geo_geom,
        boundary_vintage,
        refreshed_at
    )
    SELECT DISTINCT ON (g.geo_id)
        g.geo_id,
        CASE
            WHEN g.geo_level = 'us'     THEN 'NATIONAL'
            WHEN g.geo_level = 'state'  THEN 'STATE'
            WHEN g.geo_level = 'county' THEN 'COUNTY'
            ELSE UPPER(g.geo_level)
        END,
        CASE WHEN g.state_fips  IS NOT NULL THEN LPAD(g.state_fips::TEXT,  2, '0') ELSE NULL END,
        CASE WHEN g.county_fips IS NOT NULL THEN LPAD(g.county_fips::TEXT, 3, '0') ELSE NULL END,
        CASE WHEN g.place_fips IS NOT NULL THEN LPAD(g.place_fips::TEXT, 5, '0') ELSE NULL END,
        g.state_name,
        g.county_name,
        g.place_name,
        g.latitude,
        g.longitude,
        g.geom,
        g.boundary_vintage,
        NOW()
    FROM silver_ref.dim_geo g
    WHERE g.is_active = TRUE
    ORDER BY g.geo_id, g.source_year DESC NULLS LAST, g.ingested_at DESC
    ON CONFLICT (geo_id) DO UPDATE
    SET geo_level = EXCLUDED.geo_level,
        state_fips = EXCLUDED.state_fips,
        county_fips = EXCLUDED.county_fips,
        place_fips = EXCLUDED.place_fips,
        state_name = EXCLUDED.state_name,
        county_name = EXCLUDED.county_name,
        place_name = EXCLUDED.place_name,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        geo_geom = EXCLUDED.geo_geom,
        boundary_vintage = EXCLUDED.boundary_vintage,
        refreshed_at = NOW()
    WHERE (
        gold_glossary.dim_geo_latest.geo_level,
        gold_glossary.dim_geo_latest.state_fips,
        gold_glossary.dim_geo_latest.county_fips,
        gold_glossary.dim_geo_latest.place_fips,
        gold_glossary.dim_geo_latest.state_name,
        gold_glossary.dim_geo_latest.county_name,
        gold_glossary.dim_geo_latest.place_name,
        gold_glossary.dim_geo_latest.latitude,
        gold_glossary.dim_geo_latest.longitude,
        gold_glossary.dim_geo_latest.geo_geom,
        gold_glossary.dim_geo_latest.boundary_vintage
    ) IS DISTINCT FROM (
        EXCLUDED.geo_level,
        EXCLUDED.state_fips,
        EXCLUDED.county_fips,
        EXCLUDED.place_fips,
        EXCLUDED.state_name,
        EXCLUDED.county_name,
        EXCLUDED.place_name,
        EXCLUDED.latitude,
        EXCLUDED.longitude,
        EXCLUDED.geo_geom,
        EXCLUDED.boundary_vintage
    );

    DELETE FROM gold_glossary.dim_geo_latest d
    WHERE NOT EXISTS (
        SELECT 1
        FROM silver_ref.dim_geo g
        WHERE g.is_active = TRUE
          AND g.geo_id = d.geo_id
    );
END;
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- Schema migration state (shared, tracks DDL hash per component)
-- ─────────────────────────────────────────────────────────────────────────────
-- ─────────────────────────────────────────────────────────────────────────────
-- Contract views exposed to the API
-- ─────────────────────────────────────────────────────────────────────────────

-- Metric catalog view
CREATE OR REPLACE VIEW gold_glossary.dim_metric AS
SELECT
    metric_code,
    metric_display_name,
    source_code,
    source_object_type,
    source_object_key,
    units,
    measure_kind,
    valid_geo_grains,
    valid_time_grains,
    aggregation_characteristic,
    physical_lineage,
    publisher_contract_version,
    source_watermark,
    source_run_id,
    publication_time,
    harvested_at,
    freshness_state,
    freshness_state = 'current' AS is_active
FROM gold_glossary.dim_metric_catalog;

-- Geography catalog view
CREATE OR REPLACE VIEW gold_glossary.dim_geography AS
SELECT
    geo_id,
    geo_level,
    state_fips,
    county_fips,
    place_fips,
    state_name,
    county_name,
    place_name,
    latitude,
    longitude,
    latitude AS geo_latitude,
    longitude AS geo_longitude,
    boundary_vintage,
    refreshed_at,
    COALESCE(place_name, county_name, state_name, geo_id) AS geo_name
FROM gold_glossary.dim_geo_latest;
