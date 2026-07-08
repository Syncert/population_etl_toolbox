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
    source_object_type     TEXT NOT NULL CHECK (source_object_type IN ('ACS_VARIABLE', 'BLS_SERIES', 'FRED_SERIES', 'COMPOSITE_VIEW')),
    business_definition    TEXT,
    caveats                TEXT,
    valid_geo_grains       TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    valid_time_grains      TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    dashboard_suitability  TEXT NOT NULL DEFAULT 'PUBLIC_SAFE'
        CHECK (dashboard_suitability IN ('PUBLIC_SAFE', 'INTERNAL_ONLY', 'EXPERIMENTAL')),
    comparability_group    TEXT,
    do_not_compare_with    TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    recommended_aggregation TEXT,
    owner_team             TEXT,
    is_active              BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Bridge tables (metric ↔ source-specific series/variables)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_glossary.bridge_metric_bls_series (
    metric_catalog_sk BIGINT NOT NULL REFERENCES gold_glossary.dim_metric_catalog(metric_catalog_sk),
    bls_series_sk     BIGINT NOT NULL,
    PRIMARY KEY (metric_catalog_sk, bls_series_sk)
);

CREATE TABLE IF NOT EXISTS gold_glossary.bridge_metric_acs_variable (
    metric_catalog_sk BIGINT NOT NULL REFERENCES gold_glossary.dim_metric_catalog(metric_catalog_sk),
    acs_variable_sk   BIGINT NOT NULL,
    PRIMARY KEY (metric_catalog_sk, acs_variable_sk)
);

CREATE TABLE IF NOT EXISTS gold_glossary.bridge_metric_fred_series (
    metric_catalog_sk BIGINT NOT NULL REFERENCES gold_glossary.dim_metric_catalog(metric_catalog_sk),
    fred_series_sk    BIGINT NOT NULL,
    PRIMARY KEY (metric_catalog_sk, fred_series_sk)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Geography dimension (shared across all sources)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_glossary.dim_geo_latest (
    geo_id       TEXT PRIMARY KEY,
    geo_level    TEXT,
    state_fips   TEXT,
    county_fips  TEXT,
    state_name   TEXT,
    county_name  TEXT,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    geo_geom     geometry(MultiPolygon, 4326),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_gold_glossary_dim_geo_latest_geom
    ON gold_glossary.dim_geo_latest USING GIST (geo_geom);

DROP PROCEDURE IF EXISTS gold_glossary.refresh_dim_geo_latest();
CREATE OR REPLACE PROCEDURE gold_glossary.refresh_dim_geo_latest()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE gold_glossary.dim_geo_latest;

    INSERT INTO gold_glossary.dim_geo_latest (
        geo_id,
        geo_level,
        state_fips,
        county_fips,
        state_name,
        county_name,
        latitude,
        longitude,
        geo_geom,
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
        g.state_name,
        g.county_name,
        g.latitude,
        g.longitude,
        g.geom,
        NOW()
    FROM silver_ref.dim_geo g
    WHERE g.is_active = TRUE
    ORDER BY g.geo_id, g.source_year DESC NULLS LAST, g.ingested_at DESC;
END;
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- Schema migration state (shared, tracks DDL hash per component)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_glossary.schema_migration_state (
    component_name TEXT PRIMARY KEY,
    ddl_hash       TEXT NOT NULL,
    applied_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
    business_definition,
    caveats,
    valid_geo_grains,
    valid_time_grains,
    dashboard_suitability,
    comparability_group,
    do_not_compare_with,
    recommended_aggregation,
    owner_team,
    is_active,
    updated_at
FROM gold_glossary.dim_metric_catalog;

-- Geography catalog view
CREATE OR REPLACE VIEW gold_glossary.dim_geography AS
SELECT
    geo_id,
    geo_level,
    state_fips,
    county_fips,
    state_name,
    county_name,
    latitude,
    longitude,
    refreshed_at
FROM gold_glossary.dim_geo_latest;
