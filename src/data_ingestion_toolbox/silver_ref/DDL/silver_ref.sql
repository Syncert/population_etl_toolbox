-- Canonical shared time and versioned Census geography reference model.
CREATE SCHEMA IF NOT EXISTS silver_ref;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS silver_ref.dim_geo_type (
    geo_type TEXT PRIMARY KEY,
    display_label TEXT NOT NULL,
    canonical_code_length INTEGER,
    is_census_geography BOOLEAN NOT NULL,
    product_rank INTEGER NOT NULL,
    CHECK (canonical_code_length IS NULL OR canonical_code_length > 0),
    CHECK (product_rank >= 0)
);
INSERT INTO silver_ref.dim_geo_type VALUES
    ('nation', 'Nation', 1, TRUE, 10),
    ('state', 'State', 2, TRUE, 20),
    ('county', 'County', 5, TRUE, 30),
    ('place', 'City/place', 7, TRUE, 30),
    ('agency', 'Provider agency', NULL, FALSE, 40)
ON CONFLICT (geo_type) DO UPDATE SET
    display_label = EXCLUDED.display_label,
    canonical_code_length = EXCLUDED.canonical_code_length,
    is_census_geography = EXCLUDED.is_census_geography,
    product_rank = EXCLUDED.product_rank;

CREATE TABLE IF NOT EXISTS silver_ref.dim_geo_entity (
    geo_sk BIGSERIAL PRIMARY KEY,
    geo_id TEXT NOT NULL UNIQUE,
    geo_type TEXT NOT NULL REFERENCES silver_ref.dim_geo_type(geo_type),
    census_geoid TEXT,
    state_fips TEXT,
    county_fips TEXT,
    place_fips TEXT,
    provider_agency_code TEXT,
    first_seen_version INTEGER NOT NULL,
    last_seen_version INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (last_seen_version >= first_seen_version),
    CHECK (state_fips IS NULL OR state_fips ~ '^[0-9]{2}$'),
    CHECK (county_fips IS NULL OR county_fips ~ '^[0-9]{3}$'),
    CHECK (place_fips IS NULL OR place_fips ~ '^[0-9]{5}$'),
    CHECK (
        (geo_type = 'nation' AND geo_id = 'us:1' AND state_fips IS NULL)
        OR (geo_type = 'state' AND geo_id = 'state:' || state_fips)
        OR (geo_type = 'county' AND geo_id = 'state:' || state_fips || '|county:' || county_fips)
        OR (geo_type = 'place' AND geo_id = 'state:' || state_fips || '|place:' || place_fips)
        OR (geo_type = 'agency' AND provider_agency_code IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS silver_ref.dim_geo_entity_version (
    geo_version_sk BIGSERIAL PRIMARY KEY,
    geo_sk BIGINT NOT NULL REFERENCES silver_ref.dim_geo_entity(geo_sk),
    geography_vintage INTEGER NOT NULL,
    source_snapshot_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    geoidfq TEXT,
    name TEXT NOT NULL,
    usps TEXT,
    lsad TEXT,
    functional_status TEXT,
    legal_statistical_class TEXT,
    land_area_m2 NUMERIC,
    water_area_m2 NUMERIC,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    attribute_checksum TEXT NOT NULL CHECK (attribute_checksum ~ '^[0-9a-f]{64}$'),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (geo_sk, geography_vintage, attribute_checksum),
    CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
    CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180),
    CHECK (land_area_m2 IS NULL OR land_area_m2 >= 0),
    CHECK (water_area_m2 IS NULL OR water_area_m2 >= 0)
);
CREATE INDEX IF NOT EXISTS dim_geo_entity_version_current_idx
    ON silver_ref.dim_geo_entity_version (geo_sk, geography_vintage DESC, ingested_at DESC);

CREATE TABLE IF NOT EXISTS silver_ref.dim_geo_geometry_version (
    geo_geometry_sk BIGSERIAL PRIMARY KEY,
    geo_sk BIGINT NOT NULL REFERENCES silver_ref.dim_geo_entity(geo_sk),
    boundary_vintage INTEGER NOT NULL,
    geometry_source TEXT NOT NULL,
    resolution TEXT NOT NULL,
    source_snapshot_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    geom geometry(MultiPolygon, 4326) NOT NULL,
    geometry_checksum TEXT NOT NULL CHECK (geometry_checksum ~ '^[0-9a-f]{64}$'),
    is_valid BOOLEAN NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (geo_sk, boundary_vintage, geometry_source, resolution, geometry_checksum),
    CHECK (ST_SRID(geom) = 4326),
    CHECK (NOT ST_IsEmpty(geom))
);
CREATE INDEX IF NOT EXISTS dim_geo_geometry_version_geom_idx
    ON silver_ref.dim_geo_geometry_version USING GIST (geom);

CREATE TABLE IF NOT EXISTS silver_ref.bridge_geo_relationship_version (
    relationship_sk BIGSERIAL PRIMARY KEY,
    parent_geo_sk BIGINT NOT NULL REFERENCES silver_ref.dim_geo_entity(geo_sk),
    related_geo_sk BIGINT NOT NULL REFERENCES silver_ref.dim_geo_entity(geo_sk),
    relationship_type TEXT NOT NULL CHECK (
        relationship_type IN ('contains', 'intersects', 'serves', 'provider_crosswalk')
    ),
    geography_vintage INTEGER NOT NULL,
    overlap_area_m2 NUMERIC,
    overlap_weight NUMERIC,
    evidence_source TEXT NOT NULL,
    source_snapshot_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (parent_geo_sk, related_geo_sk, relationship_type, geography_vintage),
    CHECK (parent_geo_sk <> related_geo_sk),
    CHECK (overlap_area_m2 IS NULL OR overlap_area_m2 >= 0),
    CHECK (overlap_weight IS NULL OR overlap_weight BETWEEN 0 AND 1)
);

CREATE TABLE IF NOT EXISTS silver_ref.geography_resolution (
    resolution_sk BIGSERIAL PRIMARY KEY,
    provider_source TEXT NOT NULL,
    provider_dataset TEXT NOT NULL,
    source_geo_type TEXT NOT NULL,
    source_code TEXT NOT NULL,
    source_label TEXT,
    source_vintage INTEGER NOT NULL,
    geo_sk BIGINT REFERENCES silver_ref.dim_geo_entity(geo_sk),
    resolution_method TEXT CHECK (
        resolution_method IS NULL OR resolution_method IN ('exact_code', 'provider_crosswalk', 'effective_dated_bridge')
    ),
    evidence_capture_id UUID REFERENCES raw_capture.response_capture(capture_id),
    status TEXT NOT NULL CHECK (status IN ('resolved', 'ambiguous', 'unmapped', 'unsupported')),
    reason_code TEXT,
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
);

CREATE OR REPLACE VIEW silver_ref.dim_geo_current AS
WITH attribute_choice AS (
    SELECT DISTINCT ON (v.geo_sk) v.*
    FROM silver_ref.dim_geo_entity_version AS v
    ORDER BY v.geo_sk, v.geography_vintage DESC, v.ingested_at DESC, v.geo_version_sk DESC
), geometry_choice AS (
    SELECT DISTINCT ON (v.geo_sk) v.*
    FROM silver_ref.dim_geo_geometry_version AS v
    WHERE v.is_valid
    ORDER BY v.geo_sk, v.boundary_vintage DESC, v.ingested_at DESC, v.geo_geometry_sk DESC
)
SELECT entity.geo_sk, entity.geo_type,
    CASE WHEN entity.geo_type = 'nation' THEN 'us' ELSE entity.geo_type END AS geo_level,
    entity.geo_id, entity.census_geoid, entity.state_fips, entity.county_fips,
    entity.place_fips, attribute.name,
    CASE WHEN entity.geo_type = 'state' THEN attribute.name ELSE state_attribute.name END AS state_name,
    CASE WHEN entity.geo_type = 'county' THEN attribute.name END AS county_name,
    CASE WHEN entity.geo_type = 'place' THEN attribute.name END AS place_name,
    attribute.latitude, attribute.longitude, geometry.geom, attribute.is_active,
    'census_geography_reference'::TEXT AS source,
    attribute.geography_vintage AS source_year,
    entity.first_seen_version AS first_seen_year,
    entity.last_seen_version AS last_seen_year,
    attribute.source_snapshot_id, geometry.boundary_vintage, attribute.ingested_at
FROM silver_ref.dim_geo_entity AS entity
JOIN attribute_choice AS attribute USING (geo_sk)
LEFT JOIN silver_ref.dim_geo_entity AS state_entity
  ON state_entity.geo_type = 'state' AND state_entity.state_fips = entity.state_fips
LEFT JOIN attribute_choice AS state_attribute ON state_attribute.geo_sk = state_entity.geo_sk
LEFT JOIN geometry_choice AS geometry ON geometry.geo_sk = entity.geo_sk;

-- Read compatibility only; this projection owns no independent state.
CREATE OR REPLACE VIEW silver_ref.dim_geo AS SELECT * FROM silver_ref.dim_geo_current;

CREATE TABLE IF NOT EXISTS silver_ref.dim_time (
    time_sk SERIAL PRIMARY KEY,
    date_key DATE NOT NULL UNIQUE,
    year INT NOT NULL, quarter INT NOT NULL, month INT NOT NULL, day INT NOT NULL,
    day_of_week INT NOT NULL, day_name TEXT NOT NULL, month_name TEXT NOT NULL,
    week_of_year INT NOT NULL, is_weekend BOOLEAN NOT NULL,
    is_month_start BOOLEAN NOT NULL, is_month_end BOOLEAN NOT NULL,
    is_quarter_start BOOLEAN NOT NULL, is_quarter_end BOOLEAN NOT NULL,
    is_year_start BOOLEAN NOT NULL, is_year_end BOOLEAN NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL
);
