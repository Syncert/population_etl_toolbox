-- silver_ref/DDL/silver_ref.sql

CREATE SCHEMA IF NOT EXISTS silver_ref;

CREATE TABLE IF NOT EXISTS silver_ref.dim_geo (
    geo_sk SERIAL PRIMARY KEY,
    geo_level TEXT NOT NULL,
    geo_id TEXT NOT NULL,
    state_fips TEXT,
    county_fips TEXT,
    name TEXT,
    state_name TEXT,
    county_name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    geo_polygon_geojson TEXT,
    is_active BOOLEAN,
    source TEXT,
    source_year INT,
    first_seen_year INT,
    last_seen_year INT,
    ingested_at TIMESTAMPTZ,
    CONSTRAINT dim_geo_nk UNIQUE (geo_level, geo_id)
);

ALTER TABLE silver_ref.dim_geo
    ADD COLUMN IF NOT EXISTS first_seen_year INT;

ALTER TABLE silver_ref.dim_geo
    ADD COLUMN IF NOT EXISTS last_seen_year INT;

ALTER TABLE silver_ref.dim_geo
    ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION;

ALTER TABLE silver_ref.dim_geo
    ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;

ALTER TABLE silver_ref.dim_geo
    ADD COLUMN IF NOT EXISTS geo_polygon_geojson TEXT;


CREATE TABLE IF NOT EXISTS silver_ref.dim_time (
    time_sk SERIAL PRIMARY KEY,
    date_key DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name TEXT NOT NULL,
    month_name TEXT NOT NULL,
    week_of_year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_month_start BOOLEAN NOT NULL,
    is_month_end BOOLEAN NOT NULL,
    is_quarter_start BOOLEAN NOT NULL,
    is_quarter_end BOOLEAN NOT NULL,
    is_year_start BOOLEAN NOT NULL,
    is_year_end BOOLEAN NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL
);
