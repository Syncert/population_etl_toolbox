-- Control and silver relations for NOAA U.S. Climate Normals 1991-2020
-- (annual/seasonal by-station archive, noaa-normals).

CREATE SCHEMA IF NOT EXISTS silver_noaa_normals;

-- One run per read of the archive. A read whose bytes equal the last
-- published capture is `unchanged` and replays nothing.
CREATE TABLE IF NOT EXISTS control.noaa_normals_file (
    run_id UUID PRIMARY KEY REFERENCES control.ingestion_run(run_id),
    archive_version TEXT NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT NOT NULL CHECK (payload_checksum ~ '^[0-9a-f]{64}$'),
    station_file_count INTEGER NOT NULL DEFAULT 0 CHECK (station_file_count >= 0),
    station_count INTEGER NOT NULL DEFAULT 0 CHECK (station_count >= 0),
    -- The county boundary vintage stations were assigned against; NULL
    -- until replay, and NULL after it when no county boundaries are loaded.
    boundary_vintage INTEGER,
    status TEXT NOT NULL CHECK (status IN ('captured', 'unchanged', 'silver_ready', 'quarantined', 'published')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT noaa_normals_file_stations_within_files CHECK (station_count <= station_file_count),
    CONSTRAINT noaa_normals_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS noaa_normals_file_capture_idx ON control.noaa_normals_file (capture_id);

CREATE TABLE IF NOT EXISTS silver_noaa_normals.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- Every station of every replayed archive, with the county this warehouse
-- assigned from its coordinates. NCEI publishes no county: a station inside
-- exactly one county boundary of the recorded vintage is `resolved`; one
-- inside none (offshore, outside the United States) is `unmapped` with
-- reason `outside_counties`; one on a shared boundary is `ambiguous`.
CREATE TABLE IF NOT EXISTS silver_noaa_normals.station (
    run_id UUID NOT NULL REFERENCES control.noaa_normals_file(run_id),
    -- NCEI ids are eleven characters; CoCoRaHS stations carry lower-case
    -- letters (`US10adam002`), so the pattern admits both cases.
    station_id TEXT NOT NULL CONSTRAINT station_station_id_check CHECK (station_id ~ '^[A-Za-z0-9]{11}$'),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 1),
    latitude NUMERIC NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude NUMERIC NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    elevation_m NUMERIC,
    station_name TEXT NOT NULL,
    geo_id TEXT,
    geo_sk BIGINT,
    boundary_vintage INTEGER,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped', 'ambiguous')),
    geography_reason TEXT CHECK (
        geography_reason IS NULL
        OR geography_reason IN ('outside_counties', 'on_county_boundary', 'no_county_boundaries')
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, station_id),
    CONSTRAINT noaa_normals_station_resolved_has_county CHECK (
        (geography_status = 'resolved') = (geo_sk IS NOT NULL AND geo_id IS NOT NULL)
    ),
    CONSTRAINT noaa_normals_station_reason CHECK ((geography_status = 'resolved') = (geography_reason IS NULL))
);

CREATE INDEX IF NOT EXISTS noaa_normals_station_county_idx ON silver_noaa_normals.station (geo_id);

-- Every published annual normal of every station, with NCEI's measurement
-- and completeness flags. A withheld value (M, V, Y) has no number.
-- A warehouse built before the pattern admitted lower case keeps the old
-- CHECK; replace it so the same file is the upgrade.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'silver_noaa_normals.station'::regclass
          AND conname = 'station_station_id_check'
          AND pg_get_constraintdef(oid) NOT LIKE '%A-Za-z0-9%'
    ) THEN
        ALTER TABLE silver_noaa_normals.station DROP CONSTRAINT station_station_id_check;
        ALTER TABLE silver_noaa_normals.station ADD CONSTRAINT station_station_id_check
            CHECK (station_id ~ '^[A-Za-z0-9]{11}$');
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS silver_noaa_normals.station_normal (
    run_id UUID NOT NULL,
    station_id TEXT NOT NULL,
    variable TEXT NOT NULL,
    measure TEXT NOT NULL CHECK (
        measure IN (
            'annual_mean_temperature', 'annual_mean_maximum_temperature', 'annual_mean_minimum_temperature',
            'annual_precipitation', 'annual_heating_degree_days', 'annual_cooling_degree_days'
        )
    ),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL CHECK (value_status IN ('valid', 'missing', 'not_applicable')),
    missing_reason TEXT CHECK (
        missing_reason IS NULL OR missing_reason IN ('missing', 'too_cold_to_compute', 'insufficient_values', 'blank')
    ),
    measurement_flag TEXT CHECK (measurement_flag IS NULL OR measurement_flag IN ('M', 'V', 'X', 'Y', 'Z')),
    completeness_flag TEXT CHECK (completeness_flag IS NULL OR completeness_flag IN ('S', 'R', 'P', 'E')),
    years INTEGER CHECK (years IS NULL OR years BETWEEN 0 AND 30),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{32}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, station_id, variable),
    FOREIGN KEY (run_id, station_id) REFERENCES silver_noaa_normals.station(run_id, station_id),
    CONSTRAINT noaa_normals_valid_value_present CHECK (value_status <> 'valid' OR value IS NOT NULL),
    CONSTRAINT noaa_normals_withheld_value_absent CHECK (value_status = 'valid' OR value IS NULL),
    CONSTRAINT noaa_normals_withheld_has_reason CHECK ((value_status = 'valid') = (missing_reason IS NULL))
);

CREATE INDEX IF NOT EXISTS noaa_normals_station_normal_measure_idx
    ON silver_noaa_normals.station_normal (run_id, measure);
