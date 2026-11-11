-- Control and silver relations for the NOAA Storm Events Database and the
-- NWS zone-county correlation files (noaa-storm-events).

CREATE SCHEMA IF NOT EXISTS silver_noaa_storm_events;

-- One row per run and file: a year's details file or a zone-county file the
-- listing named. A file whose name was already published is
-- `already_loaded` and has no capture; one whose bytes equal the last
-- published capture of the same year (or zone file) is `unchanged` and
-- replays nothing.
CREATE TABLE IF NOT EXISTS control.noaa_storm_events_file (
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    file_kind TEXT NOT NULL CHECK (file_kind IN ('details', 'zone_county')),
    file_name TEXT NOT NULL CHECK (
        file_name ~ '^StormEvents_details-ftp_v[0-9]+\.[0-9]+_d[0-9]{4}_c[0-9]{8}\.csv\.gz$'
        OR file_name ~ '^bp[0-9]{2}[a-z]{2}[0-9]{2}\.dbx$'
    ),
    -- The data year of a details file; NULL for a zone-county file.
    data_year INTEGER CHECK (data_year IS NULL OR data_year >= 1950),
    -- The `c` stamp of a details file, or the valid date of a zone file.
    stamp TEXT NOT NULL CHECK (stamp ~ '^[0-9]{8}$'),
    capture_id UUID REFERENCES raw_capture.response_capture(capture_id),
    payload_checksum TEXT CHECK (payload_checksum IS NULL OR payload_checksum ~ '^[0-9a-f]{64}$'),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    quarantined_count INTEGER NOT NULL DEFAULT 0 CHECK (quarantined_count >= 0),
    -- Events (or zone rows) this read changed or added; an unchanged event is
    -- counted, never rewritten.
    written_count INTEGER NOT NULL DEFAULT 0 CHECK (written_count >= 0),
    -- Events of the year an earlier file published that this file no longer holds.
    withdrawn_count INTEGER NOT NULL DEFAULT 0 CHECK (withdrawn_count >= 0),
    status TEXT NOT NULL CHECK (
        status IN ('already_loaded', 'captured', 'unchanged', 'silver_ready', 'quarantined', 'published')
    ),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, file_name),
    CONSTRAINT noaa_storm_events_file_year_matches_kind CHECK ((file_kind = 'details') = (data_year IS NOT NULL)),
    CONSTRAINT noaa_storm_events_file_capture_unless_already_loaded CHECK (
        (status = 'already_loaded') = (capture_id IS NULL)
    ),
    CONSTRAINT noaa_storm_events_file_published_has_time CHECK (status <> 'published' OR published_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS noaa_storm_events_file_name_idx ON control.noaa_storm_events_file (file_name, status);
CREATE INDEX IF NOT EXISTS noaa_storm_events_file_year_idx ON control.noaa_storm_events_file (data_year, status, published_at);
CREATE INDEX IF NOT EXISTS noaa_storm_events_file_capture_idx ON control.noaa_storm_events_file (capture_id);

CREATE TABLE IF NOT EXISTS silver_noaa_storm_events.quarantine (
    quarantine_sk BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES control.ingestion_run(run_id),
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    -- The line number in the file; 0 when the whole file is refused.
    source_row_index INTEGER NOT NULL CHECK (source_row_index >= 0),
    error_code TEXT NOT NULL,
    error_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, source_row_index, error_code)
);

-- Every zone and county row of every published zone-county file, as NWS
-- published it. `zone_name_key` is the zone name without case, punctuation
-- or a trailing "County", the form a storm event's zone name is matched on.
CREATE TABLE IF NOT EXISTS silver_noaa_storm_events.zone_county (
    zone_file TEXT NOT NULL,
    valid_from DATE NOT NULL,
    state TEXT NOT NULL CHECK (state ~ '^[A-Z]{2}$'),
    zone TEXT NOT NULL CHECK (zone ~ '^[0-9]{3}$'),
    cwa TEXT NOT NULL,
    zone_name TEXT NOT NULL,
    zone_name_key TEXT NOT NULL,
    county_name TEXT NOT NULL,
    county_fips5 TEXT NOT NULL CHECK (county_fips5 ~ '^[0-9]{5}$'),
    time_zone TEXT NOT NULL,
    geo_id TEXT,
    geo_sk BIGINT,
    geography_status TEXT NOT NULL CHECK (geography_status IN ('resolved', 'unmapped')),
    run_id UUID NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_line INTEGER NOT NULL CHECK (source_line >= 1),
    PRIMARY KEY (zone_file, state, zone, county_fips5),
    CONSTRAINT noaa_storm_events_zone_resolved_has_geo CHECK (
        (geography_status = 'resolved') = (geo_id IS NOT NULL AND geo_sk IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS noaa_storm_events_zone_match_idx
    ON silver_noaa_storm_events.zone_county (state, zone, zone_name_key);

-- The current published version of every event, keyed by NCEI's event id.
-- A read writes a row only when NCEI's published row changed (the
-- superseded row moves to `event_revision` first), or when the event is new.
-- An event a later file of its year no longer holds is `withdrawn`: kept,
-- never counted. `placement_status` says where the event is counted:
--   county         a county-reported (C) event on a county the shared geography holds
--   zone_matched   a zone (Z) event whose zone code and name match a zone-county file
--   zone_unmatched a zone event no published zone-county file matches by code and name
--   county_unmapped a county event on a county the shared geography does not hold
--   marine         a marine-zone (M) event, not placed on land
CREATE TABLE IF NOT EXISTS silver_noaa_storm_events.event (
    event_id BIGINT PRIMARY KEY,
    episode_id BIGINT,
    year INTEGER NOT NULL CHECK (year >= 1950),
    begin_date DATE NOT NULL,
    end_date DATE,
    begin_time TEXT NOT NULL,
    end_time TEXT NOT NULL,
    begin_date_time TEXT NOT NULL,
    end_date_time TEXT NOT NULL,
    cz_timezone TEXT NOT NULL,
    state_name TEXT NOT NULL,
    state_fips TEXT NOT NULL CHECK (state_fips ~ '^[0-9]{2}$'),
    event_type TEXT NOT NULL,
    event_type_key TEXT NOT NULL CHECK (event_type_key ~ '^[a-z0-9_]+$'),
    cz_type TEXT NOT NULL CHECK (cz_type IN ('C', 'Z', 'M')),
    cz_fips TEXT NOT NULL CHECK (cz_fips ~ '^[0-9]{3}$'),
    cz_name TEXT NOT NULL,
    cz_name_key TEXT NOT NULL,
    wfo TEXT NOT NULL,
    injuries_direct INTEGER CHECK (injuries_direct IS NULL OR injuries_direct >= 0),
    injuries_indirect INTEGER CHECK (injuries_indirect IS NULL OR injuries_indirect >= 0),
    deaths_direct INTEGER CHECK (deaths_direct IS NULL OR deaths_direct >= 0),
    deaths_indirect INTEGER CHECK (deaths_indirect IS NULL OR deaths_indirect >= 0),
    damage_property_published TEXT,
    damage_property_usd NUMERIC CHECK (damage_property_usd IS NULL OR damage_property_usd >= 0),
    damage_property_status TEXT NOT NULL CHECK (damage_property_status IN ('reported', 'not_reported', 'unreadable')),
    damage_crops_published TEXT,
    damage_crops_usd NUMERIC CHECK (damage_crops_usd IS NULL OR damage_crops_usd >= 0),
    damage_crops_status TEXT NOT NULL CHECK (damage_crops_status IN ('reported', 'not_reported', 'unreadable')),
    source TEXT NOT NULL,
    magnitude NUMERIC,
    magnitude_type TEXT NOT NULL,
    flood_cause TEXT NOT NULL,
    category TEXT NOT NULL,
    tor_f_scale TEXT NOT NULL,
    tor_length NUMERIC,
    tor_width NUMERIC,
    begin_location TEXT NOT NULL,
    end_location TEXT NOT NULL,
    begin_lat NUMERIC,
    begin_lon NUMERIC,
    end_lat NUMERIC,
    end_lon NUMERIC,
    episode_narrative TEXT NOT NULL,
    event_narrative TEXT NOT NULL,
    data_source TEXT NOT NULL,
    event_status TEXT NOT NULL CHECK (event_status IN ('current', 'withdrawn')),
    placement_status TEXT NOT NULL CHECK (
        placement_status IN ('county', 'zone_matched', 'zone_unmatched', 'county_unmapped', 'marine')
    ),
    -- The zone-county file a matched zone event was placed with.
    zone_file TEXT,
    row_hash TEXT NOT NULL CHECK (row_hash ~ '^[0-9a-f]{32}$'),
    run_id UUID NOT NULL,
    capture_id UUID NOT NULL REFERENCES raw_capture.response_capture(capture_id),
    source_line INTEGER NOT NULL CHECK (source_line >= 2),
    source_record_id TEXT NOT NULL CHECK (source_record_id ~ '^[0-9a-f]{32}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT noaa_storm_events_damage_property_reported CHECK (
        (damage_property_status = 'reported') = (damage_property_usd IS NOT NULL)
    ),
    CONSTRAINT noaa_storm_events_damage_crops_reported CHECK (
        (damage_crops_status = 'reported') = (damage_crops_usd IS NOT NULL)
    ),
    CONSTRAINT noaa_storm_events_zone_file_when_matched CHECK (
        (placement_status = 'zone_matched') = (zone_file IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS noaa_storm_events_event_year_idx
    ON silver_noaa_storm_events.event (year, event_status);
CREATE INDEX IF NOT EXISTS noaa_storm_events_event_state_idx
    ON silver_noaa_storm_events.event (state_fips, year);
CREATE INDEX IF NOT EXISTS noaa_storm_events_event_zone_idx
    ON silver_noaa_storm_events.event (cz_type, state_fips, cz_fips) WHERE cz_type = 'Z';

-- Every published version of an event a later file replaced or withdrew,
-- with the run that did so.
CREATE TABLE IF NOT EXISTS silver_noaa_storm_events.event_revision (
    event_id BIGINT NOT NULL,
    year INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    state_fips TEXT NOT NULL,
    cz_type TEXT NOT NULL,
    cz_fips TEXT NOT NULL,
    event_status TEXT NOT NULL,
    row_hash TEXT NOT NULL,
    run_id UUID NOT NULL,
    capture_id UUID NOT NULL,
    source_line INTEGER NOT NULL,
    source_record_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    superseded_by_run_id UUID NOT NULL,
    -- `replaced` (NCEI changed the row) or `withdrawn` (no longer in the file).
    change_kind TEXT NOT NULL CHECK (change_kind IN ('replaced', 'withdrawn')),
    superseded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, run_id, superseded_by_run_id)
);

-- Every county a current event is counted in: a county event's own county,
-- or each county of a matched zone. Withdrawn, unplaced and marine events
-- have no row. `metric_key` is the count measure, stored so a read by type
-- uses the index.
CREATE TABLE IF NOT EXISTS silver_noaa_storm_events.event_place (
    event_id BIGINT NOT NULL REFERENCES silver_noaa_storm_events.event(event_id),
    geo_id TEXT NOT NULL,
    geo_sk BIGINT NOT NULL,
    year INTEGER NOT NULL,
    metric_key TEXT NOT NULL,
    -- `county_reported` or `zone_reported`.
    basis TEXT NOT NULL CHECK (basis IN ('county_reported', 'zone_reported')),
    PRIMARY KEY (event_id, geo_id)
);

CREATE INDEX IF NOT EXISTS noaa_storm_events_place_geo_idx
    ON silver_noaa_storm_events.event_place (geo_id, year);
CREATE INDEX IF NOT EXISTS noaa_storm_events_place_metric_idx
    ON silver_noaa_storm_events.event_place (metric_key, year);

-- Counts and impacts per place, year and measure, recomputed for the years a
-- read changed; a row whose figures did not change is not rewritten.
--   storm_events_<type>, storm_events_all_types: events counted in the place
--     (county: county events and matched zone events; state: every event of
--     the state; nation: every event of the 50 states and DC);
--   storm_event_deaths_direct ... storm_event_crop_damage: the sum over
--     events with the figure published (county: county-reported events only).
CREATE TABLE IF NOT EXISTS silver_noaa_storm_events.place_year (
    metric_key TEXT NOT NULL,
    geo_type TEXT NOT NULL CHECK (geo_type IN ('county', 'state', 'nation')),
    geo_id TEXT NOT NULL,
    geo_sk BIGINT NOT NULL,
    year INTEGER NOT NULL,
    event_type TEXT,
    value NUMERIC NOT NULL CHECK (value >= 0),
    -- Events counted (counts) or events whose figure was published (impacts).
    events_counted INTEGER NOT NULL CHECK (events_counted >= 0),
    county_reported INTEGER NOT NULL CHECK (county_reported >= 0),
    zone_reported INTEGER NOT NULL CHECK (zone_reported >= 0),
    run_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (metric_key, geo_id, year)
);

CREATE INDEX IF NOT EXISTS noaa_storm_events_place_year_geo_idx
    ON silver_noaa_storm_events.place_year (geo_id, year);
CREATE INDEX IF NOT EXISTS noaa_storm_events_place_year_metric_idx
    ON silver_noaa_storm_events.place_year (metric_key, geo_type, year);

-- Every event type a replay has counted, as NCEI wrote it: the list the
-- count measures are defined from, so a type NCEI adds is served without a
-- code change. A type is never removed; one with no current event serves
-- no row.
CREATE TABLE IF NOT EXISTS silver_noaa_storm_events.event_type (
    event_type_key TEXT PRIMARY KEY CHECK (event_type_key ~ '^[a-z0-9_]+$'),
    event_type TEXT NOT NULL,
    first_seen_run_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Per state code and year: how many current events were reported and where
-- they were counted, recomputed with `place_year`; a row whose counts did
-- not change is not rewritten. Marine-zone state codes are kept too.
CREATE TABLE IF NOT EXISTS silver_noaa_storm_events.state_year (
    state_fips TEXT NOT NULL CHECK (state_fips ~ '^[0-9]{2}$'),
    year INTEGER NOT NULL,
    state_name TEXT NOT NULL,
    events_reported INTEGER NOT NULL CHECK (events_reported >= 0),
    county_events_placed INTEGER NOT NULL CHECK (county_events_placed >= 0),
    county_events_unplaced INTEGER NOT NULL CHECK (county_events_unplaced >= 0),
    zone_events_placed INTEGER NOT NULL CHECK (zone_events_placed >= 0),
    zone_events_unplaced INTEGER NOT NULL CHECK (zone_events_unplaced >= 0),
    marine_events INTEGER NOT NULL CHECK (marine_events >= 0),
    run_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (state_fips, year)
);
