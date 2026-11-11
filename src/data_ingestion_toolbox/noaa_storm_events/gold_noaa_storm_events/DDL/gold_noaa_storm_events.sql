-- Publication views over the NOAA Storm Events Database (noaa-storm-events).
-- Applied by the bootstrap manifest in the `gold` phase and re-applied by the
-- DAG's `ensure_noaa_storm_events_schema` task; this file is the only
-- definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_noaa_storm_events;

-- The recording eras of the Storm Events Database (NCEI, "Storm Events
-- Database: event types"): which event types a year's file can hold.
CREATE OR REPLACE VIEW gold_noaa_storm_events.recording_era AS
SELECT era.first_year, era.last_year, era.event_types, era.description
FROM (VALUES
    (1950, 1954, ARRAY['Tornado']::TEXT[], 'tornado only'),
    (1955, 1995, ARRAY['Tornado', 'Thunderstorm Wind', 'Hail']::TEXT[],
     'tornado, thunderstorm wind and hail only'),
    (1996, NULL::INTEGER, NULL::TEXT[], 'all 48 event types of NWS Directive 10-1605')
) AS era(first_year, last_year, event_types, description);

-- One count measure per event type NCEI has published (keyed by the type's
-- own words), one for all types, and six impact measures.
CREATE OR REPLACE VIEW gold_noaa_storm_events.measure_definition AS
WITH counting AS (
    SELECT 'NOAA NCEI Storm Events Database: the number of events reported in the calendar '
           || 'year of the event''s begin date. A county counts the events reported for the '
           || 'county and the events reported for an NWS forecast zone that includes part of '
           || 'the county (a zone event counts in each county of the zone, and only when its '
           || 'zone code and zone name match a published NWS zone-county file); a state counts '
           || 'every event reported in the state; the nation every event of the 50 states and '
           || 'DC; marine-zone events are not counted on land. The types recorded differ by '
           || 'era: 1950-1954 tornado only; 1955-1995 tornado, thunderstorm wind and hail only; '
           || '1996 onward all 48 event types of NWS Directive 10-1605. In a year whose era '
           || 'records the type, a place with no row had no such event reported; outside it the '
           || 'type was not recorded. NCEI adds events for the current year each month. '
           || 'Source: NOAA National Centers for Environmental Information, Storm Events '
           || 'Database; NWS zone-county correlation file.' AS text
),
impact AS (
    SELECT 'NOAA NCEI Storm Events Database: the sum, over events reported in the calendar '
           || 'year of the event''s begin date with the figure published, of the figure as NCEI '
           || 'published it. A county sums the events reported for the county only: a figure '
           || 'reported for a forecast zone is not split across counties. A state sums every '
           || 'event reported in the state; the nation every event of the 50 states and DC. A '
           || 'blank figure is not reported and is not counted as zero. Dollar figures are as '
           || 'reported in the event''s year, not adjusted for inflation. Source: NOAA National '
           || 'Centers for Environmental Information, Storm Events Database.' AS text
)
SELECT 'storm_events_' || event_type.event_type_key AS measure,
       'Storm events reported: ' || event_type.event_type AS measure_label,
       'events'::TEXT AS unit,
       event_type.event_type,
       'count'::TEXT AS measure_family,
       counting.text AS observation_basis
FROM silver_noaa_storm_events.event_type AS event_type CROSS JOIN counting
UNION ALL
SELECT 'storm_events_all_types', 'Storm events reported: all types', 'events', NULL,
       'count', counting.text
FROM counting
UNION ALL
SELECT figure.measure, figure.label, figure.unit, NULL, 'impact', impact.text
FROM (VALUES
    ('storm_event_deaths_direct', 'Deaths directly caused by storm events', 'deaths'),
    ('storm_event_deaths_indirect', 'Deaths indirectly caused by storm events', 'deaths'),
    ('storm_event_injuries_direct', 'Injuries directly caused by storm events', 'injuries'),
    ('storm_event_injuries_indirect', 'Injuries indirectly caused by storm events', 'injuries'),
    ('storm_event_property_damage', 'Property damage reported for storm events', 'dollars (as reported)'),
    ('storm_event_crop_damage', 'Crop damage reported for storm events', 'dollars (as reported)')
) AS figure(measure, label, unit)
CROSS JOIN impact;

-- The geography types counts are kept for: the publisher probes one served
-- row of each, and declares only the types a row with a value was found for.
CREATE OR REPLACE VIEW gold_noaa_storm_events.place_kind AS
SELECT kind.geo_type
FROM (VALUES ('county'), ('state'), ('nation')) AS kind(geo_type);

-- The newest published details file of every data year.
CREATE OR REPLACE VIEW gold_noaa_storm_events.year_release AS
SELECT DISTINCT ON (file.data_year)
       file.data_year AS year, file.file_name, file.stamp, file.run_id, file.capture_id,
       file.published_at, file.row_count, file.quarantined_count,
       TO_DATE(file.stamp, 'YYYYMMDD') AS file_created
FROM control.noaa_storm_events_file AS file
WHERE file.file_kind = 'details' AND file.status = 'published'
ORDER BY file.data_year, file.published_at DESC;

-- Every count and impact in the neutral observation shape. Counts are
-- recomputed when a year's events change; each event's own history is in
-- `silver_noaa_storm_events.event_revision`. A year whose file NCEI is
-- still adding to says so in `coverage_note`.
CREATE OR REPLACE VIEW gold_noaa_storm_events.observation_revision AS
SELECT stored.metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       stored.year,
       MAKE_DATE(stored.year, 1, 1) AS period_start,
       MAKE_DATE(stored.year, 12, 31) AS period_end,
       stored.geo_id,
       stored.geo_sk,
       stored.geo_type,
       gold_glossary.geo_grain(stored.geo_type) AS geo_level,
       stored.event_type,
       stored.value,
       'valid'::TEXT AS value_status,
       stored.events_counted,
       stored.county_reported,
       stored.zone_reported,
       CASE WHEN release.file_created < MAKE_DATE(stored.year + 1, 3, 1)
            THEN 'Events reported through NCEI''s file of '
                 || TO_CHAR(release.file_created, 'YYYY-MM-DD')
                 || '; NCEI adds events for this year each month.'
       END AS coverage_note,
       'Storm Events ' || release.file_name AS release_key,
       release.published_at,
       md5(stored.metric_key || '|' || stored.geo_id || '|' || stored.year::TEXT) AS source_record_id,
       release.capture_id,
       release.run_id
FROM silver_noaa_storm_events.place_year AS stored
JOIN gold_noaa_storm_events.measure_definition AS definition ON definition.measure = stored.metric_key
JOIN gold_noaa_storm_events.year_release AS release ON release.year = stored.year;

-- The current figure of each place, year and measure (the same rows: a
-- count is kept as of the newest file of its year).
CREATE OR REPLACE VIEW gold_noaa_storm_events.observation_latest AS
SELECT revision.* FROM gold_noaa_storm_events.observation_revision AS revision;

-- Every current event counted in a county, newest first by begin date: the
-- event's date, type, magnitude, figures and narratives as NCEI published
-- them. Deaths, injuries and damage are the event's own; a zone event's
-- figures are for the whole zone, never a share of it.
CREATE OR REPLACE VIEW gold_noaa_storm_events.event_detail AS
SELECT place.geo_id,
       place.geo_sk,
       place.basis,
       event.event_id,
       event.episode_id,
       event.year,
       event.begin_date,
       event.begin_date_time,
       event.end_date_time,
       event.cz_timezone,
       event.event_type,
       event.state_name,
       event.cz_type,
       event.cz_name,
       event.wfo,
       event.magnitude,
       event.magnitude_type,
       event.tor_f_scale,
       event.tor_length,
       event.tor_width,
       event.flood_cause,
       event.category,
       event.deaths_direct,
       event.deaths_indirect,
       event.injuries_direct,
       event.injuries_indirect,
       event.damage_property_published,
       event.damage_property_usd,
       event.damage_crops_published,
       event.damage_crops_usd,
       event.begin_location,
       event.end_location,
       event.begin_lat,
       event.begin_lon,
       event.end_lat,
       event.end_lon,
       event.source,
       event.episode_narrative,
       event.event_narrative,
       event.zone_file,
       'Storm Events ' || release.file_name AS release_key,
       release.published_at,
       event.source_record_id,
       event.capture_id,
       event.run_id
FROM silver_noaa_storm_events.event_place AS place
JOIN silver_noaa_storm_events.event AS event USING (event_id)
JOIN gold_noaa_storm_events.year_release AS release ON release.year = event.year
WHERE event.event_status = 'current';

-- Per state and year: how many events were reported and where they were
-- counted, so a county figure can be read with the zone events that could
-- not be placed on a county.
CREATE OR REPLACE VIEW gold_noaa_storm_events.state_coverage AS
SELECT 'state:' || stored.state_fips AS geo_id,
       stored.state_fips,
       stored.state_name,
       stored.year,
       era.description AS recorded_types,
       stored.events_reported,
       stored.county_events_placed,
       stored.county_events_unplaced,
       stored.zone_events_placed,
       stored.zone_events_unplaced,
       stored.marine_events,
       CASE WHEN stored.county_events_unplaced + stored.zone_events_unplaced > 0
            THEN (stored.county_events_unplaced + stored.zone_events_unplaced)
                 || ' of ' || stored.events_reported || ' events reported in '
                 || stored.state_name || ' in ' || stored.year || ' are not counted on any '
                 || 'county: their forecast zone code and name match no published NWS '
                 || 'zone-county file, or their county is not in the shared geography.'
       END AS coverage_note,
       'Storm Events ' || release.file_name AS release_key,
       release.published_at
FROM silver_noaa_storm_events.state_year AS stored
JOIN gold_noaa_storm_events.year_release AS release ON release.year = stored.year
JOIN gold_noaa_storm_events.recording_era AS era
  ON stored.year >= era.first_year AND (era.last_year IS NULL OR stored.year <= era.last_year);

CREATE OR REPLACE VIEW gold_noaa_storm_events.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, measure_family,
       observation_basis, 'noaa_storm_events:v1'::TEXT AS schema_version,
       'ANNUAL'::TEXT AS time_grain
FROM gold_noaa_storm_events.measure_definition;

COMMENT ON SCHEMA gold_noaa_storm_events IS
    'Policy-free publication views for NOAA Storm Events Database event counts, impacts and event records.';
