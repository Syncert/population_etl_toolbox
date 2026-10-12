-- Publication views over NOAA U.S. Climate Normals 1991-2020 (annual/seasonal
-- by-station archive, noaa-normals). Applied by the bootstrap manifest in the
-- `gold` phase and re-applied by the DAG's `ensure_noaa_normals_schema` task;
-- this file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_noaa_normals;

CREATE OR REPLACE VIEW gold_noaa_normals.measure_definition AS
SELECT measure.measure, measure.measure_label, measure.unit,
       'NOAA U.S. Climate Normals 1991-2020 (annual/seasonal, by station): a 30-year normal, '
       || 'not the value of any one year. NCEI publishes normals for stations, not counties; this '
       || 'warehouse''s county figure is the unweighted mean of the stations it places inside the '
       || 'county boundary, counting only stations NCEI flags standard (S) or representative (R) '
       || 'for that element. It is a derived summary, not an NCEI product. '
       || measure.statistic || ' Source: NOAA National Centers for Environmental Information, '
       || 'U.S. Climate Normals 1991-2020.' AS observation_basis
FROM (VALUES
    ('annual_mean_temperature', 'Annual mean temperature normal, county station mean', 'degrees Fahrenheit',
     'NCEI element ANN-TAVG-NORMAL.'),
    ('annual_mean_maximum_temperature', 'Annual mean daily maximum temperature normal, county station mean',
     'degrees Fahrenheit', 'NCEI element ANN-TMAX-NORMAL.'),
    ('annual_mean_minimum_temperature', 'Annual mean daily minimum temperature normal, county station mean',
     'degrees Fahrenheit', 'NCEI element ANN-TMIN-NORMAL.'),
    ('annual_precipitation', 'Annual precipitation normal, county station mean', 'inches',
     'NCEI element ANN-PRCP-NORMAL.'),
    ('annual_heating_degree_days', 'Annual heating degree days normal (base 65 F), county station mean',
     'degree days (base 65 F)', 'NCEI element ANN-HTDD-NORMAL.'),
    ('annual_cooling_degree_days', 'Annual cooling degree days normal (base 65 F), county station mean',
     'degree days (base 65 F)', 'NCEI element ANN-CLDD-NORMAL.')
) AS measure(measure, measure_label, unit, statistic);

-- Every published station normal: NCEI's own figure, flags and the county
-- this warehouse assigned. Not dispatched (the API has no station grain); it
-- is the lineage of every county figure.
CREATE OR REPLACE VIEW gold_noaa_normals.station_observation AS
SELECT normal.run_id, normal.station_id, station.station_name, station.latitude, station.longitude,
       station.elevation_m, station.geo_id, station.geo_sk, station.boundary_vintage,
       station.geography_status, station.geography_reason,
       normal.variable, normal.measure, normal.value_source, normal.value, normal.value_status,
       normal.missing_reason, normal.measurement_flag, normal.completeness_flag, normal.years,
       normal.source_record_id, normal.capture_id, file.archive_version, file.published_at
FROM silver_noaa_normals.station_normal AS normal
JOIN silver_noaa_normals.station AS station
  ON station.run_id = normal.run_id AND station.station_id = normal.station_id
JOIN control.noaa_normals_file AS file ON file.run_id = normal.run_id
WHERE file.status = 'published';

-- The county figure for each published archive: the mean of the county's
-- standard or representative stations with a value. A county with no such
-- station has no row.
CREATE OR REPLACE VIEW gold_noaa_normals.observation_revision AS
WITH eligible AS (
    SELECT station.*
    FROM gold_noaa_normals.station_observation AS station
    WHERE station.geography_status = 'resolved'
      AND station.completeness_flag IN ('S', 'R')
      AND station.value_status = 'valid'
),
county AS (
    SELECT eligible.run_id, eligible.measure, eligible.geo_id,
           MIN(eligible.geo_sk) AS geo_sk,
           ROUND(AVG(eligible.value), 2) AS value,
           COUNT(*) AS stations,
           STRING_AGG(eligible.station_id, ',' ORDER BY eligible.station_id) AS station_ids,
           MIN(eligible.boundary_vintage) AS boundary_vintage,
           MIN(eligible.archive_version) AS archive_version,
           MIN(eligible.capture_id::TEXT)::UUID AS capture_id,
           MIN(eligible.published_at) AS published_at
    FROM eligible
    GROUP BY eligible.run_id, eligible.measure, eligible.geo_id
)
SELECT county.measure AS metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       2020 AS year,
       DATE '1991-01-01' AS period_start,
       DATE '2020-12-31' AS period_end,
       county.geo_id,
       county.geo_sk,
       'county'::TEXT AS geo_type,
       gold_glossary.geo_grain('county') AS geo_level,
       'resolved'::TEXT AS geography_status,
       county.value,
       'valid'::TEXT AS value_status,
       county.stations AS station_count,
       county.station_ids,
       county.boundary_vintage,
       'Normals 1991-2020 ' || county.archive_version || ' read '
           || TO_CHAR(county.published_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS release_key,
       county.published_at,
       md5(county.measure || '|' || county.geo_id || '|' || county.run_id::TEXT) AS source_record_id,
       county.capture_id,
       county.run_id
FROM county
JOIN gold_noaa_normals.measure_definition AS definition ON definition.measure = county.measure;

-- The newest published read of each county normal.
CREATE OR REPLACE VIEW gold_noaa_normals.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year)
       revision.*
FROM gold_noaa_normals.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year,
         revision.published_at DESC, revision.run_id DESC;

CREATE OR REPLACE VIEW gold_noaa_normals.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, observation_basis,
       'noaa_normals_csv:v1'::TEXT AS schema_version
FROM gold_noaa_normals.measure_definition;

COMMENT ON SCHEMA gold_noaa_normals IS
    'Policy-free publication views for NOAA climate normals by station and derived county figures.';
