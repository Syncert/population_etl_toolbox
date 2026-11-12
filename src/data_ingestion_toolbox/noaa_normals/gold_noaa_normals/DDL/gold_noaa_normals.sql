-- Publication views over NOAA U.S. Climate Normals 1991-2020 (annual/seasonal
-- and monthly by-station archives, noaa-normals). Applied by the bootstrap manifest in the
-- `gold` phase and re-applied by the DAG's `ensure_noaa_normals_schema` task;
-- this file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_noaa_normals;

CREATE OR REPLACE VIEW gold_noaa_normals.measure_definition AS
SELECT measure.measure, measure.measure_label, measure.unit,
       'NOAA U.S. Climate Normals 1991-2020 (' || measure.archive || ', by station): a 30-year '
       || 'normal, not the value of any one year' || measure.period_note || '. NCEI publishes '
       || 'normals for stations, not counties; this warehouse''s county figure is the unweighted '
       || 'mean of the stations it places inside the county boundary (or, for a station no county '
       || 'boundary covers, on the nearest county when that county''s boundary is within 1,000 '
       || 'metres, as named in the placement note), counting only stations NCEI '
       || 'flags standard (S) or representative (R) for that element. It is a derived summary, '
       || 'not an NCEI product. ' || measure.statistic || ' Source: NOAA National Centers for '
       || 'Environmental Information, U.S. Climate Normals 1991-2020.' AS observation_basis,
       measure.time_grain
FROM (
    SELECT annual.*, 'annual/seasonal'::TEXT AS archive, ''::TEXT AS period_note,
           'ANNUAL'::TEXT AS time_grain
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
    ) AS annual(measure, measure_label, unit, statistic)
    UNION ALL
    SELECT monthly.*, 'monthly'::TEXT,
           '; each row is one calendar month of the 1991-2020 normals (the period shown is '
           || 'that month in 1991 to that month in 2020)',
           'MONTHLY'::TEXT
    FROM (VALUES
        ('monthly_mean_temperature', 'Monthly mean temperature normal, county station mean',
         'degrees Fahrenheit', 'NCEI element MLY-TAVG-NORMAL.'),
        ('monthly_mean_maximum_temperature', 'Monthly mean daily maximum temperature normal, county station mean',
         'degrees Fahrenheit', 'NCEI element MLY-TMAX-NORMAL.'),
        ('monthly_mean_minimum_temperature', 'Monthly mean daily minimum temperature normal, county station mean',
         'degrees Fahrenheit', 'NCEI element MLY-TMIN-NORMAL.'),
        ('monthly_precipitation', 'Monthly precipitation normal, county station mean', 'inches',
         'NCEI element MLY-PRCP-NORMAL.'),
        ('monthly_snowfall', 'Monthly snowfall normal, county station mean', 'inches',
         'NCEI element MLY-SNOW-NORMAL; only stations that measure snowfall are counted.'),
        ('monthly_heating_degree_days', 'Monthly heating degree days normal (base 65 F), county station mean',
         'degree days (base 65 F)', 'NCEI element MLY-HTDD-NORMAL.'),
        ('monthly_cooling_degree_days', 'Monthly cooling degree days normal (base 65 F), county station mean',
         'degree days (base 65 F)', 'NCEI element MLY-CLDD-NORMAL.')
    ) AS monthly(measure, measure_label, unit, statistic)
) AS measure;

-- Every published station normal: NCEI's own figure, flags and the county
-- this warehouse assigned. Not dispatched (the API has no station grain); it
-- is the lineage of every county figure.
CREATE OR REPLACE VIEW gold_noaa_normals.station_observation AS
SELECT normal.run_id, normal.station_id, station.station_name, station.latitude, station.longitude,
       station.elevation_m, station.geo_id, station.geo_sk, station.boundary_vintage,
       station.geography_status, station.geography_reason,
       normal.variable, normal.measure, normal.value_source, normal.value, normal.value_status,
       normal.missing_reason, normal.measurement_flag, normal.completeness_flag, normal.years,
       normal.source_record_id, normal.capture_id, file.archive_version, file.published_at,
       station.placement_method, station.placement_distance_m
FROM silver_noaa_normals.station_normal AS normal
JOIN silver_noaa_normals.station AS station
  ON station.run_id = normal.run_id AND station.station_id = normal.station_id
JOIN control.noaa_normals_file AS file ON file.run_id = normal.run_id
WHERE file.status = 'published';

-- Every published monthly station normal, the lineage of every monthly
-- county figure.
CREATE OR REPLACE VIEW gold_noaa_normals.station_monthly_observation AS
SELECT normal.run_id, normal.station_id, station.station_name, station.latitude, station.longitude,
       station.elevation_m, station.geo_id, station.geo_sk, station.boundary_vintage,
       station.geography_status, station.geography_reason,
       normal.variable, normal.measure, normal.month, normal.value_source, normal.value,
       normal.value_status, normal.missing_reason, normal.measurement_flag, normal.completeness_flag,
       normal.years, normal.source_record_id, normal.capture_id, file.archive_version, file.published_at,
       station.placement_method, station.placement_distance_m
FROM silver_noaa_normals.station_monthly_normal AS normal
JOIN silver_noaa_normals.station AS station
  ON station.run_id = normal.run_id AND station.station_id = normal.station_id
JOIN control.noaa_normals_file AS file ON file.run_id = normal.run_id
WHERE file.status = 'published';

-- The county figure for each published archive: the mean of the county's
-- standard or representative stations with a value. A county with no such
-- station has no row.
CREATE OR REPLACE VIEW gold_noaa_normals.observation_revision AS
WITH eligible AS (
    SELECT station.run_id, station.measure, NULL::SMALLINT AS month, station.geo_id,
           station.geo_sk, station.value, station.station_id, station.boundary_vintage,
           station.archive_version, station.capture_id, station.published_at,
           station.placement_method, station.placement_distance_m
    FROM gold_noaa_normals.station_observation AS station
    WHERE station.geography_status = 'resolved'
      AND station.completeness_flag IN ('S', 'R')
      AND station.value_status = 'valid'
    UNION ALL
    SELECT station.run_id, station.measure, station.month, station.geo_id,
           station.geo_sk, station.value, station.station_id, station.boundary_vintage,
           station.archive_version, station.capture_id, station.published_at,
           station.placement_method, station.placement_distance_m
    FROM gold_noaa_normals.station_monthly_observation AS station
    WHERE station.geography_status = 'resolved'
      AND station.completeness_flag IN ('S', 'R')
      AND station.value_status = 'valid'
),
county AS (
    SELECT eligible.run_id, eligible.measure, eligible.month, eligible.geo_id,
           MIN(eligible.geo_sk) AS geo_sk,
           ROUND(AVG(eligible.value), 2) AS value,
           COUNT(*) AS stations,
           STRING_AGG(eligible.station_id, ',' ORDER BY eligible.station_id) AS station_ids,
           MIN(eligible.boundary_vintage) AS boundary_vintage,
           MIN(eligible.archive_version) AS archive_version,
           MIN(eligible.capture_id::TEXT)::UUID AS capture_id,
           MIN(eligible.published_at) AS published_at,
           STRING_AGG(eligible.station_id || ' (' || ROUND(eligible.placement_distance_m)::TEXT || ' m)',
                      ', ' ORDER BY eligible.station_id)
               FILTER (WHERE eligible.placement_method = 'nearest_county') AS nearest_stations
    FROM eligible
    GROUP BY eligible.run_id, eligible.measure, eligible.month, eligible.geo_id
)
SELECT county.measure AS metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       2020 AS year,
       -- A monthly normal covers that calendar month from 1991 to 2020.
       COALESCE(MAKE_DATE(1991, county.month, 1), DATE '1991-01-01') AS period_start,
       COALESCE(
           (MAKE_DATE(2020, county.month, 1) + INTERVAL '1 month - 1 day')::DATE,
           DATE '2020-12-31'
       ) AS period_end,
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
       md5(county.measure || '|' || county.geo_id || '|' || county.run_id::TEXT
           || COALESCE('|' || LPAD(county.month::TEXT, 2, '0'), '')) AS source_record_id,
       county.capture_id,
       county.run_id,
       county.month,
       -- Stations no county boundary covers, placed on the nearest county
       -- within 1,000 m (the loader's NEAREST_COUNTY_CAP_METRES).
       CASE WHEN county.nearest_stations IS NOT NULL
            THEN 'Includes stations no county boundary covers, placed on this county as the '
                 || 'nearest county boundary within 1,000 metres: ' || county.nearest_stations || '.'
       END AS placement_note
FROM county
JOIN gold_noaa_normals.measure_definition AS definition ON definition.measure = county.measure;

-- The newest published read of each county normal.
CREATE OR REPLACE VIEW gold_noaa_normals.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year, revision.month)
       revision.*
FROM gold_noaa_normals.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year, revision.month,
         revision.published_at DESC, revision.run_id DESC;

CREATE OR REPLACE VIEW gold_noaa_normals.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, observation_basis,
       'noaa_normals_csv:v1'::TEXT AS schema_version, time_grain
FROM gold_noaa_normals.measure_definition;

COMMENT ON SCHEMA gold_noaa_normals IS
    'Policy-free publication views for NOAA climate normals by station and derived county figures.';
