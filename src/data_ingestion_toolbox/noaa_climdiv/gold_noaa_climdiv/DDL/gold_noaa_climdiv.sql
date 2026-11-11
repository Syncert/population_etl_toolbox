-- Publication views over NOAA nClimDiv county monthly temperature and
-- precipitation (noaa-climdiv). Applied by the bootstrap manifest in the
-- `gold` phase and re-applied by the DAG's `ensure_noaa_climdiv_schema` task;
-- this file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_noaa_climdiv;

CREATE OR REPLACE VIEW gold_noaa_climdiv.measure_definition AS
SELECT measure.measure, measure.measure_label, measure.unit, measure.file_element,
       'NOAA nClimDiv county series: the value NCEI publishes for the county and calendar '
       || 'month, an area-weighted average of its 5 km nClimGrid grid interpolated from '
       || 'GHCN-Daily stations. ' || measure.statistic || ' NCEI revises the latest two '
       || 'calendar years every month; an earlier value changes only with a new version. '
       || 'Monthly values are not added into annual figures here. Source: NOAA National '
       || 'Centers for Environmental Information, nClimDiv.' AS observation_basis,
       'MONTHLY'::TEXT AS time_grain
FROM (VALUES
    ('monthly_precipitation_total', 'Monthly precipitation (nClimDiv county)', 'inches', 'pcpn',
     'Total precipitation for the month, inches to hundredths.'),
    ('monthly_average_temperature', 'Monthly average temperature (nClimDiv county)',
     'degrees Fahrenheit', 'tmpc', 'Average temperature for the month, degrees F to tenths.'),
    ('monthly_maximum_temperature', 'Monthly average daily maximum temperature (nClimDiv county)',
     'degrees Fahrenheit', 'tmax', 'Average of daily maximum temperatures, degrees F to tenths.'),
    ('monthly_minimum_temperature', 'Monthly average daily minimum temperature (nClimDiv county)',
     'degrees Fahrenheit', 'tmin', 'Average of daily minimum temperatures, degrees F to tenths.'),
    ('monthly_heating_degree_days', 'Monthly heating degree days, base 65 F (nClimDiv county)',
     'degree days (base 65 F)', 'hddc',
     'Heating degree days for the month, base 65 F; not published for Alaska or Hawaii.'),
    ('monthly_cooling_degree_days', 'Monthly cooling degree days, base 65 F (nClimDiv county)',
     'degree days (base 65 F)', 'cddc',
     'Cooling degree days for the month, base 65 F; not published for Alaska or Hawaii.')
) AS measure(measure, measure_label, unit, file_element, statistic);

-- Every published monthly value: the current one and each a later release
-- replaced, with the release that published it. A month NCEI has not
-- published yet is not served.
CREATE OR REPLACE VIEW gold_noaa_climdiv.observation_revision AS
SELECT value_row.measure AS metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       value_row.year,
       value_row.month,
       MAKE_DATE(value_row.year, value_row.month, 1) AS period_start,
       (MAKE_DATE(value_row.year, value_row.month, 1) + INTERVAL '1 month - 1 day')::DATE AS period_end,
       value_row.geo_id,
       value_row.geo_sk,
       'county'::TEXT AS geo_type,
       gold_glossary.geo_grain('county') AS geo_level,
       value_row.value_source,
       value_row.value,
       value_row.value_status,
       'nClimDiv ' || file.file_name AS release_key,
       file.published_at,
       value_row.source_record_id,
       value_row.capture_id,
       value_row.run_id,
       value_row.source_line
FROM (
    SELECT current.measure, current.year, current.month, current.geo_id, current.geo_sk,
           current.geography_status, current.value_source, current.value, current.value_status,
           current.source_record_id, current.capture_id, current.run_id, current.source_line
    FROM silver_noaa_climdiv.county_month AS current
    UNION ALL
    SELECT revision.measure, revision.year, revision.month, revision.geo_id, revision.geo_sk,
           revision.geography_status, revision.value_source, revision.value, revision.value_status,
           revision.source_record_id, revision.capture_id, revision.run_id, revision.source_line
    FROM silver_noaa_climdiv.county_month_revision AS revision
) AS value_row
JOIN gold_noaa_climdiv.measure_definition AS definition ON definition.measure = value_row.measure
JOIN control.noaa_climdiv_file AS file
  ON file.run_id = value_row.run_id AND file.file_element = definition.file_element
WHERE file.status = 'published'
  AND value_row.geography_status NOT IN ('unmapped', 'ambiguous', 'unsupported')
  AND value_row.value_status = 'valid';

-- The current published value of each county month. Read straight from the
-- current table, so a county's whole series is one index range.
CREATE OR REPLACE VIEW gold_noaa_climdiv.observation_latest AS
SELECT current.measure AS metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       current.year,
       current.month,
       MAKE_DATE(current.year, current.month, 1) AS period_start,
       (MAKE_DATE(current.year, current.month, 1) + INTERVAL '1 month - 1 day')::DATE AS period_end,
       current.geo_id,
       current.geo_sk,
       'county'::TEXT AS geo_type,
       gold_glossary.geo_grain('county') AS geo_level,
       current.value_source,
       current.value,
       current.value_status,
       'nClimDiv ' || file.file_name AS release_key,
       file.published_at,
       current.source_record_id,
       current.capture_id,
       current.run_id,
       current.source_line
FROM silver_noaa_climdiv.county_month AS current
JOIN gold_noaa_climdiv.measure_definition AS definition ON definition.measure = current.measure
JOIN control.noaa_climdiv_file AS file
  ON file.run_id = current.run_id AND file.file_element = definition.file_element
WHERE file.status = 'published'
  AND current.geography_status NOT IN ('unmapped', 'ambiguous', 'unsupported')
  AND current.value_status = 'valid';

CREATE OR REPLACE VIEW gold_noaa_climdiv.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, observation_basis,
       'noaa_climdiv_county:v1'::TEXT AS schema_version, time_grain
FROM gold_noaa_climdiv.measure_definition;

COMMENT ON SCHEMA gold_noaa_climdiv IS
    'Policy-free publication views for NOAA nClimDiv county monthly temperature and precipitation.';
