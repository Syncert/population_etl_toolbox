-- Publication views over EPA air quality monitor-years (AirData annual
-- monitor files, epa-aqs). Applied by the bootstrap manifest in the `gold`
-- phase and re-applied by the DAG's `ensure_epa_aqs_schema` task; this file
-- is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_epa_aqs;

CREATE OR REPLACE VIEW gold_epa_aqs.measure_definition AS
SELECT measure.measure, measure.measure_label, measure.unit,
       'EPA Air Quality System (AirData annual monitor file): this warehouse''s county figure '
       || 'is the highest value among the county''s monitors with a complete year (Completeness '
       || 'Indicator Y), from every measured value with exceptional events included; it is a '
       || 'derived summary, not an EPA design value or a regulatory determination. '
       || measure.statistic || ' Source: U.S. Environmental Protection Agency, Air Quality '
       || 'System (AirData).' AS observation_basis
FROM (VALUES
    ('pm25_annual_mean', 'PM2.5 annual mean, highest complete monitor', 'micrograms per cubic meter',
     'The statistic is the annual arithmetic mean under the 2024 annual PM2.5 standard (FRM/FEM, parameter 88101).'),
    ('ozone_8hour_4th_max', 'Ozone fourth-highest daily maximum 8-hour average, highest complete monitor',
     'parts per million',
     'The statistic is the year''s fourth-highest daily maximum 8-hour average under the 2015 ozone standard (parameter 44201).')
) AS measure(measure, measure_label, unit, statistic);

-- Every published monitor-year row: the provider's own figure per monitor,
-- event type and certification included. Not dispatched (the API has no
-- monitor grain); it is the lineage of every county figure.
CREATE OR REPLACE VIEW gold_epa_aqs.monitor_observation AS
SELECT fact.*, file.published_at
FROM silver_epa_aqs.monitor_fact AS fact
JOIN control.epa_aqs_file AS file ON file.run_id = fact.run_id
WHERE file.status = 'published';

-- The county figure for each published file: the highest complete monitor's
-- statistic among every-measured-value rows. A county with no complete
-- monitor has no row.
CREATE OR REPLACE VIEW gold_epa_aqs.observation_revision AS
WITH eligible AS (
    SELECT monitor.*
    FROM gold_epa_aqs.monitor_observation AS monitor
    WHERE monitor.completeness = 'Y'
      AND monitor.event_type IN ('No Events', 'Events Included')
      AND monitor.value_status = 'valid'
),
ranked AS (
    SELECT eligible.*,
           ROW_NUMBER() OVER (
               PARTITION BY eligible.run_id, eligible.measure, eligible.geo_id
               ORDER BY eligible.value DESC, eligible.monitor_id
           ) AS rank,
           COUNT(*) OVER (PARTITION BY eligible.run_id, eligible.measure, eligible.geo_id) AS monitors
    FROM eligible
)
SELECT ranked.measure AS metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       ranked.year,
       MAKE_DATE(ranked.year, 1, 1) AS period_start,
       MAKE_DATE(ranked.year, 12, 31) AS period_end,
       ranked.geo_id,
       ranked.geo_sk,
       'county'::TEXT AS geo_type,
       gold_glossary.geo_grain('county') AS geo_level,
       ranked.geography_status,
       ranked.value,
       'valid'::TEXT AS value_status,
       ranked.monitor_id AS highest_monitor,
       ranked.monitors AS complete_monitors,
       ranked.certification,
       'AirData ' || ranked.year::TEXT || ' read '
           || TO_CHAR(ranked.published_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS release_key,
       ranked.published_at,
       md5(ranked.measure || '|' || ranked.geo_id || '|' || ranked.year::TEXT || '|' || ranked.run_id::TEXT)
           AS source_record_id,
       ranked.capture_id,
       ranked.run_id
FROM ranked
JOIN gold_epa_aqs.measure_definition AS definition ON definition.measure = ranked.measure
WHERE ranked.rank = 1
  AND ranked.geography_status NOT IN ('unsupported', 'ambiguous', 'unmapped');

-- The newest published read of each county-year.
CREATE OR REPLACE VIEW gold_epa_aqs.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year)
       revision.*
FROM gold_epa_aqs.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year,
         revision.published_at DESC, revision.run_id DESC;

CREATE OR REPLACE VIEW gold_epa_aqs.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, observation_basis,
       'epa_aqs_csv:v1'::TEXT AS schema_version
FROM gold_epa_aqs.measure_definition;

COMMENT ON SCHEMA gold_epa_aqs IS
    'Policy-free publication views for EPA air quality monitor-years and derived county figures.';
