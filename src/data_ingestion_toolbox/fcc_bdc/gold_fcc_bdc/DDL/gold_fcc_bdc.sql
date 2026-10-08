-- Publication views over FCC Broadband Data Collection fixed availability
-- summaries (fcc-bdc). Applied by the bootstrap manifest in the `gold` phase
-- and re-applied by the DAG's `ensure_fcc_bdc_schema` task; this file is the
-- only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_fcc_bdc;

CREATE OR REPLACE VIEW gold_fcc_bdc.measure_definition AS
SELECT measure.measure, measure.technology, measure.tier_column, measure.measure_label, measure.unit,
       'FCC National Broadband Map, fixed broadband availability summary (total area, residential '
       || 'units): what providers report they could serve, not what households subscribe to and not '
       || 'a measured speed; distinct from the ACS broadband subscription measures. The FCC '
       || 'aggregates its Broadband Serviceable Locations to the nation, states, counties and places '
       || 'itself; this warehouse does not re-aggregate. ' || measure.definition
       || ' Source: Federal Communications Commission, National Broadband Map (Broadband Data '
       || 'Collection).' AS observation_basis
FROM (VALUES
    ('residential_units', 'Any Technology', NULL, 'Residential broadband serviceable units', 'units',
     'The count of residential units at broadband serviceable locations, the denominator of the shares.'),
    ('share_any_25_3', 'Any Technology', 'speed_25_3',
     'Share of residential units with fixed broadband reported at 25/3 Mbps or faster', 'share of units',
     'The share of units where any provider reports fixed service of at least 25 Mbps down and 3 up, by any technology.'),
    ('share_any_100_20', 'Any Technology', 'speed_100_20',
     'Share of residential units with fixed broadband reported at 100/20 Mbps or faster', 'share of units',
     'The share of units where any provider reports fixed service of at least 100 Mbps down and 20 up, by any technology.'),
    ('share_any_1000_100', 'Any Technology', 'speed_1000_100',
     'Share of residential units with fixed broadband reported at 1000/100 Mbps or faster', 'share of units',
     'The share of units where any provider reports fixed service of at least 1000 Mbps down and 100 up, by any technology.'),
    ('share_terrestrial_100_20', 'Any Terrestrial', 'speed_100_20',
     'Share of residential units with terrestrial fixed broadband reported at 100/20 Mbps or faster',
     'share of units',
     'As share_any_100_20, counting terrestrial technologies only (not satellite).'),
    ('share_wired_100_20', 'All Wired', 'speed_100_20',
     'Share of residential units with wired broadband reported at 100/20 Mbps or faster', 'share of units',
     'As share_any_100_20, counting wired technologies only (copper, cable, fiber).')
) AS measure(measure, technology, tier_column, measure_label, unit, definition);

-- Every kept summary row of every published vintage, with its file's
-- revision. Not dispatched; it is the lineage of every served figure.
CREATE OR REPLACE VIEW gold_fcc_bdc.availability_observation AS
SELECT availability.*, read.as_of_date, file.file_name, file.revision, read.published_at
FROM silver_fcc_bdc.availability_row AS availability
JOIN control.fcc_bdc_read AS read ON read.run_id = availability.run_id
JOIN control.fcc_bdc_file AS file
  ON file.run_id = availability.run_id AND file.capture_id = availability.capture_id
WHERE read.status = 'published';

CREATE OR REPLACE VIEW gold_fcc_bdc.observation_revision AS
SELECT definition.measure AS metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       EXTRACT(YEAR FROM row.as_of_date)::INTEGER AS year,
       row.as_of_date AS period_start,
       row.as_of_date AS period_end,
       row.geo_id,
       row.geo_sk,
       row.geography_type AS geo_type,
       gold_glossary.geo_grain(row.geography_type) AS geo_level,
       row.geography_status,
       CASE definition.tier_column
           WHEN 'speed_25_3' THEN row.speed_25_3
           WHEN 'speed_100_20' THEN row.speed_100_20
           WHEN 'speed_1000_100' THEN row.speed_1000_100
           ELSE row.total_units::NUMERIC
       END AS value,
       CASE WHEN definition.tier_column IS NULL THEN 'valid' ELSE row.value_status END AS value_status,
       CASE WHEN definition.tier_column IS NULL THEN NULL ELSE row.missing_reason END AS missing_reason,
       row.total_units,
       row.as_of_date::TEXT AS as_of_date,
       row.revision,
       'BDC ' || row.as_of_date::TEXT || ' rev ' || row.revision || ' read '
           || TO_CHAR(row.published_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS release_key,
       row.published_at,
       md5(definition.measure || '|' || row.geo_id || '|' || row.run_id::TEXT) AS source_record_id,
       row.capture_id,
       row.run_id
FROM gold_fcc_bdc.availability_observation AS row
JOIN gold_fcc_bdc.measure_definition AS definition ON definition.technology = row.technology
WHERE row.geography_status NOT IN ('unsupported', 'ambiguous', 'unmapped');

-- The newest published read of each vintage.
CREATE OR REPLACE VIEW gold_fcc_bdc.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year)
       revision.*
FROM gold_fcc_bdc.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year,
         revision.published_at DESC, revision.run_id DESC;

CREATE OR REPLACE VIEW gold_fcc_bdc.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, observation_basis,
       'fcc_bdc_summary_csv:v1'::TEXT AS schema_version
FROM gold_fcc_bdc.measure_definition;

COMMENT ON SCHEMA gold_fcc_bdc IS
    'Policy-free publication views for FCC fixed broadband availability by nation, state, county and place.';
