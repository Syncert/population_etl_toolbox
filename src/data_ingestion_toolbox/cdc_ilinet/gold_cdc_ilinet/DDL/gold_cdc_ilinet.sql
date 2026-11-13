-- Publication views over published CDC FluView ILINet releases
-- (outbreak-calibration-sets). Applied by the bootstrap manifest in the
-- `gold` phase and re-applied by the DAG's `ensure_cdc_ilinet_schema` task;
-- this file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_cdc_ilinet;

-- Every published revision of every served figure: the as-released surface.
-- A revision is the figure as one release first carried it or changed it.
-- HHS regions and jurisdictions CDC reports beside their state are kept in
-- silver and not served here.
CREATE OR REPLACE VIEW gold_cdc_ilinet.area_revision AS
SELECT value.measure_id,
       'ilinet:' || value.measure_id AS metric_key,
       measure.measure_label,
       measure.unit,
       measure.basis,
       value.week_start AS period_start,
       (value.week_start + 6) AS period_end,
       value.mmwr_year,
       value.mmwr_week,
       region.region AS source_region,
       region.geo_id,
       entity.geo_sk,
       region.geo_type,
       gold_glossary.geo_grain(region.geo_type) AS geo_level,
       region.geography_status,
       region.jurisdiction_note,
       value.value,
       value.value_status,
       value.value_source,
       capture.retrieved_at,
       TO_CHAR(capture.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       md5(value.region_type || '|' || value.region || '|' || value.mmwr_year::TEXT
           || '|' || value.mmwr_week::TEXT || '|' || value.measure_id) AS source_record_id,
       value.capture_id,
       release.run_id,
       release.release_id,
       release.published_at
FROM silver_cdc_ilinet.week_value AS value
JOIN control.cdc_ilinet_release AS release ON release.release_id = value.release_id
JOIN silver_cdc_ilinet.region AS region
  ON region.region_type = value.region_type AND region.region = value.region
JOIN raw_capture.response_capture AS capture ON capture.capture_id = value.capture_id
JOIN silver_cdc_ilinet.dim_measure AS measure ON measure.measure_id = value.measure_id
LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = region.geo_id
WHERE release.status = 'published'
  AND region.geography_status NOT IN ('unsupported', 'ambiguous');

-- The newest published revision of each served figure.
CREATE OR REPLACE VIEW gold_cdc_ilinet.area_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.period_start)
       revision.*
FROM gold_cdc_ilinet.area_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.period_start,
         revision.published_at DESC, revision.release_id DESC;

CREATE OR REPLACE VIEW gold_cdc_ilinet.measure_export AS
SELECT 'fluview_ilinet'::TEXT AS source_dataset,
       measure.measure_id AS source_measure_code,
       measure.measure_label AS display_name,
       measure.unit,
       measure.basis,
       measure.source_column,
       measure.parser_contract_version AS schema_version
FROM silver_cdc_ilinet.dim_measure AS measure;

COMMENT ON SCHEMA gold_cdc_ilinet IS
    'Policy-free publication views for CDC FluView ILINet weekly outpatient influenza-like illness, national from 1997 and by state from 2010.';

COMMENT ON VIEW gold_cdc_ilinet.area_latest IS
    'The newest published ILINet figure of each served region, MMWR week and measure, as CDC published it.';
