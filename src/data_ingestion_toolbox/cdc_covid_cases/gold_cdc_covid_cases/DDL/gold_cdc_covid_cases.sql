-- Publication views over the published CDC COVID-19 case surveillance
-- record counts (outbreak-calibration-sets). Applied by the bootstrap
-- manifest in the `gold` phase and re-applied by the DAG's
-- `ensure_cdc_covid_cases_schema` task; this file is the only definition of
-- these views.

CREATE SCHEMA IF NOT EXISTS gold_cdc_covid_cases;

-- Every published snapshot's counts: the as-released surface. Each value
-- counts CDC's published case records of the area's residents for the case
-- month and age group; these are records CDC published, not all cases.
CREATE OR REPLACE VIEW gold_cdc_covid_cases.area_revision AS
SELECT area.measure_id,
       area.age_key,
       'covid:' || area.measure_id || ':' || area.age_key AS metric_key,
       measure.measure_label,
       'records'::TEXT AS unit,
       measure.basis,
       area.case_month AS period_start,
       (area.case_month + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS period_end,
       area.geo_id,
       area.geo_sk,
       area.geo_type,
       gold_glossary.geo_grain(area.geo_type) AS geo_level,
       area.geography_status,
       area.value,
       area.value_status,
       area.case_record_count,
       area.unplaced_record_count,
       'count_of_published_case_records'::TEXT AS derivation,
       'Counts of the case records CDC published; CDC withholds the county where few people live, and states reported with different completeness.'::TEXT AS coverage_note,
       capture.retrieved_at,
       TO_CHAR(capture.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       area.source_record_id,
       snapshot.index_capture_id AS capture_id,
       snapshot.index_run_id AS run_id,
       snapshot.snapshot_id,
       snapshot.published_at
FROM silver_cdc_covid_cases.area_month AS area
JOIN control.cdc_covid_cases_snapshot AS snapshot ON snapshot.snapshot_id = area.snapshot_id
JOIN raw_capture.response_capture AS capture ON capture.capture_id = snapshot.index_capture_id
JOIN silver_cdc_covid_cases.dim_measure AS measure ON measure.measure_id = area.measure_id
WHERE snapshot.status = 'published'
  AND area.geography_status NOT IN ('unsupported', 'ambiguous');

-- The newest published snapshot's count of each area, month, age group and measure.
CREATE OR REPLACE VIEW gold_cdc_covid_cases.area_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.period_start)
       revision.*
FROM gold_cdc_covid_cases.area_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.period_start,
         revision.published_at DESC, revision.snapshot_id DESC;

CREATE OR REPLACE VIEW gold_cdc_covid_cases.measure_export AS
SELECT 'n8mc-b4w4'::TEXT AS source_dataset,
       measure.measure_id AS source_measure_code,
       measure.measure_label AS display_name,
       'records'::TEXT AS unit,
       measure.basis,
       measure.parser_contract_version AS schema_version
FROM silver_cdc_covid_cases.dim_measure AS measure;

COMMENT ON SCHEMA gold_cdc_covid_cases IS
    'Policy-free publication views for CDC COVID-19 case surveillance record counts by county, month and age group, 2020 to 2024.';

COMMENT ON VIEW gold_cdc_covid_cases.area_latest IS
    'Counts of CDC published COVID-19 case records by county, state and nation of residence, case month and age group.';
