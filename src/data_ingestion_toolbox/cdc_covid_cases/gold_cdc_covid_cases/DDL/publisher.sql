-- Provider-neutral glossary publisher contract for the CDC COVID-19 case
-- record counts (outbreak-calibration-sets). One row per registered count
-- and age group; grains aggregated from the rows the latest view actually
-- serves. Applied in the `publisher` phase and re-applied by the DAG's
-- `ensure_cdc_covid_cases_schema` task.
CREATE OR REPLACE VIEW gold_cdc_covid_cases.metric_publisher AS
SELECT 'CDC_COVID_CASES'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       'covid:' || measure.measure_id || ':' || ages.age_key AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (measure.measure_label || ', ' || LOWER(ages.age_label))::TEXT AS metric_display_name,
       'records'::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY['MONTHLY']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_cdc_covid_cases',
           'relation', 'area_revision',
           'key', 'covid:' || measure.measure_id || ':' || ages.age_key
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'CDC COVID-19 Case Surveillance Public Use Data with Geography'::TEXT AS source_name,
       'government-statistics'::TEXT AS source_type,
       'https://data.cdc.gov/Case-Surveillance/COVID-19-Case-Surveillance-Public-Use-Data-with-Ge/n8mc-b4w4'::TEXT AS reference_url
FROM silver_cdc_covid_cases.dim_measure AS measure
CROSS JOIN (VALUES
    ('all_ages', 'All ages'),
    ('age_0_17', 'Ages 0 to 17'),
    ('age_18_49', 'Ages 18 to 49'),
    ('age_50_64', 'Ages 50 to 64'),
    ('age_65_plus', 'Ages 65 and over'),
    ('age_unknown', 'Age not published')
) AS ages (age_key, age_label)
JOIN LATERAL (
    SELECT TO_CHAR(MAX(area.case_month), 'YYYY-MM') AS source_watermark,
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(area.geo_type)
                         ORDER BY gold_glossary.geo_grain(area.geo_type))
                   FILTER (WHERE area.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(snapshot.index_run_id ORDER BY snapshot.published_at DESC))[1] AS source_run_id,
           MAX(snapshot.published_at) AS publication_time
    FROM silver_cdc_covid_cases.area_month AS area
    JOIN control.cdc_covid_cases_snapshot AS snapshot
      ON snapshot.snapshot_id = area.snapshot_id
    WHERE area.measure_id = measure.measure_id
      AND area.age_key = ages.age_key
      AND snapshot.status = 'published'
      AND area.geography_status NOT IN ('unsupported', 'ambiguous')
) AS served ON served.publication_time IS NOT NULL;
