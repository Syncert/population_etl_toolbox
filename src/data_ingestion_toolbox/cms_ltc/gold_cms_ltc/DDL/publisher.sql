-- Provider-neutral glossary publisher contract for CMS long-term care
-- facilities (nursing-home-locations). One row per registered figure; grains
-- aggregated from the rows the latest view actually serves. Applied in the
-- `publisher` phase and re-applied by the DAG's `ensure_cms_ltc_schema` task.
CREATE OR REPLACE VIEW gold_cms_ltc.metric_publisher AS
SELECT 'CMS_LTC'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       measure.series_id || ':' || measure.measure_id AS source_object_key,
       'measure'::TEXT AS source_object_type,
       measure.measure_label::TEXT AS metric_display_name,
       measure.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       CASE measure.accrual WHEN 'quarterly' THEN ARRAY['QUARTERLY'] ELSE ARRAY['ANNUAL'] END::TEXT[]
           AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_cms_ltc',
           'relation', 'area_revision',
           'key', measure.series_id || ':' || measure.measure_id
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       CASE measure.series_id
           WHEN 'characteristics' THEN 'CMS Long-Term Care Facility Characteristics (CMS Form 671)'
           ELSE 'CMS Skilled Nursing Facility Cost Report'
       END::TEXT AS source_name,
       'government-statistics'::TEXT AS source_type,
       CASE measure.series_id
           WHEN 'characteristics' THEN 'https://data.cms.gov/quality-of-care/long-term-care-facility-characteristics'
           ELSE 'https://data.cms.gov/provider-compliance/cost-report/skilled-nursing-facility-cost-report'
       END::TEXT AS reference_url
FROM silver_cms_ltc.dim_measure AS measure
JOIN LATERAL (
    SELECT MAX(latest.period_start)::TEXT AS source_watermark,
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                         ORDER BY gold_glossary.geo_grain(latest.geo_type))
                   FILTER (WHERE latest.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(latest.run_id ORDER BY latest.retrieved_at DESC))[1] AS source_run_id,
           MAX(file.published_at) AS publication_time
    FROM gold_cms_ltc.area_latest AS latest
    JOIN control.cms_ltc_file AS file ON file.run_id = latest.run_id
    WHERE latest.series_id = measure.series_id
      AND latest.measure_id = measure.measure_id
) AS served ON served.publication_time IS NOT NULL;
