-- Provider-neutral glossary publisher contract for NCES IPEDS
-- (college-housing-capacity). One row per registered figure; grains
-- aggregated from the rows the latest view actually serves. Applied in the
-- `publisher` phase and re-applied by the DAG's `ensure_nces_ipeds_schema` task.
CREATE OR REPLACE VIEW gold_nces_ipeds.metric_publisher AS
SELECT 'NCES_IPEDS'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       'ipeds:' || measure.measure_id AS source_object_key,
       'measure'::TEXT AS source_object_type,
       measure.measure_label::TEXT AS metric_display_name,
       measure.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_nces_ipeds',
           'relation', 'area_revision',
           'key', 'ipeds:' || measure.measure_id
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'NCES Integrated Postsecondary Education Data System (IPEDS)'::TEXT AS source_name,
       'government-statistics'::TEXT AS source_type,
       'https://nces.ed.gov/ipeds/'::TEXT AS reference_url
FROM silver_nces_ipeds.dim_measure AS measure
JOIN LATERAL (
    SELECT MAX(latest.period_year)::TEXT AS source_watermark,
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(latest.geo_type)
                         ORDER BY gold_glossary.geo_grain(latest.geo_type))
                   FILTER (WHERE latest.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(latest.run_id ORDER BY latest.retrieved_at DESC))[1] AS source_run_id,
           MAX(file.published_at) AS publication_time
    FROM gold_nces_ipeds.area_latest AS latest
    JOIN control.nces_ipeds_file AS file ON file.run_id = latest.run_id
    WHERE latest.measure_id = measure.measure_id
) AS served ON served.publication_time IS NOT NULL;
