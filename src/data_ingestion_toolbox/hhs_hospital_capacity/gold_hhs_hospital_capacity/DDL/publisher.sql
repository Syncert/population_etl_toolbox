-- Provider-neutral glossary publisher contract for HHS hospital capacity
-- (hospital-capacity-and-locations). One row per registered figure; grains
-- aggregated from the rows the latest view actually serves. Applied in the
-- `publisher` phase and re-applied by the DAG's
-- `ensure_hhs_hospital_capacity_schema` task.
CREATE OR REPLACE VIEW gold_hhs_hospital_capacity.metric_publisher AS
SELECT 'HHS_HOSPITAL_CAPACITY'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       'hospital:' || measure.measure_id AS source_object_key,
       'measure'::TEXT AS source_object_type,
       measure.measure_label::TEXT AS metric_display_name,
       measure.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       served.valid_geo_grains,
       ARRAY['WEEKLY']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_hhs_hospital_capacity',
           'relation', 'area_revision',
           'key', 'hospital:' || measure.measure_id
       ) AS physical_lineage,
       served.source_watermark,
       served.source_run_id,
       served.publication_time,
       'HHS COVID-19 Reported Patient Impact and Hospital Capacity by Facility'::TEXT AS source_name,
       'government-statistics'::TEXT AS source_type,
       'https://healthdata.gov/Hospital/COVID-19-Reported-Patient-Impact-and-Hospital-Capa/anag-cw7u'::TEXT AS reference_url
FROM silver_hhs_hospital_capacity.dim_measure AS measure
JOIN LATERAL (
    SELECT MAX(area.collection_week)::TEXT AS source_watermark,
           COALESCE(
               ARRAY_AGG(DISTINCT gold_glossary.geo_grain(area.geo_type)
                         ORDER BY gold_glossary.geo_grain(area.geo_type))
                   FILTER (WHERE area.value IS NOT NULL),
               ARRAY[]::TEXT[]
           )::TEXT[] AS valid_geo_grains,
           (ARRAY_AGG(snapshot.metadata_run_id ORDER BY snapshot.published_at DESC))[1] AS source_run_id,
           MAX(snapshot.published_at) AS publication_time
    FROM silver_hhs_hospital_capacity.area_week AS area
    JOIN control.hhs_hospital_capacity_snapshot AS snapshot
      ON snapshot.snapshot_id = area.snapshot_id
    WHERE area.measure_id = measure.measure_id
      AND snapshot.status = 'published'
      AND area.geography_status NOT IN ('unsupported', 'ambiguous')
) AS served ON served.publication_time IS NOT NULL;
