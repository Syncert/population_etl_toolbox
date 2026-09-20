-- Provider-neutral glossary publisher contract for CDC.
--
-- One row per measure identity, with the geography grains derived through
-- `gold_glossary.geo_grain` and aggregated from the rows the observation view
-- actually serves. Applied in the `publisher` phase and re-applied by the
-- `cdc_ingest` DAG's `ensure_cdc_schema` task.

-- The two publishers as 018 left them. Their grains are aggregated from the
-- fact rows, so the filter belongs here too: a grain no served row carries is
-- not a grain the metric publishes.
CREATE OR REPLACE VIEW gold_cdc.metric_publisher AS
SELECT 'CDC'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       measure.asset_id || ':' || measure.measure_id || ':' ||
           measure.value_type_id AS source_object_key,
       'measure'::TEXT AS source_object_type,
       measure.measure_label::TEXT AS metric_display_name,
       measure.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       measure.valid_geo_grains,
       ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
       NULL::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_cdc',
           'relation', 'health_observation',
           'asset_id', measure.asset_id,
           'measure_id', measure.measure_id,
           'value_type_id', measure.value_type_id
       ) AS physical_lineage,
       release.release_watermark::TEXT AS source_watermark,
       release.source_run_id,
       release.published_at AS publication_time,
       'Centers for Disease Control and Prevention'::TEXT AS source_name,
       'government-public-health'::TEXT AS source_type,
       release.methodology_url::TEXT AS reference_url
FROM (
    SELECT measure.asset_id, measure.measure_id, measure.value_type_id,
           measure.measure_label, measure.unit,
           ARRAY_AGG(DISTINCT gold_glossary.geo_grain(fact.geo_type)
                     ORDER BY gold_glossary.geo_grain(fact.geo_type))::TEXT[]
               AS valid_geo_grains
    FROM silver_cdc.dim_measure AS measure
    JOIN silver_cdc.fact_health_observation AS fact
      ON fact.asset_id = measure.asset_id
     AND fact.measure_id = measure.measure_id
     AND fact.value_type_id = measure.value_type_id
    JOIN silver_cdc.dim_dataset_release AS release
      ON release.asset_id = fact.asset_id
     AND release.release_watermark = fact.release_watermark
    WHERE release.status = 'published'
      AND fact.geography_status <> 'unsupported'
    GROUP BY measure.asset_id, measure.measure_id, measure.value_type_id,
             measure.measure_label, measure.unit
) AS measure
JOIN LATERAL (
    SELECT candidate.*
    FROM silver_cdc.dim_dataset_release AS candidate
    WHERE candidate.asset_id = measure.asset_id
      AND candidate.status = 'published'
    ORDER BY candidate.release_watermark::BIGINT DESC
    LIMIT 1
) AS release ON TRUE;
