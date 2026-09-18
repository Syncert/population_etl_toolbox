-- Provider-neutral glossary publisher contract for USDA NASS.
--
-- One row per measure identity, with the geography grains derived through
-- `gold_glossary.geo_grain` and aggregated from the rows the crop observation
-- view actually serves. Applied in the `publisher` phase and re-applied by
-- the `usda_nass_crop_ingest` DAG's `ensure_nass_schema` task.

CREATE OR REPLACE VIEW gold_nass.metric_publisher AS
SELECT 'USDA_NASS'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       (statistic.product_id || ':' || statistic.statistic_sk)::TEXT
           AS source_object_key,
       'statistic'::TEXT AS source_object_type,
       statistic.short_desc::TEXT AS metric_display_name,
       statistic.unit_desc::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       statistic.valid_geo_grains,
       statistic.valid_time_grains,
       statistic.additive_behavior::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_nass',
           'relation', 'crop_observation',
           'product_id', statistic.product_id,
           'statistic_sk', statistic.statistic_sk,
           'statisticcat_desc', statistic.statisticcat_desc,
           'unit_desc', statistic.unit_desc
       ) AS physical_lineage,
       release.release_watermark::TEXT AS source_watermark,
       release.source_run_id,
       release.published_at AS publication_time,
       'USDA National Agricultural Statistics Service'::TEXT AS source_name,
       'government-agricultural-statistics'::TEXT AS source_type,
       release.methodology_url::TEXT AS reference_url
FROM (
    SELECT fact.product_id, fact.statistic_sk, statistic.short_desc,
           statistic.unit_desc, statistic.statisticcat_desc,
           statistic.additive_behavior,
           ARRAY_AGG(DISTINCT gold_glossary.geo_grain(fact.geo_type)
                     ORDER BY gold_glossary.geo_grain(fact.geo_type))::TEXT[]
               AS valid_geo_grains,
           ARRAY_AGG(DISTINCT UPPER(statistic.freq_desc)
                     ORDER BY UPPER(statistic.freq_desc))::TEXT[]
               AS valid_time_grains
    FROM silver_nass.dim_statistic AS statistic
    JOIN silver_nass.fact_crop_observation AS fact
      ON fact.statistic_sk = statistic.statistic_sk
    JOIN silver_nass.dim_dataset_release AS release
      ON release.product_id = fact.product_id
     AND release.release_watermark = fact.release_watermark
    WHERE release.status = 'published'
      AND fact.geography_status <> 'unsupported'
    GROUP BY fact.product_id, fact.statistic_sk, statistic.short_desc,
             statistic.unit_desc, statistic.statisticcat_desc,
             statistic.additive_behavior
) AS statistic
JOIN LATERAL (
    SELECT candidate.*
    FROM silver_nass.dim_dataset_release AS candidate
    WHERE candidate.product_id = statistic.product_id
      AND candidate.status = 'published'
    ORDER BY candidate.release_watermark DESC
    LIMIT 1
) AS release ON TRUE;
