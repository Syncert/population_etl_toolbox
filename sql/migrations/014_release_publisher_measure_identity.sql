-- 014: one publisher row per measure identity, not per published release.
--
-- The three release-based sources (CDC, FBI UCR, USDA NASS) each built their
-- `metric_publisher` view with `GROUP BY ... release watermark`, so the view
-- emitted one row per measure *per published release*. The glossary harvest
-- upserts on (source_code, source_object_key) and therefore failed with
-- `ON CONFLICT DO UPDATE command cannot affect row a second time` the first
-- time a second release was published -- that is, on every real refresh after
-- the initial load. `harvest_all_publishers` isolates each publisher, so the
-- failure was recorded as a sanitized error and those catalogs silently
-- stopped following the warehouse.
--
-- `gold_nass.measure_export` carried the same grouping, and it backs
-- `/api/usda-nass/measures`, so that endpoint listed every measure once per
-- published release.
--
-- A publisher row describes a measure, and its watermark names the newest
-- published release that measure appears in -- which is what the CDC and FBI
-- measure exports already did. Valid geography and time grains still come from
-- the published facts across every retained release, because a grain the
-- provider published once remains a valid grain for that measure.

CREATE OR REPLACE VIEW gold_fbi.metric_publisher AS
SELECT 'FBI_UCR'::TEXT AS source_code,
       '1.0'::TEXT AS publisher_contract_version,
       measure.product_id || ':' || measure.measure_id AS source_object_key,
       'measure'::TEXT AS source_object_type,
       (measure.offense_label || ' ' || measure.counted_entity_basis || ' ('
        || measure.measure_form || ')')::TEXT AS metric_display_name,
       measure.unit::TEXT AS units,
       'source_fact'::TEXT AS measure_kind,
       measure.valid_geo_grains,
       ARRAY['MONTHLY']::TEXT[] AS valid_time_grains,
       CASE WHEN measure.measure_form = 'absolute_total'
            THEN 'additive_within_subject'
            ELSE 'non_additive' END::TEXT AS aggregation_characteristic,
       JSONB_BUILD_OBJECT(
           'schema', 'gold_fbi',
           'relation', 'crime_observation',
           'product_id', measure.product_id,
           'measure_id', measure.measure_id
       ) AS physical_lineage,
       release.release_key::TEXT AS source_watermark,
       release.source_run_id,
       release.published_at AS publication_time,
       'Federal Bureau of Investigation Uniform Crime Reporting Program'::TEXT
           AS source_name,
       'government-law-enforcement'::TEXT AS source_type,
       release.methodology_url::TEXT AS reference_url
FROM (
    SELECT measure.product_id, measure.measure_id, measure.offense_label,
           measure.counted_entity_basis, measure.measure_form, measure.unit,
           ARRAY_AGG(DISTINCT UPPER(fact.subject_type)
                     ORDER BY UPPER(fact.subject_type))::TEXT[]
               AS valid_geo_grains
    FROM silver_fbi.dim_offense_measure AS measure
    JOIN silver_fbi.fact_crime_observation AS fact
      ON fact.product_id = measure.product_id
     AND fact.measure_id = measure.measure_id
    JOIN silver_fbi.dim_ucr_dataset_release AS release
      ON release.product_id = fact.product_id
     AND release.release_key = fact.release_key
    WHERE release.status = 'published'
    GROUP BY measure.product_id, measure.measure_id, measure.offense_label,
             measure.counted_entity_basis, measure.measure_form, measure.unit
) AS measure
JOIN LATERAL (
    SELECT candidate.*
    FROM silver_fbi.dim_ucr_dataset_release AS candidate
    WHERE candidate.product_id = measure.product_id
      AND candidate.status = 'published'
    ORDER BY candidate.refresh_date DESC
    LIMIT 1
) AS release ON TRUE;


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
           ARRAY_AGG(DISTINCT UPPER(fact.geo_type)
                     ORDER BY UPPER(fact.geo_type))::TEXT[] AS valid_geo_grains
    FROM silver_cdc.dim_measure AS measure
    JOIN silver_cdc.fact_health_observation AS fact
      ON fact.asset_id = measure.asset_id
     AND fact.measure_id = measure.measure_id
     AND fact.value_type_id = measure.value_type_id
    JOIN silver_cdc.dim_dataset_release AS release
      ON release.asset_id = fact.asset_id
     AND release.release_watermark = fact.release_watermark
    WHERE release.status = 'published'
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


CREATE OR REPLACE VIEW gold_nass.measure_export AS
SELECT statistic.product_id AS source_dataset,
       statistic.statistic_sk AS source_measure_code,
       statistic.short_desc AS display_name,
       statistic.statisticcat_desc,
       statistic.unit_desc AS unit,
       statistic.freq_desc,
       statistic.value_kind,
       statistic.calculation_basis,
       statistic.additive_behavior,
       statistic.additive_behavior_known,
       statistic.source_desc AS source_program,
       release.release_watermark AS source_watermark,
       release.methodology_url,
       release.parser_contract_version AS schema_version
FROM (
    SELECT release.product_id, statistic.statistic_sk, statistic.short_desc,
           statistic.statisticcat_desc, statistic.unit_desc,
           statistic.freq_desc, statistic.value_kind,
           statistic.calculation_basis, statistic.additive_behavior,
           statistic.additive_behavior_known, statistic.source_desc
    FROM silver_nass.dim_statistic AS statistic
    JOIN silver_nass.fact_crop_observation AS fact
      ON fact.statistic_sk = statistic.statistic_sk
    JOIN silver_nass.dim_dataset_release AS release
      ON release.product_id = fact.product_id
     AND release.release_watermark = fact.release_watermark
    WHERE release.status = 'published'
    GROUP BY release.product_id, statistic.statistic_sk, statistic.short_desc,
             statistic.statisticcat_desc, statistic.unit_desc,
             statistic.freq_desc, statistic.value_kind,
             statistic.calculation_basis, statistic.additive_behavior,
             statistic.additive_behavior_known, statistic.source_desc
) AS statistic
JOIN LATERAL (
    SELECT candidate.*
    FROM silver_nass.dim_dataset_release AS candidate
    WHERE candidate.product_id = statistic.product_id
      AND candidate.status = 'published'
    ORDER BY candidate.release_watermark DESC
    LIMIT 1
) AS release ON TRUE;


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
           ARRAY_AGG(DISTINCT UPPER(fact.geo_type)
                     ORDER BY UPPER(fact.geo_type))::TEXT[] AS valid_geo_grains,
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
