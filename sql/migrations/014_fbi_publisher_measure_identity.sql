-- 014: one FBI publisher row per measure identity, not per published release.
--
-- `gold_fbi.metric_publisher` (migration 011) grouped by `release.release_key`,
-- so the view emitted one row per measure *per published release*. The glossary
-- harvest upserts on (source_code, source_object_key) and therefore failed with
-- `ON CONFLICT DO UPDATE command cannot affect row a second time` the first
-- time a second FBI release was published -- that is, on every real refresh
-- after the initial load. `harvest_all_publishers` isolates each publisher, so
-- the failure was recorded as a sanitized error and the FBI catalog silently
-- stopped following the warehouse.
--
-- A publisher row describes a measure, and its watermark names the newest
-- published release that measure appears in. Valid geography grains still come
-- from the published facts across every retained release, because a grain the
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
