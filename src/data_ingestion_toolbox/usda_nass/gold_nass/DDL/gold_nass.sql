-- Deterministic publication views over reconciled, published NASS releases.
--
-- Applied by the bootstrap manifest in the `gold` phase and re-applied by the
-- `usda_nass_crop_ingest` DAG's `ensure_nass_schema` task. This file is the
-- only definition of these views.

-- The USDA NASS crop observation view as 012 defined it, with the same
-- predicate.
CREATE OR REPLACE VIEW gold_nass.crop_observation AS
SELECT fact.observation_sk, fact.product_id, release.label AS product_label,
       fact.release_watermark, release.slice_mode, fact.source_desc,
       commodity.sector_desc, commodity.group_desc, commodity.commodity_desc,
       commodity.class_desc, commodity.prodn_practice_desc,
       commodity.util_practice_desc, fact.commodity_sk,
       statistic.statisticcat_desc, statistic.short_desc, statistic.unit_desc,
       statistic.freq_desc, statistic.value_kind, statistic.calculation_basis,
       statistic.additive_behavior, statistic.additive_behavior_known,
       fact.statistic_sk, domain.domain_desc, domain.domaincat_desc,
       fact.domain_sk, fact.geo_id, fact.geo_sk, fact.geo_type,
       fact.geography_status, fact.geo_source_code, fact.agg_level_desc,
       fact.location_desc, fact.state_fips, fact.county_fips, fact.year,
       fact.begin_code, fact.end_code, fact.reference_period_desc,
       fact.week_ending, fact.value_source, fact.value, fact.value_status,
       fact.suppression_code, fact.cv_source, fact.cv_value, fact.cv_status,
       fact.cv_symbol, fact.load_time, release.methodology_url,
       release.release_expectation, fact.source_record_id, fact.capture_id,
       fact.slice_key
FROM silver_nass.fact_crop_observation AS fact
JOIN silver_nass.dim_dataset_release AS release
  ON release.product_id = fact.product_id
 AND release.release_watermark = fact.release_watermark
JOIN silver_nass.dim_commodity AS commodity USING (commodity_sk)
JOIN silver_nass.dim_statistic AS statistic USING (statistic_sk)
JOIN silver_nass.dim_domain AS domain USING (domain_sk)
WHERE release.status = 'published'
  AND fact.geography_status <> 'unsupported';

CREATE OR REPLACE VIEW gold_nass.crop_series AS
SELECT MD5(
           observation.product_id || '|' || observation.commodity_sk || '|' ||
           observation.statistic_sk || '|' || observation.domain_sk || '|' ||
           COALESCE(observation.geo_id, observation.geo_source_code) || '|' ||
           observation.freq_desc
       ) AS series_id,
       observation.product_id, observation.commodity_sk,
       observation.statistic_sk, observation.domain_sk,
       observation.sector_desc, observation.group_desc,
       observation.commodity_desc, observation.class_desc,
       observation.prodn_practice_desc, observation.util_practice_desc,
       observation.statisticcat_desc, observation.short_desc,
       observation.unit_desc, observation.value_kind,
       observation.additive_behavior, observation.additive_behavior_known,
       observation.domain_desc, observation.domaincat_desc,
       observation.geo_id, observation.geo_type, observation.agg_level_desc,
       observation.freq_desc, observation.source_desc,
       MIN(observation.year) AS first_year,
       MAX(observation.year) AS last_year,
       COUNT(*) AS observation_count,
       COUNT(*) FILTER (WHERE observation.value_status = 'valid')
           AS numeric_observation_count,
       COUNT(*) FILTER (WHERE observation.value_status <> 'valid')
           AS non_numeric_observation_count,
       MAX(observation.release_watermark) AS latest_release_watermark
FROM gold_nass.crop_observation AS observation
GROUP BY observation.product_id, observation.commodity_sk,
         observation.statistic_sk, observation.domain_sk,
         observation.sector_desc, observation.group_desc,
         observation.commodity_desc, observation.class_desc,
         observation.prodn_practice_desc, observation.util_practice_desc,
         observation.statisticcat_desc, observation.short_desc,
         observation.unit_desc, observation.value_kind,
         observation.additive_behavior, observation.additive_behavior_known,
         observation.domain_desc, observation.domaincat_desc,
         observation.geo_id, observation.geo_source_code,
         observation.geo_type, observation.agg_level_desc,
         observation.freq_desc, observation.source_desc;

CREATE OR REPLACE VIEW gold_nass.latest_release_observation AS
SELECT observation.*
FROM gold_nass.crop_observation AS observation
JOIN (
    SELECT product_id, MAX(release_watermark) AS release_watermark
    FROM silver_nass.dim_dataset_release
    WHERE status = 'published'
    GROUP BY product_id
) AS latest
  ON latest.product_id = observation.product_id
 AND latest.release_watermark = observation.release_watermark;

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

COMMENT ON SCHEMA gold_nass IS
    'Policy-free publication views for validated USDA NASS crop observations.';

COMMENT ON VIEW gold_nass.crop_observation IS
    'Published USDA NASS observations whose geography resolved to a served grain; '
    'unsupported aggregate levels stay in silver with the resolution ledger.';

COMMENT ON VIEW gold_nass.measure_export IS
    'Provider-neutral glossary publisher contract; owns no gold_glossary objects.';

COMMENT ON VIEW gold_nass.crop_series IS
    'Stable series identity per commodity, statistic, domain, geography, frequency.';
