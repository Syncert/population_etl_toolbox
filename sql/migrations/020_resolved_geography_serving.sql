-- An unresolved geography is not a grain the catalog publishes, nor a row the
-- API serves.
--
-- `gold_fbi.crime_observation` has excluded rows whose geography did not
-- resolve since 011: "AND fact.geography_status NOT IN ('ambiguous',
-- 'unsupported')". Its two siblings filtered on the release status alone,
-- while their fact tables admit `geography_status = 'unsupported'` -- a
-- provider grain outside the served vocabulary, with `geo_id IS NULL` by
-- constraint in NASS's case.
--
-- So `/observations` paged those rows out with `geo_id: null` and a
-- `geo_level` outside the five words the consumer guide promises, and the
-- publishers -- which aggregate the grain of every fact row through
-- `gold_glossary.geo_grain`, whose ELSE branch passes an unknown word through
-- on purpose so it "surfaces as itself in the catalog instead of hiding
-- inside a familiar word" -- advertised it. A NASS product with one
-- AGRICULTURAL DISTRICT row published valid_geo_grains = {COUNTY, STATE,
-- UNSUPPORTED}, and a client sending UNSUPPORTED back reached the NASS filter
-- and got an empty 200. That is the class of defect 018 exists to close.
--
-- What this step does not change:
--
--   * the rows stay in silver, where the resolution ledger
--     (`silver_ref.geography_resolution`) records each one with its reason
--     code and the data-quality rules keep counting them. Publication is what
--     changes here, not retention;
--   * `unmapped` stays served. It names a geography whose grain *is* in the
--     vocabulary and whose provider geo_id is real, but which the canonical
--     reference does not hold yet -- DB-003's reviewed rule is that such a
--     miss is "explicit, not silently dropped", and the guide already tells
--     clients the geography catalog is a projection that lags what the
--     observation routes can serve. Only `unsupported` is withdrawn: a grain
--     the vocabulary does not name, which no filter can ask for and no
--     attribution can qualify.
--
-- Rerun-safe: every statement is a CREATE OR REPLACE VIEW of the definition
-- the step before it left, with one predicate added.

-- The CDC observation view as 010 defined it, serving resolved geographies.
CREATE OR REPLACE VIEW gold_cdc.health_observation AS
SELECT fact.observation_sk, fact.asset_id, release.title AS dataset_title,
       fact.release_watermark, fact.measure_id, measure.measure_label,
       measure.topic, fact.value_type_id, measure.value_type_label,
       fact.period_start, fact.period_end, fact.geo_id, fact.geo_sk,
       fact.geo_type, fact.geography_status, fact.value_source, fact.value,
       fact.value_status, fact.unit, fact.adjustment_status,
       fact.confidence_lower, fact.confidence_upper, fact.footnote_code,
       fact.footnote_text, fact.stratum_id, stratum.strata,
       fact.estimate_method, fact.population_basis, fact.total_population,
       fact.population_18_plus, release.methodology_url,
       release.geography_basis, fact.source_record_id, fact.capture_id
FROM silver_cdc.fact_health_observation AS fact
JOIN silver_cdc.dim_dataset_release AS release
  ON release.asset_id = fact.asset_id
 AND release.release_watermark = fact.release_watermark
JOIN silver_cdc.dim_measure AS measure
  ON measure.asset_id = fact.asset_id
 AND measure.measure_id = fact.measure_id
 AND measure.value_type_id = fact.value_type_id
JOIN silver_cdc.dim_stratum AS stratum USING (stratum_id)
WHERE release.status = 'published'
  AND fact.geography_status <> 'unsupported';

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

-- FBI's publisher, for the same reason. `gold_fbi.crime_observation` has
-- excluded unresolved geographies since 011, but its publisher aggregates
-- `valid_geo_grains` from every fact row of a published release -- so the
-- catalog could advertise a grain taken from a row the source's own
-- observation view refuses to serve. The grains are the subject types the
-- served rows carry, and now only those.
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
      AND fact.geography_status NOT IN ('ambiguous', 'unsupported')
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

COMMENT ON VIEW gold_cdc.health_observation IS
    'Published CDC observations whose geography resolved to a served grain; '
    'unsupported provider geographies stay in silver with the resolution ledger.';
COMMENT ON VIEW gold_nass.crop_observation IS
    'Published USDA NASS observations whose geography resolved to a served grain; '
    'unsupported aggregate levels stay in silver with the resolution ledger.';
