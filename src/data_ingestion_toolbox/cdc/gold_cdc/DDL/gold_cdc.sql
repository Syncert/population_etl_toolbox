-- Publication views over validated CDC observations and measures.
--
-- Applied by the bootstrap manifest in the `gold` phase and re-applied by the
-- `cdc_ingest` DAG's `ensure_cdc_schema` task. This file is the only
-- definition of these views; a step that changes one edits it here rather
-- than restating a view body in a migration.

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

CREATE OR REPLACE VIEW gold_cdc.latest_release_observation AS
SELECT observation.*
FROM gold_cdc.health_observation AS observation
JOIN (
    SELECT asset_id, MAX(release_watermark::BIGINT) AS release_watermark
    FROM silver_cdc.dim_dataset_release
    WHERE status = 'published'
    GROUP BY asset_id
) AS latest
  ON latest.asset_id = observation.asset_id
 AND latest.release_watermark::TEXT = observation.release_watermark;

CREATE OR REPLACE VIEW gold_cdc.measure_export AS
SELECT measure.asset_id AS source_dataset,
       measure.measure_id AS source_measure_code,
       measure.value_type_id AS source_value_type_code,
       measure.measure_label AS display_name,
       measure.topic, measure.value_type_label, measure.unit,
       measure.adjustment_status, measure.estimate_method,
       measure.population_basis,
       release.release_watermark AS source_watermark,
       release.methodology_url,
       release.parser_contract_version AS schema_version
FROM silver_cdc.dim_measure AS measure
JOIN LATERAL (
    SELECT candidate.*
    FROM silver_cdc.dim_dataset_release AS candidate
    WHERE candidate.asset_id = measure.asset_id
      AND candidate.status = 'published'
    ORDER BY candidate.release_watermark::BIGINT DESC
    LIMIT 1
) AS release ON TRUE;

COMMENT ON SCHEMA gold_cdc IS
    'Policy-free publication views for validated CDC observations and measures.';

COMMENT ON VIEW gold_cdc.health_observation IS
    'Published CDC observations whose geography resolved to a served grain; '
    'unsupported provider geographies stay in silver with the resolution ledger.';

COMMENT ON VIEW gold_cdc.measure_export IS
    'Provider-neutral glossary publisher contract; owns no gold_glossary objects.';
