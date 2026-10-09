-- Publication views over reconciled Census SAIPE and SAHIE estimates
-- (census-saipe-sahie). Applied by the bootstrap manifest in the `gold` phase
-- and re-applied by the DAG's `ensure_census_sae_schema` task; this file is
-- the only definition of these views.

-- Every published capture of every estimate: the as-released surface. A
-- revised publication of the same (measure, year, place) is a second row with
-- its own capture and retrieval time, never an overwrite. The fact table's
-- CHECK admits only `resolved` and `unmapped` today; the predicate states the
-- serving rule (DB-035) so a status added later is not served by default.
CREATE OR REPLACE VIEW gold_census_sae.estimate_revision AS
SELECT fact.dataset_id,
       fact.measure_id,
       fact.dataset_id || ':' || fact.measure_id AS metric_key,
       measure.measure_label,
       measure.unit,
       measure.universe,
       measure.estimate_method,
       measure.methodology_url,
       fact.estimate_year,
       MAKE_DATE(fact.estimate_year, 1, 1) AS period_start,
       MAKE_DATE(fact.estimate_year, 12, 31) AS period_end,
       fact.geo_id,
       fact.geo_sk,
       fact.geo_type,
       gold_glossary.geo_grain(fact.geo_type) AS geo_level,
       fact.geography_status,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.confidence_lower,
       fact.confidence_upper,
       fact.margin_of_error,
       fact.retrieved_at,
       TO_CHAR(fact.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id
FROM silver_census_sae.fact_estimate AS fact
JOIN silver_census_sae.dim_measure AS measure
  ON measure.dataset_id = fact.dataset_id
 AND measure.measure_id = fact.measure_id
JOIN control.census_sae_slice AS slice
  ON slice.capture_id = fact.capture_id
WHERE slice.status = 'published'
  AND fact.geography_status NOT IN ('unsupported', 'ambiguous');

-- The newest published capture of each estimate.
CREATE OR REPLACE VIEW gold_census_sae.estimate_latest AS
SELECT DISTINCT ON (revision.dataset_id, revision.measure_id, revision.estimate_year, revision.geo_id)
       revision.*
FROM gold_census_sae.estimate_revision AS revision
ORDER BY revision.dataset_id, revision.measure_id, revision.estimate_year,
         revision.geo_id, revision.retrieved_at DESC, revision.capture_id DESC;

CREATE OR REPLACE VIEW gold_census_sae.measure_export AS
SELECT measure.dataset_id AS source_dataset,
       measure.measure_id AS source_measure_code,
       measure.measure_label AS display_name,
       measure.unit,
       measure.universe,
       measure.estimate_method,
       measure.methodology_url,
       measure.parser_contract_version AS schema_version
FROM silver_census_sae.dim_measure AS measure;

COMMENT ON SCHEMA gold_census_sae IS
    'Policy-free publication views for Census SAIPE and SAHIE model-based estimates.';

COMMENT ON VIEW gold_census_sae.estimate_latest IS
    'The newest published capture of each SAIPE/SAHIE estimate, with its 90 percent bounds.';
