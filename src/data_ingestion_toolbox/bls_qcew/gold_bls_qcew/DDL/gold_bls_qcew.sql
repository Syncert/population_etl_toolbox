-- Publication views over reconciled BLS QCEW observations
-- (bls-qcew-county-wages). Applied by the bootstrap manifest in the `gold`
-- phase and re-applied by the DAG's `ensure_bls_qcew_schema` task; this file
-- is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_bls_qcew;

-- Every published capture of every observation: the as-released surface. A
-- revised file for the same slice is a second row with its own capture and
-- retrieval time, never an overwrite. The fact's CHECK admits only
-- `resolved` and `unmapped`; the predicate states the serving rule (DB-035).
CREATE OR REPLACE VIEW gold_bls_qcew.observation_revision AS
SELECT fact.measure_id || ':' || fact.industry_code || ':' || fact.own_code AS metric_key,
       fact.measure_id,
       measure.measure_label,
       measure.unit,
       measure.period_kind,
       measure.observation_basis,
       fact.industry_code,
       industry.industry_title,
       industry.industry_level,
       fact.own_code,
       CASE fact.own_code WHEN '0' THEN 'Total covered' WHEN '5' THEN 'Private' END AS ownership_title,
       fact.year,
       fact.period,
       fact.period_start,
       fact.period_end,
       fact.geo_id,
       fact.geo_sk,
       fact.geo_type,
       gold_glossary.geo_grain(fact.geo_type) AS geo_level,
       fact.geography_status,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.disclosure_code,
       fact.retrieved_at,
       TO_CHAR(fact.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id
FROM silver_bls_qcew.fact_observation AS fact
JOIN silver_bls_qcew.dim_measure AS measure ON measure.measure_id = fact.measure_id
JOIN silver_bls_qcew.dim_industry AS industry ON industry.industry_code = fact.industry_code
JOIN control.bls_qcew_slice AS slice ON slice.capture_id = fact.capture_id
WHERE slice.status = 'published'
  AND fact.geography_status NOT IN ('unsupported', 'ambiguous');

-- The newest published capture of each observation.
CREATE OR REPLACE VIEW gold_bls_qcew.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.period_start)
       revision.*
FROM gold_bls_qcew.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.period_start,
         revision.retrieved_at DESC, revision.capture_id DESC;

CREATE OR REPLACE VIEW gold_bls_qcew.measure_export AS
SELECT measure.measure_id || ':' || industry.industry_code || ':' || ownership.own_code AS source_object_key,
       measure.measure_id,
       measure.measure_label,
       measure.unit,
       measure.period_kind,
       measure.observation_basis,
       industry.industry_code,
       industry.industry_title,
       ownership.own_code,
       measure.parser_contract_version AS schema_version
FROM silver_bls_qcew.dim_measure AS measure
CROSS JOIN silver_bls_qcew.dim_industry AS industry
CROSS JOIN (VALUES ('0'), ('5')) AS ownership(own_code)
WHERE ownership.own_code = '5' OR industry.industry_level = 'total';

COMMENT ON SCHEMA gold_bls_qcew IS
    'Policy-free publication views for BLS QCEW establishment-based employment and wages.';
