-- Publication views over reconciled BEA regional observations
-- (bea-regional-accounts). Applied by the bootstrap manifest in the `gold`
-- phase and re-applied by the DAG's `ensure_bea_schema` task; this file is
-- the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_bea;

-- Every published capture of every observation: the as-released surface.
-- A later release revising an earlier year is a second row with its own
-- release date, never an overwrite. The fact's CHECK admits only `resolved`
-- and `unmapped`; the predicate states the serving rule (DB-035).
CREATE OR REPLACE VIEW gold_bea.observation_revision AS
SELECT fact.table_code || ':' || fact.line_code AS metric_key,
       fact.table_code,
       line.table_title,
       fact.line_code,
       line.description,
       line.unit,
       line.dollar_basis,
       line.observation_basis,
       fact.year,
       MAKE_DATE(fact.year, 1, 1) AS period_start,
       MAKE_DATE(fact.year, 12, 31) AS period_end,
       fact.geo_id,
       fact.geo_sk,
       fact.geo_type,
       gold_glossary.geo_grain(fact.geo_type) AS geo_level,
       fact.geography_status,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.release_date,
       fact.release_date::TEXT AS release_key,
       fact.retrieved_at,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id
FROM silver_bea.fact_observation AS fact
JOIN silver_bea.dim_line AS line
  ON line.table_code = fact.table_code
 AND line.line_code = fact.line_code
JOIN control.bea_table_capture AS capture ON capture.capture_id = fact.capture_id
WHERE capture.status = 'published'
  AND fact.geography_status NOT IN ('unsupported', 'ambiguous');

-- The newest release of each observation; a recapture of the same release
-- is resolved by retrieval time.
CREATE OR REPLACE VIEW gold_bea.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year)
       revision.*
FROM gold_bea.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year,
         revision.release_date DESC, revision.retrieved_at DESC, revision.capture_id DESC;

CREATE OR REPLACE VIEW gold_bea.measure_export AS
SELECT line.table_code || ':' || line.line_code AS source_object_key,
       line.table_code,
       line.table_title,
       line.line_code,
       line.description,
       line.unit,
       line.dollar_basis,
       line.observation_basis,
       line.parser_contract_version AS schema_version
FROM silver_bea.dim_line AS line;

COMMENT ON SCHEMA gold_bea IS
    'Policy-free publication views for BEA regional personal income and GDP.';
