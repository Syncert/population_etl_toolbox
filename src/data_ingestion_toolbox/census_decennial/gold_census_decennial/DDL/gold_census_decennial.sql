-- Publication views over reconciled 2020 Census counts (group-quarters-by-type).
-- Applied by the bootstrap manifest in the `gold` phase and re-applied by the
-- DAG's `ensure_census_decennial_schema` task; this file is the only definition
-- of these views.

-- Every published capture of every count: the as-released surface. The count
-- describes Census Day, 2020-04-01, so the period is that one day. A block is
-- `unsupported` (no canonical block geography yet) and is never served; the
-- predicate states the serving rule (DB-035) so a status added later is not
-- served by default.
CREATE OR REPLACE VIEW gold_census_decennial.count_revision AS
SELECT fact.dataset_id,
       fact.variable_id,
       fact.dataset_id || ':' || fact.variable_id AS metric_key,
       variable.variable_label,
       variable.universe,
       variable.unit,
       variable.parent_variable,
       variable.methodology_url,
       2020 AS census_year,
       DATE '2020-04-01' AS period_start,
       DATE '2020-04-01' AS period_end,
       fact.geo_id,
       fact.geo_sk,
       fact.geo_type,
       gold_glossary.geo_grain(fact.geo_type) AS geo_level,
       fact.geography_status,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.retrieved_at,
       TO_CHAR(fact.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id
FROM silver_census_decennial.fact_count AS fact
JOIN silver_census_decennial.dim_variable AS variable
  ON variable.dataset_id = fact.dataset_id
 AND variable.variable_id = fact.variable_id
JOIN control.census_decennial_slice AS slice
  ON slice.capture_id = fact.capture_id
WHERE slice.status = 'published'
  AND fact.geography_status NOT IN ('unsupported', 'ambiguous');

-- The newest published capture of each count.
CREATE OR REPLACE VIEW gold_census_decennial.count_latest AS
SELECT DISTINCT ON (revision.dataset_id, revision.variable_id, revision.geo_id)
       revision.*
FROM gold_census_decennial.count_revision AS revision
ORDER BY revision.dataset_id, revision.variable_id, revision.geo_id,
         revision.retrieved_at DESC, revision.capture_id DESC;

CREATE OR REPLACE VIEW gold_census_decennial.measure_export AS
SELECT variable.dataset_id AS source_dataset,
       variable.variable_id AS source_measure_code,
       variable.variable_label AS display_name,
       variable.unit,
       variable.universe,
       variable.parent_variable,
       variable.methodology_url,
       variable.parser_contract_version AS schema_version
FROM silver_census_decennial.dim_variable AS variable;

COMMENT ON SCHEMA gold_census_decennial IS
    'Policy-free publication views for 2020 Census counts (redistricting data, group quarters by type).';

COMMENT ON VIEW gold_census_decennial.count_latest IS
    'The newest published capture of each 2020 Census count, as of Census Day (2020-04-01).';
