-- Publication views over reconciled Building Permits Survey observations
-- (census-building-permits). Applied by the bootstrap manifest in the `gold`
-- phase and re-applied by the DAG's `ensure_census_bps_schema` task; this
-- file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_census_bps;

-- Every published capture of every observation: the as-released surface. A
-- revised file is a second row with its own capture and retrieval time,
-- never an overwrite. The fact's CHECK admits only `resolved` and
-- `unmapped`; the predicate states the serving rule (DB-035).
CREATE OR REPLACE VIEW gold_census_bps.observation_revision AS
SELECT fact.measure_id || ':' || fact.structure_type || ':' || fact.frequency AS metric_key,
       fact.measure_id,
       measure.measure_label,
       fact.structure_type,
       measure.structure_label,
       fact.frequency,
       measure.unit,
       measure.observation_basis,
       fact.period_start,
       fact.period_end,
       EXTRACT(YEAR FROM fact.period_start)::INTEGER AS year,
       fact.geo_id,
       fact.geo_sk,
       fact.geo_type,
       gold_glossary.geo_grain(fact.geo_type) AS geo_level,
       fact.geography_status,
       fact.value_source,
       fact.value,
       fact.reported_value,
       fact.value_status,
       fact.months_reported,
       fact.retrieved_at,
       TO_CHAR(fact.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id
FROM silver_census_bps.fact_observation AS fact
JOIN silver_census_bps.dim_measure AS measure
  ON measure.measure_id = fact.measure_id
 AND measure.structure_type = fact.structure_type
JOIN control.census_bps_slice AS slice ON slice.capture_id = fact.capture_id
WHERE slice.status = 'published'
  AND fact.geography_status NOT IN ('unsupported', 'ambiguous');

-- The newest published capture of each observation.
CREATE OR REPLACE VIEW gold_census_bps.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.period_start)
       revision.*
FROM gold_census_bps.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.period_start,
         revision.retrieved_at DESC, revision.capture_id DESC;

CREATE OR REPLACE VIEW gold_census_bps.measure_export AS
SELECT measure.measure_id || ':' || measure.structure_type || ':' || frequency.frequency AS source_object_key,
       measure.measure_id,
       measure.measure_label,
       measure.structure_type,
       measure.structure_label,
       frequency.frequency,
       measure.unit,
       measure.observation_basis,
       measure.parser_contract_version AS schema_version
FROM silver_census_bps.dim_measure AS measure
CROSS JOIN (VALUES ('monthly'), ('annual')) AS frequency(frequency);

COMMENT ON SCHEMA gold_census_bps IS
    'Policy-free publication views for Census Building Permits Survey housing units authorized.';
