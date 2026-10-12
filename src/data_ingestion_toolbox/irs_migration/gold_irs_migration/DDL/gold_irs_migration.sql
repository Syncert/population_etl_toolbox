-- Publication views over reconciled IRS SOI county migration flows
-- (irs-county-migration). Applied by the bootstrap manifest in the `gold`
-- phase and re-applied by the DAG's `ensure_irs_migration_schema` task;
-- this file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_irs_migration;

-- Every published capture of every flow row: the as-released surface. A
-- revised file is a second row with its own capture and retrieval time,
-- never an overwrite. SOI names no release, so the release identity is the
-- read.
CREATE OR REPLACE VIEW gold_irs_migration.flow_revision AS
SELECT fact.direction,
       fact.year_pair,
       fact.year1,
       fact.year2,
       MAKE_DATE(fact.year1, 1, 1) AS period_start,
       MAKE_DATE(fact.year2, 12, 31) AS period_end,
       fact.subject_geo_id,
       fact.subject_geo_sk,
       fact.category,
       fact.counterpart_code,
       fact.counterpart_label,
       fact.origin_geo_id,
       fact.origin_geo_sk,
       fact.destination_geo_id,
       fact.destination_geo_sk,
       fact.returns,
       fact.individuals,
       fact.agi,
       fact.value_status,
       fact.value_source,
       fact.retrieved_at,
       TO_CHAR(fact.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id
FROM silver_irs_migration.fact_flow AS fact
JOIN control.irs_migration_file AS file ON file.capture_id = fact.capture_id
WHERE file.status = 'published';

-- The newest published capture of each row.
CREATE OR REPLACE VIEW gold_irs_migration.flow_latest AS
SELECT DISTINCT ON (revision.direction, revision.year_pair, revision.subject_geo_id, revision.counterpart_code)
       revision.*
FROM gold_irs_migration.flow_revision AS revision
ORDER BY revision.direction, revision.year_pair, revision.subject_geo_id, revision.counterpart_code,
         revision.retrieved_at DESC, revision.capture_id DESC;

-- The file totals: the six header rows each county file carries, one row
-- per measure. These are SOI's own figures for one county, so they are
-- ordinary one-geography observations; nothing here computes a net.
CREATE OR REPLACE VIEW gold_irs_migration.total_observation_revision AS
SELECT revision.direction || ':' || revision.category || ':' || measure.measure AS metric_key,
       revision.direction,
       revision.category,
       measure.measure,
       measure.unit,
       revision.year_pair,
       revision.year2 AS year,
       revision.period_start,
       revision.period_end,
       revision.subject_geo_id AS geo_id,
       revision.subject_geo_sk AS geo_sk,
       'county'::TEXT AS geo_type,
       CASE measure.measure
           WHEN 'returns' THEN revision.returns::NUMERIC
           WHEN 'individuals' THEN revision.individuals::NUMERIC
           ELSE revision.agi
       END AS value,
       revision.value_status,
       revision.value_source,
       revision.retrieved_at,
       revision.release_key,
       revision.source_record_id,
       revision.capture_id,
       revision.run_id
FROM gold_irs_migration.flow_revision AS revision
CROSS JOIN (VALUES ('returns', 'returns'), ('individuals', 'individuals'), ('agi', 'thousands of dollars'))
    AS measure(measure, unit)
WHERE revision.category IN (
    'total_us_and_foreign', 'total_us', 'total_same_state', 'total_different_state',
    'total_foreign', 'non_migrants'
);

CREATE OR REPLACE VIEW gold_irs_migration.total_observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year_pair)
       revision.*
FROM gold_irs_migration.total_observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year_pair,
         revision.retrieved_at DESC, revision.capture_id DESC;

-- Every metric the file totals can publish.
CREATE OR REPLACE VIEW gold_irs_migration.measure_export AS
SELECT direction.direction || ':' || category.category || ':' || measure.measure AS source_object_key,
       direction.direction,
       category.category,
       category.label AS category_label,
       measure.measure,
       measure.unit,
       'irs_soi_county_migration_csv:v1'::TEXT AS schema_version
FROM (VALUES ('inflow'), ('outflow')) AS direction(direction)
CROSS JOIN (VALUES
    ('total_us_and_foreign', 'Total migration, US and foreign'),
    ('total_us', 'Total migration, US'),
    ('total_same_state', 'Total migration, same state'),
    ('total_different_state', 'Total migration, different state'),
    ('total_foreign', 'Total migration, foreign'),
    ('non_migrants', 'Non-migrants')
) AS category(category, label)
CROSS JOIN (VALUES ('returns', 'returns'), ('individuals', 'individuals'), ('agi', 'thousands of dollars'))
    AS measure(measure, unit);

COMMENT ON SCHEMA gold_irs_migration IS
    'Policy-free publication views for IRS SOI county-to-county migration flows and file totals.';
