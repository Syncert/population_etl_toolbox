-- Publication views over reconciled FHFA annual House Price Index
-- observations (fhfa-house-price-index). Applied by the bootstrap manifest in
-- the `gold` phase and re-applied by the DAG's `ensure_fhfa_hpi_schema` task;
-- this file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_fhfa_hpi;

-- The two published measures and what they are: a repeat-sales index of
-- Enterprise-backed loans, not a price level, with FHFA's required notice.
-- The first-recorded-base index and the 1990-based index stay in silver: the
-- first's base year differs by county, and the 2000-based index carries the
-- same series on one base.
CREATE OR REPLACE VIEW gold_fhfa_hpi.measure_definition AS
SELECT measure.measure, measure.measure_label, measure.unit,
       'FHFA all-transactions House Price Index (annual, developmental): a '
       || 'nominal, not seasonally adjusted repeat-sales index of conventional '
       || 'single-family mortgages bought or guaranteed by Fannie Mae and Freddie '
       || 'Mac, not a price level; jumbo, FHA/VA, condominium and multi-unit '
       || 'loans are out of scope. This product uses FHFA data but is neither '
       || 'endorsed nor certified by FHFA.'::TEXT AS observation_basis
FROM (VALUES
    ('annual_change_pct', 'Annual change in the house price index', 'percent'),
    ('hpi_base_2000', 'House price index, 2000 = 100', 'index, 2000 = 100')
) AS measure(measure, measure_label, unit);

-- Every published vintage of every observation. The release identity is the
-- workbook's own "Last updated" date; a second file with the same date and
-- different bytes is that date with a sequence suffix, never an overwrite.
-- The fact's CHECK admits only `resolved` and `unmapped`; the predicate
-- states the serving rule (DB-035).
CREATE OR REPLACE VIEW gold_fhfa_hpi.observation_revision AS
WITH published AS (
    SELECT file.run_id, file.capture_id, file.provider_vintage, file.published_at,
           TO_CHAR(file.provider_vintage, 'YYYY-MM-DD')
           || CASE WHEN ROW_NUMBER() OVER (
                       PARTITION BY file.kind, file.provider_vintage ORDER BY file.published_at, file.run_id
                   ) > 1
                   THEN '.' || ROW_NUMBER() OVER (
                       PARTITION BY file.kind, file.provider_vintage ORDER BY file.published_at, file.run_id
                   )::TEXT
                   ELSE '' END AS release_key
    FROM control.fhfa_hpi_file AS file
    WHERE file.status = 'published'
)
SELECT fact.measure AS metric_key,
       fact.measure,
       measure.measure_label,
       measure.unit,
       measure.observation_basis,
       fact.year,
       MAKE_DATE(fact.year, 1, 1) AS period_start,
       MAKE_DATE(fact.year, 12, 31) AS period_end,
       fact.geo_id,
       fact.geo_sk,
       'county'::TEXT AS geo_type,
       gold_glossary.geo_grain('county') AS geo_level,
       fact.geography_status,
       fact.fips_code,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.missing_reason,
       published.provider_vintage,
       published.release_key,
       published.published_at,
       fact.retrieved_at,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id
FROM silver_fhfa_hpi.fact_observation AS fact
JOIN published ON published.run_id = fact.run_id
JOIN gold_fhfa_hpi.measure_definition AS measure ON measure.measure = fact.measure
WHERE fact.geography_status NOT IN ('unsupported', 'ambiguous', 'unmapped');

-- The newest published vintage of each observation.
CREATE OR REPLACE VIEW gold_fhfa_hpi.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year)
       revision.*
FROM gold_fhfa_hpi.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year,
         revision.provider_vintage DESC, revision.published_at DESC, revision.run_id DESC;

CREATE OR REPLACE VIEW gold_fhfa_hpi.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, observation_basis,
       'fhfa_hpi_xlsx:v1'::TEXT AS schema_version
FROM gold_fhfa_hpi.measure_definition;

COMMENT ON SCHEMA gold_fhfa_hpi IS
    'Policy-free publication views for the FHFA annual county House Price Index.';
