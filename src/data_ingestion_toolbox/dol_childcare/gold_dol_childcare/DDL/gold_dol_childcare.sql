-- Publication views over the DOL Women's Bureau National Database of
-- Childcare Prices (county-childcare-prices). Applied by the bootstrap
-- manifest in the `gold` phase and re-applied by the DAG's
-- `ensure_dol_childcare_schema` task; this file is the only definition of
-- these views.

CREATE SCHEMA IF NOT EXISTS gold_dol_childcare;

-- The published measures: the Women's Bureau's median weekly full-time
-- price for each type of care and age group. Kept equal to
-- dol_childcare/registry.py by a unit test.
CREATE OR REPLACE VIEW gold_dol_childcare.measure_definition AS
SELECT measure.metric_key, measure.care_type, measure.age_group, measure.ndcp_column,
       measure.measure_label, measure.unit,
       ('Median weekly price of full-time childcare charged by providers in the '
        || 'county, from the state''s market rate survey, in that year''s dollars. '
        || 'Some county values are imputed by the Women''s Bureau; imputed values '
        || 'are flagged. Source: U.S. Department of Labor, Women''s Bureau, National '
        || 'Database of Childcare Prices.')::TEXT AS observation_basis
FROM (VALUES
    ('center_infant', 'center', 'infant', 'MCINFANT', 'Center-based childcare, infant (birth to 23 months): median weekly full-time price', 'dollars per week'),
    ('center_toddler', 'center', 'toddler', 'MCTODDLER', 'Center-based childcare, toddler (24 to 35 months): median weekly full-time price', 'dollars per week'),
    ('center_preschool', 'center', 'preschool', 'MCPRESCHOOL', 'Center-based childcare, preschool (36 to 54 months): median weekly full-time price', 'dollars per week'),
    ('center_school_age', 'center', 'school_age', 'MCSA', 'Center-based childcare, school age: median weekly full-time price', 'dollars per week'),
    ('home_infant', 'home', 'infant', 'MFCCINFANT', 'Home-based (family) childcare, infant (birth to 23 months): median weekly full-time price', 'dollars per week'),
    ('home_toddler', 'home', 'toddler', 'MFCCTODDLER', 'Home-based (family) childcare, toddler (24 to 35 months): median weekly full-time price', 'dollars per week'),
    ('home_preschool', 'home', 'preschool', 'MFCCPRESCHOOL', 'Home-based (family) childcare, preschool (36 to 54 months): median weekly full-time price', 'dollars per week'),
    ('home_school_age', 'home', 'school_age', 'MFCCSA', 'Home-based (family) childcare, school age: median weekly full-time price', 'dollars per week')
) AS measure(metric_key, care_type, age_group, ndcp_column, measure_label, unit);

-- Every published price as stored: a price a later workbook restates is a
-- second row with its own capture, never an overwrite. A price a later read
-- repeats is not stored again (CHURN RULE), so the newest stored row of a
-- county, year and measure is the one in force. The predicate states the
-- served geography rule (DB-035).
CREATE OR REPLACE VIEW gold_dol_childcare.observation_revision AS
SELECT fact.metric_key,
       fact.care_type,
       fact.age_group,
       measure.measure_label,
       measure.unit,
       measure.observation_basis,
       fact.study_year AS year,
       MAKE_DATE(fact.study_year, 1, 1) AS period_start,
       MAKE_DATE(fact.study_year, 12, 31) AS period_end,
       fact.geo_id,
       fact.geo_sk,
       fact.geo_type,
       gold_glossary.geo_grain(fact.geo_type) AS geo_level,
       fact.fips_code,
       fact.county_name,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.missing_reason,
       fact.imputed,
       fact.imputation_code,
       fact.release,
       fact.release AS release_key,
       file.published_at,
       fact.retrieved_at,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id
FROM silver_dol_childcare.fact_price AS fact
JOIN control.dol_childcare_file AS file ON file.run_id = fact.run_id
JOIN gold_dol_childcare.measure_definition AS measure ON measure.metric_key = fact.metric_key
WHERE file.status = 'published'
  AND fact.geography_status NOT IN ('unmapped', 'unsupported', 'ambiguous');

-- The price in force for each measure, county and study year: the newest
-- stored row.
CREATE OR REPLACE VIEW gold_dol_childcare.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year)
       revision.*
FROM gold_dol_childcare.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year,
         revision.retrieved_at DESC, revision.capture_id DESC;

CREATE OR REPLACE VIEW gold_dol_childcare.measure_export AS
SELECT metric_key AS source_object_key, metric_key, care_type, age_group, ndcp_column,
       measure_label, unit, observation_basis,
       'dol_ndcp_xlsx:v1'::TEXT AS schema_version
FROM gold_dol_childcare.measure_definition;

COMMENT ON SCHEMA gold_dol_childcare IS
    'Policy-free publication views for the DOL Women''s Bureau National Database of Childcare Prices by county.';

COMMENT ON VIEW gold_dol_childcare.observation_latest IS
    'One NDCP median weekly childcare price per measure, county and study year: the newest stored row.';
