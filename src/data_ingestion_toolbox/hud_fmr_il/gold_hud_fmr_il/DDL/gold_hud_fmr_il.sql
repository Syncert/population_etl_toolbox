-- Publication views over reconciled HUD Fair Market Rents and income limits
-- (hud-fair-market-rents-and-income-limits). Applied by the bootstrap
-- manifest in the `gold` phase and re-applied by the DAG's
-- `ensure_hud_fmr_il_schema` task; this file is the only definition of these
-- views.

CREATE SCHEMA IF NOT EXISTS gold_hud_fmr_il;

-- The published measures and what they are: program reference values set
-- for a HUD FMR area, not a survey estimate of this county's rents or
-- incomes.
CREATE OR REPLACE VIEW gold_hud_fmr_il.measure_definition AS
SELECT measure.measure, measure.measure_label, measure.unit, measure.dataset,
       CASE measure.dataset
           WHEN 'fmr' THEN
               'HUD Fair Market Rent: the gross rent (shelter plus utilities) HUD sets '
               || 'for the fiscal year from the 40th percentile of rents recent movers '
               || 'paid for standard-quality units in the HUD FMR area this county '
               || 'belongs to; an area value repeated for each county, not a county '
               || 'estimate and not the ACS median rent. Source: U.S. Department of '
               || 'Housing and Urban Development, HUD User. This product uses the HUD User Data '
               || 'API but is not endorsed or certified by HUD User.'
           ELSE
               'HUD Section 8 income limits: program eligibility thresholds and the '
               || 'area median family income they are based on, set for the fiscal year '
               || 'for the HUD area this county belongs to; an area value repeated for '
               || 'each county, not a county estimate and not the ACS median income. '
               || 'Source: U.S. Department of Housing and Urban Development, HUD User. This '
               || 'product uses the HUD User Data API but is not endorsed or certified by HUD User.'
       END::TEXT AS observation_basis
FROM (VALUES
    ('fmr_0br', 'Fair Market Rent, efficiency', 'dollars per month', 'fmr'),
    ('fmr_1br', 'Fair Market Rent, one bedroom', 'dollars per month', 'fmr'),
    ('fmr_2br', 'Fair Market Rent, two bedrooms', 'dollars per month', 'fmr'),
    ('fmr_3br', 'Fair Market Rent, three bedrooms', 'dollars per month', 'fmr'),
    ('fmr_4br', 'Fair Market Rent, four bedrooms', 'dollars per month', 'fmr'),
    ('median_family_income', 'HUD area median family income', 'dollars per year', 'il'),
    ('income_limit_30_4p', 'Extremely low income limit (30%), four-person household', 'dollars per year', 'il'),
    ('income_limit_50_4p', 'Very low income limit (50%), four-person household', 'dollars per year', 'il'),
    ('income_limit_80_4p', 'Low income limit (80%), four-person household', 'dollars per year', 'il')
) AS measure(measure, measure_label, unit, dataset);

-- Every published edition of every county observation. The release identity
-- is the fiscal year and edition (`FY2026`, `FY2026-revised`); a reissued
-- fiscal year is a second row, never an overwrite. A town row is held in
-- silver as `unsupported`; the predicate states the serving rule (DB-035).
CREATE OR REPLACE VIEW gold_hud_fmr_il.observation_revision AS
SELECT fact.measure AS metric_key,
       fact.measure,
       measure.measure_label,
       measure.unit,
       measure.observation_basis,
       fact.dataset,
       fact.fiscal_year AS year,
       MAKE_DATE(fact.fiscal_year - 1, 10, 1) AS period_start,
       MAKE_DATE(fact.fiscal_year, 9, 30) AS period_end,
       fact.effective_date,
       fact.edition,
       fact.geo_id,
       fact.geo_sk,
       fact.geo_type,
       gold_glossary.geo_grain(fact.geo_type) AS geo_level,
       fact.geography_status,
       fact.fips_code,
       fact.hud_area_code,
       fact.hud_area_name,
       fact.metro,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.missing_reason,
       'FY' || fact.fiscal_year::TEXT || CASE fact.edition WHEN 'revised' THEN '-revised' ELSE '' END
           AS release_key,
       file.published_at,
       fact.retrieved_at,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id
FROM silver_hud_fmr_il.fact_observation AS fact
JOIN control.hud_fmr_il_file AS file ON file.run_id = fact.run_id
JOIN gold_hud_fmr_il.measure_definition AS measure ON measure.measure = fact.measure
WHERE file.status = 'published'
  AND fact.geography_status NOT IN ('unsupported', 'ambiguous', 'unmapped');

-- The edition in force for each county and fiscal year: a revised edition
-- over the original, then the newest publication.
CREATE OR REPLACE VIEW gold_hud_fmr_il.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.geo_id, revision.year)
       revision.*
FROM gold_hud_fmr_il.observation_revision AS revision
ORDER BY revision.metric_key, revision.geo_id, revision.year,
         (revision.edition = 'revised') DESC, revision.published_at DESC, revision.run_id DESC;

CREATE OR REPLACE VIEW gold_hud_fmr_il.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, observation_basis,
       'hud_fmr_il_xlsx:v1'::TEXT AS schema_version
FROM gold_hud_fmr_il.measure_definition;

COMMENT ON SCHEMA gold_hud_fmr_il IS
    'Policy-free publication views for HUD Fair Market Rents and Section 8 income limits by county.';
