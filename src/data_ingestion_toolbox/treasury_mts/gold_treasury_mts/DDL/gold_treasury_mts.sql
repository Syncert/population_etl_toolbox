-- Publication views over the Monthly Treasury Statement, Table 9: federal
-- receipts by source and outlays by budget function, for the nation
-- (where-public-money-goes slice 3). Applied by the bootstrap manifest in the
-- `gold` phase and re-applied by the DAG's `ensure_treasury_mts_schema` task;
-- this file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_treasury_mts;

-- The registered Table 9 lines (registry.LINES; a test holds the two equal).
CREATE OR REPLACE VIEW gold_treasury_mts.line_definition AS
SELECT line.line_code, line.measure AS metric_key, line.line_label, line.kind, line.is_total,
       CASE line.kind
           WHEN 'outlays' THEN 'Federal outlays (Monthly Treasury Statement, Table 9)'
           ELSE 'Federal receipts (Monthly Treasury Statement, Table 9)'
       END || ': ' || line.line_label AS measure_label,
       'US dollars'::TEXT AS unit,
       'Monthly Treasury Statement, Table 9, line ' || line.line_code || ' ('
       || line.line_label || '), as the Bureau of the Fiscal Service publishes it '
       || 'through the Fiscal Data API. A fiscal-year value is the September '
       || 'statement''s fiscal-year-to-date column (the federal fiscal year ends '
       || 'September 30), never a sum of months. Outlays are net of offsetting '
       || 'collections, so a line can be negative. Federal government only: not '
       || 'state or local spending, and not where the money was spent.' AS observation_basis,
       'ANNUAL'::TEXT AS time_grain
FROM (VALUES
    ('20', 'receipts_individual_income_taxes', 'Individual income taxes', 'receipts', FALSE),
    ('30', 'receipts_corporation_income_taxes', 'Corporation income taxes', 'receipts', FALSE),
    ('50', 'receipts_employment_general_retirement', 'Social insurance: employment and general retirement', 'receipts', FALSE),
    ('60', 'receipts_unemployment_insurance', 'Social insurance: unemployment insurance', 'receipts', FALSE),
    ('70', 'receipts_other_retirement', 'Social insurance: other retirement', 'receipts', FALSE),
    ('80', 'receipts_excise_taxes', 'Excise taxes', 'receipts', FALSE),
    ('90', 'receipts_estate_gift_taxes', 'Estate and gift taxes', 'receipts', FALSE),
    ('100', 'receipts_customs_duties', 'Customs duties', 'receipts', FALSE),
    ('110', 'receipts_miscellaneous', 'Miscellaneous receipts', 'receipts', FALSE),
    ('120', 'receipts_total', 'Total receipts', 'receipts', TRUE),
    ('140', 'outlays_national_defense', 'National defense', 'outlays', FALSE),
    ('150', 'outlays_international_affairs', 'International affairs', 'outlays', FALSE),
    ('160', 'outlays_science_space_technology', 'General science, space, and technology', 'outlays', FALSE),
    ('170', 'outlays_energy', 'Energy', 'outlays', FALSE),
    ('180', 'outlays_natural_resources_environment', 'Natural resources and environment', 'outlays', FALSE),
    ('190', 'outlays_agriculture', 'Agriculture', 'outlays', FALSE),
    ('200', 'outlays_commerce_housing_credit', 'Commerce and housing credit', 'outlays', FALSE),
    ('210', 'outlays_transportation', 'Transportation', 'outlays', FALSE),
    ('220', 'outlays_community_regional_development', 'Community and regional development', 'outlays', FALSE),
    ('230', 'outlays_education_training_employment_social', 'Education, training, employment, and social services', 'outlays', FALSE),
    ('240', 'outlays_health', 'Health', 'outlays', FALSE),
    ('250', 'outlays_medicare', 'Medicare', 'outlays', FALSE),
    ('260', 'outlays_income_security', 'Income security', 'outlays', FALSE),
    ('270', 'outlays_social_security', 'Social Security', 'outlays', FALSE),
    ('280', 'outlays_veterans_benefits', 'Veterans benefits and services', 'outlays', FALSE),
    ('290', 'outlays_administration_of_justice', 'Administration of justice', 'outlays', FALSE),
    ('300', 'outlays_general_government', 'General government', 'outlays', FALSE),
    ('320', 'outlays_net_interest', 'Net interest', 'outlays', FALSE),
    ('330', 'outlays_undistributed_offsetting_receipts', 'Undistributed offsetting receipts', 'outlays', FALSE),
    ('340', 'outlays_total', 'Total outlays', 'outlays', TRUE)
) AS line(line_code, measure, line_label, kind, is_total);

-- Every statement line served: rows of a published page only, current and
-- each a later read replaced.
CREATE OR REPLACE VIEW gold_treasury_mts.statement_line_revision AS
SELECT line_row.record_date, line_row.line_code, line_row.classification_desc,
       line_row.fiscal_year, line_row.calendar_month,
       line_row.current_month_amount, line_row.current_fytd_amount,
       line_row.prior_fytd_amount, line_row.value_status,
       page.published_at, line_row.source_record_id, line_row.capture_id, line_row.run_id
FROM (
    SELECT current.record_date, current.line_code, current.classification_desc,
           current.fiscal_year, current.calendar_month, current.current_month_amount,
           current.current_fytd_amount, current.prior_fytd_amount, current.value_status,
           current.source_record_id, current.capture_id, current.run_id
    FROM silver_treasury_mts.statement_line AS current
    UNION ALL
    SELECT revision.record_date, revision.line_code, revision.classification_desc,
           revision.fiscal_year, revision.calendar_month, revision.current_month_amount,
           revision.current_fytd_amount, revision.prior_fytd_amount, revision.value_status,
           revision.source_record_id, revision.capture_id, revision.run_id
    FROM silver_treasury_mts.statement_line_revision AS revision
) AS line_row
JOIN control.treasury_mts_page AS page
  ON page.run_id = line_row.run_id AND page.capture_id = line_row.capture_id
WHERE page.status = 'published';

-- The fiscal-year total of each line as published: the September statement's
-- fiscal-year-to-date column, with the read that published it.
CREATE OR REPLACE VIEW gold_treasury_mts.observation_revision AS
SELECT definition.metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       line_row.fiscal_year AS year,
       MAKE_DATE(line_row.fiscal_year - 1, 10, 1) AS period_start,
       line_row.record_date AS period_end,
       'us:1'::TEXT AS geo_id,
       entity.geo_sk,
       'nation'::TEXT AS geo_type,
       gold_glossary.geo_grain('nation') AS geo_level,
       line_row.current_fytd_amount AS value,
       line_row.value_status,
       'MTS ' || TO_CHAR(line_row.record_date, 'YYYY-MM') || ' read '
       || TO_CHAR(line_row.published_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS release_key,
       line_row.published_at,
       line_row.source_record_id,
       line_row.capture_id,
       line_row.run_id
FROM gold_treasury_mts.statement_line_revision AS line_row
JOIN gold_treasury_mts.line_definition AS definition ON definition.line_code = line_row.line_code
LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = 'us:1'
WHERE line_row.calendar_month = 9
  AND line_row.value_status = 'valid';

-- The current fiscal-year total of each line, read from the current table.
CREATE OR REPLACE VIEW gold_treasury_mts.observation_latest AS
SELECT definition.metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       current.fiscal_year AS year,
       MAKE_DATE(current.fiscal_year - 1, 10, 1) AS period_start,
       current.record_date AS period_end,
       'us:1'::TEXT AS geo_id,
       entity.geo_sk,
       'nation'::TEXT AS geo_type,
       gold_glossary.geo_grain('nation') AS geo_level,
       current.current_fytd_amount AS value,
       current.value_status,
       'MTS ' || TO_CHAR(current.record_date, 'YYYY-MM') || ' read '
       || TO_CHAR(page.published_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS release_key,
       page.published_at,
       current.source_record_id,
       current.capture_id,
       current.run_id
FROM silver_treasury_mts.statement_line AS current
JOIN control.treasury_mts_page AS page
  ON page.run_id = current.run_id AND page.capture_id = current.capture_id
JOIN gold_treasury_mts.line_definition AS definition ON definition.line_code = current.line_code
LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = 'us:1'
WHERE page.status = 'published'
  AND current.calendar_month = 9
  AND current.value_status = 'valid';

-- Federal outlays by budget function and each function's share of total
-- outlays (line 340), from the current published statements:
--   fiscal_year          the September statement's fiscal-year-to-date column;
--   fiscal_year_to_date  the newest statement of a fiscal year with no
--                        September statement yet, through its month;
--   month                each statement's current-month column.
-- The function lines add to line 340 as published. Undistributed offsetting
-- receipts (line 330) is negative, so the other shares can add above 100.
CREATE OR REPLACE VIEW gold_treasury_mts.outlays_by_function AS
WITH served AS (
    SELECT current.record_date, current.line_code, current.fiscal_year,
           current.calendar_month, current.current_month_amount, current.current_fytd_amount,
           current.value_status, current.source_record_id, current.capture_id, current.run_id,
           page.published_at
    FROM silver_treasury_mts.statement_line AS current
    JOIN control.treasury_mts_page AS page
      ON page.run_id = current.run_id AND page.capture_id = current.capture_id
    WHERE page.status = 'published'
), newest AS (
    SELECT fiscal_year, MAX(record_date) AS record_date, BOOL_OR(calendar_month = 9) AS complete
    FROM served
    GROUP BY fiscal_year
), periods AS (
    SELECT 'fiscal_year'::TEXT AS period_kind, served.*, served.current_fytd_amount AS amount
    FROM served
    WHERE served.calendar_month = 9
    UNION ALL
    SELECT 'fiscal_year_to_date', served.*, served.current_fytd_amount
    FROM served
    JOIN newest ON newest.fiscal_year = served.fiscal_year AND newest.record_date = served.record_date
    WHERE NOT newest.complete
    UNION ALL
    SELECT 'month', served.*, served.current_month_amount
    FROM served
)
SELECT part.period_kind,
       part.fiscal_year,
       part.record_date AS statement_date,
       part.calendar_month,
       CASE WHEN part.period_kind = 'month'
            THEN DATE_TRUNC('month', part.record_date)::DATE
            ELSE MAKE_DATE(part.fiscal_year - 1, 10, 1) END AS period_start,
       part.record_date AS period_end,
       'us:1'::TEXT AS geo_id,
       'nation'::TEXT AS geo_type,
       definition.line_code,
       definition.metric_key,
       definition.line_label AS budget_function,
       part.amount AS outlays_amount,
       total.amount AS total_outlays_amount,
       CASE WHEN part.amount IS NULL OR total.amount IS NULL OR total.amount = 0 THEN NULL
            ELSE ROUND(100 * part.amount / total.amount, 2) END AS share_of_total_outlays_pct,
       CASE WHEN part.amount IS NULL THEN 'not published for this line and period'
            WHEN total.amount IS NULL THEN 'total outlays not published for this period'
            WHEN total.amount = 0 THEN 'total outlays are zero'
       END AS unavailable_reason,
       'MTS ' || TO_CHAR(part.record_date, 'YYYY-MM') AS release_key,
       part.published_at,
       part.source_record_id,
       part.capture_id,
       part.run_id
FROM periods AS part
JOIN gold_treasury_mts.line_definition AS definition
  ON definition.line_code = part.line_code AND definition.kind = 'outlays' AND NOT definition.is_total
LEFT JOIN periods AS total
  ON total.period_kind = part.period_kind
 AND total.record_date = part.record_date
 AND total.line_code = '340';

CREATE OR REPLACE VIEW gold_treasury_mts.measure_export AS
SELECT metric_key AS source_object_key, metric_key AS measure, measure_label, unit,
       observation_basis, 'treasury_mts_table9:v1'::TEXT AS schema_version, time_grain
FROM gold_treasury_mts.line_definition;

COMMENT ON VIEW gold_treasury_mts.outlays_by_function IS
    'Federal outlays by budget function and share of total outlays (Monthly Treasury Statement '
    'Table 9): fiscal years, the open fiscal year to date, and months, as published.';
COMMENT ON SCHEMA gold_treasury_mts IS
    'Policy-free publication views for the Monthly Treasury Statement, Table 9 (federal receipts '
    'by source and outlays by budget function, nation).';
