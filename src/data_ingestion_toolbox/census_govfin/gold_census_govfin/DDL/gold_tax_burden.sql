-- State and local tax burden (taxes-and-public-money T-5): taxes collected
-- in a place over the personal income of the people living there, and per
-- person. Applied by the bootstrap manifest in the `gold` phase after
-- `gold_bea`, and re-applied by the DAG's `ensure_census_govfin_schema`
-- task.
--
-- A derived product, labelled as one: Census taxes (thousands of dollars)
-- over BEA CAINC1 line 1 personal income (thousands of dollars) and line 2
-- population, the fiscal year matched to the calendar year it ends in. A
-- state's and the nation's figure is Census's published state-and-local
-- total (`asslgf`); a county's is its local governments' taxes in a Census
-- of Governments year (`cog_units`, county-area roll-up), so a county has a
-- figure only for years ending in 2 and 7. Where a tax is collected is not
-- who bears it (a tourist's sales tax, a severance tax, business property),
-- so the wording is "collected in". No row is a score or a rank.
CREATE OR REPLACE VIEW gold_census_govfin.tax_burden AS
SELECT taxes.dataset_id,
       taxes.government_level,
       taxes.geo_id,
       taxes.geo_type,
       taxes.geo_level,
       taxes.fiscal_year,
       taxes.fiscal_year AS income_year,
       taxes.value AS taxes_thousands,
       income.value AS personal_income_thousands,
       population.value AS population,
       CASE WHEN income.value > 0
            THEN ROUND(taxes.value / income.value * 100, 3) END AS taxes_pct_of_personal_income,
       CASE WHEN population.value > 0
            THEN ROUND(taxes.value * 1000 / population.value, 2) END AS taxes_per_person_dollars,
       CASE WHEN income.value IS NULL THEN 'denominator_missing'
            WHEN income.value <= 0 THEN 'denominator_zero' END AS unavailable_reason,
       'BEA CAINC1 line 1 personal income and line 2 population, calendar year '
           || taxes.fiscal_year AS denominator_source,
       taxes.release_key AS taxes_release_key,
       income.release_key AS income_release_key,
       TRUE AS derived
FROM gold_census_govfin.observation_latest AS taxes
-- Each denominator is the newest release of one BEA figure for this place and
-- year, read by `bea_fact_lookup_idx` (table, line, place, year). Joining
-- `gold_bea.observation_latest` on `metric_key` instead ranked every CAINC1
-- line 1 and 2 row in the country (228,592 each) on every read, because a
-- filter on the concatenated key reaches no index: 3.6 s for one county on
-- OFFICE_LAB on 2026-11-06. The newest release is chosen first and then
-- required to be valid, as the DISTINCT ON view did.
LEFT JOIN LATERAL (
    SELECT revision.value, revision.value_status, revision.release_key
    FROM gold_bea.observation_revision AS revision
    WHERE revision.table_code = 'CAINC1'
      AND revision.line_code = '1'
      AND revision.geo_id = taxes.geo_id
      AND revision.year = taxes.fiscal_year
    ORDER BY revision.release_date DESC, revision.retrieved_at DESC, revision.capture_id DESC
    LIMIT 1
) AS income ON income.value_status = 'valid'
LEFT JOIN LATERAL (
    SELECT revision.value, revision.value_status
    FROM gold_bea.observation_revision AS revision
    WHERE revision.table_code = 'CAINC1'
      AND revision.line_code = '2'
      AND revision.geo_id = taxes.geo_id
      AND revision.year = taxes.fiscal_year
    ORDER BY revision.release_date DESC, revision.retrieved_at DESC, revision.capture_id DESC
    LIMIT 1
) AS population ON population.value_status = 'valid'
WHERE taxes.measure_id = 'taxes_total'
  AND taxes.quarter = 0
  AND taxes.value_status = 'valid'
  AND ((taxes.dataset_id = 'asslgf' AND taxes.government_level = 'state_and_local')
       OR (taxes.dataset_id = 'cog_units' AND taxes.derivation = 'county_area_rollup'));

COMMENT ON VIEW gold_census_govfin.tax_burden IS
    'Derived: taxes collected in a place over BEA personal income and per person; states every fiscal year, county areas in Census of Governments years.';
