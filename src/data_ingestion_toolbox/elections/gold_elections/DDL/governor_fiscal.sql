-- Governor terms with taxes and public money (taxes-and-public-money T-6).
-- Applied by the bootstrap manifest in the `gold` phase after
-- `gold_elections`, `gold_bea` and `gold_census_govfin`; this file is the
-- only definition of these views.
--
-- ADR-0014, with its fiscal section:
--   * A governor's tenure is one row: consecutive terms of one holder and
--     one party, a month or less apart, are read as one span.
--   * The window runs from the last full year before the tenure starts to
--     the last full year inside it. A tenure still serving is "so far" and
--     ends on the newest year the state and the nation both publish.
--   * Each measure is the state's change over the window minus the nation's
--     change over the same years, in percentage points. One row per measure;
--     nothing is added together, scored or ranked.
--   * A fiscal year is the calendar year it ends in (46 states end theirs on
--     June 30), and taxes are "collected in" a state, not borne by it.
--   * A completed tenure whose first or last year was not published has no
--     figure and says which end is missing; it is never moved to a nearby
--     year.

CREATE SCHEMA IF NOT EXISTS gold_elections;

-- The fiscal measures the governor table shows, in display order.
CREATE OR REPLACE VIEW gold_elections.governor_fiscal_measure_definition AS
SELECT measure, measure_label, change_unit, source_label, observation_basis, display_order
FROM (VALUES
    ('tax_burden', 'State and local taxes, share of personal income', 'percentage points',
     'Census Annual Survey of State and Local Government Finances; BEA personal income (CAINC1 line 1)',
     'Taxes collected by the state and its local governments in the fiscal year ending in the '
     || 'year shown, divided by the personal income of its residents in that calendar year. '
     || 'Where a tax is collected is not who bears it.', 1),
    ('payments_share', 'Payments from government and others, share of personal income',
     'percentage points', 'BEA SAINC35 line 1000 over SAINC50 line 10',
     'Personal current transfer receipts (Social Security, Medicare, Medicaid, income support, '
     || 'unemployment insurance, veterans'' benefits and others) as a share of residents'' '
     || 'personal income. Payments respond to a state''s age and income; they do not cause them.', 2)
) AS definition(measure, measure_label, change_unit, source_label, observation_basis, display_order);

-- Each governor's tenure: consecutive terms of one holder and party joined.
CREATE OR REPLACE VIEW gold_elections.governor_tenure AS
WITH term AS (
    SELECT seat.*,
           CASE WHEN LAG(seat.holder_id) OVER state_order = seat.holder_id
                     AND LAG(seat.party_code) OVER state_order = seat.party_code
                     AND LAG(seat.end_date) OVER state_order >= seat.start_date - 31
                THEN 0 ELSE 1 END AS starts_tenure
    FROM gold_elections.seat_term AS seat
    WHERE seat.office = 'governor'
    WINDOW state_order AS (PARTITION BY seat.state_fips ORDER BY seat.start_date, seat.term_key)
),
numbered AS (
    SELECT term.*,
           SUM(term.starts_tenure) OVER (PARTITION BY term.state_fips ORDER BY term.start_date, term.term_key)
               AS tenure_number
    FROM term
)
SELECT numbered.state_fips,
       MIN(numbered.state_code) AS state_code,
       MIN(numbered.geo_id) AS geo_id,
       numbered.tenure_number,
       MIN(numbered.term_key) FILTER (WHERE numbered.starts_tenure = 1) AS tenure_key,
       numbered.holder_id,
       MIN(numbered.holder_name) AS holder_name,
       MIN(numbered.party_code) AS party_code,
       MIN(numbered.party_text) AS party_text,
       MIN(numbered.start_date) AS start_date,
       -- A serving term (no end date) leaves the whole tenure open.
       CASE WHEN BOOL_OR(numbered.end_date IS NULL) THEN NULL ELSE MAX(numbered.end_date) END AS end_date,
       COUNT(*)::INTEGER AS term_count,
       MIN(numbered.source_label) AS source_label
FROM numbered
GROUP BY numbered.state_fips, numbered.tenure_number, numbered.holder_id;

-- Each tenure's window, one row per fiscal measure, with the state's and the
-- nation's values at both ends and the change net of the nation.
CREATE OR REPLACE VIEW gold_elections.governor_term_fiscal AS
WITH tenure AS (
    SELECT tenure.*,
           EXTRACT(YEAR FROM tenure.start_date)::INTEGER - 1 AS window_start_year,
           CASE WHEN tenure.end_date IS NULL OR tenure.end_date > CURRENT_DATE THEN NULL
                WHEN EXTRACT(MONTH FROM tenure.end_date) = 12 AND EXTRACT(DAY FROM tenure.end_date) = 31
                    THEN EXTRACT(YEAR FROM tenure.end_date)::INTEGER
                ELSE EXTRACT(YEAR FROM tenure.end_date)::INTEGER - 1
           END AS last_full_year,
           (tenure.end_date IS NULL OR tenure.end_date > CURRENT_DATE) AS so_far
    FROM gold_elections.governor_tenure AS tenure
),
-- The newest release of the two state BEA lines, filtered on the table and
-- line columns so `bea_fact_lookup_idx` reads only them; a filter on
-- `gold_bea.observation_latest.metric_key` ranked every BEA row first.
bea_latest AS (
    SELECT DISTINCT ON (revision.table_code, revision.line_code, revision.geo_id, revision.year)
           revision.table_code, revision.line_code, revision.geo_id, revision.year,
           revision.value, revision.value_status
    FROM gold_bea.observation_revision AS revision
    WHERE (revision.table_code, revision.line_code) IN (('SAINC35', '1000'), ('SAINC50', '10'))
      AND revision.geo_type IN ('state', 'nation')
    ORDER BY revision.table_code, revision.line_code, revision.geo_id, revision.year,
             revision.release_date DESC, revision.retrieved_at DESC, revision.capture_id DESC
),
series AS (
    SELECT 'tax_burden'::TEXT AS measure, burden.geo_id, burden.fiscal_year AS year,
           burden.taxes_pct_of_personal_income AS value
    FROM gold_census_govfin.tax_burden AS burden
    WHERE burden.dataset_id = 'asslgf'
      AND burden.government_level = 'state_and_local'
      AND burden.taxes_pct_of_personal_income IS NOT NULL
    UNION ALL
    SELECT 'payments_share', payments.geo_id, payments.year,
           ROUND(payments.value / income.value * 100, 3)
    FROM bea_latest AS payments
    JOIN bea_latest AS income
      ON income.table_code = 'SAINC50' AND income.line_code = '10'
     AND income.geo_id = payments.geo_id
     AND income.year = payments.year
     AND income.value_status = 'valid'
     AND income.value > 0
    WHERE payments.table_code = 'SAINC35' AND payments.line_code = '1000'
      AND payments.value_status = 'valid'
      AND payments.value IS NOT NULL
),
windowed AS (
    SELECT tenure.*, definition.measure, definition.measure_label, definition.display_order,
           -- A finished tenure ends on its last full year; one serving ends
           -- on the newest year the state and the nation both publish.
           CASE WHEN tenure.so_far THEN (
                    SELECT MAX(place.year)
                    FROM series AS place
                    JOIN series AS nation
                      ON nation.measure = place.measure AND nation.geo_id = 'us:1' AND nation.year = place.year
                    WHERE place.measure = definition.measure
                      AND place.geo_id = tenure.geo_id
                      AND place.year > tenure.window_start_year)
                ELSE tenure.last_full_year
           END AS window_end_year
    FROM tenure
    CROSS JOIN gold_elections.governor_fiscal_measure_definition AS definition
),
valued AS (
    SELECT windowed.*,
           place_start.value AS place_start_value,
           place_end.value AS place_end_value,
           nation_start.value AS nation_start_value,
           nation_end.value AS nation_end_value
    FROM windowed
    LEFT JOIN series AS place_start
      ON place_start.measure = windowed.measure AND place_start.geo_id = windowed.geo_id
     AND place_start.year = windowed.window_start_year
    LEFT JOIN series AS nation_start
      ON nation_start.measure = windowed.measure AND nation_start.geo_id = 'us:1'
     AND nation_start.year = windowed.window_start_year
    LEFT JOIN series AS place_end
      ON place_end.measure = windowed.measure AND place_end.geo_id = windowed.geo_id
     AND place_end.year = windowed.window_end_year
    LEFT JOIN series AS nation_end
      ON nation_end.measure = windowed.measure AND nation_end.geo_id = 'us:1'
     AND nation_end.year = windowed.window_end_year
)
SELECT valued.state_fips,
       valued.state_code,
       valued.geo_id,
       valued.tenure_key,
       valued.holder_id,
       valued.holder_name,
       valued.party_code,
       valued.party_text,
       valued.start_date,
       valued.end_date,
       valued.term_count,
       valued.so_far,
       valued.measure,
       valued.measure_label,
       valued.display_order,
       valued.window_start_year,
       CASE WHEN valued.window_end_year > valued.window_start_year THEN valued.window_end_year END
           AS window_end_year,
       valued.place_start_value,
       valued.place_end_value,
       valued.nation_start_value,
       valued.nation_end_value,
       ROUND(valued.place_end_value - valued.place_start_value, 3) AS place_change,
       ROUND(valued.nation_end_value - valued.nation_start_value, 3) AS nation_change,
       CASE WHEN valued.window_end_year > valued.window_start_year THEN
           ROUND((valued.place_end_value - valued.place_start_value)
                 - (valued.nation_end_value - valued.nation_start_value), 3)
       END AS net_change,
       CASE WHEN valued.window_end_year IS NULL AND valued.so_far THEN 'no_year_published_since_start'
            WHEN valued.window_end_year IS NULL OR valued.window_end_year <= valued.window_start_year
                THEN 'tenure_shorter_than_a_full_year'
            WHEN valued.place_start_value IS NULL OR valued.nation_start_value IS NULL
                THEN 'start_year_not_published'
            WHEN valued.place_end_value IS NULL OR valued.nation_end_value IS NULL
                THEN 'end_year_not_published'
       END AS unavailable_reason,
       valued.source_label
FROM valued;

-- Each tenure's tax mix in its window's last year: the share of state and
-- local taxes from property, general sales, income (individual and
-- corporate) and everything else. Shares of one total in one year; never a
-- score.
--
-- "Everything else" is the total less the three named groups, not a sum of
-- published parts. Census's annual survey publishes selective sales but no
-- "other taxes" aggregate (licences, severance, estate and the rest are only
-- in the unit files), so summing the parts left Alaska at 72 percent and
-- Alabama at 94 on OFFICE_LAB on 2026-11-06. A year missing the total or any
-- named part has no mix ("Not published"), never a partial one; a remainder
-- below -0.5 points means the parts and total disagree and is not served.
CREATE OR REPLACE VIEW gold_elections.governor_term_tax_mix AS
WITH tenure AS (
    SELECT DISTINCT fiscal.state_fips, fiscal.geo_id, fiscal.tenure_key, fiscal.so_far,
           fiscal.window_start_year, fiscal.window_end_year
    FROM gold_elections.governor_term_fiscal AS fiscal
    WHERE fiscal.measure = 'tax_burden'
),
named AS (
    SELECT tax_mix.geo_id, tax_mix.fiscal_year,
           SUM(tax_mix.share_of_taxes_pct) FILTER (WHERE tax_mix.measure_id = 'tax_property') AS property,
           SUM(tax_mix.share_of_taxes_pct) FILTER (WHERE tax_mix.measure_id = 'tax_general_sales') AS general_sales,
           SUM(tax_mix.share_of_taxes_pct) FILTER (
               WHERE tax_mix.measure_id IN ('tax_individual_income', 'tax_corporate_income')) AS income,
           COUNT(DISTINCT tax_mix.measure_id) FILTER (WHERE tax_mix.measure_id IN (
               'tax_property', 'tax_general_sales', 'tax_individual_income', 'tax_corporate_income')
           ) AS named_parts
    FROM gold_census_govfin.tax_mix AS tax_mix
    WHERE tax_mix.dataset_id = 'asslgf'
      AND tax_mix.government_level = 'state_and_local'
      AND tax_mix.quarter = 0
      AND tax_mix.share_of_taxes_pct IS NOT NULL
    GROUP BY tax_mix.geo_id, tax_mix.fiscal_year
),
complete AS (
    SELECT named.*, 100 - (named.property + named.general_sales + named.income) AS remainder
    FROM named
    WHERE named.named_parts = 4
),
mix AS (
    SELECT complete.geo_id, complete.fiscal_year, part.tax_group, part.share_of_taxes_pct
    FROM complete
    CROSS JOIN LATERAL (VALUES
        ('property'::TEXT, complete.property),
        ('general_sales'::TEXT, complete.general_sales),
        ('income'::TEXT, complete.income),
        ('other'::TEXT, GREATEST(complete.remainder, 0))
    ) AS part(tax_group, share_of_taxes_pct)
    WHERE complete.remainder >= -0.5
)
SELECT tenure.state_fips,
       tenure.geo_id,
       tenure.tenure_key,
       tenure.so_far,
       tenure.window_end_year AS fiscal_year,
       mix.tax_group,
       ROUND(mix.share_of_taxes_pct, 2) AS share_of_taxes_pct
FROM tenure
JOIN mix ON mix.geo_id = tenure.geo_id AND mix.fiscal_year = tenure.window_end_year;

COMMENT ON VIEW gold_elections.governor_tenure IS
    'Each governor''s tenure: consecutive terms of one holder and party, a month or less apart, as one span.';
COMMENT ON VIEW gold_elections.governor_term_fiscal IS
    'Derived (ADR-0014 fiscal section): each governor tenure''s change in state and local tax burden and in '
    'payments share of income, net of the nation''s change over the same years, one measure per row.';
COMMENT ON VIEW gold_elections.governor_term_tax_mix IS
    'Derived (ADR-0014 fiscal section): the shares of state and local taxes by type in the last year of each '
    'governor tenure''s window; other taxes are the total less property, general sales and income.';
