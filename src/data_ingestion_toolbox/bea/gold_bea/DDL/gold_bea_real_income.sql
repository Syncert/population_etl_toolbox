-- Disposable personal income per person after inflation, by state and for
-- the nation (taxes-and-public-money, T-1). Applied by the bootstrap manifest
-- after `gold-fred` and `gold-bea`, and re-applied by the DAG's
-- `ensure_bea_schema` task; this file is the only definition of the view.
--
-- Two views of one BEA figure (SAINC51 line 53, current dollars):
--
--   * national prices: converted to 2017 dollars with the national PCE price
--     index (FRED PCEPI, 2017 = 100). A year's index is the mean of its twelve
--     months and only a year with all twelve is used, as the lean groups do
--     (lean_groups.sql). The annual index deflates here and is never itself
--     published: the site does not roll a monthly series to a year for
--     serving.
--   * regional prices: the national-prices figure divided again by the
--     state's all-items regional price parity (SARPP line 1, U.S. = 100), so
--     a state where prices run 6 percent below the nation's reads 6 percent
--     higher. This is BEA's own method for real personal income by state.
--     BEA publishes parities from 2008, so earlier years have none.
--
-- Each release is read by its own newest capture. The two inputs come from
-- different BEA releases (state income each September, parities each
-- December); each row names both release dates. Reads filter on
-- `table_code` and `line_code`, which reach the fact's lookup index; the
-- general `observation_latest` view is keyed on `metric_key` and would scan
-- every BEA fact.

CREATE SCHEMA IF NOT EXISTS gold_bea;

CREATE OR REPLACE VIEW gold_bea.disposable_income_real AS
WITH income AS (
    SELECT DISTINCT ON (revision.geo_id, revision.year)
           revision.geo_id, revision.geo_type, revision.geo_level, revision.year,
           revision.value, revision.value_status, revision.release_date
    FROM gold_bea.observation_revision AS revision
    WHERE revision.table_code = 'SAINC51'
      AND revision.line_code = '53'
    ORDER BY revision.geo_id, revision.year,
             revision.release_date DESC, revision.retrieved_at DESC, revision.capture_id DESC
),
parity AS (
    SELECT DISTINCT ON (revision.geo_id, revision.year)
           revision.geo_id, revision.year, revision.value, revision.release_date
    FROM gold_bea.observation_revision AS revision
    WHERE revision.table_code = 'SARPP'
      AND revision.line_code = '1'
      AND revision.value_status = 'valid'
    ORDER BY revision.geo_id, revision.year,
             revision.release_date DESC, revision.retrieved_at DESC, revision.capture_id DESC
),
price_level AS (
    SELECT EXTRACT(YEAR FROM observation.observation_date)::INTEGER AS year,
           AVG(observation.value) AS level
    FROM gold_fred.fact_fred_observation AS observation
    JOIN gold_fred.dim_fred_series AS series
      ON series.fred_series_sk = observation.fred_series_sk
    WHERE series.series_id = 'PCEPI'
      AND observation.value IS NOT NULL
    GROUP BY 1
    HAVING COUNT(DISTINCT EXTRACT(MONTH FROM observation.observation_date)) = 12
)
SELECT income.geo_id,
       income.geo_type,
       income.geo_level,
       income.year,
       income.value AS current_dollars,
       income.value_status,
       income.release_date AS income_release_date,
       CASE WHEN income.value_status = 'valid' AND price_level.level > 0
            THEN ROUND(income.value * 100 / price_level.level)
       END AS national_prices_2017_dollars,
       parity.value AS regional_price_parity,
       parity.release_date AS parity_release_date,
       CASE WHEN income.value_status = 'valid' AND price_level.level > 0
                 AND parity.value > 0
            THEN ROUND(income.value * 100 / price_level.level * 100 / parity.value)
       END AS regional_prices_2017_dollars,
       'BEA SAINC51 line 53 (per capita disposable personal income); FRED PCEPI; BEA SARPP line 1'::TEXT
           AS sources
FROM income
LEFT JOIN price_level ON price_level.year = income.year
LEFT JOIN parity ON parity.geo_id = income.geo_id AND parity.year = income.year;

COMMENT ON VIEW gold_bea.disposable_income_real IS
    'Per capita disposable personal income by state and for the nation, in current dollars and '
    'in 2017 dollars at national prices (PCE) and at the state''s own prices (PCE and regional '
    'price parity). Derived from BEA figures; never written over them.';
