-- Publication views over reconciled Census government finance
-- (taxes-and-public-money T-5). Applied by the bootstrap manifest in the
-- `gold` phase and re-applied by the DAG's `ensure_census_govfin_schema`
-- task; this file is the only definition of these views.

CREATE SCHEMA IF NOT EXISTS gold_census_govfin;

-- Every published capture of every observation: the as-released surface. A
-- revised publication is a second row with its own capture and retrieval
-- time, never an overwrite. A fiscal year is dated by the calendar year it
-- ends in (46 states end theirs on June 30); a QTAX quarter is a calendar
-- quarter. The fact's CHECK admits only `resolved` and `unmapped`; the
-- predicate states the serving rule (DB-035).
CREATE OR REPLACE VIEW gold_census_govfin.observation_revision AS
SELECT fact.dataset_id || ':' || fact.measure_id || ':' || fact.government_level AS metric_key,
       fact.dataset_id,
       fact.measure_id,
       fact.government_level,
       measure.measure_label,
       measure.definition,
       measure.unit,
       measure.cadence,
       measure.methodology_url,
       fact.fiscal_year,
       fact.fiscal_year AS year,
       fact.quarter,
       CASE WHEN fact.quarter = 0 THEN MAKE_DATE(fact.fiscal_year, 1, 1)
            ELSE MAKE_DATE(fact.fiscal_year, fact.quarter * 3 - 2, 1) END AS period_start,
       CASE WHEN fact.quarter = 0 THEN MAKE_DATE(fact.fiscal_year, 12, 31)
            ELSE (MAKE_DATE(fact.fiscal_year, fact.quarter * 3 - 2, 1)
                  + INTERVAL '3 months' - INTERVAL '1 day')::DATE END AS period_end,
       CASE WHEN fact.quarter = 0
            THEN 'fiscal year ending in ' || fact.fiscal_year
            ELSE fact.fiscal_year || ' Q' || fact.quarter END AS period_label,
       fact.geo_id,
       fact.geo_sk,
       fact.geo_type,
       gold_glossary.geo_grain(fact.geo_type) AS geo_level,
       fact.geography_status,
       fact.value_source,
       fact.value,
       fact.value_status,
       fact.government_count,
       fact.derivation,
       fact.record_coverage,
       fact.retrieved_at,
       TO_CHAR(fact.retrieved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS release_key,
       fact.source_record_id,
       fact.capture_id,
       fact.run_id
FROM silver_census_govfin.fact_observation AS fact
JOIN silver_census_govfin.dim_measure AS measure
  ON measure.dataset_id = fact.dataset_id
 AND measure.measure_id = fact.measure_id
JOIN control.census_govfin_slice AS slice
  ON slice.capture_id = fact.capture_id
WHERE slice.status = 'published'
  AND fact.geography_status NOT IN ('unsupported', 'ambiguous');

-- The newest published capture of each observation.
CREATE OR REPLACE VIEW gold_census_govfin.observation_latest AS
SELECT DISTINCT ON (revision.metric_key, revision.dataset_id, revision.measure_id, revision.government_level, revision.geo_id, revision.fiscal_year, revision.quarter)
       revision.*
FROM gold_census_govfin.observation_revision AS revision
ORDER BY revision.metric_key, revision.dataset_id, revision.measure_id, revision.government_level, revision.geo_id, revision.fiscal_year, revision.quarter,
         revision.retrieved_at DESC, revision.capture_id DESC;

-- The governments a county area's figures sum, from each edition's newest
-- published directory: a government spanning counties is counted whole in
-- the home county Census gives it, and a page saying "collected in" a
-- county names this rule.
CREATE OR REPLACE VIEW gold_census_govfin.county_area_government AS
SELECT DISTINCT ON (government.census_year, government.government_code)
       government.census_year,
       government.government_code,
       government.government_name,
       government.government_type,
       government.county_name,
       'state:' || government.fips_state || '|county:' || government.fips_county AS geo_id,
       government.population,
       government.capture_id,
       government.run_id
FROM silver_census_govfin.unit_government AS government
JOIN control.census_govfin_slice AS slice ON slice.capture_id = government.capture_id
JOIN raw_capture.response_capture AS capture ON capture.capture_id = government.capture_id
WHERE slice.status = 'published'
  AND government.type_code <> '0'
  AND government.fips_county IS NOT NULL
ORDER BY government.census_year, government.government_code,
         capture.retrieved_at DESC, government.capture_id DESC;

-- Each tax type's share of a place's taxes: same dataset, same government
-- level, same period and place (ADR-0009). No share where the total is
-- missing or zero.
CREATE OR REPLACE VIEW gold_census_govfin.tax_mix AS
SELECT part.dataset_id,
       part.government_level,
       part.measure_id,
       part.measure_label,
       part.fiscal_year,
       part.quarter,
       part.geo_id,
       part.geo_type,
       part.value AS tax_amount,
       total.value AS taxes_total,
       CASE WHEN part.value_status = 'valid' AND total.value_status = 'valid'
                 AND total.value <> 0
            THEN ROUND(part.value / total.value * 100, 2) END AS share_of_taxes_pct,
       CASE WHEN part.value_status <> 'valid' OR total.value_status <> 'valid' THEN 'suppressed_input'
            WHEN total.value = 0 THEN 'denominator_zero' END AS unavailable_reason,
       part.release_key
FROM gold_census_govfin.observation_latest AS part
JOIN gold_census_govfin.observation_latest AS total
  ON total.dataset_id = part.dataset_id
 AND total.government_level = part.government_level
 AND total.measure_id = 'taxes_total'
 AND total.fiscal_year = part.fiscal_year
 AND total.quarter = part.quarter
 AND total.geo_id = part.geo_id
WHERE part.measure_id IN (
    'tax_property', 'tax_general_sales', 'tax_selective_sales',
    'tax_individual_income', 'tax_corporate_income', 'tax_other'
);

-- Each spending line's share of direct general spending: same dataset,
-- government level, year and place (ADR-0009). The lines are Census's own
-- aggregate lines whose item codes partition line 66, so the shares of a
-- place add to 100 (where-public-money-goes slice 1; the list is
-- registry.SPENDING_MIX_LINES). Utilities, liquor stores and insurance trust
-- payments are outside general spending and are never in these shares.
-- County areas exist only for Census of Governments years (cog_units);
-- the nation and states every year from 2017 (asslgf). No share where the
-- total is missing or zero.
CREATE OR REPLACE VIEW gold_census_govfin.spending_mix AS
SELECT part.dataset_id,
       part.government_level,
       part.measure_id,
       part.measure_label,
       part.fiscal_year,
       part.geo_id,
       part.geo_type,
       part.value AS spending_amount,
       total.value AS general_spending_total,
       CASE WHEN part.value_status = 'valid' AND total.value_status = 'valid'
                 AND total.value <> 0
            THEN ROUND(part.value / total.value * 100, 2) END AS share_of_general_spending_pct,
       CASE WHEN part.value_status <> 'valid' OR total.value_status <> 'valid' THEN 'suppressed_input'
            WHEN total.value = 0 THEN 'denominator_zero' END AS unavailable_reason,
       part.government_count,
       part.release_key
FROM gold_census_govfin.observation_latest AS part
JOIN gold_census_govfin.observation_latest AS total
  ON total.dataset_id = part.dataset_id
 AND total.government_level = part.government_level
 AND total.measure_id = 'spend_general_total'
 AND total.fiscal_year = part.fiscal_year
 AND total.quarter = part.quarter
 AND total.geo_id = part.geo_id
WHERE part.dataset_id IN ('cog_units', 'asslgf')
  AND part.measure_id IN (
    'spend_higher_education', 'spend_elementary_secondary_education',
    'spend_other_education', 'spend_libraries', 'spend_public_welfare',
    'spend_hospitals', 'spend_health', 'spend_social_insurance_administration',
    'spend_veterans_services', 'spend_highways', 'spend_airports',
    'spend_parking', 'spend_sea_inland_ports', 'spend_police', 'spend_fire',
    'spend_correction', 'spend_protective_inspection', 'spend_natural_resources',
    'spend_parks_recreation', 'spend_housing_community_development',
    'spend_sewerage', 'spend_solid_waste', 'spend_financial_administration',
    'spend_judicial_legal', 'spend_public_buildings', 'spend_other_administration',
    'spend_interest_general_debt', 'spend_misc_commercial', 'spend_other_unallocable'
);

CREATE OR REPLACE VIEW gold_census_govfin.measure_export AS
SELECT measure.dataset_id || ':' || measure.measure_id || ':' || level.government_level AS source_object_key,
       measure.dataset_id AS source_dataset,
       measure.measure_id AS source_measure_code,
       level.government_level,
       measure.measure_label AS display_name,
       measure.definition,
       measure.unit,
       measure.cadence,
       measure.methodology_url,
       measure.parser_contract_version AS schema_version
FROM silver_census_govfin.dim_measure AS measure
CROSS JOIN (VALUES ('state_and_local'), ('state'), ('local')) AS level(government_level)
WHERE measure.dataset_id <> 'qtax' OR level.government_level = 'state';

COMMENT ON SCHEMA gold_census_govfin IS
    'Policy-free publication views for Census state and local government finance: taxes, revenue, aid, spending and debt.';

COMMENT ON VIEW gold_census_govfin.observation_latest IS
    'The newest published capture of each government finance figure; fiscal years are dated by the calendar year they end in.';

COMMENT ON VIEW gold_census_govfin.spending_mix IS
    'Each Census spending line as a share of direct general spending, same dataset, level, year and place; county areas in Census of Governments years only.';

COMMENT ON VIEW gold_census_govfin.county_area_government IS
    'The governments a county-area figure sums: each counted whole in its Census home county, Census of Governments years only.';
