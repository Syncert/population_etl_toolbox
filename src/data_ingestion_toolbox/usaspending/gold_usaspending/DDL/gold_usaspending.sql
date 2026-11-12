-- Publication views over USAspending federal award obligations by place of
-- performance (where-public-money-goes slice 4). Applied by the bootstrap
-- manifest in the `gold` phase and re-applied by the DAG's
-- `ensure_usaspending_schema` task; this file is the only definition of
-- these views.

CREATE SCHEMA IF NOT EXISTS gold_usaspending;

-- The registered measures: all awards and each award type group (registry.AWARD_GROUPS).
CREATE OR REPLACE VIEW gold_usaspending.measure_definition AS
SELECT measure.measure, measure.cut_kind, measure.cut_key, measure.measure_label,
       'US dollars'::TEXT AS unit,
       'USAspending spending_by_geography, scope place_of_performance, spending level '
       || 'transactions: the sum of award transaction obligations dated in the federal '
       || 'fiscal year (October 1 to September 30), as USAspending publishes it. '
       || 'Recorded where the awarded work is performed, which is not always where the money is felt: pass-through grants sit with the state agency that receives them, and some payments (Medicare) where the claims processor is. Obligations by award transaction date in the federal fiscal year; not all federal spending (salaries and most interest are not awards). Separate from what local governments in the place spend; never add the two.'::TEXT AS observation_basis,
       'ANNUAL'::TEXT AS time_grain
FROM (VALUES
    ('federal_award_obligations', 'all', '', 'Federal award obligations by place of performance (USAspending)'),
    ('federal_award_obligations_contracts', 'award_type', 'contracts', 'Federal award obligations by place of performance, contracts (USAspending)'),
    ('federal_award_obligations_grants', 'award_type', 'grants', 'Federal award obligations by place of performance, grants (USAspending)'),
    ('federal_award_obligations_direct_payments', 'award_type', 'direct_payments', 'Federal award obligations by place of performance, direct payments (USAspending)'),
    ('federal_award_obligations_loans', 'award_type', 'loans', 'Federal award obligations by place of performance, loans (USAspending)'),
    ('federal_award_obligations_other_assistance', 'award_type', 'other_assistance', 'Federal award obligations by place of performance, other financial assistance (USAspending)')
) AS measure(measure, cut_kind, cut_key, measure_label);

-- Rows of a published request only, current and each a later answer replaced
-- or withdrew, placed on a geography the shared reference holds.
CREATE OR REPLACE VIEW gold_usaspending.geography_amount_served AS
SELECT amount_row.fiscal_year, amount_row.geo_layer, amount_row.cut_kind, amount_row.cut_key,
       request.agency_name, amount_row.shape_code, amount_row.display_name,
       amount_row.value_source, amount_row.amount, amount_row.value_status,
       amount_row.geo_id, entity.geo_sk, amount_row.geo_layer AS geo_type,
       request.published_at, amount_row.source_record_id, amount_row.capture_id,
       amount_row.run_id, amount_row.is_current
FROM (
    SELECT current.fiscal_year, current.geo_layer, current.cut_kind, current.cut_key,
           current.shape_code, current.display_name, current.value_source, current.amount,
           current.value_status, current.geo_id, current.geography_status,
           current.source_record_id, current.capture_id, current.run_id, TRUE AS is_current
    FROM silver_usaspending.geography_amount AS current
    UNION ALL
    SELECT revision.fiscal_year, revision.geo_layer, revision.cut_kind, revision.cut_key,
           revision.shape_code, revision.display_name, revision.value_source, revision.amount,
           revision.value_status, revision.geo_id, revision.geography_status,
           revision.source_record_id, revision.capture_id, revision.run_id, FALSE
    FROM silver_usaspending.geography_amount_revision AS revision
) AS amount_row
JOIN control.usaspending_request AS request
  ON request.run_id = amount_row.run_id
 AND request.fiscal_year = amount_row.fiscal_year
 AND request.geo_layer = amount_row.geo_layer
 AND request.cut_kind = amount_row.cut_kind
 AND request.cut_key = amount_row.cut_key
JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = amount_row.geo_id
WHERE request.status = 'published'
  AND amount_row.geography_status <> 'unsupported'
  AND amount_row.value_status = 'valid';

-- Every published fiscal-year amount of each registered measure, current and replaced.
CREATE OR REPLACE VIEW gold_usaspending.observation_revision AS
SELECT definition.measure AS metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       served.fiscal_year AS year,
       MAKE_DATE(served.fiscal_year - 1, 10, 1) AS period_start,
       MAKE_DATE(served.fiscal_year, 9, 30) AS period_end,
       served.geo_id,
       served.geo_sk,
       served.geo_type,
       gold_glossary.geo_grain(served.geo_type) AS geo_level,
       served.value_source,
       served.amount AS value,
       served.value_status,
       'USAspending FY' || served.fiscal_year || ' read '
       || TO_CHAR(served.published_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS release_key,
       served.published_at,
       served.source_record_id,
       served.capture_id,
       served.run_id
FROM gold_usaspending.geography_amount_served AS served
JOIN gold_usaspending.measure_definition AS definition
  ON definition.cut_kind = served.cut_kind AND definition.cut_key = served.cut_key;

-- The current published amount of each measure, place and fiscal year,
-- read straight from the current table so a place is one index range.
CREATE OR REPLACE VIEW gold_usaspending.observation_latest AS
SELECT definition.measure AS metric_key,
       definition.measure_label,
       definition.unit,
       definition.observation_basis,
       current.fiscal_year AS year,
       MAKE_DATE(current.fiscal_year - 1, 10, 1) AS period_start,
       MAKE_DATE(current.fiscal_year, 9, 30) AS period_end,
       current.geo_id,
       entity.geo_sk,
       current.geo_layer AS geo_type,
       gold_glossary.geo_grain(current.geo_layer) AS geo_level,
       current.value_source,
       current.amount AS value,
       current.value_status,
       'USAspending FY' || current.fiscal_year || ' read '
       || TO_CHAR(request.published_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS release_key,
       request.published_at,
       current.source_record_id,
       current.capture_id,
       current.run_id
FROM silver_usaspending.geography_amount AS current
JOIN gold_usaspending.measure_definition AS definition
  ON definition.cut_kind = current.cut_kind AND definition.cut_key = current.cut_key
JOIN control.usaspending_request AS request
  ON request.run_id = current.run_id
 AND request.fiscal_year = current.fiscal_year
 AND request.geo_layer = current.geo_layer
 AND request.cut_kind = current.cut_kind
 AND request.cut_key = current.cut_key
JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = current.geo_id
WHERE request.status = 'published'
  AND current.geography_status <> 'unsupported'
  AND current.value_status = 'valid';

-- Each award type group's obligations in a place and fiscal year, and its
-- share of all award obligations there (the `all` read of the same year and
-- layer). Groups can be negative (loan deobligations); a place's groups add
-- to its total within USAspending's own rounding of uncategorised awards.
CREATE OR REPLACE VIEW gold_usaspending.awards_by_award_type AS
SELECT part.fiscal_year,
       part.geo_id,
       part.geo_type,
       part.cut_key AS award_group,
       definition.measure_label,
       part.amount AS obligated_amount,
       total.amount AS place_total_obligated_amount,
       CASE WHEN total.amount IS NULL OR total.amount = 0 THEN NULL
            ELSE ROUND(100 * part.amount / total.amount, 2) END AS share_of_place_total_pct,
       CASE WHEN total.amount IS NULL THEN 'all-award total not published for this place and year'
            WHEN total.amount = 0 THEN 'all-award total is zero'
       END AS unavailable_reason,
       definition.observation_basis AS place_of_performance_note,
       'USAspending FY' || part.fiscal_year AS release_key,
       part.published_at,
       part.source_record_id,
       part.capture_id,
       part.run_id
FROM gold_usaspending.geography_amount_served AS part
JOIN gold_usaspending.measure_definition AS definition
  ON definition.cut_kind = 'award_type' AND definition.cut_key = part.cut_key
LEFT JOIN gold_usaspending.geography_amount_served AS total
  ON total.is_current
 AND total.cut_kind = 'all'
 AND total.fiscal_year = part.fiscal_year
 AND total.geo_layer = part.geo_layer
 AND total.geo_id = part.geo_id
WHERE part.is_current AND part.cut_kind = 'award_type';

-- Each awarding agency's obligations in a place and fiscal year, and its
-- share of all award obligations there.
CREATE OR REPLACE VIEW gold_usaspending.awards_by_agency AS
SELECT part.fiscal_year,
       part.geo_id,
       part.geo_type,
       part.cut_key AS agency_code,
       part.agency_name,
       part.amount AS obligated_amount,
       total.amount AS place_total_obligated_amount,
       CASE WHEN total.amount IS NULL OR total.amount = 0 THEN NULL
            ELSE ROUND(100 * part.amount / total.amount, 2) END AS share_of_place_total_pct,
       CASE WHEN total.amount IS NULL THEN 'all-award total not published for this place and year'
            WHEN total.amount = 0 THEN 'all-award total is zero'
       END AS unavailable_reason,
       'Recorded where the awarded work is performed, which is not always where the money is felt: pass-through grants sit with the state agency that receives them, and some payments (Medicare) where the claims processor is. Obligations by award transaction date in the federal fiscal year; not all federal spending (salaries and most interest are not awards). Separate from what local governments in the place spend; never add the two.'::TEXT AS place_of_performance_note,
       'USAspending FY' || part.fiscal_year AS release_key,
       part.published_at,
       part.source_record_id,
       part.capture_id,
       part.run_id
FROM gold_usaspending.geography_amount_served AS part
LEFT JOIN gold_usaspending.geography_amount_served AS total
  ON total.is_current
 AND total.cut_kind = 'all'
 AND total.fiscal_year = part.fiscal_year
 AND total.geo_layer = part.geo_layer
 AND total.geo_id = part.geo_id
WHERE part.is_current AND part.cut_kind = 'agency';

CREATE OR REPLACE VIEW gold_usaspending.measure_export AS
SELECT measure AS source_object_key, measure, measure_label, unit, observation_basis,
       'usaspending_geography:v1'::TEXT AS schema_version, time_grain
FROM gold_usaspending.measure_definition;

COMMENT ON VIEW gold_usaspending.awards_by_agency IS
    'Federal award obligations by awarding agency and place of performance (USAspending), with each '
    'agency''s share of the place''s all-award total for the fiscal year.';
COMMENT ON SCHEMA gold_usaspending IS
    'Policy-free publication views for USAspending federal award obligations by place of performance '
    '(states and counties, fiscal years from 2008).';
