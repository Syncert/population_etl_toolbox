-- Grains are derived from the served relation, never declared.
--
-- This view used to declare valid_geo_grains from the dataset code: acs1
-- publishes NATIONAL and STATE, everything else all three. On the
-- development warehouse that declared STATE for 4,447 current metrics of
-- which 1,658 serve no state row, and COUNTY for 1,927 of which 829 serve
-- none -- 2,487 metric/grain pairs the catalog advertised and nothing could
-- answer. A consumer following the catalog got an empty page it could not
-- tell from a geography that publishes nothing. BLS derives its grains from
-- its facts and has never had this problem; this does the same over the
-- relation the API actually serves for the latest scope.
--
-- A variable with no served row publishes no grain. That is the honest
-- statement, and the catalog-serving agreement guards (DB-025, DB-028)
-- report a current code with nothing behind it rather than this view
-- inventing a grain for it.
CREATE OR REPLACE VIEW gold_census.metric_publisher AS
SELECT DISTINCT ON (variable.dataset_code, variable.variable_code)
    'CENSUS_ACS'::TEXT AS source_code,
    '1.0'::TEXT AS publisher_contract_version,
    (variable.dataset_code || ':' || variable.variable_code)::TEXT AS source_object_key,
    'variable'::TEXT AS source_object_type,
    COALESCE(NULLIF(variable.variable_label, ''), variable.variable_code)::TEXT AS metric_display_name,
    NULL::TEXT AS units,
    variable.value_role::TEXT AS measure_kind,
    COALESCE(served.valid_geo_grains, ARRAY[]::TEXT[]) AS valid_geo_grains,
    ARRAY['ANNUAL']::TEXT[] AS valid_time_grains,
    NULL::TEXT AS aggregation_characteristic,
    JSONB_BUILD_OBJECT('schema', 'gold_census', 'relation', 'fact_acs_observation', 'key', variable.dataset_code || ':' || variable.variable_code) AS physical_lineage,
    variable.updated_at::TEXT AS source_watermark,
    NULL::UUID AS source_run_id,
    variable.updated_at AS publication_time,
    'U.S. Census Bureau American Community Survey'::TEXT AS source_name,
    'official-statistics'::TEXT AS source_type,
    table_definition.reference_url::TEXT AS reference_url
FROM gold_census.dim_acs_variable AS variable
JOIN gold_census.dim_acs_table AS table_definition USING (acs_table_sk)
LEFT JOIN (
    -- One row per served metric with the grains its latest rows carry, in
    -- the vocabulary the API filters on (gold_glossary.geo_grain is defined
    -- in the glossary phase, after this file runs at bootstrap, so the
    -- served relation's own upper-cased word is used; it already is that
    -- vocabulary for ACS).
    SELECT latest.metric_code,
           ARRAY_AGG(DISTINCT UPPER(latest.geo_level)
                     ORDER BY UPPER(latest.geo_level))::TEXT[] AS valid_geo_grains
    FROM gold_census.mv_acs_latest AS latest
    GROUP BY latest.metric_code
) AS served
  ON served.metric_code = 'CENSUS_ACS:' || variable.dataset_code || ':' || variable.variable_code
ORDER BY variable.dataset_code, variable.variable_code,
         variable.vintage_year DESC, variable.updated_at DESC;
