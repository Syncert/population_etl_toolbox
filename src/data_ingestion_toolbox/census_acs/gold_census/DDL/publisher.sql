CREATE OR REPLACE VIEW gold_census.metric_publisher AS
SELECT DISTINCT ON (variable.dataset_code, variable.variable_code)
    'CENSUS_ACS'::TEXT AS source_code,
    '1.0'::TEXT AS publisher_contract_version,
    (variable.dataset_code || ':' || variable.variable_code)::TEXT AS source_object_key,
    'variable'::TEXT AS source_object_type,
    COALESCE(NULLIF(variable.variable_label, ''), variable.variable_code)::TEXT AS metric_display_name,
    NULL::TEXT AS units,
    variable.value_role::TEXT AS measure_kind,
    CASE variable.dataset_code
        WHEN 'acs1' THEN ARRAY['NATIONAL', 'STATE']::TEXT[]
        ELSE ARRAY['NATIONAL', 'STATE', 'COUNTY']::TEXT[]
    END AS valid_geo_grains,
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
ORDER BY variable.dataset_code, variable.variable_code,
         variable.vintage_year DESC, variable.updated_at DESC;
