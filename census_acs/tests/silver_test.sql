-- census_acs/tests/silver_test.sql
-- Data Integrity Tests for silver_census.fact_demographics
-- Run this after silver layer transforms complete

-- ============================================================================
-- 1. BASIC STATISTICS
-- ============================================================================

SELECT '=== CENSUS ACS SILVER LAYER - BASIC STATISTICS ===' AS test_section;

SELECT 
    'Total Records' AS metric,
    COUNT(*) AS value
FROM silver_census.fact_demographics

UNION ALL

SELECT 
    'Unique Datasets' AS metric,
    COUNT(DISTINCT dataset) AS value
FROM silver_census.fact_demographics

UNION ALL

SELECT 
    'Unique Tables' AS metric,
    COUNT(DISTINCT table_id) AS value
FROM silver_census.fact_demographics

UNION ALL

SELECT 
    'Unique Variables' AS metric,
    COUNT(DISTINCT variable_code) AS value
FROM silver_census.fact_demographics

UNION ALL

SELECT 
    'Year Range (Min)' AS metric,
    MIN(estimate_year)::TEXT AS value
FROM silver_census.fact_demographics

UNION ALL

SELECT 
    'Year Range (Max)' AS metric,
    MAX(estimate_year)::TEXT AS value
FROM silver_census.fact_demographics

UNION ALL

SELECT 
    'Records with NULL estimates' AS metric,
    COUNT(*) AS value
FROM silver_census.fact_demographics
WHERE estimate_value IS NULL

UNION ALL

SELECT 
    'Unique Geographies' AS metric,
    COUNT(DISTINCT (geo_level, geo_id)) AS value
FROM silver_census.fact_demographics;

-- ============================================================================
-- 2. PRIMARY KEY & UNIQUENESS TESTS
-- ============================================================================

SELECT '=== PRIMARY KEY & UNIQUENESS TESTS ===' AS test_section;

-- Check for NULL primary keys (should be 0)
SELECT 
    'Null Primary Keys' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE demographic_sk IS NULL;

-- Check unique constraint (dataset, table_id, variable_code, geo_id, estimate_year)
SELECT 
    'Duplicate (dataset, table_id, variable_code, geo_id, estimate_year)' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT dataset, table_id, variable_code, geo_id, estimate_year, COUNT(*) as cnt
    FROM silver_census.fact_demographics
    GROUP BY dataset, table_id, variable_code, geo_id, estimate_year
    HAVING COUNT(*) > 1
) dupes;

-- ============================================================================
-- 3. FOREIGN KEY INTEGRITY TESTS
-- ============================================================================

SELECT '=== FOREIGN KEY INTEGRITY TESTS ===' AS test_section;

-- Check time_sk references valid dim_time records
SELECT 
    'Invalid time_sk (orphaned FK)' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics f
WHERE NOT EXISTS (
    SELECT 1 FROM silver_ref.dim_time t
    WHERE t.time_sk = f.time_sk
);

-- Check geo_sk references valid dim_geo records
SELECT 
    'Invalid geo_sk (orphaned FK)' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics f
WHERE NOT EXISTS (
    SELECT 1 FROM silver_ref.dim_geo g
    WHERE g.geo_sk = f.geo_sk
);

-- Check NULL time_sk (should be 0 - NOT NULL constraint)
SELECT 
    'NULL time_sk values' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE time_sk IS NULL;

-- Check NULL geo_sk (should be 0 - NOT NULL constraint)
SELECT 
    'NULL geo_sk values' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE geo_sk IS NULL;

-- ============================================================================
-- 4. DATE CONSISTENCY TESTS
-- ============================================================================

SELECT '=== DATE CONSISTENCY TESTS ===' AS test_section;

-- duration_start should be <= duration_end
SELECT 
    'duration_start > duration_end' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE duration_start > duration_end;

-- time_sk should match duration_start in dim_time
SELECT 
    'time_sk mismatch with duration_start' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics f
JOIN silver_ref.dim_time t ON f.time_sk = t.time_sk
WHERE t.date_key != f.duration_start;

-- Check for NULL required date fields
SELECT 
    'NULL date fields' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE duration_start IS NULL 
   OR duration_end IS NULL;

-- estimate_year should be reasonable
SELECT 
    'Unreasonable estimate_year' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE estimate_year < 2005 
   OR estimate_year > EXTRACT(YEAR FROM CURRENT_DATE) + 1;

-- Check NULL estimate_year
SELECT 
    'NULL estimate_year' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE estimate_year IS NULL;

-- ============================================================================
-- 5. GEOGRAPHY CONSISTENCY TESTS
-- ============================================================================

SELECT '=== GEOGRAPHY CONSISTENCY TESTS ===' AS test_section;

-- geo_sk should match (geo_level, geo_id) in dim_geo
SELECT 
    'geo_sk mismatch with (geo_level, geo_id)' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics f
JOIN silver_ref.dim_geo g ON f.geo_sk = g.geo_sk
WHERE g.geo_level != f.geo_level 
   OR g.geo_id != f.geo_id;

-- state_fips should be consistent with geo_level
SELECT 
    'state_fips NULL for state/county level' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE geo_level IN ('state', 'county')
  AND (state_fips IS NULL OR state_fips = '');

-- county_fips should only exist for county level
SELECT 
    'county_fips set for non-county level' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM silver_census.fact_demographics
WHERE geo_level != 'county'
  AND county_fips IS NOT NULL 
  AND county_fips != '';

-- ============================================================================
-- 6. DATA QUALITY TESTS
-- ============================================================================

SELECT '=== DATA QUALITY TESTS ===' AS test_section;

-- Check for NULL dataset (required field)
SELECT 
    'NULL dataset' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE dataset IS NULL OR dataset = '';

-- Check for NULL table_id (required field)
SELECT 
    'NULL table_id' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE table_id IS NULL OR table_id = '';

-- Check for NULL variable_code (required field)
SELECT 
    'NULL variable_code' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE variable_code IS NULL OR variable_code = '';

-- Check for reasonable estimate values
SELECT 
    'Unreasonable estimate values' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE estimate_value IS NOT NULL 
  AND (ABS(estimate_value) > 1e15);

-- Check margin_of_error is not negative
SELECT 
    'Negative margin_of_error' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE margin_of_error IS NOT NULL 
  AND margin_of_error < 0;

-- Check margin_of_error_pct is reasonable (0-100 or negative for special codes)
SELECT 
    'Unreasonable margin_of_error_pct' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM silver_census.fact_demographics
WHERE margin_of_error_pct IS NOT NULL 
  AND margin_of_error_pct > 100;

-- Check source_system consistency
SELECT 
    'Invalid source_system' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE source_system IS NULL 
   OR source_system != 'CENSUS_ACS';

-- Check for NULL load_batch_id
SELECT 
    'NULL load_batch_id' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE load_batch_id IS NULL;

-- Check for NULL ingested_at
SELECT 
    'NULL ingested_at' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_census.fact_demographics
WHERE ingested_at IS NULL;

-- ============================================================================
-- 7. DATASET-SPECIFIC TESTS
-- ============================================================================

SELECT '=== DATASET-SPECIFIC TESTS ===' AS test_section;

-- Records per dataset and year
SELECT 
    dataset,
    estimate_year,
    COUNT(*) AS record_count,
    COUNT(DISTINCT table_id) AS unique_tables,
    COUNT(DISTINCT variable_code) AS unique_variables,
    COUNT(DISTINCT geo_id) AS unique_geographies
FROM silver_census.fact_demographics
GROUP BY dataset, estimate_year
ORDER BY dataset, estimate_year DESC;

-- Geography distribution by dataset
SELECT 
    dataset,
    geo_level,
    COUNT(*) AS record_count,
    COUNT(DISTINCT geo_id) AS unique_geographies
FROM silver_census.fact_demographics
GROUP BY dataset, geo_level
ORDER BY dataset, geo_level;

-- ============================================================================
-- 8. TABLE COVERAGE ANALYSIS
-- ============================================================================

SELECT '=== TABLE COVERAGE ANALYSIS ===' AS test_section;

-- Top 20 tables by record count
SELECT 
    table_id,
    COUNT(*) AS record_count,
    COUNT(DISTINCT variable_code) AS unique_variables,
    COUNT(DISTINCT geo_id) AS unique_geographies,
    COUNT(DISTINCT estimate_year) AS year_coverage
FROM silver_census.fact_demographics
GROUP BY table_id
ORDER BY record_count DESC
LIMIT 20;

-- ============================================================================
-- 9. METADATA COMPLETENESS TESTS
-- ============================================================================

SELECT '=== METADATA COMPLETENESS ===' AS test_section;

SELECT 
    'Records missing variable_label' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM silver_census.fact_demographics
WHERE variable_label IS NULL OR TRIM(variable_label) = '';

SELECT 
    'Records missing variable_concept' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM silver_census.fact_demographics
WHERE variable_concept IS NULL OR TRIM(variable_concept) = '';

SELECT 
    'Records missing universe' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM silver_census.fact_demographics
WHERE universe IS NULL OR TRIM(universe) = '';

-- ============================================================================
-- 10. ESTIMATE VS MARGIN OF ERROR ANALYSIS
-- ============================================================================

SELECT '=== ESTIMATE VS MOE ANALYSIS ===' AS test_section;

-- Records with estimate but no MOE
SELECT 
    'Estimate without MOE' AS test_name,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) = 0 THEN 'INFO' ELSE 'INFO' END AS status
FROM silver_census.fact_demographics
WHERE estimate_value IS NOT NULL 
  AND margin_of_error IS NULL;

-- Records with MOE but no estimate
SELECT 
    'MOE without estimate' AS test_name,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM silver_census.fact_demographics
WHERE estimate_value IS NULL 
  AND margin_of_error IS NOT NULL;

-- Records where MOE > estimate (high uncertainty)
SELECT 
    'MOE exceeds estimate' AS test_name,
    COUNT(*) AS count,
    CASE WHEN COUNT(*) = 0 THEN 'INFO' ELSE 'INFO' END AS status
FROM silver_census.fact_demographics
WHERE estimate_value IS NOT NULL 
  AND margin_of_error IS NOT NULL
  AND margin_of_error > ABS(estimate_value);

-- ============================================================================
-- 11. DATA FRESHNESS
-- ============================================================================

SELECT '=== DATA FRESHNESS ===' AS test_section;

SELECT 
    dataset,
    estimate_year,
    MAX(ingested_at) AS latest_ingestion,
    AGE(CURRENT_TIMESTAMP, MAX(ingested_at)) AS time_since_last_load,
    COUNT(*) AS record_count
FROM silver_census.fact_demographics
GROUP BY dataset, estimate_year
ORDER BY dataset, estimate_year DESC;

-- ============================================================================
-- 12. DETAILED FAILURE REPORT
-- ============================================================================

SELECT '=== DETAILED FAILURE EXAMPLES (IF ANY) ===' AS test_section;

-- Show examples of records with issues (limit to 10 per issue type)
SELECT 'ORPHANED TIME_SK' AS issue_type, *
FROM silver_census.fact_demographics f
WHERE NOT EXISTS (
    SELECT 1 FROM silver_ref.dim_time t WHERE t.time_sk = f.time_sk
)
LIMIT 10;

SELECT 'ORPHANED GEO_SK' AS issue_type, *
FROM silver_census.fact_demographics f
WHERE NOT EXISTS (
    SELECT 1 FROM silver_ref.dim_geo g WHERE g.geo_sk = f.geo_sk
)
LIMIT 10;

SELECT 'DATE RANGE VIOLATIONS' AS issue_type, *
FROM silver_census.fact_demographics
WHERE duration_start > duration_end
LIMIT 10;

SELECT 'MISSING REQUIRED FIELDS' AS issue_type, *
FROM silver_census.fact_demographics
WHERE dataset IS NULL 
   OR table_id IS NULL
   OR variable_code IS NULL
   OR time_sk IS NULL 
   OR geo_sk IS NULL
   OR load_batch_id IS NULL
LIMIT 10;

SELECT 'NEGATIVE MARGIN OF ERROR' AS issue_type, *
FROM silver_census.fact_demographics
WHERE margin_of_error IS NOT NULL 
  AND margin_of_error < 0
LIMIT 10;

-- ============================================================================
-- SUMMARY
-- ============================================================================

SELECT '=== TEST SUMMARY ===' AS test_section;

SELECT 
    'Test Complete' AS status,
    CURRENT_TIMESTAMP AS tested_at,
    COUNT(*) AS total_records
FROM silver_census.fact_demographics;
