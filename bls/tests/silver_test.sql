-- bls/tests/silver_test.sql
-- Data Integrity Tests for silver_bls.fact_labor_statistics
-- Run this after silver layer transforms complete

-- ============================================================================
-- 1. BASIC STATISTICS
-- ============================================================================

SELECT '=== BLS SILVER LAYER - BASIC STATISTICS ===' AS test_section;

SELECT 
    'Total Records' AS metric,
    COUNT(*)::TEXT AS value
FROM silver_bls.fact_labor_statistics

UNION ALL

SELECT 
    'Unique Series' AS metric,
    COUNT(DISTINCT series_id)::TEXT AS value
FROM silver_bls.fact_labor_statistics

UNION ALL

SELECT 
    'Unique Programs' AS metric,
    COUNT(DISTINCT program)::TEXT AS value
FROM silver_bls.fact_labor_statistics

UNION ALL

SELECT 
    'Date Range (Min)' AS metric,
    MIN(period_date)::TEXT AS value
FROM silver_bls.fact_labor_statistics

UNION ALL

SELECT 
    'Date Range (Max)' AS metric,
    MAX(period_date)::TEXT AS value
FROM silver_bls.fact_labor_statistics

UNION ALL

SELECT 
    'Records with NULL values' AS metric,
    COUNT(*)::TEXT AS value
FROM silver_bls.fact_labor_statistics
WHERE value IS NULL

UNION ALL

SELECT 
    'Unique Geographies' AS metric,
    COUNT(DISTINCT (geo_level, geo_id))::TEXT AS value
FROM silver_bls.fact_labor_statistics;

-- ============================================================================
-- 2. PRIMARY KEY & UNIQUENESS TESTS
-- ============================================================================

SELECT '=== PRIMARY KEY & UNIQUENESS TESTS ===' AS test_section;

-- Check for NULL primary keys (should be 0)
SELECT 
    'Null Primary Keys' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE labor_stat_sk IS NULL;

-- Check unique constraint (series_id, period_date)
SELECT 
    'Duplicate (series_id, period_date)' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT series_id, period_date, COUNT(*) as cnt
    FROM silver_bls.fact_labor_statistics
    GROUP BY series_id, period_date
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
FROM silver_bls.fact_labor_statistics f
WHERE NOT EXISTS (
    SELECT 1 FROM silver_ref.dim_time t
    WHERE t.time_sk = f.time_sk
);

-- Check geo_sk references valid dim_geo records
SELECT 
    'Invalid geo_sk (orphaned FK)' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics f
WHERE NOT EXISTS (
    SELECT 1 FROM silver_ref.dim_geo g
    WHERE g.geo_sk = f.geo_sk
);

-- Check NULL time_sk (should be 0 - NOT NULL constraint)
SELECT 
    'NULL time_sk values' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE time_sk IS NULL;

-- Check NULL geo_sk (should be 0 - NOT NULL constraint)
SELECT 
    'NULL geo_sk values' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE geo_sk IS NULL;

-- ============================================================================
-- 4. DATE CONSISTENCY TESTS
-- ============================================================================

SELECT '=== DATE CONSISTENCY TESTS ===' AS test_section;

-- period_date should be within duration range
SELECT 
    'period_date outside duration range' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE period_date < duration_start 
   OR period_date > duration_end;

-- duration_start should be <= duration_end
SELECT 
    'duration_start > duration_end' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE duration_start > duration_end;

-- time_sk should match duration_start in dim_time
SELECT 
    'time_sk mismatch with duration_start' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics f
JOIN silver_ref.dim_time t ON f.time_sk = t.time_sk
WHERE t.date_key != f.duration_start;

-- Check for NULL required date fields
SELECT 
    'NULL date fields' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE duration_start IS NULL 
   OR duration_end IS NULL 
   OR period_date IS NULL;

-- ============================================================================
-- 5. GEOGRAPHY CONSISTENCY TESTS
-- ============================================================================

SELECT '=== GEOGRAPHY CONSISTENCY TESTS ===' AS test_section;

-- geo_sk should match (geo_level, geo_id) in dim_geo
SELECT 
    'geo_sk mismatch with (geo_level, geo_id)' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics f
JOIN silver_ref.dim_geo g ON f.geo_sk = g.geo_sk
WHERE g.geo_level != f.geo_level 
   OR g.geo_id != f.geo_id;

-- state_fips should be consistent with geo_level
SELECT 
    'state_fips NULL for state/county level' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE geo_level IN ('state', 'county')
  AND (state_fips IS NULL OR state_fips = '');

-- county_fips should only exist for county level
SELECT 
    'county_fips set for non-county level' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM silver_bls.fact_labor_statistics
WHERE geo_level != 'county'
  AND county_fips IS NOT NULL 
  AND county_fips != '';

-- ============================================================================
-- 6. DATA QUALITY TESTS
-- ============================================================================

SELECT '=== DATA QUALITY TESTS ===' AS test_section;

-- Check for NULL series_id (required field)
SELECT 
    'NULL series_id' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE series_id IS NULL OR series_id = '';

-- Check for NULL program (required field)
SELECT 
    'NULL program' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE program IS NULL OR program = '';

-- Check for reasonable value ranges
SELECT 
    'Unreasonable numeric values' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE value IS NOT NULL 
  AND (ABS(value) > 1e15);

-- Check year consistency with period_date
SELECT 
    'year mismatch with period_date' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE EXTRACT(YEAR FROM period_date) != year;

-- Check for NULL year
SELECT 
    'NULL year' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE year IS NULL;

-- Check for NULL period
SELECT 
    'NULL period' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE period IS NULL OR period = '';

-- Check source_system consistency
SELECT 
    'Invalid source_system' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE source_system IS NULL 
   OR source_system != 'BLS';

-- Check for NULL load_batch_id
SELECT 
    'NULL load_batch_id' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE load_batch_id IS NULL;

-- Check for NULL ingested_at
SELECT 
    'NULL ingested_at' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_bls.fact_labor_statistics
WHERE ingested_at IS NULL;

-- Check seasonal_adjustment values
SELECT 
    'Invalid seasonal_adjustment' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM silver_bls.fact_labor_statistics
WHERE seasonal_adjustment IS NOT NULL 
  AND seasonal_adjustment NOT IN ('S', 'U', 'N');

-- ============================================================================
-- 7. PROGRAM-SPECIFIC TESTS
-- ============================================================================

SELECT '=== PROGRAM-SPECIFIC TESTS ===' AS test_section;

-- Records per program
SELECT 
    program,
    COUNT(*) AS record_count,
    COUNT(DISTINCT series_id) AS unique_series,
    COUNT(DISTINCT geo_id) AS unique_geographies,
    MIN(period_date) AS earliest_date,
    MAX(period_date) AS latest_date
FROM silver_bls.fact_labor_statistics
GROUP BY program
ORDER BY record_count DESC;

-- Geography distribution by program
SELECT 
    program,
    geo_level,
    COUNT(*) AS record_count,
    COUNT(DISTINCT series_id) AS unique_series
FROM silver_bls.fact_labor_statistics
GROUP BY program, geo_level
ORDER BY program, geo_level;

-- ============================================================================
-- 8. PERIOD TYPE DISTRIBUTION
-- ============================================================================

SELECT '=== PERIOD TYPE DISTRIBUTION ===' AS test_section;

SELECT 
    period,
    period_name,
    COUNT(*) AS record_count,
    COUNT(DISTINCT series_id) AS unique_series
FROM silver_bls.fact_labor_statistics
GROUP BY period, period_name
ORDER BY 
    CASE 
        WHEN period ~ '^M\d{2}$' THEN 1  -- Monthly
        WHEN period ~ '^Q\d{1}$' THEN 2  -- Quarterly
        WHEN period = 'A01' THEN 3       -- Annual
        ELSE 4
    END,
    period;

-- ============================================================================
-- 9. MEASURE CODE ANALYSIS
-- ============================================================================

SELECT '=== MEASURE CODE ANALYSIS ===' AS test_section;

SELECT 
    program,
    measure_code,
    COUNT(*) AS record_count,
    COUNT(DISTINCT series_id) AS unique_series
FROM silver_bls.fact_labor_statistics
WHERE measure_code IS NOT NULL
GROUP BY program, measure_code
ORDER BY program, record_count DESC;

-- ============================================================================
-- 10. DATA FRESHNESS
-- ============================================================================

SELECT '=== DATA FRESHNESS ===' AS test_section;

SELECT 
    program,
    geo_level,
    MAX(period_date) AS latest_period,
    MAX(ingested_at) AS latest_ingestion,
    AGE(CURRENT_TIMESTAMP, MAX(ingested_at)) AS time_since_last_load
FROM silver_bls.fact_labor_statistics
GROUP BY program, geo_level
ORDER BY program, geo_level;

-- ============================================================================
-- 11. DETAILED FAILURE REPORT
-- ============================================================================

SELECT '=== DETAILED FAILURE EXAMPLES (IF ANY) ===' AS test_section;

-- Show examples of records with issues (limit to 10 per issue type)
SELECT 'ORPHANED TIME_SK' AS issue_type, *
FROM silver_bls.fact_labor_statistics f
WHERE NOT EXISTS (
    SELECT 1 FROM silver_ref.dim_time t WHERE t.time_sk = f.time_sk
)
LIMIT 10;

SELECT 'ORPHANED GEO_SK' AS issue_type, *
FROM silver_bls.fact_labor_statistics f
WHERE NOT EXISTS (
    SELECT 1 FROM silver_ref.dim_geo g WHERE g.geo_sk = f.geo_sk
)
LIMIT 10;

SELECT 'DATE RANGE VIOLATIONS' AS issue_type, *
FROM silver_bls.fact_labor_statistics
WHERE period_date < duration_start 
   OR period_date > duration_end
   OR duration_start > duration_end
LIMIT 10;

SELECT 'MISSING REQUIRED FIELDS' AS issue_type, *
FROM silver_bls.fact_labor_statistics
WHERE series_id IS NULL 
   OR program IS NULL
   OR time_sk IS NULL 
   OR geo_sk IS NULL
   OR load_batch_id IS NULL
LIMIT 10;

-- ============================================================================
-- SUMMARY
-- ============================================================================

SELECT '=== TEST SUMMARY ===' AS test_section;

SELECT 
    'Test Complete' AS status,
    CURRENT_TIMESTAMP AS tested_at,
    COUNT(*) AS total_records
FROM silver_bls.fact_labor_statistics;
