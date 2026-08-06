-- tests/sql/fred_silver_diagnostics.sql
-- Data Integrity Tests for silver_fred.fact_economic_indicators
-- Run this after silver layer transforms complete

-- ============================================================================
-- 1. BASIC STATISTICS
-- ============================================================================

SELECT '=== FRED SILVER LAYER - BASIC STATISTICS ===' AS test_section;

SELECT 
    'Total Records' AS metric,
    COUNT(*) AS value
FROM silver_fred.fact_economic_indicators

UNION ALL

SELECT 
    'Unique Series' AS metric,
    COUNT(DISTINCT series_id) AS value
FROM silver_fred.fact_economic_indicators

UNION ALL

SELECT 
    'Unique Domains' AS metric,
    COUNT(DISTINCT domain) AS value
FROM silver_fred.fact_economic_indicators

UNION ALL

SELECT 
    'Date Range (Min)' AS metric,
    MIN(observation_date)::TEXT AS value
FROM silver_fred.fact_economic_indicators

UNION ALL

SELECT 
    'Date Range (Max)' AS metric,
    MAX(observation_date)::TEXT AS value
FROM silver_fred.fact_economic_indicators

UNION ALL

SELECT 
    'Records with NULL values' AS metric,
    COUNT(*) AS value
FROM silver_fred.fact_economic_indicators
WHERE value IS NULL

UNION ALL

SELECT 
    'Records marked as missing' AS metric,
    COUNT(*) AS value
FROM silver_fred.fact_economic_indicators
WHERE is_missing = TRUE;

-- ============================================================================
-- 2. PRIMARY KEY & UNIQUENESS TESTS
-- ============================================================================

SELECT '=== PRIMARY KEY & UNIQUENESS TESTS ===' AS test_section;

-- Check for NULL primary keys (should be 0)
SELECT 
    'Null Primary Keys' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_fred.fact_economic_indicators
WHERE economic_indicator_sk IS NULL;

-- Check unique constraint (series_id, observation_date)
SELECT 
    'Duplicate (series_id, observation_date)' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT series_id, observation_date, COUNT(*) as cnt
    FROM silver_fred.fact_economic_indicators
    GROUP BY series_id, observation_date
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
FROM silver_fred.fact_economic_indicators f
WHERE NOT EXISTS (
    SELECT 1 FROM silver_ref.dim_time t
    WHERE t.time_sk = f.time_sk
);

-- Check NULL time_sk (should be 0 - NOT NULL constraint)
SELECT 
    'NULL time_sk values' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_fred.fact_economic_indicators
WHERE time_sk IS NULL;

-- ============================================================================
-- 4. DATE CONSISTENCY TESTS
-- ============================================================================

SELECT '=== DATE CONSISTENCY TESTS ===' AS test_section;

-- duration_start should be <= observation_date <= duration_end
SELECT 
    'observation_date outside duration range' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_fred.fact_economic_indicators
WHERE observation_date < duration_start 
   OR observation_date > duration_end;

-- duration_start should be <= duration_end
SELECT 
    'duration_start > duration_end' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_fred.fact_economic_indicators
WHERE duration_start > duration_end;

-- time_sk should match duration_start in dim_time
SELECT 
    'time_sk mismatch with duration_start' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_fred.fact_economic_indicators f
JOIN silver_ref.dim_time t ON f.time_sk = t.time_sk
WHERE t.date_key != f.duration_start;

-- Check for NULL required date fields
SELECT 
    'NULL date fields' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_fred.fact_economic_indicators
WHERE duration_start IS NULL 
   OR duration_end IS NULL 
   OR observation_date IS NULL;

-- ============================================================================
-- 5. DATA QUALITY TESTS
-- ============================================================================

SELECT '=== DATA QUALITY TESTS ===' AS test_section;

-- Check for NULL series_id (required field)
SELECT 
    'NULL series_id' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_fred.fact_economic_indicators
WHERE series_id IS NULL OR series_id = '';

-- Check for reasonable value ranges (not infinity, not NaN represented as NULL in numeric)
SELECT 
    'Unreasonable numeric values' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_fred.fact_economic_indicators
WHERE value IS NOT NULL 
  AND (ABS(value) > 1e15);  -- Sanity check for extremely large values

-- Check for records with is_missing=FALSE but NULL value
SELECT 
    'is_missing=FALSE with NULL value' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM silver_fred.fact_economic_indicators
WHERE is_missing = FALSE AND value IS NULL;

-- Check source_system consistency
SELECT 
    'Invalid source_system' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_fred.fact_economic_indicators
WHERE source_system IS NULL 
   OR source_system != 'FRED';

-- Check for NULL load_batch_id
SELECT 
    'NULL load_batch_id' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_fred.fact_economic_indicators
WHERE load_batch_id IS NULL;

-- Check for NULL ingested_at
SELECT 
    'NULL ingested_at' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM silver_fred.fact_economic_indicators
WHERE ingested_at IS NULL;

-- ============================================================================
-- 6. DOMAIN-SPECIFIC TESTS
-- ============================================================================

SELECT '=== DOMAIN-SPECIFIC TESTS ===' AS test_section;

-- Records per domain
SELECT 
    domain,
    COUNT(*) AS record_count,
    COUNT(DISTINCT series_id) AS unique_series,
    MIN(observation_date) AS earliest_date,
    MAX(observation_date) AS latest_date
FROM silver_fred.fact_economic_indicators
GROUP BY domain
ORDER BY record_count DESC;

-- Check for invalid/unexpected domains
SELECT 
    'Records with NULL or empty domain' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM silver_fred.fact_economic_indicators
WHERE domain IS NULL OR TRIM(domain) = '';

-- ============================================================================
-- 7. FREQUENCY DISTRIBUTION TESTS
-- ============================================================================

SELECT '=== FREQUENCY DISTRIBUTION ===' AS test_section;

SELECT 
    frequency,
    COUNT(*) AS record_count,
    COUNT(DISTINCT series_id) AS unique_series
FROM silver_fred.fact_economic_indicators
GROUP BY frequency
ORDER BY record_count DESC;

-- ============================================================================
-- 8. METADATA COMPLETENESS TESTS
-- ============================================================================

SELECT '=== METADATA COMPLETENESS ===' AS test_section;

SELECT 
    'Records missing series_title' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM silver_fred.fact_economic_indicators
WHERE series_title IS NULL OR TRIM(series_title) = '';

SELECT 
    'Records missing unit_of_measure' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM silver_fred.fact_economic_indicators
WHERE unit_of_measure IS NULL OR TRIM(unit_of_measure) = '';

SELECT 
    'Records missing frequency' AS test_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status
FROM silver_fred.fact_economic_indicators
WHERE frequency IS NULL OR TRIM(frequency) = '';

-- ============================================================================
-- 9. RECENT DATA FRESHNESS
-- ============================================================================

SELECT '=== DATA FRESHNESS ===' AS test_section;

SELECT 
    domain,
    MAX(observation_date) AS latest_observation,
    MAX(ingested_at) AS latest_ingestion,
    AGE(CURRENT_TIMESTAMP, MAX(ingested_at)) AS time_since_last_load
FROM silver_fred.fact_economic_indicators
GROUP BY domain
ORDER BY domain;

-- ============================================================================
-- 10. DETAILED FAILURE REPORT
-- ============================================================================

SELECT '=== DETAILED FAILURE EXAMPLES (IF ANY) ===' AS test_section;

-- Show examples of records with issues (limit to 10 per issue type)
SELECT 'ORPHANED TIME_SK' AS issue_type, *
FROM silver_fred.fact_economic_indicators f
WHERE NOT EXISTS (
    SELECT 1 FROM silver_ref.dim_time t WHERE t.time_sk = f.time_sk
)
LIMIT 10;

SELECT 'DATE RANGE VIOLATIONS' AS issue_type, *
FROM silver_fred.fact_economic_indicators
WHERE observation_date < duration_start 
   OR observation_date > duration_end
   OR duration_start > duration_end
LIMIT 10;

SELECT 'MISSING REQUIRED FIELDS' AS issue_type, *
FROM silver_fred.fact_economic_indicators
WHERE series_id IS NULL 
   OR time_sk IS NULL 
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
FROM silver_fred.fact_economic_indicators;
