# Implementation Checklist: Source-First Schema Migration

## Files Provided ✅

### SQL Schema Files (Ready to Use)
- ✅ `src/data_ingestion_toolbox/bls/gold_bls/DDL/gold_bls_v2_source_first.sql`
- ✅ `src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs_v2_source_first.sql`
- ✅ `src/data_ingestion_toolbox/fred/gold_fred/DDL/gold_fred_v2_source_first.sql`
- ✅ `src/data_ingestion_toolbox/gold/DDL/v_comparison_views.sql` (template + examples)

### Python Utilities (Ready to Use)
- ✅ `apps/api/services/source_router.py` — Source detection & routing
- ✅ `apps/api/services/comparison_service_v2.py` — Updated comparison service

### Documentation (Ready to Use)
- ✅ `SCHEMA_REFACTORING_GUIDE.md` — Executive summary & rationale
- ✅ Session memory: `schema_assessment.md`, `source_first_migration_plan.md`

---

## Phase 1: Database Schema Migration

### Step 1.1: Backup Current Data
```bash
# Backup existing unified serving tables
pg_dump -h <host> -U <user> -d <db> -t gold.rpt_observation_dashboard -t gold.mv_latest_dashboard > backup_unified_serving_tables.sql
```

### Step 1.2: Create Per-Source Serving Tables (Non-Breaking)
```sql
-- Each script creates its own schema and tables
-- Run in this order:

psql -f src/data_ingestion_toolbox/bls/gold_bls/DDL/gold_bls_v2_source_first.sql
psql -f src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs_v2_source_first.sql
psql -f src/data_ingestion_toolbox/fred/gold_fred/DDL/gold_fred_v2_source_first.sql
```

**Result**: New tables exist alongside old unified infrastructure:
- `gold_bls.rpt_bls_observations` (empty)
- `gold_census.rpt_acs_observations` (empty)
- `gold_fred.rpt_fred_observations` (empty)
- Plus their MVs: `mv_bls_latest`, `mv_acs_latest`, `mv_fred_latest`

### Step 1.3: Migrate Data from Unified to Per-Source Tables
Create migration script:
```sql
-- Migrate BLS data
INSERT INTO gold_bls.rpt_bls_observations (
    source_code, observation_date, geo_id, geo_level, state_fips, county_fips,
    state_name, county_name, geo_latitude, geo_longitude,
    series_id, program_code, survey_name, series_title,
    measure_name, measure_category, observation_basis, units, value, value_type,
    seasonal_adjustment_status, gold_metric_name, comparison_warning,
    metric_code, metric_display_name, dashboard_suitability,
    business_definition, caveats, comparability_group, do_not_compare_with,
    recommended_aggregation, owner_team, time_sk, as_of_date, updated_at,
    duration_start, duration_end
)
SELECT
    source_code, observation_date, geo_id, geo_level, state_fips, county_fips,
    state_name, county_name, geo_latitude, geo_longitude,
    series_id, program_code, survey_name, series_title,
    measure_name, measure_category, observation_basis, units, value, value_type,
    seasonal_adjustment_status, gold_metric_name, comparison_warning,
    metric_code, metric_display_name, dashboard_suitability,
    business_definition, caveats, comparability_group, do_not_compare_with,
    recommended_aggregation, owner_team, time_sk, as_of_date, updated_at,
    duration_start, duration_end
FROM gold.rpt_observation_dashboard
WHERE source_code = 'BLS';

-- Migrate ACS data
INSERT INTO gold_census.rpt_acs_observations (
    source_code, observation_date, geo_id, geo_level, state_fips, county_fips,
    state_name, county_name, geo_latitude, geo_longitude,
    dataset_code, vintage_year, table_id, table_title,
    variable_code, variable_label, concept, universe, denominator_hint,
    is_publishable_default, estimate_value, margin_of_error, margin_of_error_pct,
    estimate_annotation, moe_annotation, value_type, units,
    metric_code, metric_display_name, dashboard_suitability,
    business_definition, caveats, comparability_group, do_not_compare_with,
    recommended_aggregation, owner_team, time_sk, as_of_date, updated_at,
    duration_start, duration_end
)
SELECT
    source_code, observation_date, geo_id, geo_level, state_fips, county_fips,
    state_name, county_name, geo_latitude, geo_longitude,
    dataset_code, vintage_year, table_id, table_title,
    variable_code, variable_label, concept, universe, denominator_hint,
    is_publishable_default, estimate_value, margin_of_error, margin_of_error_pct,
    estimate_annotation, moe_annotation, value_type, units,
    metric_code, metric_display_name, dashboard_suitability,
    business_definition, caveats, comparability_group, do_not_compare_with,
    recommended_aggregation, owner_team, time_sk, as_of_date, updated_at,
    duration_start, duration_end
FROM gold.rpt_observation_dashboard
WHERE source_code = 'CENSUS_ACS';

-- Migrate FRED data
INSERT INTO gold_fred.rpt_fred_observations (
    source_code, observation_date, geo_id, geo_level,
    series_id, series_title, value, value_type, units, frequency,
    seasonal_adjustment_status, source_provider, original_source_name,
    is_primary_source_series, is_republished_series, transformation_method,
    realtime_start, realtime_end,
    metric_code, metric_display_name, dashboard_suitability,
    business_definition, caveats, comparability_group, do_not_compare_with,
    recommended_aggregation, owner_team, time_sk, as_of_date, updated_at,
    duration_start, duration_end, state_fips, county_fips, state_name, county_name,
    geo_latitude, geo_longitude
)
SELECT
    source_code, observation_date, geo_id, geo_level,
    series_id, series_title, value, value_type, units, frequency,
    seasonal_adjustment_status, source_provider, original_source_name,
    is_primary_source_series, is_republished_series, transformation_method,
    realtime_start, realtime_end,
    metric_code, metric_display_name, dashboard_suitability,
    business_definition, caveats, comparability_group, do_not_compare_with,
    recommended_aggregation, owner_team, time_sk, as_of_date, updated_at,
    duration_start, duration_end, state_fips, county_fips, state_name, county_name,
    geo_latitude, geo_longitude
FROM gold.rpt_observation_dashboard
WHERE source_code = 'FRED';

-- Populate MVs
CALL gold_bls.refresh_mv_bls_latest();
CALL gold_census.refresh_mv_acs_latest();
CALL gold_fred.refresh_mv_fred_latest();
```

### Step 1.4: Verify Data Integrity
```sql
-- Check row counts match
SELECT 'BLS' AS source, COUNT(*) FROM gold_bls.rpt_bls_observations
UNION ALL
SELECT 'ACS', COUNT(*) FROM gold_census.rpt_acs_observations
UNION ALL
SELECT 'FRED', COUNT(*) FROM gold_fred.rpt_fred_observations;

-- Compare with old table
SELECT source_code, COUNT(*) FROM gold.rpt_observation_dashboard GROUP BY source_code;
```

### Step 1.5: Create Cross-Source Comparison Views
```sql
-- Apply the template
psql -f src/data_ingestion_toolbox/gold/DDL/v_comparison_views.sql

-- Test that views work
SELECT COUNT(*) FROM gold.v_comparison_labor_vs_income;
```

---

## Phase 2: API Service Updates

### Step 2.1: Deploy source_router Module
✅ File ready: `apps/api/services/source_router.py`

No changes needed; just deploy as-is.

### Step 2.2: Update observations_service.py
**Current**: Queries `gold.v_metric_latest_by_geo` or `gold.mv_latest_dashboard`

**New**: Use `source_router` to route to source-specific table:

```python
# At the top of observations_service.py, add import:
from apps.api.services.source_router import get_source_from_metric, get_table_for_source

# Update list_latest_observations() function:
def list_latest_observations(db, metric_code, geo_level, state_fips, limit, offset):
    # NEW: Detect source and route
    source_code = get_source_from_metric(db, metric_code)
    if not source_code:
        raise ValueError(f"Metric {metric_code} not found")
    
    table_name = get_table_for_source(source_code, 'latest')
    
    # Then query from table_name (not the unified table)
    query = text(f"""
        SELECT ...
        FROM {table_name}
        WHERE metric_code = :metric_code
        ...
    """)
    ...
```

### Step 2.3: Update comparison_service.py
✅ Template ready: `apps/api/services/comparison_service_v2.py`

Replace `comparison_service.py` content with `comparison_service_v2.py`, or update the existing file to use `source_router`:

```python
from apps.api.services.source_router import validate_sources_for_comparison, get_table_for_source, get_sources_from_metrics

def list_metric_comparison(db, metric_code_a, metric_code_b, geo_level, state_fips, limit, offset):
    # NEW: Validate same source
    is_valid, error = validate_sources_for_comparison(db, [metric_code_a, metric_code_b])
    if not is_valid:
        raise ValueError(error)
    
    # Get source and route
    sources = get_sources_from_metrics(db, [metric_code_a, metric_code_b])
    source_code = sources[metric_code_a]
    table_name = get_table_for_source(source_code, 'latest')
    
    # Query from per-source table (not unified)
    ...
```

### Step 2.4: Update distribution_service.py
Similar pattern to comparison_service:
```python
from apps.api.services.source_router import get_source_from_metric, get_table_for_source

def list_distribution_bins(db, metric_code, geo_level, state_fips, bin_count):
    source_code = get_source_from_metric(db, metric_code)
    table_name = get_table_for_source(source_code, 'latest')
    
    # Query from per-source table
    ...
```

### Step 2.5: Test API Endpoints
```bash
# After updating services, run tests
pytest apps/api/tests/test_observations.py -v
pytest apps/api/tests/test_comparison.py -v
pytest apps/api/tests/test_distribution.py -v
```

---

## Phase 3: DAG Updates

### Step 3.1: Update Procedure Calls
Files: `dags/bls_ingest_dag.py`, `dags/acs_ingest_dag.py`, `dags/fred_ingest_dag.py`

**Find and replace**:
```python
# OLD (wrong namespace, causes stale data)
db.execute("CALL gold.refresh_dashboard_serving_layer_bls(:start, :end)")
db.execute("CALL gold.refresh_dashboard_serving_layer_acs(:start, :end)")
db.execute("CALL gold.refresh_dashboard_serving_layer_fred(:start, :end)")

# NEW (correct, source-scoped)
db.execute("CALL gold_bls.refresh_dashboard_serving_layer_bls(:start, :end)")
db.execute("CALL gold_census.refresh_dashboard_serving_layer_acs(:start, :end)")
db.execute("CALL gold_fred.refresh_dashboard_serving_layer_fred(:start, :end)")
```

### Step 3.2: Test Full DAG Run
```bash
# Trigger a full DAG run and verify:
# 1. Data lands in per-source tables (not unified table)
# 2. MVs are refreshed (not empty)
# 3. No errors in procedure calls

# After DAG completes, verify data:
SELECT COUNT(*) FROM gold_bls.rpt_bls_observations WHERE as_of_date = CURRENT_DATE;
SELECT COUNT(*) FROM gold_census.rpt_acs_observations WHERE as_of_date = CURRENT_DATE;
SELECT COUNT(*) FROM gold_fred.rpt_fred_observations WHERE as_of_date = CURRENT_DATE;
```

---

## Phase 4: Testing & Validation

### Step 4.1: Unit Tests
Create `tests/test_source_router.py`:
```python
from apps.api.services.source_router import *

def test_get_source_from_metric():
    db = get_test_db()
    assert get_source_from_metric(db, "BLS:LAUS123") == "BLS"
    assert get_source_from_metric(db, "ACS:acs5:B01001") == "CENSUS_ACS"
    assert get_source_from_metric(db, "FRED:UNRATE") == "FRED"

def test_get_table_for_source():
    assert get_table_for_source('BLS', 'latest') == 'gold_bls.mv_bls_latest'
    assert get_table_for_source('CENSUS_ACS', 'latest') == 'gold_census.mv_acs_latest'
    assert get_table_for_source('FRED', 'latest') == 'gold_fred.mv_fred_latest'

def test_validate_sources_same():
    db = get_test_db()
    is_valid, error = validate_sources_for_comparison(db, ["BLS:A", "BLS:B"])
    assert is_valid is True
    assert error is None

def test_validate_sources_different():
    db = get_test_db()
    is_valid, error = validate_sources_for_comparison(db, ["BLS:A", "ACS:B"])
    assert is_valid is False
    assert "different sources" in error.lower()
```

### Step 4.2: Integration Tests
```bash
# Test API endpoints with per-source routing
pytest apps/api/tests/test_observations.py::test_list_latest_observations -v
pytest apps/api/tests/test_comparison.py::test_comparison_same_source -v
pytest apps/api/tests/test_distribution.py::test_distribution -v
```

### Step 4.3: Performance Comparison
```sql
-- Measure per-source MV refresh time
-- Before: CALL gold.refresh_dashboard_serving_layer_bls() (unified table)
-- After: CALL gold_bls.refresh_dashboard_serving_layer_bls() (per-source)

-- Expected: Per-source should be faster or equal (no NULL pollution)
```

---

## Phase 5: Cleanup (After Confidence)

### Step 5.1: Drop Unified Infrastructure
```sql
-- Only after all services verified with per-source tables
DROP MATERIALIZED VIEW IF EXISTS gold.mv_latest_dashboard CASCADE;
DROP TABLE IF EXISTS gold.rpt_observation_dashboard CASCADE;
```

### Step 5.2: Remove Fallback Code
Search for and remove legacy fallback logic:
```python
# Remove these fallback checks (now unnecessary)
_latest_relation_name() function calls
_relation_exists() fallback logic
```

### Step 5.3: Update Documentation
- Update API docs to mention source-aware routing
- Add guide for creating cross-source comparison views
- Document source_router module

---

## Rollback Procedure

If issues are found post-migration:

1. **Stop using per-source tables**:
   - Revert API services to query unified table
   - Revert DAG procedure calls to old names

2. **Keep both infrastructures alive**:
   - Don't drop unified table yet
   - Per-source tables can remain (they're just not being used)

3. **Investigate and fix**:
   - Identify root cause
   - Fix and test thoroughly
   - Only then retry cutover

---

## Success Criteria

✅ All per-source tables populated with correct data
✅ API endpoints route to correct source tables
✅ DAG procedures execute without errors
✅ Query latency unchanged or improved
✅ Cross-source views work for test cases
✅ Integration tests pass
✅ Performance metrics acceptable
✅ Documentation updated

---

## Timeline Estimate

- **Phase 1 (SQL)**: 2-4 hours
- **Phase 2 (API)**: 4-6 hours
- **Phase 3 (DAG)**: 1-2 hours
- **Phase 4 (Testing)**: 2-4 hours
- **Phase 5 (Cleanup)**: 1 hour

**Total**: ~10-17 hours of work

