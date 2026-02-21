# ACTION PLAN: Fixing BLS & Census Silver Layer Transforms

## Problem Statement

Both `transform_to_silver_by_program` tasks failed with:
- **Return code -9** (SIGKILL due to out-of-memory)
- **Missing geo_sk**: BLS dropped 4320 rows, Census dropped unknown number

## Root Causes

### 1. Memory Exhaustion (Primary Issue)
- Both transforms loaded the entire `silver_ref.dim_geo` table into memory
- For millions of rows, this created huge intermediate dataframes in Polars
- OOM killer terminated the process (signal 9)

### 2. Geography Dimension Sync (Secondary Issue)  
- 4320 BLS rows missing `geo_sk` values suggests dim_geo is incompletely populated or out of sync
- Possible timing issue: silver_ref DAG (05:00) runs before BLS DAG (07:00)

---

## IMMEDIATE ACTIONS (Do These Now)

### Step 1: Verify silver_ref Is Current
```bash
# In Airflow UI:
1. Navigate to DAGs → silver_ref
2. Click "Trigger DAG" 
3. Wait for it to complete successfully
4. Check logs to verify dim_geo and dim_time synced
```

**Why**: Ensures geography and time dimensions are up-to-date before retry.

### Step 2: Review the Applied Code Changes

The following fixes have been implemented:

**Files Modified**:
- `bls/silver_bls/transform.py`
- `census_acs/silver_census/transform.py`

**Key Change**: Memory-efficient geography loading
```python
# Instead of loading entire dim_geo table:
geo_df = _load_geo_dim(hook)  # ← OLD (takes entire table)

# Now only load geographies present in data:
unique_geos = df.select(["geo_level", "geo_id"]).unique()
geo_df = _load_geo_dim_for_list(hook, unique_geos)  # ← NEW (selective load)
```

### Step 3: Run Diagnostic Script
```bash
# From workspace root:
python scripts/diagnose_geo_missing.py

# Or in Airflow:
cd /opt/airflow
python population_etl_toolbox/scripts/diagnose_geo_missing.py
```

**What It Does**:
- Identifies missing geographic combinations in dim_geo
- Shows which geographies would cause row drops
- Reports dimension sync status
- Suggests next steps

### Step 4: Retry the Failed DAGs
```bash
# In Airflow UI:
1. DAGs → bls_raw_ingest → "Trigger DAG"
2. DAGs → acs_raw_ingest → "Trigger DAG"
3. Monitor logs for:
   - No return code -9 (memory fixed!)
   - Dropped row counts (should be 0 or very low)
```

---

## EXPECTED OUTCOMES

### After Fix Applied (Should See)

✓ **No return code -9 errors**
- Memory usage stays within container limits
- Task completes successfully or fails on actual data issues, not OOM

✓ **Minimal or zero dropped rows**
```
[After fix] Dropped X BLS rows with missing geo_sk. Ensure silver_ref.dim_geo is synced.
```
- If X is still high after `silver_ref` sync, investigate further
- If X is 0, everything is working!

✓ **Faster data joins**
- Only relevant geographies loaded
- Reduced memory pressure overall

---

## VERIFICATION CHECKLIST

After running the DAGs, verify:

- [ ] BLS transform completes without error
- [ ] Census ACS transform completes without error  
- [ ] No task has return code -9
- [ ] Log shows reasonable number of rows processed
- [ ] Minimal (ideally 0) "dropped rows" messages
- [ ] Silver tables (`silver_bls.fact_labor_statistics`, `silver_census.fact_demographics`) updated
- [ ] Data freshness checked in silver layer

```sql
-- Quick verification queries:
SELECT COUNT(*) FROM silver_bls.fact_labor_statistics;
SELECT COUNT(*) FROM silver_census.fact_demographics;
SELECT MAX(ingested_at) FROM silver_bls.fact_labor_statistics;
SELECT MAX(ingested_at) FROM silver_census.fact_demographics;
```

---

## IF ISSUES PERSIST

### Debug Step 1: Run Diagnostic
```bash
python scripts/diagnose_geo_missing.py
```
Review output for:
- Missing geographies (would explain dropped rows)
- Dimension sync timestamp (should be recent)

### Debug Step 2: Manual Geography Check
```sql
-- What geographies does BLS data need?
SELECT DISTINCT 
    SUBSTRING(series_id, 1, 5) as series_prefix,
    geo_level, 
    COUNT(*) as count
FROM (
    SELECT DISTINCT
        series_id,
        CASE 
            WHEN series_id LIKE 'LNS%' THEN 'us'
            WHEN series_id LIKE 'LASST%' THEN 'state'
            WHEN series_id LIKE 'LAUCN%' THEN 'county'
            ELSE 'unknown'
        END AS geo_level
    FROM raw_bls.bls_long
) x
GROUP BY series_prefix, geo_level;

-- What's in dim_geo?
SELECT geo_level, COUNT(*) FROM silver_ref.dim_geo GROUP BY geo_level;
```

### Debug Step 3: Monitor Resource Usage
During next DAG run, check:
```bash
# In container/pod:
top -b           # Monitor memory usage
ps aux | grep python  # Check python process memory
```

Memory should stay well under container limit (usually 3-4GB).

### Debug Step 4: Check Time Dimension
If Census is missing rows despite memory fix:
```sql
-- Verify dim_time covers the years in the data
SELECT MIN(year), MAX(year) FROM silver_ref.dim_time;
SELECT MIN(year), MAX(year) FROM raw_census.acs_long;
```

---

## ROLLBACK (If Needed)

If the changes cause issues, revert to full table load:

**In `bls/silver_bls/transform.py` and `census_acs/silver_census/transform.py`**:

Find this line:
```python
geo_df = _load_geo_dim_for_list(hook, unique_geos)
```

Replace with:
```python
geo_df = _load_geo_dim(hook)
```

⚠️ **Warning**: This will likely cause -9 errors again with large datasets.

---

## LONG-TERM IMPROVEMENTS

### Consider For Future:

1. **Batch Size Limits**
   - Process data in chunks instead of full load
   - Reduces peak memory even more

2. **Database Streaming**
   - Instead of Polars joins, use cursor-based iteration
   - Avoids dataframe altogether

3. **Dimension Pre-filtering**
   - Add WHERE clause to dim_geo selects based on data patterns
   - Example: Only state-level geos if data is small

4. **Scheduling Dependency**
   - Make BLS/Census DAGs explicitly depend on `silver_ref` completion
   - Guarantees dimensions are fresh

5. **Monitoring**
   - Alert on memory usage thresholds
   - Track row drop percentages over time

---

## Documentation

- See [FIXES_APPLIED.md](FIXES_APPLIED.md) for technical details
- See [scripts/diagnose_geo_missing.py](scripts/diagnose_geo_missing.py) for troubleshooting
- See log files in Airflow for detailed error traces

---

## Contact & Support

If issues persist after these steps:
1. Run diagnostic script and save output
2. Check Airflow task logs for full error traces
3. Verify database connection is working
4. Confirm disk space is available
