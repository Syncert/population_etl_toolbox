# ETL Fixes Applied - BLS and Census ACS Silver Transforms

## Issues Identified

### 1. **Memory Exhaustion (Return Code -9)**
**Root Cause**: Both BLS and Census transforms were loading the *entire* `silver_ref.dim_geo` table into memory with:
```python
geo_df = _load_geo_dim(hook)  # SELECT * FROM silver_ref.dim_geo
```

When joined against millions of rows of raw data in Polars, this caused out-of-memory kills (SIGKILL, signal 9).

**Symptoms**:
- Task exited with return code -9
- Detected zombie jobs in Airflow logs

### 2. **Missing Geography Surrogate Keys (4320 BLS rows dropped)**
The BLS transform was dropping 4320 rows due to missing `geo_sk` values. This could be caused by:
- `silver_ref` geographies not synchronized before transform runs
- BLS data containing invalid state/county FIPS codes
- Timing issue between DAG execution order

### 3. **Census `margin_of_error_pct` Transform Crash (division by zero Decimal)**
**Root Cause**: The Census ACS silver transform computed `margin_of_error_pct` as `margin_of_error / estimate_value * 100`. For Polars `Decimal` columns, the expression could still attempt to divide by zero even when wrapped in a `when/then` guard, causing:

```
polars.exceptions.ComputeError: division by zero Decimal
```

## Solutions Applied

### Solution 1: Memory Optimization (Primary Fix)

**Files Modified**:
- `bls/silver_bls/transform.py`
- `census_acs/silver_census/transform.py`

**Change**: Replaced full `dim_geo` load with selective load

**Before**:
```python
def _load_geo_dim(hook: PostgresHook) -> pl.DataFrame:
    sql = "SELECT geo_sk, geo_level, geo_id FROM silver_ref.dim_geo;"
    # Loads entire table!
```

**After**:
```python
def _load_geo_dim_for_list(hook: PostgresHook, geo_df: pl.DataFrame) -> pl.DataFrame:
    """Load only geographic records that exist in the provided dataframe."""
    geo_tuples = geo_df.select(["geo_level", "geo_id"]).unique().to_records()
    
    # SQL with WHERE clause for only needed geographies
    placeholders = ", ".join(["(%s, %s)"] * len(geo_tuples))
    sql = f"""
        SELECT geo_sk, geo_level, geo_id
        FROM silver_ref.dim_geo
        WHERE (geo_level, geo_id) IN ({placeholders});
    """
```

**Usage in both transforms**:
```python
# Get unique geo combinations from data first
unique_geos = df.select(["geo_level", "geo_id"]).unique()
# Load only those geographies from database
geo_df = _load_geo_dim_for_list(hook, unique_geos)
```

**Impact**: 
- Reduces memory footprint by loading only ~100-500 records instead of thousands
- Keeps unused geography data out of memory entirely
- Should resolve the -9 SIGKILL errors

### Solution 2: Add Diagnostic Logging (Secondary Fix)

The existing logging already helps identify missing geographies:
```python
missing_geo = df.filter(pl.col("geo_sk").is_null()).height
if missing_geo:
    logger.warning(
        "Dropped %s BLS rows with missing geo_sk. Ensure silver_ref.dim_geo is synced.",
        missing_geo,
    )
```

## Recommended Actions

### Additional Fix: Safe Percent Calculation (Census)

**File Modified**:
- `census_acs/silver_census/transform.py`

**Change**: Replace zero (or NULL) denominators with NULL *before* division.

**Why**: This prevents Polars Decimal division-by-zero at compute time.

**Also**: Added `orient="row"` when constructing the Polars DataFrame from DB tuples to silence `DataOrientationWarning`.

### Immediate (Before Next Run)

1. **Verify silver_ref is up-to-date**:
   ```bash
   # Trigger the silver_ref DAG manually to ensure dim_geo and dim_time are current
   ```

2. **Check Airflow DAG Dependencies**:
   - The `silver_ref` DAG runs at 05:00 UTC monthly
   - The `bls_raw_ingest` DAG runs at 07:00 UTC monthly
   - Verify `silver_ref` completes successfully before BLS/Census transforms

3. **Enable database-level logging** (optional):
   Add this diagnostic query to monitor transform execution:

### Ongoing Monitoring

1. **Watch the transform task logs** for the warning message:
   ```
   Dropped X BLS rows with missing geo_sk
   ```
   
   If this number remains above 0 after next run, investigate further.

2. **Verify dimension sync** by running this query:
   ```sql
   -- Should match unique geographies used in BLS/Census data
   SELECT COUNT(*) FROM silver_ref.dim_geo;
   SELECT COUNT(DISTINCT geo_level, geo_id) FROM raw_bls.bls_long;
   SELECT COUNT(DISTINCT geo_level, state_fips, county_fips) FROM raw_census.acs_long;
   ```

## Testing Recommendations

1. **Test locally** with a subset of data if possible
2. **Monitor the next DAG run** - should complete without -9 errors
3. **Check task duration** - should be slightly longer due to more SQL queries, but memory usage should be much lower
4. **Verify row counts** - should not drop unexpectedly large numbers of rows

## Technical Details

### How the Optimization Works

**Before**:
```
raw_data (M rows) → parse geography → polars_df → join with dim_geo (T rows) → huge_intermediate_df (M×T)
```

**After**:
```
raw_data (M rows) → parse geography → polars_df → identify unique geographies (U rows) 
→ load from DB: SELECT...WHERE IN (U items) → geo_df (U rows) → join → result (~M rows)
```

### Why This Solves Both Issues

1. **Memory**: Only loads the geographies actually in the data
2. **Missing geo_sk**: The WHERE clause ensures we're joining against valid geographic records from the database

## Files Modified

1. `bls/silver_bls/transform.py`:
   - Added `_load_geo_dim_for_list()` function
   - Updated `transform_bls_to_silver()` to use selective loading

2. `census_acs/silver_census/transform.py`:
   - Added `_load_geo_dim_for_list()` function
   - Updated `transform_census_to_silver()` to use selective loading

## Rollback Plan

If issues occur, revert to full `dim_geo` load by replacing:
```python
geo_df = _load_geo_dim_for_list(hook, unique_geos)
```

With:
```python
geo_df = _load_geo_dim(hook)
```

Note: This will likely cause -9 errors again if you have large data volumes.

## Follow-up Issues to Monitor

- If missing geo_sk still occurs after 1-2 runs, investigate:
  1. Whether `silver_ref` DAG is completing successfully
  2. Whether BLS/Census data contains invalid FIPS codes
  3. Whether there's a timing issue with DAG scheduling
