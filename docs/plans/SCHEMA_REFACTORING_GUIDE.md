# Schema Refactoring Summary: Unified → Source-First

## The Problem You Identified (Correct Diagnosis)

Your current schema tries to force BLS, ACS, and FRED into one wide table (`rpt_observation_dashboard`) with ~50 columns, where ~30 are always NULL depending on the source. This creates several issues:

1. **Lost Entity Semantics** — BLS and ACS are hierarchical (Survey→Series, Table→Variable), FRED is flat. The wide table hides these differences.
2. **Coupling** — Adding a new BLS field requires updating the global schema. Sources can't evolve independently.
3. **Scale Pain** — As BLS/ACS grow (40k series × 1000s geographies), the unified materialized view becomes a bottleneck.
4. **Query Complexity** — Consumers must use conditional logic to detect source (`series_id IS NOT NULL` → BLS) instead of schema-level clarity.
5. **Namespace Bug** — DAGs call `gold.refresh_dashboard_serving_layer_bls()`, but DDL creates `gold_bls.refresh_dashboard_serving_layer_bls()`, causing stale data.

---

## The Solution: Source-First Architecture

**Keep your per-source DAGs and fact tables. Change only the serving layer.**

### Before (Unified):
```
gold.rpt_observation_dashboard
├─ All sources mixed in one table
├─ ~50 columns (series_id, variable_code, realtime_start, ... all NULLs except for one source)
└─ gold.mv_latest_dashboard (unified MV)
```

### After (Source-First):
```
gold_bls.rpt_bls_observations (BLS-specific columns only)
├─ series_id, program_code, survey_name, measure_category, ...
└─ gold_bls.mv_bls_latest

gold_census.rpt_acs_observations (ACS-specific columns only)
├─ dataset_code, vintage_year, variable_code, margin_of_error, ...
└─ gold_census.mv_acs_latest

gold_fred.rpt_fred_observations (FRED-specific columns only)
├─ series_id, frequency, realtime_start, realtime_end, ...
└─ gold_fred.mv_fred_latest

gold.v_comparison_labor_vs_income  ← On-demand cross-source views
gold.v_comparison_economic_indicators
```

---

## Why This Works for Your Requirements

1. ✅ **Source-aware consumers** — API knows to route to per-source table
2. ✅ **Independent refresh windows** — Each source can refresh on its own schedule without lock contention
3. ✅ **Modular expansion** — Add new source (Zillow, NOAA) → new schema, zero impact on existing code
4. ✅ **Cross-source comparisons** — Create explicit views for use cases (labor vs income), not unified table
5. ✅ **Clean schema semantics** — Each table has source-specific columns; no NULLs masking truth

---

## Files I've Created for You

### 1. **SQL Schema (Example)**
📄 `src/data_ingestion_toolbox/bls/gold_bls/DDL/gold_bls_v2_source_first.sql`

Shows the pattern for BLS:
- `gold_bls.rpt_bls_observations` — per-source serving table (BLS columns only)
- `gold_bls.mv_bls_latest` — per-source latest MV
- `gold_bls.refresh_rpt_bls_observations()` — updated procedure
- `gold_glossary.*` — shared (unchanged)

**Use this as a template for ACS and FRED.**

### 2. **API Source Routing**
📄 `apps/api/services/source_router.py`

Core utilities:
```python
get_source_from_metric(db, 'BLS:LAUS123')  # → 'BLS'
get_table_for_source('BLS', 'latest')  # → 'gold_bls.mv_bls_latest'
validate_sources_for_comparison(db, ['BLS:...', 'ACS:...'])  # → Error if different sources
```

### 3. **Updated Comparison Service**
📄 `apps/api/services/comparison_service_v2.py`

Shows how to validate sources before comparison and route to correct table:
```python
# NEW: Validates metrics are from same source before querying
is_valid, error = validate_sources_for_comparison(db, [metric_a, metric_b])
source_code = get_sources_from_metrics(db, [metric_a, metric_b])[metric_a]
table_name = get_table_for_source(source_code, 'latest')
# Then query from table_name (not a unified table)
```

### 4. **Cross-Source View Template**
📄 `src/data_ingestion_toolbox/gold/DDL/v_comparison_views.sql`

Example: BLS unemployment vs ACS income
```sql
CREATE OR REPLACE VIEW gold.v_comparison_labor_vs_income AS
SELECT
    b.geo_id,
    b.unemployment_value,
    b.unemployment_date,
    a.income_value,
    a.income_date
FROM gold_bls.mv_bls_latest b
FULL OUTER JOIN gold_census.mv_acs_latest a ON b.geo_id = a.geo_id
WHERE b.metric_code LIKE 'BLS:LAUS%'
  AND a.metric_code LIKE 'ACS:%B19013%';
```

---

## What Stays the Same (No Breaking Changes to Core)

✅ Per-source DAGs (bls_ingest_dag, acs_ingest_dag, fred_ingest_dag)
✅ Per-source fact views (gold_bls.fact_bls_observation, etc.)
✅ Per-source dimensions (dim_bls_survey, dim_acs_table, dim_fred_series)
✅ Shared glossary (dim_geo, dim_source_system, dim_metric_catalog, bridges)

---

## Migration Path (High Level)

### Stage 1: Create Per-Source Tables (Parallel, No Breaking Changes)
1. Apply `gold_bls_v2_source_first.sql` → creates `gold_bls.rpt_bls_observations` + MV
2. Apply `gold_acs_v2_source_first.sql` → creates `gold_census.rpt_acs_observations` + MV
3. Apply `gold_fred_v2_source_first.sql` → creates `gold_fred.rpt_fred_observations` + MV
4. Populate from existing unified table (data migration query)
5. Run new DAG tasks → verify per-source tables fill with fresh data

### Stage 2: Update API Layer
1. Update `observations_service.py` to use `source_router`
2. Update `comparison_service.py` to validate + route
3. Update `distribution_service.py` to route
4. Add cross-source comparison views as needed
5. Test end-to-end

### Stage 3: Update DAGs (Minor Changes)
Update procedure calls:
```python
# OLD (wrong schema in current code)
db.execute("CALL gold.refresh_dashboard_serving_layer_bls()")

# NEW (correct, source-specific)
db.execute("CALL gold_bls.refresh_dashboard_serving_layer_bls()")
```

### Stage 4: Retire Unified Table (Cleanup)
Once confident in per-source tables:
- Drop `gold.rpt_observation_dashboard`
- Drop `gold.mv_latest_dashboard`
- Remove fallback logic from API
- Update documentation

---

## Key Differences: Before vs After

| Aspect | Before | After |
|--------|--------|-------|
| **Serving Table** | `gold.rpt_observation_dashboard` (1 wide table, all sources) | `gold_bls.rpt_bls_observations` + `gold_census.rpt_acs_observations` + `gold_fred.rpt_fred_observations` |
| **Columns** | ~50 (many NULLs per source) | 15-25 per source (no NULLs for source-specific fields) |
| **Materialized Views** | 1 unified: `gold.mv_latest_dashboard` | 3 per-source: `mv_bls_latest`, `mv_acs_latest`, `mv_fred_latest` |
| **Cross-Source Queries** | Implicit (all in one table) | Explicit (use-case-specific views) |
| **Consumer Routing** | "Query from gold.mv_latest_dashboard" | "Look up source, query from gold_[source].mv_*_latest" |
| **Scale** | Single MV refresh (all sources) | Independent refreshes per source |
| **Future Sources** | Unified table gets wider | New source schema, zero coupling |

---

## What To Do Next

1. **Review** the files I've created (bls_v2_source_first.sql, source_router.py, v_comparison_views.sql)
2. **Create ACS and FRED equivalents** using bls_v2_source_first.sql as template
3. **Populate per-source tables** from existing unified table (data migration)
4. **Test** end-to-end DAG runs
5. **Update API services** to use source_router
6. **Add cross-source views** for your specific use cases (labor vs income, etc.)
7. **Retire unified infrastructure** once confident

---

## Questions to Ask While Implementing

- **Performance**: Is per-source MV refresh faster than unified? (Should be.)
- **API latency**: Does source routing add overhead? (Should be negligible—just a lookup.)
- **Coverage**: Are all metrics properly mapped to sources in dim_metric_catalog?
- **Cross-source needs**: What comparisons do you actually need? Create only those views.

---

## Rollback Safety

Until you fully retire the unified table:
- Both infrastructures can coexist
- API can query per-source tables while unified table is still populated
- If issues, fall back to unified table immediately
- Only drop unified table when you're confident in per-source implementation

