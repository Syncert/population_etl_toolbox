# Gold Analytics Layer Schema Documentation

**Version:** 1.1  
**Last Updated:** May 2026  
**Schema Owner:** data-eng  

---

## Overview

The gold schema is the read-optimized analytics layer for three source domains:

- CENSUS_ACS
- BLS
- FRED

The implementation combines:

- Conformed reference views and helper tables for geography and time
- Source-specific metadata dimensions
- Source-specific fact views over silver tables
- A unified metric catalog and bridge tables
- Shared serving tables refreshed per source

---

## Current Architecture

### Conformed Objects

- gold.dim_geo is a view over silver_ref.dim_geo in all source DDL files.
- gold.dim_time is a view over silver_ref.dim_time in the ACS DDL path.
- gold.dim_geo_latest is a physical helper table refreshed by gold.refresh_dim_geo_latest().

### Source Metadata Dimensions

- gold.dim_source_system
- gold.dim_metric_catalog
- gold.dim_acs_table
- gold.dim_acs_variable
- gold.dim_bls_survey
- gold.dim_bls_series
- gold.dim_fred_series

### Fact Layer

Fact objects are implemented as views (not physical fact tables):

- gold.fact_acs_observation
- gold.fact_bls_observation
- gold.fact_fred_observation

These views project and normalize rows from silver-layer fact tables and attach source-specific dimensional keys.

### Serving Layer

Serving objects are shared across sources:

- gold.rpt_observation_dashboard
- gold.mv_latest_dashboard

Each source-specific refresh procedure deletes only that source slice, then inserts refreshed rows.

---

## Conformed Dimensions

### gold.dim_geo (view)

Shared canonical geography reference from silver_ref.dim_geo.

Key columns include:

- geo_sk, geo_level, geo_id
- state_fips, county_fips
- name, state_name, county_name
- latitude, longitude
- geom, geo_polygon_geojson
- is_active, source, source_year
- first_seen_year, last_seen_year
- ingested_at

### gold.dim_time (view)

Shared canonical day-level calendar reference from silver_ref.dim_time.

Key columns include:

- time_sk, date_key
- year, quarter, month, day
- day_of_week, day_name, month_name
- week_of_year
- is_weekend
- is_month_start, is_month_end
- is_quarter_start, is_quarter_end
- is_year_start, is_year_end
- ingested_at

Note:
This view is defined in the ACS gold DDL and can be reused by all queries once created.

---

## Source-Specific Dimensions

### gold.dim_source_system

Source registry seeded with CENSUS_ACS, BLS, and FRED.

### gold.dim_acs_table

One row per dataset_code + vintage_year + table_id.

### gold.dim_acs_variable

One row per dataset_code + vintage_year + variable_code.

### gold.dim_bls_survey

One row per BLS program_code.

### gold.dim_bls_series

One row per BLS series_id.

### gold.dim_fred_series

One row per FRED series_id.

---

## Fact Views

## gold.fact_acs_observation (view)

Purpose:
Normalized ACS observations from silver_census.fact_demographics.

Primary projected fields:

- geo_id, geo_level
- time_sk
- observation_date (MAKE_DATE(estimate_year, 1, 1))
- duration_start, duration_end
- acs_table_sk, acs_variable_sk
- dataset_code, vintage_year
- estimate_value, margin_of_error, margin_of_error_pct
- estimate_annotation, moe_annotation
- as_of_date, updated_at

## gold.fact_bls_observation (view)

Purpose:
Normalized BLS observations from silver_bls.fact_labor_statistics.

Primary projected fields:

- geo_id, geo_level
- time_sk
- period_date
- duration_start, duration_end
- bls_survey_sk, bls_series_sk
- program_code
- value
- period_code
- seasonal_adjustment_status
- observation_basis
- measure_category
- value_type
- as_of_date, updated_at

## gold.fact_fred_observation (view)

Purpose:
Normalized FRED observations from silver_fred.fact_economic_indicators.

Primary projected fields:

- geo_id (us:1)
- geo_level (NATIONAL)
- time_sk
- observation_date
- duration_start, duration_end
- fred_series_sk
- value
- realtime_start, realtime_end (currently null in this projection)
- frequency
- units
- seasonal_adjustment
- transform_applied
- source_provider
- as_of_date, updated_at

---

## Metric Catalog and Bridges

### gold.dim_metric_catalog

Unified discoverability and governance layer for ACS, BLS, and FRED metric codes.

Core columns:

- metric_code (unique)
- metric_display_name
- source_code
- source_object_type
- business_definition
- caveats
- valid_geo_grains
- valid_time_grains
- dashboard_suitability
- comparability_group
- do_not_compare_with
- recommended_aggregation
- owner_team
- is_active
- updated_at

### Bridge tables

- gold.bridge_metric_acs_variable
- gold.bridge_metric_bls_series
- gold.bridge_metric_fred_series

---

## Serving Tables

### gold.rpt_observation_dashboard

Shared denormalized serving table containing rows for all sources, identified by source_code.

Refresh procedures:

- gold.refresh_rpt_acs_observation_dashboard(date, date)
- gold.refresh_rpt_bls_observation_dashboard(date, date)
- gold.refresh_rpt_fred_observation_dashboard(date, date)

Refresh behavior (implemented):

1. Refresh geo helper rows via gold.refresh_dim_geo_latest().
2. Delete existing rows for the source (optionally bounded by date).
3. Insert refreshed rows from source fact view plus metadata joins.
4. Analyze serving tables.

### gold.mv_latest_dashboard

Shared latest-snapshot table (table, not PostgreSQL materialized view), populated by source-specific procedures:

- gold.refresh_mv_acs_latest_dashboard(date, date)
- gold.refresh_mv_bls_latest_dashboard(date, date)
- gold.refresh_mv_fred_latest_dashboard(date, date)

Each refresh deletes the source slice in gold.mv_latest_dashboard and inserts source-latest rows from gold.rpt_observation_dashboard.

### One-shot source refresh

```sql
CALL gold.refresh_dashboard_serving_layer_acs(NULL, NULL);
CALL gold.refresh_dashboard_serving_layer_bls(NULL, NULL);
CALL gold.refresh_dashboard_serving_layer_fred(NULL, NULL);
```

---

## Example Queries

### Example 1: Latest ACS population metric from shared latest table

```sql
SELECT
    observation_date,
    geo_level,
    state_name,
    county_name,
    metric_code,
    COALESCE(metric_display_name, variable_label) AS metric_name,
    estimate_value,
    margin_of_error,
    dashboard_suitability
FROM gold.mv_latest_dashboard
WHERE source_code = 'CENSUS_ACS'
  AND geo_level IN ('STATE', 'COUNTY')
  AND metric_code = 'ACS:acs5:B01003_001E'
ORDER BY observation_date DESC, state_name, county_name;
```

### Example 2: State-level BLS employment series from fact view

```sql
SELECT
    s.series_id,
    s.series_title,
    s.measure_category,
    s.unit_of_measure,
    s.seasonal_adjustment_status,
    sy.survey_name,
    s.geographic_level
FROM gold.dim_bls_series s
JOIN gold.dim_bls_survey sy ON sy.bls_survey_sk = s.bls_survey_sk
WHERE s.geographic_level = 'STATE'
  AND s.measure_category = 'EMPLOYMENT'
ORDER BY sy.survey_name, s.series_title;
```

### Example 3: Active public-safe metrics

```sql
SELECT
    metric_code,
    metric_display_name,
    source_object_type,
    valid_geo_grains,
    valid_time_grains,
    business_definition
FROM gold.dim_metric_catalog
WHERE dashboard_suitability = 'PUBLIC_SAFE'
  AND is_active = TRUE
ORDER BY source_object_type, metric_code;
```

---

## Data Flow and Refresh

### Metadata refresh pattern

1. Source silver facts are ingested.
2. Source transform refreshes source dimensions.
3. Source transform seeds and links metric catalog rows.

### Fact-layer pattern

The fact layer is view-based. Rows are not duplicated into standalone physical fact tables in gold.

### Serving-layer pattern

Source refresh procedures repopulate source slices in shared serving tables.

---

## Performance Notes

Implemented serving indexes include:

- Unique natural-key style index on gold.rpt_observation_dashboard
- Source/geo/date and metric/date indexes on gold.rpt_observation_dashboard
- BRIN index on observation_date in gold.rpt_observation_dashboard
- Unique and supporting indexes on gold.mv_latest_dashboard

Suggested stats maintenance:

```sql
ANALYZE gold.rpt_observation_dashboard;
ANALYZE gold.mv_latest_dashboard;
ANALYZE gold.dim_metric_catalog;
```

---

## Governance and SLA

| Element | Owner | Refresh Frequency | Latency SLA |
|---------|-------|-------------------|------------|
| dim_geo, dim_time | data-eng | periodic | N/A |
| ACS metadata | data-eng | monthly | within 3 days of Census release |
| BLS metadata | data-eng | monthly | within 2 days of BLS release |
| FRED metadata | data-eng | daily | same day |
| serving refresh procedures | data-eng | source cadence | aligned to source ingestion |
| metric catalog | data-eng | on source metadata refresh | immediate after refresh |

**End of Document**