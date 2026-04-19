# Gold Analytics Layer Schema Documentation

**Version:** 1.0  
**Last Updated:** April 2026  
**Schema Owner:** data-eng  

---

## Table of Contents

1. [Overview](#overview)
2. [Architectural Design](#architectural-design)
3. [Conformed Dimensions](#conformed-dimensions)
4. [Source-Specific Dimensions](#source-specific-dimensions)
5. [Fact Tables](#fact-tables)
6. [Metric Catalog & Bridges](#metric-catalog--bridges)
7. [User-Facing Views](#user-facing-views)
8. [Use Cases](#use-cases)
9. [Data Flow & Refresh Strategy](#data-flow--refresh-strategy)
10. [Query Examples](#query-examples)

---

## Overview

The **gold** schema is the read-optimized analytics layer of the population ETL toolbox, designed to serve dashboards, reports, and analytical queries across three primary data sources:

- **CENSUS_ACS** — US Census American Community Survey (annual demographic snapshots)
- **BLS** — Bureau of Labor Statistics (labor market, employment, wages)
- **FRED** — Federal Reserve Economic Data (macroeconomic indicators)

The schema follows a **dimensional modeling** approach with:
- **Conformed dimensions** (shared geo, time) to enable cross-source analytics
- **Source-specific dimensions** (tables, variables, series metadata)
- **Fact tables** (observations) partitioned by source system
- **Unified metric catalog** for discovery and governance

---

## Architectural Design

### Design Principles

1. **Separation of Concerns**
   - Conformed dimensions (`dim_geo`, `dim_time`) serve all sources
   - Source-specific dimensions (`dim_acs_*`, `dim_bls_*`, `dim_fred_*`) encode domain knowledge
   - Fact tables remain independent; no direct foreign keys between fact tables

2. **Metric Catalog Pattern**
   - Single source of truth for discoverable metrics across sources
   - Bridges map metrics to their underlying source tables
   - Enables lineage tracking and comparison warnings

3. **Temporal Grain & Flexibility**
   - ACS observations are annual (Jan 1 only)
   - BLS observations are monthly (or other periods per series)
   - FRED observations are high-frequency (daily or monthly)
   - Each fact table carries `observation_date`, `duration_start`, `duration_end` for flexibility

4. **Geography Normalization**
   - All geo IDs follow a canonical format (`us:1`, `state:XX`, `state:XX|county:XXX`)
   - `dim_geo` provides multi-level FIPS codes and human names
   - Facts inherit geo level (`NATIONAL`, `STATE`, `COUNTY`) from both source and lookup

5. **Observability & Audit**
   - Every dimension and fact includes `updated_at` (refresh timestamp)
   - Facts include `as_of_date` (logical snapshot date)
   - Conflict-free design ensures reliable upserts

---

## Conformed Dimensions

### dim_geo (View over silver_ref.dim_geo)

**Purpose:** Shared, canonical geography reference  
**Grain:** One row per unique geo entity (US, state, county)  
**Updated Via:** `silver_ref` refresh pipeline (independent of gold)

| Column | Type | Notes |
|--------|------|-------|
| `geo_sk` | BIGSERIAL | Surrogate key, stable across refreshes |
| `geo_level` | TEXT | `us`, `state`, `county` |
| `geo_id` | TEXT | Canonical ID: `us:1`, `state:02`, `state:02\|county:001` |
| `state_fips` | INT | 01–56, zero-padded in fact tables |
| `county_fips` | INT | 001–840, zero-padded in fact tables |
| `name` | TEXT | Human-friendly geo name |
| `state_name`, `county_name` | TEXT | Denormalized convenience fields |
| `latitude`, `longitude` | DOUBLE PRECISION | Gazetteer internal point coordinates for map plotting |
| `geo_polygon_geojson` | TEXT | GIS polygon geometry (GeoJSON geometry object) for choropleth/polygon map layers |
| `is_active` | BOOLEAN | TRUE if currently valid; FALSE for historical/obsolete geos |
| `source`, `source_year` | TEXT, INT | Provenance; e.g., `CENSUS`, 2020 |
| `first_seen_year`, `last_seen_year` | INT | Temporal coverage in source data |

**Use Cases:**
- Join facts to get human-readable geography names
- Filter to active geographies only for dashboards
- Trace geography data lineage to original source year

---

### dim_time (View over silver_ref.dim_time)

**Purpose:** Shared, canonical time reference  
**Grain:** One row per calendar day  
**Updated Via:** `silver_ref` refresh pipeline (independent of gold)

| Column | Type | Notes |
|--------|------|-------|
| `time_sk` | INT | Surrogate key (e.g., 20260401 for 2026-04-01) |
| `date_key` | DATE | The actual date |
| `year`, `quarter`, `month`, `day` | INT | Calendar components |
| `day_of_week`, `day_name`, `month_name` | INT, TEXT | Names and ordinals |
| `is_month_start`, `is_month_end` | BOOLEAN | TRUE for first/last day of month |
| `is_quarter_start`, `is_quarter_end` | BOOLEAN | TRUE for boundary dates |
| `is_year_start`, `is_year_end` | BOOLEAN | TRUE for Jan 1 and Dec 31 |
| `is_weekend` | BOOLEAN | TRUE for Sat/Sun |
| `week_of_year` | INT | ISO week number |

**Use Cases:**
- Denormalize calendar attributes into reports
- Filter to specific months, quarters, years efficiently
- Identify month-over-month or year-over-year periods

---

## Source-Specific Dimensions

### dim_source_system

**Purpose:** Registry of data sources (BLS, FRED, Census ACS)  
**Grain:** One row per source system  
**Uniqueness:** `source_code` is unique

| Column | Type | Notes |
|--------|------|-------|
| `source_system_sk` | BIGSERIAL | Surrogate key |
| `source_code` | TEXT | Canonical code (`CENSUS_ACS`, `BLS`, `FRED`) |
| `source_name` | TEXT | Human-friendly name |
| `source_type` | TEXT | `PRIMARY`, `REPUBLISHER`, `CURATED` |
| `reference_url` | TEXT | Link to source agency website |
| `updated_at` | TIMESTAMPTZ | When this record was last updated |

**Use Cases:**
- Filter metrics by trusted sources in dashboards
- Document source provenance for data governance
- Track source maintenance status

---

### dim_acs_table

**Purpose:** Catalog of Census ACS tables (e.g., B01003 for total population)  
**Grain:** One row per `(dataset_code, vintage_year, table_id)` combination  
**Uniqueness:** `(dataset_code, vintage_year, table_id)` is unique

| Column | Type | Notes |
|--------|------|-------|
| `acs_table_sk` | BIGSERIAL | Surrogate key, stable across vintages |
| `dataset_code` | TEXT | `acs1` (1-year) or `acs5` (5-year estimates) |
| `vintage_year` | INT | Year the survey was conducted/released |
| `table_id` | TEXT | Census table ID (e.g., `B01003`) |
| `table_title` | TEXT | Human-readable table title |
| `concept` | TEXT | Conceptual area (e.g., "Total Population") |
| `universe` | TEXT | Population universe (e.g., "Total population") |
| `survey_span_years` | INT | 1 for acs1, 5 for acs5 |
| `reference_url` | TEXT | Link to Census API/documentation |
| `updated_at` | TIMESTAMPTZ | Last refresh timestamp |

**Use Cases:**
- Understand which variables belong to which Census table
- Compare ACS 1-year vs. 5-year estimate coverage by year
- Document survey scope and universe for each table

---

### dim_acs_variable

**Purpose:** Catalog of Census ACS variables (columns within tables)  
**Grain:** One row per `(dataset_code, vintage_year, variable_code)` combination  
**Uniqueness:** `(dataset_code, vintage_year, variable_code)` is unique

| Column | Type | Notes |
|--------|------|-------|
| `acs_variable_sk` | BIGSERIAL | Surrogate key |
| `acs_table_sk` | BIGINT | FK to `dim_acs_table` (which table this variable belongs to) |
| `dataset_code` | TEXT | `acs1` or `acs5` |
| `vintage_year` | INT | Year the survey was conducted |
| `variable_code` | TEXT | Census variable code (e.g., `B01003_001E` for estimate, `B01003_001M` for MOE) |
| `variable_label` | TEXT | Human-readable variable name |
| `concept` | TEXT | Conceptual category |
| `universe` | TEXT | Population universe for this variable |
| `value_role` | TEXT | `ESTIMATE`, `MOE` (margin of error), or `ANNOTATION` |
| `denominator_hint` | TEXT | If a ratio/rate, notes the denominator |
| `is_publishable_default` | BOOLEAN | TRUE if recommended for public dashboards |
| `updated_at` | TIMESTAMPTZ | Last refresh timestamp |

**Use Cases:**
- Find all variables available in a given Census table and year
- Identify which variables are estimates vs. margins of error
- Filter to publishable variables for public-facing dashboards
- Track variable definitions across ACS vintage years

---

### dim_bls_survey

**Purpose:** Catalog of BLS survey programs (Current Employment Statistics, QCEW, etc.)  
**Grain:** One row per survey program  
**Uniqueness:** `program_code` is unique

| Column | Type | Notes |
|--------|------|-------|
| `bls_survey_sk` | BIGSERIAL | Surrogate key |
| `program_code` | TEXT | BLS program code (e.g., `CE`, `QU`, `EW`) |
| `survey_name` | TEXT | Human-friendly name (e.g., "Current Employment Statistics") |
| `survey_universe` | TEXT | Population or jobs surveyed |
| `observation_basis` | TEXT | `PEOPLE`, `JOBS`, `PRICES`, or `FLOWS` |
| `primary_concept` | TEXT | Main measurement focus |
| `id_construction_type` | TEXT | How series IDs are structured |
| `comparison_warning` | TEXT | Caveats when comparing across series |
| `reference_url` | TEXT | Link to BLS documentation |
| `updated_at` | TIMESTAMPTZ | Last refresh timestamp |

**Use Cases:**
- Understand which BLS survey a series belongs to
- Group analyses by employment vs. price surveys
- Document methodological comparability warnings

---

### dim_bls_series

**Purpose:** Catalog of BLS time series (specific labor market metrics)  
**Grain:** One row per series ID  
**Uniqueness:** `series_id` is unique

| Column | Type | Notes |
|--------|------|-------|
| `bls_series_sk` | BIGSERIAL | Surrogate key |
| `bls_survey_sk` | BIGINT | FK to `dim_bls_survey` |
| `program_code` | TEXT | Denormalized for convenience |
| `series_id` | TEXT | BLS series ID (e.g., `CEUSM050000001`) |
| `series_title` | TEXT | Human-readable series description |
| `measure_name` | TEXT | What is being measured |
| `measure_category` | TEXT | `EMPLOYMENT`, `UNEMPLOYMENT`, `EARNINGS`, etc. |
| `unit_of_measure` | TEXT | e.g., "thousands of persons" |
| `value_type` | TEXT | `LEVEL`, `RATE`, `INDEX`, `PERCENT`, `CURRENCY`, etc. |
| `seasonal_adjustment_status` | TEXT | `SA`, `NSA`, `Both` |
| `geographic_level` | TEXT | `NATIONAL`, `STATE`, `MSA`, `COUNTY` |
| `gold_metric_name` | TEXT | Optional standardized metric name for gold layer |
| `analytic_role` | TEXT | `PRIMARY`, `DENOMINATOR`, `SUPPORTING` |
| `semantic_notes` | TEXT | Additional context for analysts |
| `updated_at` | TIMESTAMPTZ | Last refresh timestamp |

**Use Cases:**
- Look up details of a specific BLS series
- Find all series in a given geographic region with a specific measure
- Identify primary vs. supporting measures for a domain
- Check seasonal adjustment status before aggregating

---

### dim_fred_series

**Purpose:** Catalog of FRED series (macroeconomic time series)  
**Grain:** One row per series ID  
**Uniqueness:** `series_id` is unique

| Column | Type | Notes |
|--------|------|-------|
| `fred_series_sk` | BIGSERIAL | Surrogate key |
| `series_id` | TEXT | FRED series ID (e.g., `UNRATE`) |
| `series_title` | TEXT | Human-readable series title |
| `source_provider` | TEXT | Direct source (e.g., BLS, Census, etc.) |
| `original_source_name` | TEXT | Original agency |
| `is_primary_source_series` | BOOLEAN | TRUE if original publisher |
| `is_republished_series` | BOOLEAN | TRUE if FRED republishes from another source |
| `frequency` | TEXT | `D` (daily), `M` (monthly), `Q` (quarterly), `A` (annual) |
| `units` | TEXT | e.g., "Percent", "Thousands" |
| `seasonal_adjustment` | TEXT | `SA`, `NSA`, or both |
| `transformation_method` | TEXT | e.g., "lin" (level), "chg" (change), "pc1" (% change) |
| `realtime_available` | BOOLEAN | TRUE if vintage/real-time data available |
| `lineage_notes` | TEXT | Provenance and derivation notes |
| `reference_url` | TEXT | Link to FRED page |
| `updated_at` | TIMESTAMPTZ | Last refresh timestamp |

**Use Cases:**
- Find macroeconomic indicators without manually searching FRED
- Identify series that are primary sources vs. republished copies
- Check data transformation (level vs. change vs. % change)
- Track FRED metadata and lineage

---

## Fact Tables

### fact_acs_observation

**Purpose:** Individual Census ACS observations (demographic estimates)  
**Grain:** One row per `(geo_id, observation_date, acs_variable_sk, dataset_code)` combination  
**Uniqueness:** `(geo_id, observation_date, acs_variable_sk, dataset_code)` is unique

| Column | Type | Notes |
|--------|------|-------|
| `acs_observation_sk` | BIGSERIAL | Surrogate key |
| `geo_id` | TEXT | Geography ID; joins to `dim_geo.geo_id` |
| `geo_level` | TEXT | Denormalized: `NATIONAL`, `STATE`, `COUNTY` |
| `state_id`, `state_name` | TEXT | Denormalized from `dim_geo` |
| `county_id`, `county_name` | TEXT | Denormalized from `dim_geo` |
| `time_sk` | INT | FK to `dim_time.time_sk` (or NULL if date not in `dim_time`) |
| `observation_date` | DATE | The date of the observation (always Jan 1 for ACS) |
| `duration_start`, `duration_end` | DATE | Survey period (e.g., 2020-01-01 to 2024-12-31 for 5-year estimate) |
| `acs_table_sk` | BIGINT | FK to `dim_acs_table` |
| `acs_variable_sk` | BIGINT | FK to `dim_acs_variable` |
| `dataset_code` | TEXT | Denormalized: `acs1` or `acs5` |
| `vintage_year` | INT | Denormalized year of estimate |
| `estimate_value` | NUMERIC | The demographic estimate (or NULL if not available) |
| `margin_of_error` | NUMERIC | 90% confidence margin of error |
| `margin_of_error_pct` | NUMERIC | MOE as % of estimate |
| `estimate_annotation` | TEXT | Annotation from Census API (e.g., "N/A", "X") |
| `moe_annotation` | TEXT | Annotation on the margin of error |
| `as_of_date` | DATE | Logical snapshot date of the fact record |
| `updated_at` | TIMESTAMPTZ | When this observation was last inserted/updated |

**Indexes:**
- `ix_fact_acs_obs_date` on `observation_date`
- `ix_fact_acs_geo_date` on `(geo_id, observation_date)`

**Use Cases:**
- Retrieve demographic estimates for a specific geography, vintage, and year
- Compare ACS 1-year vs. 5-year estimates for the same variable
- Track margin of error for statistical confidence
- Build time series of population, income, education metrics

---

### fact_bls_observation

**Purpose:** Individual BLS labor market observations  
**Grain:** One row per `(geo_id, period_date, bls_series_sk)` combination  
**Uniqueness:** `(geo_id, period_date, bls_series_sk)` is unique

| Column | Type | Notes |
|--------|------|-------|
| `bls_observation_sk` | BIGSERIAL | Surrogate key |
| `geo_id` | TEXT | Geography ID; joins to `dim_geo.geo_id` |
| `geo_level` | TEXT | Denormalized: `NATIONAL`, `STATE`, `COUNTY` |
| `state_id`, `state_name`, `county_id`, `county_name` | TEXT | Denormalized from `dim_geo` |
| `time_sk` | INT | FK to `dim_time.time_sk` (or NULL) |
| `period_date` | DATE | Reference date for the observation (e.g., last day of reporting month) |
| `duration_start`, `duration_end` | DATE | Period covered by the observation |
| `bls_survey_sk` | BIGINT | FK to `dim_bls_survey` |
| `bls_series_sk` | BIGINT | FK to `dim_bls_series` |
| `program_code` | TEXT | Denormalized BLS program code |
| `value` | NUMERIC | The metric value (employment level, unemployment rate, etc.) |
| `period_code` | TEXT | BLS period code (M01–M13 for months, Q01–Q05 for quarters, etc.) |
| `seasonal_adjustment_status` | TEXT | Denormalized: `SA`, `NSA` |
| `observation_basis` | TEXT | Denormalized: `PEOPLE`, `JOBS`, `PRICES`, `FLOWS` |
| `measure_category` | TEXT | Denormalized: `EMPLOYMENT`, `UNEMPLOYMENT`, etc. |
| `value_type` | TEXT | Denormalized: `LEVEL`, `RATE`, `INDEX`, etc. |
| `as_of_date` | DATE | Logical snapshot date |
| `updated_at` | TIMESTAMPTZ | When this observation was last inserted/updated |

**Indexes:**
- `ix_fact_bls_period_date` on `period_date`
- `ix_fact_bls_geo_date` on `(geo_id, period_date)`
- `ix_fact_bls_program` on `(program_code, period_date)`

**Use Cases:**
- Query employment levels or unemployment rates for states or counties
- Track monthly earnings and hours worked by industry
- Compare seasonally adjusted vs. not seasonally adjusted values
- Build labor market dashboards with multi-month/year trends

---

### fact_fred_observation

**Purpose:** Individual FRED macroeconomic observations  
**Grain:** One row per `(observation_date, fred_series_sk, realtime_start, realtime_end)` combination  
**Uniqueness:** `(observation_date, fred_series_sk, realtime_start, realtime_end)` is unique

| Column | Type | Notes |
|--------|------|-------|
| `fred_observation_sk` | BIGSERIAL | Surrogate key |
| `geo_id` | TEXT | Always `us:1` (FRED is national only) |
| `geo_level` | TEXT | Always `NATIONAL` |
| `time_sk` | INT | FK to `dim_time.time_sk` (or NULL) |
| `observation_date` | DATE | The date of the observation |
| `duration_start`, `duration_end` | DATE | Period covered (e.g., start/end of quarter for quarterly data) |
| `fred_series_sk` | BIGINT | FK to `dim_fred_series` |
| `value` | NUMERIC | The metric value (e.g., unemployment rate, GDP) |
| `realtime_start`, `realtime_end` | DATE | Vintage dates (when this data was published/revised) |
| `frequency` | TEXT | Denormalized: `D`, `M`, `Q`, `A` |
| `units` | TEXT | Denormalized: e.g., "Percent", "Billions" |
| `seasonal_adjustment` | TEXT | Denormalized: `SA`, `NSA` |
| `transform_applied` | TEXT | Denormalized transformation (if any) |
| `source_provider` | TEXT | Denormalized source |
| `as_of_date` | DATE | Logical snapshot date |
| `updated_at` | TIMESTAMPTZ | When this observation was last inserted/updated |

**Indexes:**
- `ix_fact_fred_obs_date` on `observation_date`
- `ix_fact_fred_series_date` on `(fred_series_sk, observation_date)`

**Use Cases:**
- Retrieve national macroeconomic indicators (GDP, inflation, unemployment rate)
- Build time series of interest rates, asset prices, economic indices
- Track data revisions over time using `realtime_start` / `realtime_end`
- Compare seasonally adjusted vs. not seasonally adjusted series

---

## Metric Catalog & Bridges

### dim_metric_catalog

**Purpose:** Unified discovery layer for all metrics across sources  
**Grain:** One row per discoverable metric  
**Uniqueness:** `metric_code` is unique

| Column | Type | Notes |
|--------|------|-------|
| `metric_catalog_sk` | BIGSERIAL | Surrogate key |
| `metric_code` | TEXT | Globally unique metric identifier (e.g., `ACS:acs5:B01003_001E`) |
| `metric_display_name` | TEXT | Friendly name for dashboards |
| `source_code` | TEXT | FK to `dim_source_system.source_code` |
| `source_object_type` | TEXT | `ACS_VARIABLE`, `BLS_SERIES`, `FRED_SERIES`, or `COMPOSITE_VIEW` |
| `business_definition` | TEXT | Plain-English description of what the metric measures |
| `caveats` | TEXT | Warnings or limitations (e.g., "5-year pooled estimate") |
| `valid_geo_grains` | TEXT[] | Array of applicable geographies (e.g., `ARRAY['NATIONAL','STATE','COUNTY']`) |
| `valid_time_grains` | TEXT[] | Array of applicable time periods (e.g., `ARRAY['ANNUAL','MONTHLY']`) |
| `dashboard_suitability` | TEXT | `PUBLIC_SAFE`, `INTERNAL_ONLY`, or `EXPERIMENTAL` |
| `comparability_group` | TEXT | Group ID for related metrics (e.g., all unemployment rates) |
| `do_not_compare_with` | TEXT[] | Array of metric codes to avoid comparing with (e.g., ACS 1-year vs. 5-year) |
| `recommended_aggregation` | TEXT | Aggregation method: `FIRST`, `LAST`, `SUM`, `AVG`, etc. |
| `owner_team` | TEXT | Team responsible for this metric |
| `is_active` | BOOLEAN | FALSE if deprecated or in development |
| `updated_at` | TIMESTAMPTZ | Last refresh timestamp |

**Indexes:**
- `ix_metric_catalog_source` on `(source_code, is_active)`
- `ix_metric_catalog_group` on `comparability_group`
- `ix_metric_catalog_geo_grains` GIN on `valid_geo_grains`
- `ix_metric_catalog_time_grains` GIN on `valid_time_grains`

**Use Cases:**
- Discover available metrics for a specific geography and time period
- Understand if a metric is safe for public dashboards
- Get lineage and caveats before using a metric
- Find comparable metrics and metrics to avoid comparing

---

### bridge_metric_acs_variable

**Purpose:** Link ACS variables to unified metric catalog  
**Grain:** One row per `(metric_catalog_sk, acs_variable_sk)` pair  
**Uniqueness:** `(metric_catalog_sk, acs_variable_sk)` is unique (PK)

| Column | Type | Notes |
|--------|------|-------|
| `metric_catalog_sk` | BIGINT | FK to `dim_metric_catalog` |
| `acs_variable_sk` | BIGINT | FK to `dim_acs_variable` |

**Purpose:** Enables queries like "find all ACS variables in the Public Safe metrics" or "what is the underlying variable for this metric code?"

---

### bridge_metric_bls_series

**Purpose:** Link BLS series to unified metric catalog  
**Grain:** One row per `(metric_catalog_sk, bls_series_sk)` pair  
**Uniqueness:** `(metric_catalog_sk, bls_series_sk)` is unique (PK)

| Column | Type | Notes |
|--------|------|-------|
| `metric_catalog_sk` | BIGINT | FK to `dim_metric_catalog` |
| `bls_series_sk` | BIGINT | FK to `dim_bls_series` |

---

### bridge_metric_fred_series

**Purpose:** Link FRED series to unified metric catalog  
**Grain:** One row per `(metric_catalog_sk, fred_series_sk)` pair  
**Uniqueness:** `(metric_catalog_sk, fred_series_sk)` is unique (PK)

| Column | Type | Notes |
|--------|------|-------|
| `metric_catalog_sk` | BIGINT | FK to `dim_metric_catalog` |
| `fred_series_sk` | BIGINT | FK to `dim_fred_series` |

---

## User-Facing Views

### vw_metric_catalog

**Purpose:** Filtered, analyst-friendly view of active metrics  
**Definition:** Selects from `dim_metric_catalog` where `is_active = TRUE`

```sql
SELECT
    metric_catalog_sk, metric_code, metric_display_name,
    source_code, source_object_type,
    business_definition, caveats,
    valid_geo_grains, valid_time_grains,
    dashboard_suitability, comparability_group,
    do_not_compare_with, recommended_aggregation,
    owner_team, updated_at
FROM gold.dim_metric_catalog
WHERE is_active = TRUE;
```

**Use Cases:**
- Discover available metrics for a report
- Filter to public-safe metrics only
- Understand metric definitions and limitations

---

### vw_headline_macro_metrics

**Purpose:** Quick headline view combining BLS and FRED observations with metadata  
**Union of:**
- BLS observations joined with metric catalog (labor market headlines)
- FRED observations joined with metric catalog (macroeconomic headlines)

**Columns:**
- `metric_code`, `metric_display_name`, `source_code`
- `observation_date`, `geo_id` (always `us:1` for FRED)
- `value`, `caveats`, `comparability_group`

**Use Cases:**
- Build executive dashboards with latest labor + macro indicators
- Quick snapshot of current economic conditions
- Track revisions and data freshness across sources

---

### vw_labor_market_overview

**Purpose:** Comprehensive labor market fact view with survey context  
**Definition:** Joins `fact_bls_observation` with `dim_bls_survey`, filtered to labor-related measures

**Columns:**
- `observation_date` (period_date from fact)
- `geo_id`, `program_code`, `survey_name`
- `measure_category`, `value_type`, `value`
- `comparison_warning`

**Use Cases:**
- Build labor market dashboards (employment, unemployment, openings, etc.)
- Understand which survey each series comes from
- Access survey-level methodological warnings

---

### vw_acs_dashboard_metrics

**Purpose:** Dashboard-ready ACS view with analyst-friendly labels, geography fields, and governance metadata  
**Definition:** Builds from `fact_acs_observation`, joins ACS metadata + metric catalog, restricts to active geographies, and keeps the single most recent available observation per `(geo_id, variable_code)`.

**Columns:**
- Time/filtering: `observation_date`, `duration_start`, `duration_end`, `dataset_code`, `vintage_year`
- Geography/filtering: `geo_id`, `geo_level`, `state_fips`, `county_fips`, `state_name`, `county_name`
- Variable identity: `table_id`, `table_title`, `variable_code`, `variable_label`, `metric_code`, `metric_display_name`
- Definition/context: `concept`, `universe`, `denominator_hint`, `is_publishable_default`
- Governance: `dashboard_suitability`, `business_definition`, `caveats`, `comparability_group`, `do_not_compare_with`, `recommended_aggregation`, `owner_team`
- Values/quality: `estimate_value`, `margin_of_error`, `margin_of_error_pct`, `estimate_annotation`, `moe_annotation`, `as_of_date`, `updated_at`

**Use Cases:**
- Build state and county ACS dashboards without joining surrogate keys
- Filter metrics to public-safe (`dashboard_suitability = 'PUBLIC_SAFE'`) or publishable defaults
- Drive metric pickers by `metric_display_name` while preserving canonical `metric_code`
- Support KPI tiles with latest available ACS values across mixed ACS1/ACS5 coverage

**Query Pattern:**
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
FROM gold.vw_acs_dashboard_metrics
WHERE geo_level IN ('STATE', 'COUNTY')
  AND metric_code = 'ACS:acs5:B01003_001E'
  AND observation_date >= DATE '2020-01-01'
ORDER BY observation_date DESC, state_name, county_name;
```

---

## Use Cases

### 1. **Build a Population Growth Dashboard**
**Goal:** Show US population trends by state over time  
**Query Pattern:**
```sql
SELECT
    d.state_name,
    ao.observation_date,
    ao.estimate_value,
    ao.margin_of_error
FROM gold.fact_acs_observation ao
JOIN gold.dim_geo d ON d.geo_id = ao.geo_id
WHERE ao.acs_variable_sk = <population_variable_sk>
  AND ao.dataset_code = 'acs5'
  AND d.geo_level = 'STATE'
ORDER BY d.state_name, ao.observation_date DESC;
```

**Key Schema Elements:**
- `fact_acs_observation` (data points)
- `dim_geo` (state names)
- `dim_acs_variable` (find population variable)
- Margin of error for confidence bounds

---

### 2. **Compare Labor Market Health Across States**
**Goal:** See current employment and unemployment rates by state  
**Query Pattern:**
```sql
SELECT
    d.state_name,
    MAX(CASE WHEN s.measure_category = 'EMPLOYMENT' THEN fo.value END) AS employment_level,
    MAX(CASE WHEN s.measure_category = 'UNEMPLOYMENT' THEN fo.value END) AS unemployment_rate
FROM gold.fact_bls_observation fo
JOIN gold.dim_geo d ON d.geo_id = fo.geo_id
JOIN gold.dim_bls_series s ON s.bls_series_sk = fo.bls_series_sk
WHERE fo.period_date = CURRENT_DATE - INTERVAL '1 month'
  AND d.geo_level = 'STATE'
GROUP BY d.state_name
ORDER BY unemployment_rate DESC;
```

**Key Schema Elements:**
- `fact_bls_observation` (monthly labor data)
- `dim_geo` (state filtering)
- `dim_bls_series` (series metadata for filtering)
- Seasonal adjustment status is queryable

---

### 3. **Discover Metrics & Check Governance**
**Goal:** Find all public-safe demographic metrics available for counties  
**Query Pattern:**
```sql
SELECT
    mc.metric_code,
    mc.metric_display_name,
    mc.business_definition,
    mc.caveats,
    mc.do_not_compare_with
FROM gold.vw_metric_catalog mc
WHERE mc.source_code = 'CENSUS_ACS'
  AND mc.dashboard_suitability = 'PUBLIC_SAFE'
  AND 'COUNTY' = ANY(mc.valid_geo_grains)
ORDER BY mc.metric_code;
```

**Key Schema Elements:**
- `dim_metric_catalog` (governance and discovery)
- `valid_geo_grains`, `dashboard_suitability` (filtering)
- `do_not_compare_with` (lineage warnings)

---

### 4. **Track Data Lineage: From Metric to Source**
**Goal:** Find all underlying variables for a specific metric  
**Query Pattern:**
```sql
SELECT
    mc.metric_code,
    mc.business_definition,
    v.variable_code,
    v.variable_label,
    v.dataset_code,
    v.vintage_year,
    t.table_id,
    t.table_title
FROM gold.dim_metric_catalog mc
JOIN gold.bridge_metric_acs_variable b ON b.metric_catalog_sk = mc.metric_catalog_sk
JOIN gold.dim_acs_variable v ON v.acs_variable_sk = b.acs_variable_sk
JOIN gold.dim_acs_table t ON t.acs_table_sk = v.acs_table_sk
WHERE mc.metric_code = 'ACS:acs5:B01003_001E'
ORDER BY v.vintage_year DESC;
```

**Key Schema Elements:**
- `dim_metric_catalog` (unified metric)
- `bridge_metric_acs_variable` (linkage)
- `dim_acs_variable`, `dim_acs_table` (source details)

---

### 5. **Cross-Source Economic Analysis**
**Goal:** Correlate population growth (ACS) with employment (BLS) and GDP (FRED)  
**Query Pattern:**
```sql
WITH acs_data AS (
    SELECT ao.observation_date, d.state_name, ao.estimate_value AS population
    FROM gold.fact_acs_observation ao
    JOIN gold.dim_geo d ON d.geo_id = ao.geo_id
    WHERE ao.dataset_code = 'acs5' AND d.geo_level = 'STATE'
),
bls_data AS (
    SELECT fo.period_date::DATE AS obs_date, d.state_name, fo.value AS employment
    FROM gold.fact_bls_observation fo
    JOIN gold.dim_geo d ON d.geo_id = fo.geo_id
    WHERE <employment_series_filter>
),
fred_data AS (
    SELECT fo.observation_date, 'NATIONAL' AS state_name, fo.value AS gdp
    FROM gold.fact_fred_observation fo
    WHERE <gdp_series_filter>
)
SELECT *
FROM acs_data
LEFT JOIN bls_data USING (state_name)
LEFT JOIN fred_data USING (observation_date);
```

**Key Schema Elements:**
- All three fact tables (`fact_acs_observation`, `fact_bls_observation`, `fact_fred_observation`)
- `dim_geo` for geography joining across sources
- Dimension tables for series/variable filtering

---

## Data Flow & Refresh Strategy

### Ingestion Pipeline

```
SILVER_CENSUS.fact_demographics
       ↓
   [Transform: refresh_acs_elements]
       ↓
GOLD.dim_acs_table + dim_acs_variable
       ↓
   [Transform: _seed_acs_metric_catalog]
       ↓
GOLD.dim_metric_catalog + bridge_metric_acs_variable
       ↓
   [Transform: merge_acs_shard by month]
       ↓
GOLD.fact_acs_observation
```

### Refresh Cadence

- **Silver Dimensions** (`silver_ref.dim_geo`, `silver_ref.dim_time`)  
  Refreshed independently; gold views read from these constantly  
  
- **Metadata Dimensions** (`dim_acs_table`, `dim_acs_variable`, `dim_bls_survey`, `dim_bls_series`, `dim_fred_series`)  
  Refreshed during ingestion of each source; typically monthly or as new definitions emerge  
  
- **Metric Catalog** (`dim_metric_catalog`, bridge tables)  
  Rebuilt or upserted after metadata dimensions are populated; ensures all metrics are current  
  
- **Fact Tables** (`fact_acs_observation`, `fact_bls_observation`, `fact_fred_observation`)  
  Upserted monthly (or per source refresh cycle) by date shard; supports idempotent reruns

### Upsert Strategy

- All dimensions and facts use **ON CONFLICT DO UPDATE** to handle reruns safely
- Unique keys enforce no duplicates
- `updated_at` timestamp tracks freshness per row
- `as_of_date` in facts provides logical snapshot date for audit trails

---

## Query Examples

### Example 1: Get Latest Population Estimate for a County

```sql
SELECT
    d.state_name,
    d.county_name,
    ao.estimate_value AS population,
    ao.margin_of_error,
    ao.observation_date,
    v.dataset_code,
    v.variable_label
FROM gold.fact_acs_observation ao
JOIN gold.dim_geo d ON d.geo_id = ao.geo_id
JOIN gold.dim_acs_variable v ON v.acs_variable_sk = ao.acs_variable_sk
WHERE d.county_name = 'San Francisco County'
  AND d.state_name = 'California'
  AND v.variable_code = 'B01003_001E'
  AND ao.dataset_code = 'acs5'
ORDER BY ao.observation_date DESC
LIMIT 1;
```

---

### Example 2: List All Employment Series Available at State Level

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

---

### Example 3: Find All Active Metrics Safe for Public Dashboards

```sql
SELECT
    metric_code,
    metric_display_name,
    source_object_type,
    valid_geo_grains,
    valid_time_grains,
    business_definition
FROM gold.vw_metric_catalog
WHERE dashboard_suitability = 'PUBLIC_SAFE'
ORDER BY source_object_type, metric_code;
```

---

## Schema Maintenance

### Adding a New ACS Variable

1. **Silver layer** ingests new `silver_census.fact_demographics` rows
2. **`refresh_acs_elements()`** task:
   - Groups by `(dataset, estimate_year, table_id)` → `dim_acs_table` (GROUP BY + MIN aggregates)
   - Groups by `(dataset, estimate_year, variable_code)` → `dim_acs_variable` (GROUP BY + MIN aggregates)
   - Calls **`_seed_acs_metric_catalog()`**:
     - DISTINCT ON `(dataset_code, variable_code)` to pick latest vintage
     - Inserts into `dim_metric_catalog` with metadata
     - Creates bridge rows in `bridge_metric_acs_variable`

### Adding a New BLS Survey

1. Silver layer ingests series definitions
2. Manual or scheduled **`refresh_bls_elements()`** populates:
   - `dim_bls_survey`
   - `dim_bls_series`
   - Corresponding `dim_metric_catalog` rows and bridges

### Deprecating a Metric

1. Set `is_active = FALSE` in `dim_metric_catalog`
2. Queries using `vw_metric_catalog` automatically exclude it
3. Existing fact rows remain for historical analysis

---

## Performance Tuning

### Indexes

All fact tables have:
- Composite index on `(geo_id, date_key)` for typical time-series queries
- Separate index on date columns for period filtering

Catalog tables have:
- GIN indexes on array columns (`valid_geo_grains`, `valid_time_grains`)
- B-tree on source codes and comparison groups

### Recommended Statistics

```sql
ANALYZE gold.fact_acs_observation;
ANALYZE gold.fact_bls_observation;
ANALYZE gold.fact_fred_observation;
ANALYZE gold.dim_metric_catalog;
```

### Materialized Views (Optional)

For heavy analytical workloads, consider materializing:
- Monthly/quarterly summaries of facts
- Pre-aggregated geography hierarchies
- Pre-joined catalog + source metadata

---

## Governance & SLA

| Element | Owner | Refresh Frequency | Latency SLA |
|---------|-------|-------------------|------------|
| `dim_geo`, `dim_time` | data-eng | Monthly (static) | N/A |
| Census metadata | data-eng | Monthly | Within 3 days of Census release |
| BLS metadata | data-eng | Monthly | Within 2 days of BLS release |
| FRED metadata | data-eng | Daily | Same day |
| ACS observations | data-eng | Monthly | Within 5 days of Census release |
| BLS observations | data-eng | Monthly | Within 10 days of BLS release |
| FRED observations | data-eng | Daily | Same day |
| Metric catalog | data-eng | On ingestion | Realtime after metadata refresh |

---

## Contact & Support

For questions or issues with the gold schema:
- **Data Engineering Team:** data-eng@company.internal
- **Documentation:** See linked DDL files and transform modules
- **Runbook:** gold schema rebuild, validation, and rollback procedures in ops documentation

---

**End of Document**
