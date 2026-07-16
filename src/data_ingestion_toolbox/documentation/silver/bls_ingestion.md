# BLS SILVER Transformation

## Scope
Documents transformation from raw_bls data into silver_bls.fact_labor_statistics.

## Target Object
- silver_bls.fact_labor_statistics

## Table Contract (From DDL)

### silver_bls.fact_labor_statistics

| Column | Type | Nullable | Notes |
|---|---|---|---|
| labor_stat_sk | BIGSERIAL | No | Primary key |
| time_sk | INTEGER | No | FK to silver_ref.dim_time(time_sk) |
| geo_sk | INTEGER | No | FK to silver_ref.dim_geo(geo_sk) |
| duration_start | DATE | No | Observation window start |
| duration_end | DATE | No | Observation window end |
| period_date | DATE | No | Period anchor date |
| series_id | VARCHAR(255) | No | Series id |
| program | VARCHAR(50) | No | Program code |
| geo_level | VARCHAR(50) | Yes | Denormalized geo level |
| geo_id | VARCHAR(255) | Yes | Denormalized geo id |
| state_fips | VARCHAR(2) | Yes | Denormalized state fips |
| county_fips | VARCHAR(3) | Yes | Denormalized county fips |
| value | NUMERIC | Yes | Observation value |
| year | INTEGER | No | Source year |
| period | VARCHAR(10) | No | Source period token |
| period_name | VARCHAR(100) | Yes | Period label |
| measure_code | VARCHAR(10) | Yes | Measure code |
| measure_name | TEXT | Yes | Measure label |
| seasonal_adjustment | VARCHAR(1) | Yes | Default U |
| source_system | VARCHAR(50) | Yes | Default BLS |
| load_batch_id | UUID | No | Batch lineage |
| ingested_at | TIMESTAMPTZ | No | Default NOW() |

Constraints and indexes:

- Primary key: labor_stat_sk
- Unique constraint: fact_labor_stats_uk on (series_id, period_date)
- Indexes: idx_fact_labor_time_sk, idx_fact_labor_geo_sk, idx_fact_labor_series_id, idx_fact_labor_program

## Grain and Key
Expected analytic grain includes:

- series_id
- period_date

This forms the natural uniqueness key used for idempotent writes.

## Core Transformation Steps
1. Parse BLS period code semantics to derive:
   - period_date
   - duration_start
   - duration_end
2. Normalize geography fields and canonical geo_id.
3. Join duration_start to silver_ref.dim_time for time_sk.
4. Join geo_id/geo_level to silver_ref.dim_geo for geo_sk.
5. Preserve series and measure metadata needed by GOLD dimensions.

## Period Semantics
BLS supports monthly, quarterly, semiannual, and annual period codes; transformation logic maps each to concrete date windows.

## Missing-Dimension Handling
For join misses:

- emit warning counts
- preserve diagnostic examples
- filter invalid rows before merge

## Write Strategy
Use ON CONFLICT-based upsert keyed by natural grain to support safe reruns and late correction.

## Quality Metrics
Track at minimum:

- rows by program and period range
- time dimension hit/miss counts
- geo dimension hit/miss counts
- deduplicated rows
- merged row count and duration

## Troubleshooting
1. Period parsing errors: validate incoming period code set.
2. Geo misses: verify geography parser output and dim_geo sync status.
3. Low output ratio: inspect filtering thresholds and null value handling.
