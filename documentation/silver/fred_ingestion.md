# FRED SILVER Transformation

## Scope
Documents transformation from raw_fred data into silver_fred.fact_economic_indicators.

## Target Object
- silver_fred.fact_economic_indicators

## Table Contract (From DDL)

### silver_fred.fact_economic_indicators

| Column | Type | Nullable | Notes |
|---|---|---|---|
| economic_indicator_sk | BIGSERIAL | No | Primary key |
| time_sk | INTEGER | No | FK to silver_ref.dim_time(time_sk) |
| duration_start | DATE | No | Observation window start |
| duration_end | DATE | No | Observation window end |
| observation_date | DATE | No | Observation date |
| series_id | VARCHAR(255) | No | Series id |
| domain | VARCHAR(100) | Yes | Domain label |
| value | NUMERIC | Yes | Observation value |
| is_missing | BOOLEAN | Yes | Default FALSE |
| series_title | TEXT | Yes | Series title |
| unit_of_measure | VARCHAR(255) | Yes | Units |
| frequency | VARCHAR(50) | Yes | Frequency label |
| seasonal_adjustment | VARCHAR(50) | Yes | Seasonal adjustment label |
| source_system | VARCHAR(50) | Yes | Default FRED |
| load_batch_id | UUID | No | Batch lineage |
| ingested_at | TIMESTAMPTZ | No | Default NOW() |

Constraints and indexes:

- Primary key: economic_indicator_sk
- Unique constraint: fact_economic_indicators_uk on (series_id, observation_date)
- Indexes: idx_fact_econ_time_sk, idx_fact_econ_series_id, idx_fact_econ_domain, idx_fact_econ_duration_start

## Grain and Key
Expected analytic grain includes:

- series_id
- observation_date

This forms the natural uniqueness key for idempotent writes in SILVER.

## Core Transformation Steps
1. Select latest applicable revision rows from RAW where revision windows exist.
2. Parse frequency semantics into duration_start/duration_end windows.
3. Join duration_start to silver_ref.dim_time for time_sk.
4. Preserve source metadata (domain, frequency, unit, seasonal adjustment, title).
5. Enforce national-only geography assumptions for downstream serving.

## Missing-Dimension Handling
FRED primarily depends on time dimension conformance.

For missing time joins:

- log counts and affected date range
- filter invalid rows prior to merge

## Write Strategy
Use ON CONFLICT-based upsert keyed by series_id and observation_date to support safe reruns and revision updates.

## Quality Metrics
Track at minimum:

- rows by domain and date range
- missing time_sk counts
- missing value proportions
- merged row counts and timing

## Troubleshooting
1. Revision drift confusion: confirm latest revision selection rule ordering.
2. Time coverage gaps: verify dim_time includes full observation window.
3. Missing-value spikes: inspect upstream series publication behavior.
