# Census ACS SILVER Transformation

## Scope
Documents transformation from raw_census data into silver_census.fact_demographics.

## Target Object
- silver_census.fact_demographics

## Table Contract (From DDL)

### silver_census.fact_demographics

| Column | Type | Nullable | Notes |
|---|---|---|---|
| demographic_sk | BIGSERIAL | No | Primary key |
| time_sk | INTEGER | No | FK to silver_ref.dim_time(time_sk) |
| geo_sk | INTEGER | No | FK to silver_ref.dim_geo(geo_sk) |
| duration_start | DATE | No | Observation window start |
| duration_end | DATE | No | Observation window end |
| estimate_year | INTEGER | No | ACS estimate year |
| dataset | VARCHAR(50) | No | Dataset code |
| table_id | VARCHAR(50) | No | ACS table id |
| variable_code | VARCHAR(100) | No | Variable code |
| geo_level | VARCHAR(50) | Yes | Denormalized geo level |
| geo_id | VARCHAR(255) | Yes | Denormalized geo id |
| state_fips | VARCHAR(2) | Yes | Denormalized state fips |
| county_fips | VARCHAR(3) | Yes | Denormalized county fips |
| estimate_value | NUMERIC | Yes | Estimate value |
| margin_of_error | NUMERIC | Yes | Margin of error |
| margin_of_error_pct | NUMERIC | Yes | Derived MOE percent |
| variable_label | TEXT | Yes | Variable label |
| variable_concept | TEXT | Yes | Variable concept |
| universe | TEXT | Yes | Universe label |
| source_system | VARCHAR(50) | Yes | Default CENSUS_ACS |
| load_batch_id | UUID | No | Batch lineage |
| ingested_at | TIMESTAMPTZ | No | Default NOW() |

Constraints and indexes:

- Primary key: demographic_sk
- Unique constraint: fact_demographics_uk on (dataset, table_id, variable_code, geo_id, estimate_year)
- Indexes: idx_fact_demo_time_sk, idx_fact_demo_geo_sk, idx_fact_demo_dataset, idx_fact_demo_table_id, idx_fact_demo_source_year
- Table setting: custom autovacuum scale factors and vacuum cost limit

## Grain and Key
Expected analytic grain includes:

- dataset
- table_id
- variable_code
- geo_id
- estimate_year

This combination serves as the natural uniqueness key for idempotent writes.

## Core Transformation Steps
1. Normalize ACS rows by dataset/year/table/variable/geography.
2. Compute observation window from ACS year and dataset type:
   - acs1 uses single-year window.
   - acs5 uses rolling five-year window.
3. Build canonical geo_id values.
4. Join duration start date to silver_ref.dim_time for time_sk.
5. Join geo_id and geo_level to silver_ref.dim_geo for geo_sk.
6. Calculate and preserve estimate and margin-of-error fields.

## Missing-Dimension Handling
When time_sk or geo_sk joins fail:

- log miss counts and representative examples
- validate FIPS padding and geo_id format
- drop/skip invalid rows before final write

## Write Strategy
Use conflict-safe idempotent merge by natural key, or net-new insert strategy where defined by transform implementation.

## Quality Metrics
Track at minimum:

- raw input rows
- transformed output rows
- missing time dimension rows
- missing geo dimension rows
- rows inserted/updated

## Troubleshooting
1. Excess geo misses: inspect geo_id construction and county FIPS formatting.
2. Unexpected duplicates: verify natural key selection and anti-join/upsert path.
3. Coverage gaps: confirm dim_time and dim_geo refresh windows.
