# Census ACS RAW Ingestion

## Scope
Documents RAW ingestion for Census ACS in the census_acs module.

## Primary Objects
- raw_census.acs_long
- raw_census.acs_datasets
- raw_census.acs_variables
- raw_census.acs_tables
- raw_census.geo_dim
- raw_census.acs_ingestion_slices

## Table Contracts (From DDL)

### raw_census.acs_long

| Column | Type | Nullable | Notes |
|---|---|---|---|
| id | BIGSERIAL | No | Primary key |
| dataset | TEXT | No | acs1 or acs5 |
| year | INTEGER | No | Estimate year |
| geo_level | TEXT | No | us, state, county |
| geo_id | TEXT | No | Source geo key |
| state_fips | TEXT | Yes | Optional for non-state rows |
| county_fips | TEXT | Yes | Optional for non-county rows |
| table_id | TEXT | No | Census table id |
| variable_name | TEXT | No | Variable with E/M suffix |
| measure_type | TEXT | No | Check: E or M |
| value | NUMERIC | Yes | Raw value |
| load_batch_id | UUID | No | Batch lineage |
| ingested_at | TIMESTAMPTZ | No | Default now() |

Constraints and indexes:

- Primary key: id
- Unique index: acs_long_uniq on (dataset, year, geo_level, geo_id, variable_name)
- Check constraint: acs_long_measure_type_chk (measure_type IN ('E','M'))

### raw_census.acs_datasets

| Column | Type | Nullable | Notes |
|---|---|---|---|
| dataset | TEXT | No | acs1 or acs5 |
| year | INTEGER | No | Year availability |
| census_id | TEXT | Yes | Source identifier |
| title | TEXT | Yes | Dataset title |
| is_available | BOOLEAN | No | Default TRUE |
| first_seen_at | TIMESTAMPTZ | No | Default now() |
| last_checked_at | TIMESTAMPTZ | No | Default now() |
| last_ingested_at | TIMESTAMPTZ | Yes | Last load time |

Constraints:

- Primary key: (dataset, year)

### raw_census.acs_tables

| Column | Type | Nullable | Notes |
|---|---|---|---|
| dataset | TEXT | No | Dataset code |
| table_id | TEXT | No | Group/table id |
| concept | TEXT | Yes | Concept text |
| product | TEXT | Yes | Product designation |

Constraints:

- Primary key: (dataset, table_id)

### raw_census.acs_variables

| Column | Type | Nullable | Notes |
|---|---|---|---|
| dataset | TEXT | No | Dataset code |
| year | INTEGER | No | Vintage year |
| variable_name | TEXT | No | Variable id |
| table_id | TEXT | No | Parent table/group |
| label | TEXT | Yes | Variable label |
| concept | TEXT | Yes | Concept |
| predicate_type | TEXT | Yes | API metadata |
| group_name | TEXT | Yes | API metadata |

Constraints:

- Primary key: (dataset, year, variable_name)

### raw_census.geo_dim

| Column | Type | Nullable | Notes |
|---|---|---|---|
| geo_level | TEXT | No | us, state, county |
| geo_id | TEXT | No | Canonical id |
| state_fips | TEXT | Yes | State FIPS |
| county_fips | TEXT | Yes | County FIPS |
| name | TEXT | Yes | Display name |
| state_name | TEXT | Yes | State display |
| county_name | TEXT | Yes | County display |
| is_active | BOOLEAN | No | Default TRUE |
| source | TEXT | No | Default census_gazetteer |
| source_year | INTEGER | Yes | Snapshot year |
| ingested_at | TIMESTAMPTZ | No | Default now() |

Constraints and indexes:

- Primary key: (geo_level, geo_id)
- Indexes: geo_dim_state_idx(state_fips), geo_dim_county_idx(state_fips, county_fips)

### raw_census.acs_ingestion_slices

| Column | Type | Nullable | Notes |
|---|---|---|---|
| id | BIGSERIAL | No | Primary key |
| dataset | TEXT | No | acs1 or acs5 |
| year | INTEGER | No | Slice year |
| geo_level | TEXT | No | us, state, county |
| state_fips | TEXT | Yes | Required for county by check |
| variables_hash | TEXT | Yes | Selected variable fingerprint |
| variables_count | INTEGER | Yes | Selected variable count |
| status | TEXT | No | planned/running/success/empty/failed |
| rows_loaded | BIGINT | No | Default 0 |
| started_at | TIMESTAMPTZ | Yes | Start timestamp |
| finished_at | TIMESTAMPTZ | Yes | End timestamp |
| variables_hash_seen_at | TIMESTAMPTZ | Yes | Hash observation time |
| last_error | TEXT | Yes | Last error message |

Constraints and indexes:

- Check constraints: dataset, geo_level, status, year bounds, rows_loaded non-negative
- Check constraints: started_at <= finished_at when finished_at is set
- Check constraints: state_fips nullability/format by geo_level
- Unique indexes:
	- acs_ingestion_slices_uniq_nostate on (dataset, year, geo_level) where state_fips is null
	- acs_ingestion_slices_uniq_state on (dataset, year, geo_level, state_fips) where state_fips is not null
	- acs_ingestion_slices_uniq on (dataset, year, geo_level, COALESCE(state_fips,''))

## Ingestion Grain
Raw observation grain is dataset + year + geo row + variable.

Common dimensions:

- dataset (acs1, acs5)
- estimate year
- geography level (us, state, county)
- geo_id composed from level and FIPS grammar

## Geography Contract
Canonical geography IDs:

- us:1
- state:XX
- state:XX|county:YYY

County ingestion is state-scoped and requires state FIPS planning.

## Slice Ledger
ACS slice identity includes:

- dataset
- estimate year
- geo_level
- state_fips (nullable for non-county slices)

Ledger stores a variables hash for skip logic when selected variable scope is unchanged.

## API and Fetch Behavior
- Source endpoint family: api.census.gov
- Variable requests are chunked to remain within URL and API limits.
- HTTP 204 and empty payload cases are treated as empty slice outcomes.

## Idempotency
Raw uniqueness is enforced via natural keys in DDL plus conflict-safe merge behavior.

Planner-level idempotency uses hash comparison in slice ledgers.

## Retry Strategy
Retryable HTTP and transient failures are retried with bounded exponential backoff.

Validation/configuration errors fail immediately.

## Configuration Controls
Important controls in census_acs/config.py:

- datasets (acs1, acs5)
- selected table and variable controls
- geography levels
- connection id and concurrency knobs

## DAG Notes
The ACS DAG plans slices by geography and year, including county-by-state slicing and metadata sync prerequisites.

## Troubleshooting
Common issues:

1. Missing county rows for a state: verify state_fips planning and geo sync.
2. Excess empty slices: verify selected variable scope and year availability.
3. Join failures downstream: verify geo_id padding and grammar consistency.
