# FRED RAW Ingestion

## Scope
Documents RAW ingestion for Federal Reserve Economic Data in the fred module.

## Primary Objects
- raw_fred.fred_long
- raw_fred.fred_series
- raw_fred.fred_datasets
- raw_fred.fred_ingestion_slices

## Table Contracts (From DDL)

### raw_fred.fred_long

| Column | Type | Nullable | Notes |
|---|---|---|---|
| id | BIGSERIAL | No | Primary key |
| domain | TEXT | Yes | Optional grouping label |
| series_id | TEXT | No | FRED series id |
| obs_date | DATE | No | Observation date |
| value | NUMERIC | Yes | Observation value |
| is_missing | BOOLEAN | No | Default FALSE |
| realtime_start | DATE | Yes | Revision window start |
| realtime_end | DATE | Yes | Revision window end |
| load_batch_id | UUID | No | Batch lineage |
| ingested_at | TIMESTAMPTZ | No | Default now() |

Constraints and indexes:

- Primary key: id
- Unique index: fred_long_uniq on (series_id, obs_date, realtime_start, realtime_end)

### raw_fred.fred_datasets

| Column | Type | Nullable | Notes |
|---|---|---|---|
| domain | TEXT | No | Domain group |
| series_id | TEXT | No | Series id |
| is_available | BOOLEAN | No | Default TRUE |
| first_seen_at | TIMESTAMPTZ | No | Default now() |
| last_checked_at | TIMESTAMPTZ | No | Default now() |
| last_ingested_at | TIMESTAMPTZ | Yes | Last load timestamp |

Constraints:

- Primary key: (domain, series_id)

### raw_fred.fred_series

| Column | Type | Nullable | Notes |
|---|---|---|---|
| series_id | TEXT | No | Primary key |
| title | TEXT | Yes | Series title |
| units | TEXT | Yes | Units text |
| frequency | TEXT | Yes | Frequency text |
| seasonal_adjustment | TEXT | Yes | Seasonal adjustment label |
| observation_start | DATE | Yes | Series start date |
| observation_end | DATE | Yes | Series end date |
| notes | TEXT | Yes | Notes |
| raw_metadata | JSONB | Yes | Raw metadata payload |
| first_seen_at | TIMESTAMPTZ | No | Default now() |
| last_checked_at | TIMESTAMPTZ | No | Default now() |

Constraints:

- Primary key: (series_id)

### raw_fred.fred_ingestion_slices

| Column | Type | Nullable | Notes |
|---|---|---|---|
| id | BIGSERIAL | No | Primary key |
| domain | TEXT | No | Domain label |
| date_start | DATE | No | Slice start |
| date_end | DATE | No | Slice end |
| series_hash | TEXT | Yes | Planned series fingerprint |
| series_count | INTEGER | Yes | Planned series count |
| status | TEXT | No | planned/running/success/empty/failed |
| rows_loaded | BIGINT | No | Default 0 |
| started_at | TIMESTAMPTZ | Yes | Start timestamp |
| finished_at | TIMESTAMPTZ | Yes | End timestamp |
| series_hash_seen_at | TIMESTAMPTZ | Yes | Hash observation time |
| last_error | TEXT | Yes | Last error |

Constraints and indexes:

- Check constraints: status enum, date_end >= date_start, rows_loaded non-negative
- Check constraints: started_at <= finished_at when finished_at is set
- Unique index: fred_ingestion_slices_uniq on (domain, date_start, date_end)

## Ingestion Grain
Raw observation grain is series_id + observation date + realtime window.

Revision-aware fields (realtime_start/realtime_end) are preserved in RAW.

## Domain Model
FRED ingestion is organized by configured domain groupings and selected series sets.

## Slice Ledger
FRED slice identity typically includes:

- domain
- date window (start/end)

Hash fingerprinting supports skip behavior when selected series scope is unchanged.

## API and Fetch Behavior
- Uses FRED series observations endpoints.
- Missing values (for example '.') are preserved with explicit flags.
- Realtime revision metadata is stored for downstream latest-revision selection.

## Idempotency
RAW uniqueness protects against duplicate writes for identical revision-grain rows.

Reruns are safe and append/update according to natural constraints.

## Retry Strategy
Retry transient API failures with bounded backoff; classify empty responses as non-failure slice outcomes when expected.

## Configuration Controls
Important controls in fred/config.py:

- domains
- selected series identifiers and domain organization
- API key and connection id
- pool/concurrency settings

## DAG Notes
FRED DAG commonly applies rolling windows for recent periods and separate handling for historical windows.

## Troubleshooting
Common issues:

1. High missing-value ratio: confirm series semantics and upstream publication lag.
2. Revision confusion: verify realtime fields are retained in downstream transformations.
3. Unexpected skips: compare selected series scope to ledger hash inputs.
