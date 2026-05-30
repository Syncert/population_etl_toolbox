# BLS RAW Ingestion

## Scope
Documents RAW ingestion for Bureau of Labor Statistics data in the bls module.

## Primary Objects
- raw_bls.bls_long
- raw_bls.bls_datasets
- raw_bls.bls_series
- raw_bls.bls_ingestion_slices

## Table Contracts (From DDL)

### raw_bls.bls_long

| Column | Type | Nullable | Notes |
|---|---|---|---|
| id | BIGSERIAL | No | Primary key |
| program | TEXT | No | Program code (la, ln, ce, cu, jt, etc.) |
| series_id | TEXT | No | BLS series id |
| year | INTEGER | No | Observation year |
| period | TEXT | No | Period token (for example M01, Q01) |
| period_name | TEXT | Yes | Friendly period label |
| value | NUMERIC | Yes | Numeric value |
| footnotes | JSONB | Yes | Raw footnotes payload |
| is_latest | BOOLEAN | Yes | API latest flag |
| geo_level | TEXT | Yes | Optional derived geography level |
| geo_id | TEXT | Yes | Optional derived geo id |
| state_fips | TEXT | Yes | Optional state fips |
| county_fips | TEXT | Yes | Optional county fips |
| load_batch_id | UUID | No | Batch lineage |
| ingested_at | TIMESTAMPTZ | No | Default now() |

Constraints and indexes:

- Primary key: id
- Unique index: bls_long_uniq on (program, series_id, year, period)
- Check constraint: bls_long_period_chk with regex ^[A-Z][0-9]{2}$

### raw_bls.bls_datasets

| Column | Type | Nullable | Notes |
|---|---|---|---|
| program | TEXT | No | Program key |
| year | INTEGER | No | Year key |
| title | TEXT | Yes | Program/year title |
| is_available | BOOLEAN | No | Default TRUE |
| first_seen_at | TIMESTAMPTZ | No | Default now() |
| last_checked_at | TIMESTAMPTZ | No | Default now() |
| last_ingested_at | TIMESTAMPTZ | Yes | Last ingestion time |

Constraints:

- Primary key: (program, year)

### raw_bls.bls_series

| Column | Type | Nullable | Notes |
|---|---|---|---|
| program | TEXT | No | Program code |
| series_id | TEXT | No | Series id |
| title | TEXT | Yes | Series title |
| seasonal | TEXT | Yes | Seasonal metadata |
| measure | TEXT | Yes | Measure metadata |
| area_code | TEXT | Yes | Area code |
| area_text | TEXT | Yes | Area description |
| raw_metadata | JSONB | Yes | Raw metadata payload |
| first_seen_at | TIMESTAMPTZ | No | Default now() |
| last_checked_at | TIMESTAMPTZ | No | Default now() |

Constraints and indexes:

- Primary key: (program, series_id)
- Index: bls_series_program_idx(program)

### raw_bls.bls_ingestion_slices

| Column | Type | Nullable | Notes |
|---|---|---|---|
| id | BIGSERIAL | No | Primary key |
| program | TEXT | No | Program code |
| year_start | INTEGER | No | Window start year |
| year_end | INTEGER | No | Window end year |
| geo_level | TEXT | Yes | Optional geography scope |
| state_fips | TEXT | Yes | Optional state fips for county scope |
| series_hash | TEXT | Yes | Planned series fingerprint |
| series_count | INTEGER | Yes | Planned series count |
| status | TEXT | No | planned/running/success/empty/failed |
| rows_loaded | BIGINT | No | Default 0 |
| started_at | TIMESTAMPTZ | Yes | Start timestamp |
| finished_at | TIMESTAMPTZ | Yes | End timestamp |
| series_hash_seen_at | TIMESTAMPTZ | Yes | Hash observation time |
| last_error | TEXT | Yes | Last error |

Constraints and indexes:

- Check constraints: status enum, year bounds/order, rows_loaded non-negative
- Check constraints: started_at <= finished_at when finished_at is set
- Unique index: bls_ingestion_slices_uniq on (program, year_start, year_end, COALESCE(geo_level,''), COALESCE(state_fips,''))

## Ingestion Grain
Raw observation grain is program + series_id + year + period.

## Program Model
Available programs can include labor-force, employment, prices, and flow-oriented domains.

Certain program paths (for example LAUS-style geography series) may require expansion logic from selected measure codes.

## Slice Ledger
BLS slice identity includes:

- program
- time range (year start/end)
- geography scope fields when relevant

Ledger hash fingerprints capture scoped series selection state for skip decisions.

## API Constraints and Fetch Behavior
- Uses BLS API v2 request model.
- Request planning must account for API limits such as series count and year span.
- Quota/threshold responses are handled via retry/deferral logic.

## Idempotency
Natural key uniqueness in RAW ensures reruns do not duplicate rows.

Conflict-safe writes preserve latest ingested payload state for the same source grain.

## Retry Strategy
- Retry transient HTTP/timeouts with bounded backoff.
- Distinguish daily-threshold exhaustion from hard failures.
- Record failure status and reason in ledger for triage.

## Configuration Controls
Important controls in bls/config.py:

- programs list
- selectors by program
- API key and connection id
- pool and concurrency controls

## DAG Notes
BLS DAG planning is program-centric and writes slice state transitions through the ledger for observability.

## Troubleshooting
Common issues:

1. Threshold reached: check retry/defer behavior and schedule window.
2. Sparse series output: verify selected selectors for the target program.
3. Invalid area codes: validate geography parser and source series format assumptions.
