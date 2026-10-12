# Operating the BEA regional accounts pipeline

The `bea_regional_ingest` DAG captures and publishes the Bureau of Economic
Analysis's regional economic accounts for the nation, every state and every
county: personal income in total and per capita, its major components,
earnings by industry, and county gross domestic product. Every figure keeps
its dollar basis, so a chained-dollar series is never read as current
dollars.

## What is registered

`src/data_ingestion_toolbox/bea/registry.py` names every table and line the
pipeline loads. Each table is one zip at
`https://apps.bea.gov/regional/zip/<TABLE>.zip`. These bulk files need no
credential, unlike the BEA API, so the adapter holds no key.

| Table | What | Lines | Basis |
| --- | --- | --- | --- |
| `CAINC1` | Personal income summary, from 1969 | 1 personal income, 2 population, 3 per capita personal income | current dollars, persons, per-capita current dollars |
| `CAINC4` | Personal income by major component, from 1969 | 35 earnings by place of work, 47 personal current transfer receipts, 50 wages and salaries | current dollars |
| `CAINC5N` | Earnings by NAICS industry, from 2001 | 81 farm earnings and the NAICS sector lines 100 to 2000 | current dollars |
| `CAGDP1` | GDP summary, from 2001 | 1 real GDP, 3 current-dollar GDP | chained 2017 dollars, current dollars |
| `CAGDP2` | GDP by industry, from 2001 | 1 all industries and the NAICS sector lines | current dollars |

Each zip holds one every-area CSV,
`<TABLE>__ALL_AREAS_<first>_<last>.csv`, with one column per year and a
footer that states the release (`Last updated: February 5, 2026`). A file
without that footer is refused whole: its rows are quarantined as
`release_date_missing` and nothing is published from it.

Cells BEA does not publish are codes, not numbers. Each keeps its status
and carries no value:

| Code | `value_status` |
| --- | --- |
| `(D)` | `withheld`, to avoid disclosing an individual business |
| `(NA)` | `not_available` |
| `(NM)` | `not_meaningful` |
| `(L)` | `below_threshold` |

Each of these is counted out of scope, not loaded:

- lines not registered above;
- BEA regions (`91000` to `98000`);
- BEA's combined areas: the Virginia independent cities merged with their
  surrounding county (`51901` to `51958`) and Kalawao merged into Maui
  (`15901`). They are BEA's own geography, not counties.

## Schedule and scope

The DAG runs at 14:00 UTC every Wednesday. BEA releases county personal
income each November and county GDP each December, and revises earlier years
with every release. A run that finds the same bytes adds a capture and no
observation. A new release is kept beside the one it revised, and
`observation_latest` serves the newest release date.

Every run reads every year of every registered table, so a fresh warehouse
needs no separate history run. Each table is one mapped task in the
one-slot `bea_files` pool. `CAINC5N` and `CAGDP2` are the largest zips, at
tens of megabytes each.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_bea_schema`, re-applies the package's DDL before any capture.
Create the pool if deployment automation has not:

```text
airflow pools set bea_files 1 'BEA regional bulk files (serialized)'
```

## What a run does

1. Commits one capture per table and records it in
   `control.bea_table_capture`.
2. Replays each capture into `silver_bea.observation_revision`: one row per
   registered line, geography and year. Rows whose table, geography code or
   column count cannot be read go to `observation_quarantine`. It writes the
   `silver_ref.geography_resolution` ledger and conforms
   `silver_bea.fact_observation`. The run commits only if every in-scope row
   is accounted for.
3. Marks the table `published`, which exposes it through
   `gold_bea.observation_revision` and `observation_latest`, and signals the
   glossary harvest. Each `<table>:<line>` is one metric, for example
   `BEA:CAINC1:3`.

## Checks after a run

```sql
SELECT table_code, release_date, status, captured_row_count, in_scope_row_count
FROM control.bea_table_capture ORDER BY created_at DESC;

SELECT dollar_basis, value_status, COUNT(*)
FROM gold_bea.observation_latest GROUP BY dollar_basis, value_status;
```

## Quality rules

- `DQ-BEA-001` (uniqueness) and `DQ-BEA-003` (a provider code carries no
  number) are enforced by the fact's key and named CHECK constraints.
- `DQ-BEA-002` is the table-ledger reconciliation and runs in the daily
  sweep.
- `DQ-BEA-004` (year continuity) is declared and not yet executed.
