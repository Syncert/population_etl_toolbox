# Operating the IRS SOI county migration pipeline

The `irs_migration_ingest` DAG captures and publishes the IRS Statistics of
Income (SOI) county-to-county migration data: for every county, where the
tax filers who moved in came from and where those who moved out went,
between two consecutive filing years. Each row counts returns, individuals
and adjusted gross income. A flow names two counties, so it is stored in
its own fact shape ([ADR-0008](../decisions/0008-origin-destination-flow-facts.md)).

## What is registered

`src/data_ingestion_toolbox/irs_migration/registry.py` names every file the
pipeline requests from `https://www.irs.gov/pub/irs-soi/`. No credential is
needed.

| File | Path | Subject county |
| --- | --- | --- |
| County inflow | `countyinflowYYYY.csv` (`2223` for 2022-2023) | the destination (year 2) |
| County outflow | `countyoutflowYYYY.csv` | the origin (year 1) |

The pairs of filing years from 2018-2019 to 2022-2023 are registered. From
2018-2019 SOI deletes any county count below 20 returns instead of moving it
into another county's category, and the files no longer carry state
totals, so earlier years follow different rules and are not registered.
SOI labels a pair by the calendar years the returns were filed (2022-2023
is returns filed in 2022 matched to returns filed in 2023); the warehouse
keeps that label as `year_pair`.

Each county's rows, in SOI's order:

| Counterpart code | What it is |
| --- | --- |
| `96000`, `97000`, `97001`, `97003`, `98000` | The file's totals: US and foreign, US, same state, different state, foreign |
| the county itself | Non-migrants |
| a state and county | A county-to-county flow of 20 or more returns |
| `58000`, `59000`, `59001`-`59007` | Other flows: same state, different state, and by region |
| `57001`-`57009` | Foreign: overseas, Puerto Rico, APO/FPO, US Virgin Islands, other |

A category SOI deleted is `-1` in all three measures and is loaded as
`withheld` with no value. Nothing is moved from a category into counties.

## Schedule and scope

The DAG runs at 15:00 UTC on the 5th of each month. SOI publishes a new
pair of years about once a year; until it does, every run captures the
same bytes and adds no flow. Every run reads every registered file, ten
files of about 4.5 MB each, through the one-slot `irs_soi_files` pool, so a
fresh warehouse needs no separate history run. When SOI publishes a new
pair, add it to `YEAR_PAIRS` after checking its documentation for changes
to the layout or the disclosure rules.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_irs_migration_schema`, re-applies the package's DDL before any
capture. The shared geography reference must be loaded: a flow is admitted
only when both of its counties resolve. Create the pool if deployment
automation has not:

```text
airflow pools set irs_soi_files 1 'IRS SOI migration files (serialized)'
```

## What a run does

1. Commits one capture per file and records it in
   `control.irs_migration_file`.
2. Replays each capture into `silver_irs_migration.flow_revision`, one row
   per file row. Rows whose column count, codes or measures cannot be read
   go to `flow_quarantine`. Each row is then conformed: a county flow whose
   subject, origin or destination is not in `silver_ref.dim_geo_entity` is
   refused into `flow_quarantine` as `subject_unresolved`,
   `origin_unresolved` or `destination_unresolved`. The rest land in
   `silver_irs_migration.fact_flow`. The run commits only if every captured
   row is accounted for.
3. Marks the file `published`, which exposes it through
   `gold_irs_migration.flow_revision` and `flow_latest` (served by
   `/api/v1/migration-flows`) and the file totals through
   `total_observation_revision` and `total_observation_latest` (served by
   `/api/v1/observations`), and signals the glossary harvest.

## Checks after a run

```sql
SELECT direction, year_pair, status, captured_row_count, parsed_row_count, refused_row_count
FROM control.irs_migration_file ORDER BY created_at DESC;

SELECT error_code, COUNT(*) FROM silver_irs_migration.flow_quarantine GROUP BY error_code;
```

A refused county is usually a county-equivalent the shared reference does
not hold under that code. Check `silver_ref.dim_geo_entity` before treating
it as a parser fault.

## Quality rules

- `DQ-IRS-001` (uniqueness) and `DQ-IRS-003` (both ends of a county flow
  resolve, a category names no county, a withheld row carries no number)
  are enforced by the fact's key and named CHECK constraints.
- `DQ-IRS-002` is the file-ledger reconciliation and runs in the daily
  sweep.
- `DQ-IRS-004` warns when, in the newest published file, a county's flows,
  Other flows and foreign rows do not sum to the file's own total
  migration (exactly when nothing is withheld, at most when something is).
