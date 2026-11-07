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

| Years | File | Path | Subject county |
| --- | --- | --- | --- |
| 2011-2012 on | County inflow | `countyinflowYYYY.csv` (`2223` for 2022-2023) | the destination (year 2) |
| 2011-2012 on | County outflow | `countyoutflowYYYY.csv` | the origin (year 1) |
| 2004-2005 to 2010-2011 | Both directions, one zip | `countyYYYY.zip` (`county0405.zip`) | as above |
| 1990-1991 to 2003-2004 | Both directions, one zip | `YYYYtoYYYYcountymigration.zip` | as above |

All 33 pairs of filing years from 1990-1991 to 2022-2023 are registered,
in these layouts:

| Pairs | Layout | `n2` (served as `individuals`) | Third measure | State summary rows |
| --- | --- | --- | --- | --- |
| 2018-2019 on | CSV | Individuals | AGI | None |
| 2011-2012 to 2017-2018 | CSV, same columns | Exemptions claimed | AGI | Five per state, subject county `000`: counted, not loaded |
| 1995-1996 to 2010-2011 | Zip of one workbook per state and direction, the CSV's nine columns | Exemptions | AGI | As above |
| 1992-1993 to 1994-1995 | Zip of workbooks, earlier codes (below) | Exemptions | Total money income: **not loaded as AGI** (kept in `value_source`) | None |
| 1990-1991, 1991-1992 | Zip of one fixed-width text file per state and direction | Exemptions | None | None |

Personal exemptions ended with tax year 2018, so 2018-2019 is the first
pair whose `n2` counts people directly. Before 2011-2012 SOI matched
returns on the primary filer only and counted returns processed by late
September, about 95 to 98 percent of a year's returns; the 2011-2012 file
starts SOI's full-year series. Read the years either side of that break as
two series.

Gold's `flow_revision`, `flow_latest`, `total_observation_revision` and
`total_observation_latest` carry `people_basis` (`exemptions` or
`individuals`) on every row. The totals publish no AGI row before
1995-1996. `/api/v1/migration-flows` states the era's caveats with an
older pair.

A zip member's name does not say reliably what it holds (`co0304OH0.xls`
beside `co0304OHo.xls`; `co1011ohi.xls` is Hawaii's outflow), so a
workbook's direction is read from its title row and a member whose bytes
or cells repeat another's is read once (`co0304OH0.xls` and
`co0304OHo.xls` differ in bytes only). The US-level members of 1998-1999
to 2001-2002 (`co989usi.xls`, `co990usi.xls`, `co001usir.xls`,
`co102usi.xls` and their outflow twins) are bare Excel 4.0 worksheet
files with no title row and a tenth "Median AGI" column: they are read
too, their direction comes from their folder (`...CountyMigrationInflow/`),
the median is not loaded, and their five US rows (subject `00 000`) are
quarantined as `subject_not_county`. A workbook whose direction cannot be
read, or bytes that are not a workbook, refuse the whole file
(`quarantined`). Workbooks are read by `utility/xls.py`, with the standard
library only.

SOI labels a pair by the calendar years the returns were filed (2022-2023
is returns filed in 2022 matched to returns filed in 2023); the warehouse
keeps that label as `year_pair`.

Each county's rows, in SOI's order (files from 1995-1996):

| Counterpart code | What it is |
| --- | --- |
| `96000`, `97000`, `97001`, `97003`, `98000` | The file's totals: US and foreign, US, same state, different state, foreign |
| the county itself | Non-migrants |
| a state and county | A county-to-county flow of 20 or more returns (10 or 20 before 2018-2019) |
| `58000`, `59000`, `59001`-`59007` | Other flows: same state, different state, and by region |
| `57001`-`57009` | Foreign: overseas, Puerto Rico, APO/FPO, US Virgin Islands, other |

Before 1995-1996 the same rows carry other codes, kept as written:

| Counterpart code | What it is | Category |
| --- | --- | --- |
| `00001` | The county's total migration | `total_us_and_foreign` |
| a state and county | A county-to-county flow (10 returns or more) | `county` |
| `57001`-`57009` | Foreign, as above | `foreign_*` |
| `63010` | Remaining flows within the state, beside the four regions | `other_flows_same_state` |
| `63011`-`63014` | Region 1 to 4: the remaining flows to other states, by region | `other_flows_northeast` ... `other_flows_west` |
| `63020` | Remaining flows within the state, beside `63021` and `63022` | `other_flows_same_state` |
| `63021` | Remaining flows, same region, different state | `other_flows_same_region_different_state` |
| `63022` | Remaining flows, different region | `other_flows_different_region` |
| `63030` | "Suppress All Flows": every remaining flow in one row, when any smaller group would disclose a filer | `other_flows_all` |
| `63015` | Remaining foreign flows | `foreign_other_flows` |
| `63050` | Non-migrants | `non_migrants` |

A county uses one of three groupings: `63010` with the four regions,
`63020` to `63022`, or `63030` alone.

The 1990-1991 and 1991-1992 text files write these as lines and are stored
under the same codes: a county's total is its one unindented line (the
label is cut at 25 characters, so "Anchorage Borough Total" never says
"Total Migration"); then its flows, then `Same State`, `Region n:`,
`Same Region, Diff. State`, `Different Region`, `All Migration Flows`
(`63030`) and `Foreign` lines, and `County Non-Migrants`. Those years have
only the total and non-migrants as file totals. Each state's text and
workbook files also hold a foreign "state" (`57 001`, `57 003`, ...) as a
subject: its rows are quarantined as `subject_not_county`, because the same
flows are already in each county's rows as counterparts `57xxx`.

A category SOI deleted is `-1` in the CSVs and `d` in the workbooks, in
all three measures, and is loaded as `withheld` with no value. Nothing is
moved from a category into counties. The 1998-1999 Idaho workbooks mark a
revised figure `r  128`; it loads as 128 (`value_source` keeps the mark).
In 2009-2010 and 2010-2011, 81 rows carry `d` before an AGI figure
(`d36`) with returns and exemptions shown: they stay quarantined as
`unreadable_value`, because a valid row from 1995-1996 needs its AGI.

## Schedule and scope

The DAG runs at 15:00 UTC on the 5th of each month. SOI publishes a new
pair of years about once a year and seldom revises an older one. Each
scheduled run (`irs_migration.plan`) captures the newest two registered
pairs again, and every registered pair whose inflow or outflow file has
never been published under the parser contract that reads it now
(`registry.PARSE_REVISIONS`). On a fresh warehouse that is every pair: 66 files
through the one-slot `irs_soi_files` pool (24 CSVs of about 4.5 MB and,
before 2011-2012, each pair's zip of 2.5 to 15 MB fetched once per
direction and stored once). Once all are in it is four files a run. A file that failed or was quarantined is
not published, so its pair is asked for again on the next run. No manual
history run is needed.

When the parser changes how it reads a published file, the pair goes into
`PARSE_REVISIONS` with the next contract version (`...:v2`). The next
scheduled run captures that pair again; the bytes are the same, so
nothing new is stored in `raw_capture.payload_blob`, but the newer
contract makes the capture `captured`, not `unchanged`, and it replays and
publishes. Gold's `*_latest` views then read the newer capture. The older
capture's silver rows stay as history until they are cleaned up.

A task that fails after its capture is committed (a deadlock, a lost
worker) leaves the file `captured`. Its retry, or the next run, replays
that capture instead of fetching the file again (`capture.pending_run`).
Each run's `plan_files` task marks `unchanged` any `captured` row that a
later published capture of the same bytes has superseded
(`capture.settle_superseded_captures`), so `DQ-IRS-002` stops counting it.

`{"history": true}` in the run's conf re-captures every registered pair:

```text
airflow dags trigger irs_migration_ingest --conf '{"history": true}'
```

When SOI publishes a new pair, add it to `YEAR_PAIRS` after checking its
documentation for changes to the layout or the disclosure rules.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_irs_migration_schema`, re-applies the package's DDL before any
capture. The shared geography reference must be loaded: a flow is admitted
only when both of its counties resolve. Create the pool if deployment
automation has not:

```text
airflow pools set irs_soi_files 1 'IRS SOI migration files (serialized)'
```

Apply DDL, or run a pre-merge DDL check, only while the DAG is idle. A
replay locks its `control.irs_migration_file` row before it writes to
silver, in the order the DDL file takes its locks, so a DDL session that
arrives mid-replay waits for it rather than deadlocking it; the
2019-2020 outflow replay of 2026-11-05 15:01Z was deadlocked by a DDL
check before this ordering.

## What a run does

1. Commits one capture per file and records it in
   `control.irs_migration_file`.
2. Replays each capture into `silver_irs_migration.flow_revision`, one row
   per file row. Rows whose column count, codes or measures cannot be read
   go to `flow_quarantine`, as does a second row for the same subject and
   counterpart (`duplicate_row`, or `repeated_row` when SOI repeats the row
word for word, as in the 2013-2014 CSVs, so nothing is lost). State summary rows (subject county `000`,
   files before 2018-2019) are counted and not loaded. Each row is then
   conformed: a county flow whose
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

-- Every registered pair published in both directions (33 rows, 1990-1991 on):
SELECT year_pair, COUNT(DISTINCT direction) AS directions
FROM control.irs_migration_file WHERE status = 'published'
GROUP BY year_pair ORDER BY year_pair;
```

For a pair before 2018-2019, `captured_row_count` is larger than
`parsed_row_count` by the state summary rows, about 255 (five per state
and DC); that difference is expected.

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
  Other flows (before 1995-1996, every remaining-flow group, Region rows
  included) and foreign rows do not
  sum to the file's own total
  migration (exactly when nothing is withheld, at most when something is).
