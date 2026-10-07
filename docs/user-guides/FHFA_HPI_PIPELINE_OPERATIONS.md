# Operating the FHFA House Price Index pipeline

The `fhfa_hpi_ingest` DAG captures and publishes the Federal Housing Finance
Agency's annual all-transactions House Price Index for counties. It is a
repeat-sales index of conventional single-family mortgages bought or
guaranteed by Fannie Mae and Freddie Mac: nominal, not seasonally adjusted,
and labelled developmental by FHFA. It is not a price level, and every row
says so, with FHFA's notice: "This product uses FHFA data but is neither
endorsed nor certified by FHFA."

## What is registered

`src/data_ingestion_toolbox/fhfa_hpi/registry.py` names one file,
`https://www.fhfa.gov/hpi/download/annual/hpi_at_county.xlsx`, sheet
`county`. No credential is needed. The ZIP-code and tract files are not
registered: USPS ZIP codes are not ZCTAs, and tract identities wait on the
sub-county geography work.

The workbook has five preamble rows, then the header `State, County, FIPS
code, Year, Annual Change (%), HPI, HPI with 1990 base, HPI with 2000 base`.
The adapter finds the header by matching it, and reads the release date
from the preamble's `Last updated: <Month> <D>, <YYYY>.` A response that is
not a workbook, has no `county` sheet or no registered header fails the
capture. The workbook is read by `utility/workbook.py`, a small reader
that keeps each cell as the file stores it; no spreadsheet library is
needed in the Airflow image.

## What is served

| Measure | Column | Unit |
| --- | --- | --- |
| `annual_change_pct` | Annual Change (%) | percent |
| `hpi_base_2000` | HPI with 2000 base | index, 2000 = 100 |

Silver also keeps the first-recorded-base index (`hpi`) and the 1990-based
index. The first is not served because its base year differs by county; the
2000-based index is the same series on one base.

The `FIPS code` cell is text where the code has a leading zero and a number
elsewhere; both are read as text and left-padded to five digits, and the
cell as stored is kept in `fips_source`. Counties resolve through the
shared geography by code, never by name. The file reports Connecticut by
planning region (`09110` to `09190`) and Alaska's Chugach as `02063`; they
resolve only where the shared dimension holds those codes, and a county
that does not resolve is recorded as `canonical_geography_absent` and not
served.

| Cell | Served as |
| --- | --- |
| Empty or `.` index | `missing`, no value, `missing_reason = provider_missing` |
| Empty 2000-based index where the county has an index | `missing`, `base_year_unavailable` (not indexed in 2000) |
| Empty annual change in a series' first year | `not_applicable`, `first_recorded_year` |
| Empty annual change after a missing year | `not_applicable`, `prior_year_missing` |

Values are the workbook's two-decimal figures; the file stores doubles such
as `600.16999999999996`, kept verbatim in `value_source`.

## Schedule and scope

The DAG runs at 15:00 UTC on the 25th of each month. FHFA revises every
year's index in each new file and publishes no calendar for the annual
files, and the download sends no validator, so every run downloads the
file (about 5 MB). A read whose bytes equal the last published file's is
recorded `unchanged` and replays nothing; the payload store keeps one copy
of identical bytes. A changed file is a new vintage: `observation_revision`
keeps every published vintage keyed by its "Last updated" date, and
`observation_latest` serves the newest. A second file with the same date
and different bytes gets the release `<date>.2`.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_fhfa_hpi_schema`, re-applies the package's DDL before any capture.
Create the pool if deployment automation has not:

```text
airflow pools set fhfa_hpi_files 1 'FHFA HPI workbooks (serialized)'
```

## What a run does

1. Commits one capture of the workbook and records it in
   `control.fhfa_hpi_file` with its checksum, as `captured` or `unchanged`.
2. Replays a `captured` file into `silver_fhfa_hpi.observation_revision`,
   one row per county, year and measure. A row with an unreadable code,
   year or value, or a repeated county-year, goes to
   `observation_quarantine`; a wrong header or a missing "Last updated"
   date refuses the file. It writes the geography resolution ledger and
   conforms `silver_fhfa_hpi.fact_observation`, committing only if the
   counts reconcile.
3. Marks the file `published`, which exposes it through
   `gold_fhfa_hpi.observation_revision` and `observation_latest`, and
   signals the glossary harvest.

## Checks after a run

```sql
SELECT run_id, status, provider_vintage, row_count, county_count
FROM control.fhfa_hpi_file ORDER BY created_at DESC LIMIT 5;

SELECT metric_key, value_status, missing_reason, COUNT(*)
FROM gold_fhfa_hpi.observation_latest GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;
```

## Quality rules

- `DQ-HPI-001` (uniqueness) and `DQ-HPI-003` (a missing cell carries no
  number) are enforced by the fact's key and named CHECK constraints.
- `DQ-HPI-002` fails on a captured file left unreplayed or a replayed one
  that reached no fact, and runs in the daily sweep.
- `DQ-HPI-004` warns on a non-positive index, a 2000-based index that is
  not 100 in 2000, or a county that did not resolve.
