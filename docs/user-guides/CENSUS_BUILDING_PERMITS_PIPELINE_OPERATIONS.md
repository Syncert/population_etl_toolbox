# Operating the Census Building Permits pipeline

The `census_building_permits_ingest` DAG captures and publishes the Census
Bureau's Building Permits Survey: new privately owned housing units
**authorized** by building permits, with buildings and valuation, by
structure type (1 unit, 2 units, 3-4 units, 5 or more). A permit is an
authorization, not a start or a completion. Every published row and
catalog name says so.

## What is registered

`src/data_ingestion_toolbox/census_bps/registry.py` names every file the
pipeline requests from `https://www2.census.gov/econ/bps/`. No credential is
needed.

| File | Path | Frequency | Grains |
| --- | --- | --- | --- |
| County | `County/coYYMMc.txt` | monthly | county |
| County, year to date in December | `County/coYY12y.txt` | annual | county |
| State | `State/stYYMMc.txt`, `stYY12y.txt` | monthly, annual | state, nation (`US`) |
| Place, by region | `Place/<Region> Region/<rr>YYYYa.txt` | annual, from 2007 | place |

Every file has two header rows and a blank line. Each data row carries the
Bureau's estimate, which imputes for jurisdictions that did not report, and
then what the jurisdictions reported themselves. Both are kept: `value` and
`reported_value`.

Each of these is counted out of scope, not loaded:

- county rows `000`;
- place rows without a FIPS place code: `00000`, the New England minor civil
  divisions;
- `99990`, the unincorporated remainders;
- place files before 2007, which name places only by the Bureau's own IDs.

The state file's valuation is in thousands of dollars, so the state and
national rows publish buildings and units only. A place that reported no
month of the year is `not_reported`, with no number.

## Schedule and scope

The DAG runs at 13:00 UTC on the 25th of each month. An ordinary run asks
for the last six calendar months (`BpsConfig.recent_months`) and the annual
files of the years they fall in. A month not yet released answers 404 and
is recorded `empty`.

A fresh warehouse is loaded with a history run:

```text
airflow dags trigger census_building_permits_ingest --conf '{"history": true}'
```

That asks for every month from January 2000 and every annual file: two
files a month plus two (or six, from 2007, with the four place regions) a
year, about 750 requests. The county files are about 400 KB and the place
files about 700 KB each. Each (frequency, year, month) is one mapped task
in the one-slot `census_bps_files` pool, with requests spaced by
`min_spacing_seconds` (1 s).

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_census_bps_schema`, re-applies the package's DDL before any capture.
Create the pool if deployment automation has not:

```text
airflow pools set census_bps_files 1 'Census Building Permits files (serialized)'
```

## What a run does

1. Commits one capture per file and records it in
   `control.census_bps_slice`.
2. Replays each capture into `silver_census_bps.observation_revision`: one
   row per structure type and measure, holding the estimate, the reported
   figure and, for places, the months reported. Rows whose date, codes or
   column count cannot be read go to `observation_quarantine`. It writes the
   `silver_ref.geography_resolution` ledger and conforms
   `silver_census_bps.fact_observation`. The run commits only if every
   in-scope row is accounted for.
3. Marks the files `published`, which exposes them through
   `gold_census_bps.observation_revision` and `observation_latest`, and
   signals the glossary harvest.

## Checks after a run

```sql
SELECT frequency, year, month, slice_key, status, captured_row_count, in_scope_row_count
FROM control.census_bps_slice ORDER BY year DESC, month DESC, slice_key;

SELECT value_status, COUNT(*) FROM gold_census_bps.observation_latest GROUP BY value_status;
```

## Quality rules

- `DQ-BPS-001` (uniqueness) and `DQ-BPS-003` (a not-reported or missing
  figure carries no number) are enforced by the fact's key and named CHECK
  constraints.
- `DQ-BPS-002` is the file-ledger reconciliation and runs in the daily
  sweep.
- `DQ-BPS-004` (month continuity) is declared and not yet executed.
