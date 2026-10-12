# Operating the County Business Patterns pipeline

The `census_cbp_ingest` DAG captures and publishes the Census Bureau's County
Business Patterns: employer establishments, employment in the pay period
including March 12, first-quarter payroll and annual payroll, for the
nation, every state and every county, by two-digit NAICS sector and in
total. It counts employer establishments only: the self-employed, private
households, railroads, crop and animal production and most government
employees are out of scope, and every row says so.

## What is registered

`src/data_ingestion_toolbox/census_cbp/registry.py` names every file the
pipeline requests from `https://www2.census.gov/programs-surveys/cbp/datasets/`.
No credential is needed: the bulk files are used, not the CBP API.

| File | Path | Geography columns |
| --- | --- | --- |
| Counties | `<YYYY>/cbp<YY>co.zip` | `fipstate`, `fipscty` |
| States | `<YYYY>/cbp<YY>st.zip` | `fipstate`, `lfo` |
| Nation | `<YYYY>/cbp<YY>us.zip` | `uscode`, `lfo` |

The years 2016 to 2023 are registered. Columns are read by name, so the
2016-2017 layout (with `empflag`) and the 2018-2023 layouts read the same
way. Only the all-sectors total (`------`) and the two-digit sectors
(`11----` to `99----`) are loaded; the state and nation files' rows for one
legal form of organization are left out (`lfo` other than `-`); each county
file's statewide row (county `999`) is counted, not loaded.

Each of employment, first-quarter payroll and annual payroll has a flag:

| Flag | Meaning | Served as |
| --- | --- | --- |
| `G`, `H`, `J` | Noise under 2%, 2 to under 5%, 5% or more | the value, with `uncertainty.noise_flag` |
| `D` | Withheld to avoid disclosing an establishment (through 2016) | `withheld`, no value; `employment_range` keeps the size letter |
| `S` | Below publication standards | `suppressed`, no value |

The file writes `0` for a `D` or `S` cell; that zero is never served. From
2017 a cell of fewer than three establishments is not published at all.

## Schedule and scope

The DAG runs at 13:00 UTC on the 15th of each month. The Bureau publishes
one year a year (2023 came out in June 2025); until a new year is
registered, every run captures the same bytes and adds no observation. To
add a year, add it to `YEARS` after checking its record layout. Every run
reads every registered file, 24 files through the one-slot `census_cbp_files`
pool; the state files are the largest, about 15 MB zipped each.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_census_cbp_schema`, re-applies the package's DDL before any capture.
Create the pool if deployment automation has not:

```text
airflow pools set census_cbp_files 1 'Census County Business Patterns files (serialized)'
```

## What a run does

1. Commits one capture per file and records it in `control.census_cbp_file`.
2. Replays each capture into `silver_census_cbp.observation_revision`, one
   row per in-scope cell and measure. Rows whose column count, state code
   or flag cannot be read go to `observation_quarantine`. It writes the
   `silver_ref.geography_resolution` ledger and conforms
   `silver_census_cbp.fact_observation`. The run commits only if every
   in-scope cell is accounted for.
3. Marks the file `published`, which exposes it through
   `gold_census_cbp.observation_revision` and `observation_latest`, and
   signals the glossary harvest. Each `<measure>:<sector>` is one metric, for
   example `CENSUS_CBP:emp:72`.

## Checks after a run

```sql
SELECT kind, year, status, captured_row_count, in_scope_row_count
FROM control.census_cbp_file ORDER BY year DESC, kind;

SELECT measure, value_status, noise_flag, COUNT(*)
FROM gold_census_cbp.observation_latest GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;
```

## Quality rules

- `DQ-CBP-001` (uniqueness) and `DQ-CBP-003` (a withheld or suppressed cell
  carries no number) are enforced by the fact's key and named CHECK
  constraints.
- `DQ-CBP-002` is the file-ledger reconciliation and runs in the daily
  sweep.
- `DQ-CBP-004` warns when, for a geography and year, the sectors'
  establishments sum past the total: establishments carry no noise, and an
  unpublished sector can only make the sum smaller.
