# Operating the HUD Fair Market Rent and income-limit pipeline

The `hud_fmr_il_ingest` DAG captures and publishes the Department of Housing
and Urban Development's Fair Market Rents (FMRs) and Section 8 income limits
for counties. Both are program reference values HUD sets for each fiscal
year and each HUD area -- a metro area, a HUD metro subdivision, or a
nonmetropolitan county -- and repeats for each county in the area. Every
served row names its area and says it is not a county estimate.

## What is registered

`src/data_ingestion_toolbox/hud_fmr_il/registry.py` names each edition the
pipeline reads under `https://www.huduser.gov/portal/datasets/`. They need no
credential: the keyless workbooks are read, and the HUD User API (which needs
a token) is not used.

| Edition | Path | Sheet | Takes effect |
| --- | --- | --- | --- |
| FY 2026 FMRs | `fmr/fmr2026/FY26_FMRs.xlsx` | `FY26_FMRs` | 2025-10-01 |
| FY 2026 FMRs, revised | `fmr/fmr2026/FY26_FMRs_revised.xlsx` | `FY26_FMRs_revised` | 2026-05-21 (91 FR 21301) |
| FY 2027 FMRs | `fmr/fmr2027/FY27_FMRs.xlsx` | `FY27_FMRs` | 2026-10-01 (91 FR 56156) |
| FY 2026 income limits | `il/il26/Section8-FY26.xlsx` | `Section8-FY26` | 2026-05-01 |

Each workbook's data sheet has its header in the first row and a
`Field_Descriptions` sheet beside it. Columns are read by name, including
the ones that carry a year (`median2026`); a response that is not a
workbook, has no registered sheet or lacks a column it is read by fails the
capture. To add a fiscal year or a reissue, add its edition to
`REGISTERED_FILES` after checking its columns.

The workbooks are read by `utility/workbook.py`, which keeps each cell as
stored; no spreadsheet library is needed in the Airflow image (openpyxl
refuses the FY 2026 revised file over a malformed document date).

## What is served

| Measure | Column | Unit |
| --- | --- | --- |
| `fmr_0br` .. `fmr_4br` | `fmr_0` .. `fmr_4` | dollars per month |
| `median_family_income` | `median<FY>` | dollars per year |
| `income_limit_30_4p` | `ELI_4` | dollars per year |
| `income_limit_50_4p` | `l50_4` | dollars per year |
| `income_limit_80_4p` | `l80_4` | dollars per year |

Silver keeps every household size from one to eight. `year` is HUD's fiscal
year, running from October 1 of the year before.

`fips` is ten digits: state, county and county subdivision. A row ending
`99999` is a whole county and resolves through the shared geography by
code, never by name. In Connecticut, Maine, Massachusetts, New Hampshire,
Rhode Island and Vermont HUD publishes town rows instead; a county's towns
can sit in different HUD areas, so a town row is kept in silver as an
`unsupported` county subdivision and is not served as its county. The
territories HUD covers (American Samoa, Guam, the Northern Mariana Islands,
Puerto Rico, the U.S. Virgin Islands) are read and resolve only where the
shared dimension holds their codes. An empty cell is `missing` with no
value; HUD's current files have none.

## Schedule and scope

The DAG runs at 16:00 UTC on the 25th of each month and reads every
registered edition. HUD User sends no validator, so every read downloads the
workbook (under 1 MB each); a read whose bytes equal that edition's last
published file is recorded `unchanged` and replays nothing. A revised
edition is a second release (`FY2026-revised`) beside the original;
`observation_latest` serves the revision.

HUD User's site sometimes answers automated reads with an empty `202`
while it challenges the client. The adapter retries it and then fails the
run as unavailable, not as a changed file; the DAG's retries pick it up
later. The adapter does not disguise its user agent.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_hud_fmr_il_schema`, re-applies the package's DDL before any capture.
Create the pool if deployment automation has not:

```text
airflow pools set hud_fmr_il_files 1 'HUD FMR and income-limit workbooks (serialized)'
```

## What a run does

1. Commits one capture per edition and records it in
   `control.hud_fmr_il_file` with its checksum and effective date, as
   `captured` or `unchanged`.
2. Replays a `captured` edition into `silver_hud_fmr_il.observation_revision`,
   one row per county or town and measure. A row with an unreadable `fips`,
   `metro` flag or value, or a repeated `fips`, goes to
   `observation_quarantine`; a wrong header refuses the file. It writes the
   geography resolution ledger and conforms
   `silver_hud_fmr_il.fact_observation`, committing only if the counts
   reconcile.
3. Marks the edition `published`, which exposes its county rows through
   `gold_hud_fmr_il.observation_revision` and `observation_latest`, and
   signals the glossary harvest.

## Checks after a run

```sql
SELECT dataset, fiscal_year, edition, status, row_count, county_row_count
FROM control.hud_fmr_il_file ORDER BY created_at DESC LIMIT 8;

SELECT metric_key, release_key, COUNT(*)
FROM gold_hud_fmr_il.observation_latest GROUP BY 1, 2 ORDER BY 1, 2;
```

## Quality rules

- `DQ-HUD-001` (uniqueness) and `DQ-HUD-003` (a missing cell carries no
  number; a town is never a county) are enforced by the fact's key and named
  CHECK constraints.
- `DQ-HUD-002` fails on a captured edition left unreplayed or a replayed one
  that reached no fact, and runs in the daily sweep.
- `DQ-HUD-004` warns when an FMR falls as bedrooms are added, the
  four-person 30% limit exceeds the 50% or the 50% the 80%, or a
  whole-county row does not resolve.
