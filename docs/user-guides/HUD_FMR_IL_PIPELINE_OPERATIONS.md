# Operating the HUD Fair Market Rent and income-limit pipeline

The `hud_fmr_il_ingest` DAG captures and publishes the Department of Housing
and Urban Development's Fair Market Rents (FMRs) and Section 8 income limits
for counties. Both are program reference values HUD sets for each fiscal
year and each HUD area -- a metro area, a HUD metro subdivision, or a
nonmetropolitan county -- and repeats for each county in the area. Every
served row names its area and says it is not a county estimate.

## What the schedule reads: the HUD User Data API

HUD User answers automated workbook downloads with an empty `202` challenge,
so the scheduled DAG reads HUD's sanctioned automated channel, the HUD User
Data API (`https://www.huduser.gov/hudapi/public/`), with a token:
`HUD_USER_API_TOKEN` in `infra/docker/stack.env` (see
`docs/plans/human_testing/completed/REGISTER_A_HUD_USER_API_TOKEN.md`).
Compose passes it to the Airflow containers. The token is sent only as an
`Authorization: Bearer` header and reaches no capture, parameter set, log or
error. HUD allows 60 calls a minute; the client spaces calls 1.05 seconds
apart.

`src/data_ingestion_toolbox/hud_fmr_il/api.py` registers one read per
dataset and fiscal year, each labelled with the edition the API was checked
to serve:

| Read | Calls | Edition served (checked 2026-10-07) |
| --- | --- | --- |
| `api:fmr:fy2026` | `fmr/listStates`, then `fmr/statedata/<ST>?year=2026` (57 calls) | the May 21, 2026 reissue (`revised`) |
| `api:fmr:fy2027` | the same for 2027 | `original`, effective 2026-10-01 |
| `api:il:fy2026` | `fmr/listStates`, `fmr/listCounties/<ST>`, then `il/data/<fips>?year=2026` for each whole county (about 3,300 calls, about an hour) | `original`, effective 2026-05-01 |

Every answer is its own capture, listed in `control.hud_fmr_il_api_capture`;
the run's checksum is the checksum of its answers' checksums, so an
unchanged read replays nothing. The API names a HUD area code only for metro
areas; a nonmetropolitan county's row carries its area's name and no code
(the code is not derivable: Virginia's independent cities share their
county's area). Income limits are requested for whole counties only. HUD's
API terms require the notice "This product uses the HUD User Data API but is
not endorsed or certified by HUD User.", which every served row's basis
carries. When HUD reissues a year, the API's answer changes; update the
read's edition and effective date in the registry, and the live contract
check (`tests/external/test_hud_fmr_il_source_contracts.py`) fails until you
do.

## The workbooks (loading by hand)

`src/data_ingestion_toolbox/hud_fmr_il/registry.py` names each workbook
edition under `https://www.huduser.gov/portal/datasets/`. They need no
credential, but automated downloads are challenged, so they are not on the
schedule; `capture_file` loads one when it can be fetched.

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

The DAG runs at 16:00 UTC on the 25th of each month and makes every
registered API read, one mapped task each through the one-slot pool (the
income-limit task has a three-hour timeout). A read equal to its last
published read is recorded `unchanged` and replays nothing. A revised
edition is a second release (`FY2026-revised`) beside the original;
`observation_latest` serves the revision.

A refused token (401/403) fails the read as `token_refused`; a `202`,
`429` or server error is retried and then reported as unavailable. Neither
the API client nor the workbook client disguises its user agent.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_hud_fmr_il_schema`, re-applies the package's DDL before any capture.
Create the pool if deployment automation has not:

```text
airflow pools set hud_fmr_il_files 1 'HUD FMR and income-limit workbooks (serialized)'
```

## What a run does

1. Commits one capture per API answer (or per workbook) and records the
   read in `control.hud_fmr_il_file` with its channel, checksum and
   effective date, as `captured` or `unchanged`.
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
