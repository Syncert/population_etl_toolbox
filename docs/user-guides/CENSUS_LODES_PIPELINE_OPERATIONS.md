# Operating the LEHD LODES pipeline

The `census_lodes_ingest` DAG captures and publishes the Census Bureau's
LEHD Origin-Destination Employment Statistics (LODES8): for every county and
state, the jobs held by people who live there, the jobs located there, and
how the two overlap. LODES publishes census blocks only; every county and
state figure served here is this warehouse's sum of the state's block rows,
and every row says so.

## What is registered

`src/data_ingestion_toolbox/census_lodes/registry.py` names every file the
pipeline requests under `https://lehd.ces.census.gov/data/lodes/LODES8/`. No
credential is needed. For each state (the 50 states and the District of
Columbia) and year (2002 to 2023) it reads four files, all for all jobs
(`JT00`) and all workers (`S000`):

| Family | Path | What it counts |
| --- | --- | --- |
| Residence (`rac`) | `<st>/rac/<st>_rac_S000_JT00_<year>.csv.gz` | jobs by the worker's home block |
| Workplace (`wac`) | `<st>/wac/<st>_wac_S000_JT00_<year>.csv.gz` | jobs by the work block |
| Origin-destination, in state (`od_main`) | `<st>/od/<st>_od_main_JT00_<year>.csv.gz` | jobs by home and work block, both in the state |
| Origin-destination, from out of state (`od_aux`) | `<st>/od/<st>_od_aux_JT00_<year>.csv.gz` | jobs in the state held by people living in another |

Before any data file, a run reads the state's `version.txt` (the data
vintage and format version) and `lodes_<st>.sha256sum`. A data file is kept
only when the SHA-256 of its decompressed bytes matches the list; any
mismatch, a non-gzip response or a file missing from the list fails the
capture. A file the list does not name (Alaska publishes no workplace or
origin-destination file from 2017, Michigan none for 2022-2023) is recorded
as `not_published` and no measure it would feed is served, never a zero.
Only format version 8.4 is registered; any other fails the capture.

## Measures

| Measure | From | County | State |
| --- | --- | --- | --- |
| `resident_workers` | residence `C000` | yes | yes |
| `jobs` | workplace `C000` | yes | yes |
| `live_and_work` | in-state flows whose home and work county are the same | yes | yes (every in-state flow) |
| `inbound` | flows into the county from any other county, in state or not | yes | yes (out-of-state homes) |
| `outbound_in_state` | flows from the county to another county in the same state | yes | no |

Out-of-state outflow needs other states' `od_aux` files and is not served.
`live_and_work + inbound = jobs` for every county; DQ-LODES-004 checks it.

Silver keeps every residence and workplace column summed to the county
(`silver_census_lodes.fact_area`) and every county pair
(`silver_census_lodes.fact_flow`). A column the Bureau writes as zeros
because it publishes none (race, ethnicity, education and sex before 2009;
firm age and size outside 2011-2023 or outside `JT02`, so always for `JT00`)
is `value_status = 'not_available'` with no value; `value_source` keeps the
file's own sum.

County identity is the block code's first five digits, resolved through the
shared geography dimension by code; names are never read. Connecticut's 2020
blocks carry its eight legacy county codes, so its counties resolve only
where the dimension holds them; a county that does not resolve is not
served (its blocks still count toward the state) and is recorded in
`silver_ref.geography_resolution` as `canonical_geography_absent`.

## Schedule and scope

The DAG runs at 14:00 UTC on the 20th of each month and plans the newest
`recent_years` registered years (default 1) for every state. When a state's
vintage equals the one already published for that year, the run records
the slice `unchanged` after reading the two small metadata files and fetches
nothing else. A new vintage captures all four files again and is served
beside the old one: `observation_revision` keeps both releases, keyed by the
vintage, and `observation_latest` serves the newest. State-years run one at a
time through the one-slot `census_lodes_files` pool; a large state's
origin-destination file is tens of megabytes.

The DAG takes no backfill parameter; the years before the newest are loaded
once with the history command below.

## Loading the history

Run inside the scheduler container on the internal stack, detached so it
survives the terminal:

```text
docker exec docker-airflow-scheduler-1 \
    python -m data_ingestion_toolbox.census_lodes.history --years 2023-2002
```

It plans every state for every year asked (newest first) and:

- skips, without a request, a state-year that already has a `silver_ready`
  or `published` slice, so a stopped run resumes where it stopped and a
  rerun fetches nothing (`--refresh` asks the Bureau again; the capture then
  downloads only a new vintage);
- replays, without downloading again, a state-year whose files were
  captured but whose replay never finished;
- captures and replays the rest through the same code the DAG runs,
  `--workers` state-years at a time (default 6), each in a spawned (never
  forked) worker process with its own connection; the log names each
  state-year when it starts and when it ends;
- stops a worker whose state-year is still running after
  `--slice-timeout` seconds (default 1800), records that state-year
  `timed_out` and lists it under `stuck` in the summary;
- withdraws a capture that was stopped part way (fewer file rows than
  registered files) and captures that state-year again, never replaying it
  with files missing;
- records a failed or quarantined state-year and carries on with the rest;
- publishes only when every requested state-year is in: the replayed slices
  turn `published` and their county and state sums are computed into
  `gold_census_lodes.observation_fact` in one transaction, then the glossary
  harvest is signalled. A run with any failure publishes nothing and exits
  1; rerun the same command and only the failures are retried.

`--plan` prints, per year, which states would load and exits without
fetching. `--states de,wi` limits the run. Only one history load runs at a
time: a second exits 2 rather than wait. Pause `census_lodes_ingest` while
it runs so the DAG does not capture the newest year beside it.

The command prints a JSON summary per year (states by outcome, rows, worker
seconds). To size a run before it starts, read what the newest year cost:

```sql
SELECT slice.year, COUNT(*) AS states,
       pg_size_pretty(SUM(blob.payload_size)) AS captured,
       MAX(slice.updated_at) - MIN(slice.created_at) AS wall_clock
FROM control.census_lodes_slice AS slice
JOIN control.census_lodes_file AS file USING (run_id)
JOIN raw_capture.response_capture AS capture ON capture.capture_id = file.capture_id
JOIN raw_capture.payload_blob AS blob USING (payload_checksum)
WHERE slice.status = 'published'
GROUP BY slice.year ORDER BY slice.year DESC;
```

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_census_lodes_schema`, re-applies the package's DDL before any
capture. Create the pool if deployment automation has not:

```text
airflow pools set census_lodes_files 1 'Census LEHD LODES files (serialized)'
```

## What a run does

1. Commits the version file, the checksum list and each verified data file
   as captures, and records the state-year in `control.census_lodes_slice`
   and each file in `control.census_lodes_file`. A failed capture leaves its
   run and request errors in the ingestion ledger and withdraws the slice.
2. Replays each capture: block rows are summed to counties; a row whose
   column count, block code or value cannot be read goes to
   `silver_census_lodes.quarantine`, and a header that is not the family's
   quarantines the file and stops publication. It writes the geography
   resolution ledger. The run commits only if every aggregate was written.
3. Marks the slice `published` and, in the same transaction, computes its
   county and state sums into `gold_census_lodes.observation_fact`
   (`refresh_observation_fact`), which `observation_revision` and
   `observation_latest` read by key; then signals the glossary harvest. Each measure is one metric, for example
   `CENSUS_LODES:jobs`.

## Checks after a run

```sql
SELECT state, year, data_vintage, status FROM control.census_lodes_slice
ORDER BY updated_at DESC LIMIT 20;

SELECT family, status, row_count, quarantined_count
FROM control.census_lodes_file WHERE run_id = '<run_id>';

SELECT metric_key, geo_level, COUNT(*)
FROM gold_census_lodes.observation_latest GROUP BY 1, 2 ORDER BY 1, 2;
```

## Quality rules

- `DQ-LODES-001` (uniqueness) and `DQ-LODES-003` (an unavailable column
  carries no number) are enforced by the facts' keys and named CHECK
  constraints.
- `DQ-LODES-002` fails on a slice left unreplayed or a readable file that
  reached no silver row, and runs in the daily sweep.
- `DQ-LODES-004` warns when a county's origin-destination jobs by work
  county differ from its workplace total.
- `DQ-LODES-005` fails a published state-year with readable residence or
  workplace rows that serves no sums (a publish whose refresh never ran).
  `SELECT gold_census_lodes.refresh_observation_fact(NULL)` computes every
  published state-year still missing its sums.
- `DQ-LODES-006` warns, naming each state, when a published year is missing
  a registered state: a history load that stopped part way.
- `DQ-LODES-007` warns when a state's jobs move by more than a quarter
  between consecutive published years, a coverage break to explain.
