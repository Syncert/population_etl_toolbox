# v1.0 build-out: handoff (2026-10-26, ~16:20 UTC)

Brief: project file `prompts/v1.0-build-out-prompt.md`. Rules: feature branch
-> PR into `dev`, self-merge once green (merge commit, delete branch), one
`dev` -> `main` release PR per wave for Nick. This note is not a dispatcher
plan; delete it when the next session has taken over.

## Why a new session

`gh pr merge` was refused by the session's auto-mode check even with the
allow rules Nick added to `.claude/settings.local.json` (git-ignored by his
global ignore). Nick started a fresh thread so the rules load. Nothing has
been merged into `dev` by me; `dev` = `main` (`e52a630`).

## Open PRs into `dev`, in merge order

| # | Branch | State | Notes |
|---|---|---|---|
| #136 | `fix/next-security-15-5-28` | green | Next.js 15.5.25 -> 15.5.28 (four advisories fail the audit gate). **Merge first.** |
| #137 | `chore/h-drive-paths` | green | Contains #136's commit (merged in). |
| #132 | `claude/project-thread-q26jq3` | green | Front-end readiness (Nick's earlier PR) + my browser-test fixes; contains #136. |
| #135 | `fix/almanac-plan-conflict-markers` | was rerunning CI | Conflict markers + ENV-024 test. Contains #136. |
| #133 | `claude/plan-usage-cost-logging-iqhj0f` | clean | Wave 0.4: after #132, merge `dev` in, `python -m tests.support.plan_environments --write`, green, merge. |
| #138 | `docs/adr-derived-views-and-record` | **awaiting Nick** | ADRs 0009-0011 (per-capita, cross-level compare, record wording). Blocks A-3, A-4, D-6, D-7, D-9. On acceptance set Status to Accepted with date. |

Catalog counters: each branch added rows from 624 independently (ENV-024,
ENV-025, ETL-085..091, API-170/171, WEB-144/145). After every merge of `dev`
into a branch, regenerate totals per brief rule 8 (`TESTING_CONTRACT.md` totals
and ranges, `tests/support/catalog_evidence.py` `AUDITED_COUNTS`, OpenAPI
snapshot via `PYTHONPATH=src python -m tests.support.regenerate_openapi_contract`,
`python -m tests.support.plan_environments --write`). Do not hand-merge them.

## Pushed branches without PRs (stacked; open PRs in this order after the above)

Each is built on the previous one, so open each PR after its parent merges,
then merge `dev` in.

1. `feat/acs-backfill-measure-and-scope` (W-1): measure module, declared year
   scopes, disk guard (ETL-085/086), content-keyed resume (ETL-087).
2. `feat/acs-decade-geography-and-serving` (W-2): grain-step serving
   (ETL-088), Census 2000 tracts (ETL-089). 401 pre-2010 places stay unmapped
   (no 2000 place reference yet; recorded in the backfill plan).
3. `feat/acs-full-city-history` (W-3 code): scopes "2005-2024",
   `scripts/run_acs_backfill.py` (ETL-090).
4. `feat/acs-tract-tables-expanded` (W-4 code): 22 tract tables, tract years
   2009-2024 (Nick approved the full list).
5. `feat/api-place-search` (A-1, API-171) -> `feat/web-find-your-place`
   (D-1, WEB-145; also fixes `Permissions-Policy: geolocation=()` which broke
   "Use my location").
6. `feat/metro-figures` (W-7 part 1, **WIP commit**): ACS metro grain +
   migration 036 (031/032 now leave 036's checks alone). Still to do: catalog
   row ETL-091; full DB tier was running when I stopped (rebuild the test DB
   with `docker compose -f infra/docker/docker-compose.test.yml down -v` then
   `up --wait postgres` and rerun `tests/integration/database`); one live
   metro year; BLS LAUS/CPI metro and BEA metro/RPP still to build.

Independent of the W chain (based on `dev`):

- `feat/warehouse-backups` (W-6, ENV-025): `scripts/backup_warehouse.py`,
  `scripts/register_backup_task.ps1`, compose backup mount, runbook section 8.
  Plan `docs/plans/in_progress/WAREHOUSE_BACKUPS_PLAN.md` needs the drill
  result, the scheduled task registered and `BACKUP_HOST_DIR` /
  `CAPTURE_EXPORT_ROOT` set in Nick's `stack.env`, then move to needs_review.
- `feat/api-plain-labels` (A-2, API-170) -> `feat/web-plain-labels-everywhere`
  (D-2, WEB-144). 106 draft definitions in `docs/semantics/metric_definitions.json`
  for Nick to review in the release.

## Running on the machine

- **Restore drill** (started ~15:50 UTC): restores
  `E:/population_backups/warehouse/warehouse-20261026T151539Z` (the first
  dump: 36 GB, 11 min, DB 756 GB) into database
  `population_etl_restore_drill` on the warehouse server; the script then
  compares counts, writes `restore-drill.json` beside the dump and drops the
  scratch DB. Log: `E:\population_backups\drill-first.log`. It runs from a
  copy of the script in my scratchpad with `ANALYTICS_DB_*` set in the shell.
  Check: `docker exec docker-analytics_postgres-1 psql -U analytics -d population_etl -tAc "select count(*) from pg_stat_activity where datname='population_etl_restore_drill'"`.
  If it was killed with the session, drop the scratch DB and rerun
  `python scripts/backup_warehouse.py restore-drill --target E:/population_backups/warehouse --jobs 6`
  from the W-6 branch.
- **No ACS load has been started.** The city backfill (W-3) was to start after
  the drill: from a checkout containing W-1..W-4, run silver_ref once (for the
  Census 2000 tracts), then
  `python scripts/run_acs_backfill.py --grain place --years 2023-2005`, then
  `--grain tract --years 2024-2009`. Note 2009/2010 serving chunks were left
  RUNNING/PENDING by October's failed runs; the first run's serving step
  re-serves them.
- Test PostGIS (`population-testing-postgres-1`, port 55432) may still be up.

## Gotchas

- **The Airflow scheduler bind-mounts this checkout** (`H:\population_etl_toolbox`
  -> `/opt/data_ingestion_toolbox`). Switching branches changes the code a
  running load executes; during loads stay on branches containing the W code.
- The stack starts with `make deploy-up` (no `make up`). Docker's data disk
  was grown to 5.9 TB today (4.9 TB free); measured projection: city history
  ~1,370 GiB + wider tracts ~1,140 GiB, ~41% free after.
- PowerShell 5.1 here: `[IO.File]` resolves relative paths against the
  process directory, not `cd`; `Set-Content -Encoding utf8` writes a BOM;
  never pipe `python -` without stdin (it hangs).
- `explainers.test.js` "refuses HTML" fails on Windows only (also on `main`).
- Wave D next after merges: D-3 city pages (needs W-3 loaded), D-4/D-5 need
  A-5; P-items untouched; 0.6 unit-suite budget needs three green hosted
  coverage runs recorded.
