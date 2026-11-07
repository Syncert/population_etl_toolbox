---
id: warehouse-backups
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/tooling/test_backup_warehouse.py tests/unit/deployment tests/unit/shared -q
  - ruff check .
  - python scripts/backup_warehouse.py dump --target E:/population_backups/warehouse --jobs 6
  - python scripts/backup_warehouse.py restore-drill --target E:/population_backups/warehouse --jobs 6
---

# The warehouse is backed up off its volume, and a restore has been proven

## Status

In progress on `feat/warehouse-backups` (v1.0 build-out, item W-6),
2026-10-26.

## Why

The internal stack's warehouse now lives on H:, two 4 TB NVMe drives striped
(Windows Storage Spaces, Simple) into one ~7.3 TB volume with no redundancy.
One drive failing loses every capture and every multi-day load. Until this
plan there was no backup at all: `raw_capture_export` (DAG-019) existed but
`CAPTURE_EXPORT_ROOT` was unset on the stack, so it had never run, and
nothing dumped the database.

## Deliverables

1. **Raw captures mirrored nightly.** Both compose stacks mount
   `BACKUP_HOST_DIR` (a drive outside the warehouse's volume) into Airflow at
   `/opt/backups` and pass `CAPTURE_EXPORT_ROOT`, still with no default, so
   `raw_capture_export` writes to another drive once the two are set in
   `stack.env`.
2. **A periodic warehouse dump.** `scripts/backup_warehouse.py dump` runs
   `pg_dump` (directory format, parallel, zstd) from a throwaway container of
   the warehouse's own image, writes under a `.partial` name renamed on
   success, records a manifest (sizes, duration, exact counts of every table
   under 2 GiB) and exports the account deletion log beside it. It skips
   rather than competes when a long statement (a load) is running. `prune`
   keeps the newest dumps. `scripts/register_backup_task.ps1` schedules both
   weekly.
3. **A restore drill.** `restore-drill` restores the newest dump into a
   scratch database, applies the deletion log, compares counts and records
   `restore-drill.json`; its first run is recorded here.
4. **Runbook.** `BETA_RESET_REINGESTION.md` section 8.

## Acceptance criteria

- ENV-025 passes; the stack-configuration contracts pass with the new
  settings in both env examples.
- One full dump of the internal warehouse exists on a drive outside H:, and
  one restore drill of it passed; both are recorded below with sizes and
  durations.
- The weekly task is registered on OFFICE_LAB and the capture export target
  is set, or the exact remaining step is recorded for Nick.

## Evidence

- `pytest tests/unit` (branch, 2026-10-26): 2605 passed.
- First dump `warehouse-20261026T151539Z` to `E:/population_backups/warehouse`:
  36 GB from a 756 GB database in 11 minutes. Its restore drill restored
  correctly (608 GB, 52 minutes) but was marked `fail`: two capture-log tables
  restored 27 and 21 rows longer than the manifest, because the counts were
  taken before `pg_dump`'s snapshot and Airflow wrote in between.
- Fix: the dump now opens a REPEATABLE READ session, exports its snapshot,
  counts inside it and passes `--snapshot` to `pg_dump`
  (`SnapshotSession`; unit tests in `test_backup_warehouse.py`; checked live
  against the warehouse).
- Second dump `warehouse-20261026T163324Z` (snapshot-consistent): 36.2 GB in
  699 s. Restore drill: **pass**, 191 tables compared, no mismatches,
  607.8 GB restored in 3,967 s; scratch database dropped afterwards. The
  deletion log was not exported (`APP_API_DSN` unset in the drill's shell).

## Checkpoint

Blocked on a backup destination (Nick, 2026-10-26: "I don't have a way to
backup at this point so no directory there"). The code, runbook and a
passing drill are done; the weekly task is not registered and
`BACKUP_HOST_DIR` / `CAPTURE_EXPORT_ROOT` stay unset. Remaining step for
Nick once a destination exists: set both in `stack.env` and run
`scripts/register_backup_task.ps1`. The drill's dump is still on E: (36 GB,
plus the first 36 GB dump, which can be deleted).
