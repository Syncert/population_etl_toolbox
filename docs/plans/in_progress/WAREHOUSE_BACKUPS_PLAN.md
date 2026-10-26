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

## Checkpoint

First dump running to `E:/population_backups/warehouse` (started 15:15 UTC).
Next: record its manifest, run the restore drill, register the task, set the
capture export target.
