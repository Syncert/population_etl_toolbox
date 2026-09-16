---
id: raw-capture-retention-decision
branch: claude/raw-capture-retention-decision
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/shared tests/unit/tooling -q
  - RUN_DAG_TESTS=1 python -m pytest -m dag tests/dags -q
  - ruff format --check . ; ruff check .
---

# What happens to raw capture when the warehouse is reset

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit.
  **Decision taken 2026-09-16 by the repository owner: capture history
  survives a reset.** The ADR amendment records that decision; it is not
  reopened by the implementer.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

`raw_capture` is the one layer a reset cannot reproduce. ADR-0001 makes
captures append-only and protects them with statement triggers
(`sql/migrations/001_raw_capture_control_foundation.sql:256-276`, DB-021),
and it also accepts destroying a beta environment. Those two statements are
compatible only if a reset re-captures the same evidence, and it does not:
`docs/reference/BETA_RESET_REINGESTION.md` §5 re-ingests *current* provider
data. FRED vintages (`realtime_start`/`realtime_end`), CDC releases the
provider has since superseded, NASS revisions and FBI refreshes captured
earlier are gone after a reset, and with them the revision history the
repository's invariants ask to preserve.

There is no backup or export story anywhere: `pg_dump`, `backup`, `restore`,
`pg_basebackup` and WAL archiving appear under `docs/reference`, `infra` and
`README.md` only inside completed plan prose. The deployment has one Docker
volume (`infra/docker/docker-compose.yml`, `analytics_postgres_data`), and
`raw_capture.payload_blob` is `BYTEA` in the main database
(`001:185-192`), so it is also the largest thing to back up.

## Deliverables

### 1. The decision, recorded

An amendment to ADR-0001 (or a new ADR-0006) stating that capture history
survives a beta reset: a reset destroys silver and gold, which are
reproducible, and never the captures and the `control` rows that identify
them. Record the date and the decision owner; the reasoning is the one in
*Why* above.

### 2. The export

An export path that copies `raw_capture.*` and the `control` rows it
references (requests, runs, attempts and slices the captures point at) to a
location outside the database volume, configured by the deployment as a
path or connection string. The recommended shape is a maintenance DAG under
`dags/` with the same operator notes the other maintenance DAGs carry,
writing one export per run with a manifest naming the capture id range and
checksums; a `pg_dump -t 'raw_capture.*' -t 'control.*'` wrapper is
acceptable if the reviewer prefers one file per export.

### 3. The restore

A restore step in `BETA_RESET_REINGESTION.md` §2 that reloads the export
before re-ingestion, so replay runs against the original responses and the
first ingestion after a reset extends the history rather than restarting
it. The append-only triggers from migration 001 must be honoured by the
restore (load with the triggers in place, never disabled).

### 4. The round trip is a test

An integration test that exports the fixture captures, drops and
re-bootstraps the warehouse, restores, and runs `DQ-SHARED-001` (checksum
verification) green over every restored capture.

## Acceptance criteria

- [ ] The ADR amendment exists and records the decision, its date and its
      owner.
- [ ] The export DAG parses in `tests/dags` and its callable is unit-tested
      against a fixture export directory.
- [ ] The round-trip test passes: restored captures verify under
      `DQ-SHARED-001`, and the append-only triggers are still present after
      the restore.
- [ ] `docs/reference/BETA_RESET_REINGESTION.md` §2 carries the restore
      step and §5 no longer describes re-ingestion as the only path back.
- [ ] `TESTING_CONTRACT.md` gains `DAG-` and `DB-` rows; `CI_EVIDENCE_MAP.md`
      names the DAG and the test.

## Definition of done

A reader of the reset procedure knows, before running it, whether the
evidence in `raw_capture` will exist afterwards, and if it will, the path
that makes it so is tested.

## What this plan deliberately does not do

- It does not choose object storage or a cloud provider; the export target
  is a path or connection the deployment configures.
- It does not back up silver or gold; those are reproducible by design.
