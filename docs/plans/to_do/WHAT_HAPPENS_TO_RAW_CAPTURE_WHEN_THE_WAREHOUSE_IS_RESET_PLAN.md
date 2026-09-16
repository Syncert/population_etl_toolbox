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

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit. The
  first deliverable is a decision recorded as an ADR amendment; the rest
  follows the decision.
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

### 1. The decision

An amendment to ADR-0001 (or a new ADR-0006) choosing one of:

- **(a) Capture history survives a reset.** An export path (a DAG or
  script that copies `raw_capture.*` and the `control` rows it references
  per run, or a `pg_dump -t 'raw_capture.*' -t 'control.*'`) to a location
  outside the database volume, and a documented restore step in
  `BETA_RESET_REINGESTION.md` §2 that reloads it before re-ingestion, so
  replay runs against the original responses.
- **(b) Capture history is disposable during beta.** An explicit statement,
  with the consequence named (revision history restarts at the reset), and
  the invariant text in `AGENTS.md`/ADR-0001 qualified accordingly.

Record which was chosen, by whom, and why, in the ADR and in this plan.

### 2. Under (a): the export and restore

Implement the export as a DAG under `dags/` with the same operator notes the
other maintenance DAGs carry; a restore procedure in §2; and a round-trip
test that exports the fixture captures, drops the schema, restores, and runs
`DQ-SHARED-001` (checksum verification) green.

### 3. Under (b): the documentation

`BETA_RESET_REINGESTION.md` §2 states plainly that captures are lost at
reset and that any vintage a later analysis needs must be re-captured while
the provider still serves it.

## Acceptance criteria

- [ ] The ADR amendment exists with a recorded decision.
- [ ] Under (a): the round-trip test passes and the DAG parses in `tests/dags`;
      under (b): the reference document carries the statement and this plan
      records that no code changed.
- [ ] `docs/reference/BETA_RESET_REINGESTION.md` §2 and §5 agree with the
      decision.

## Definition of done

A reader of the reset procedure knows, before running it, whether the
evidence in `raw_capture` will exist afterwards, and if it will, the path
that makes it so is tested.

## What this plan deliberately does not do

- It does not choose object storage or a cloud provider; under (a) the
  export target is a path the deployment configures.
- It does not back up silver or gold; those are reproducible by design.
