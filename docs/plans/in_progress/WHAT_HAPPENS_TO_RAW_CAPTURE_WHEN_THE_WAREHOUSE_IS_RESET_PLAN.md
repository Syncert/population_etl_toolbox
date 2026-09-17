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
- **Status:** All four deliverables are implemented on
  `claude/plans-folder-iteration-4x6itr`. Every tier a cloud session can run
  is green, including the Airflow DAG tier. **It stays in `in_progress/` for
  one reason:** the round-trip test in deliverable 4 has never been *run* --
  it needs PostgreSQL, which this container has no way to provide. It is
  written, it collects, and a machine session runs one command. See "What a
  machine session must still do".
- **Last updated:** 2026-09-17
- **Current milestone:** the round trip, on a machine.

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

- [x] The ADR amendment exists and records the decision, its date and its
      owner: [ADR-0006](../../decisions/0006-capture-history-survives-a-reset.md),
      with ADR-0001's beta-reset paragraph narrowed in place so the two do not
      contradict each other.
- [x] The export DAG parses in `tests/dags` and its callable is unit-tested
      against a fixture export directory -- 16 unit tests over the export,
      the verification and the restore, and four over the DAG's own
      structure.
- [ ] The round-trip test passes. **Written, never run:** it needs a
      PostgreSQL instance. The whole file is
      `tests/integration/database/test_capture_export_round_trip.py`.
- [x] `BETA_RESET_REINGESTION.md` §2 carries the export step, §4 carries the
      restore, and §5 no longer describes re-ingestion as the path back.
- [x] `TESTING_CONTRACT.md` gains DAG-019, DB-046 and DB-047;
      `CI_EVIDENCE_MAP.md` names the DAG, the module and both tests.

## Implementation evidence

### The decision, and what it narrows

ADR-0006 records it. ADR-0001's beta-reset paragraph is amended in place
rather than left to be read alongside a newer document that contradicts it:
"destroying and rebuilding a beta environment" now means silver, gold and the
serving projections, and explicitly not the captures.

The ADR also records what is deliberately *not* carried forward, which the
plan left open: `control.capture_quarantine` (it records a parser's failure
against a capture, and replaying restored captures through the current parser
produces current quarantine state -- restoring the old rows would reinstate a
claim about a parser version no longer running) and the slice ledgers (they
are watermarks for planning, and a reset intends to re-plan).

### The export, and what makes it verifiable

`data_ingestion_toolbox.capture_export` writes newline-delimited JSON for the
rows and one file per payload **named by its own sha256**. The name is the
verification: `verify_export` recomputes each digest and compares it to the
filename, so a corrupted or substituted export fails before a restore rather
than inside one. A payload whose *stored* checksum disagrees with its bytes
fails the export outright -- carrying it forward would propagate corruption
under a name claiming it is fine.

The export only reads, asserted over every statement it produces. A read that
could change what it is reading is not evidence.

The payloads stream one at a time. `payload_blob.payload` is `BYTEA` and holds
every response body ever captured; holding the set in memory would make the
export fail on exactly the warehouse that most needs one.

### The restore, and the property it is restoring

Inserts, in foreign-key order, with `ON CONFLICT DO NOTHING`. Not an upsert:
an upsert is an update, and an update on these relations is what the
append-only triggers exist to refuse -- so a restore built on upserts would
fail against the schema, correctly. The unit tier asserts over every statement
that none is a `DELETE`, `TRUNCATE`, `DROP`, `ALTER`, `DISABLE TRIGGER` or
`DO UPDATE`, and the integration tier asserts the triggers present before the
export are present after the restore.

`payload_size` is recomputed from the bytes actually read rather than carried
from the manifest, because the column's own CHECK compares it to
`OCTET_LENGTH(payload)` -- trusting a number over the bytes beside it is the
shape of defect this plan is about.

### Where the decisions live

`resolve_export_root` and `export_directory_for` are in the module, not the
DAG. Importing a DAG file needs Airflow installed, and a decision that can
only be read with a scheduler present is a decision nobody checks. The DAG is
its docstring, two helpers that reach for Airflow at task runtime, and one
`PythonOperator`.

There is **no default export path**, and that is the load-bearing part. A
default would be a path inside the container, and an export inside the
container is destroyed by the reset it exists for -- the failure this plan is
about, wearing the appearance of success. An unset `CAPTURE_EXPORT_ROOT`
fails the task with that sentence.

### The DAG tier was run, in its own environment

`RUN_DAG_TESTS=1 pytest -m dag tests/dags` reports **126 passed, 5 skipped**.

Two notes for whoever runs it next, because both cost time here:

- Airflow was installed into a **separate** virtualenv under the scratchpad,
  not into `.venv`. The `airflow-dev` extra pins SQLAlchemy 1.4 against the
  API's 2.x; installing it into the working venv is how an earlier session in
  this branch broke its own toolchain.
- Three tests fail on a fresh Airflow venv with
  `sqlite3.OperationalError: no such table: connection`. They are not
  failures: the metadata database has never been initialised.
  `airflow db init` and one `airflow connections add public_data` make them
  pass, and the tier is green. CI's `dag-parse` job does neither explicitly,
  so a reader comparing a local run to CI should expect this difference.

`raw_capture_export` was added to `ORDERED_PIPELINE_DAGS` rather than parked
in `OPERATOR_TRIGGERED_DAGS`: that exemption is for DAGs that genuinely cannot
be scheduled, and a backup an operator has to remember is not a backup. It
runs last in the pipeline order, so it exports a warehouse the DAGs above it
have just filled rather than an empty one, and the suite supplies an export
root exactly as a deployment does.

### What a machine session must still do

Bring up the disposable warehouse and run the round trip:

```bash
docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres
RUN_INTEGRATION_TESTS=1 \
  TEST_POSTGRES_HOST=127.0.0.1 TEST_POSTGRES_PORT=55432 \
  TEST_POSTGRES_USER=population_test TEST_POSTGRES_PASSWORD=population_test \
  TEST_POSTGRES_DATABASE=population_etl_test \
  python -m pytest -m "integration and database" \
  tests/integration/database/test_capture_export_round_trip.py -q
```

Three tests: the round trip with DQ-SHARED-001 at `release` cadence over the
restored captures, a second restore of the same export changing nothing, and a
corrupted export reaching no statement. Then, if the Docker-backed DAG
pipeline is being run, `make test-dag-pipeline` now includes
`raw_capture_export`. Record both results here and move the plan to
`needs_review/`.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/shared tests/unit/tooling -q` | 337 passed (was 321) |
| `RUN_DAG_TESTS=1 python -m pytest -m dag tests/dags -q` | 126 passed, 5 skipped (was 122, 5) |
| `ruff format --check .` / `ruff check .` | clean, 487 files |
| `python -m pytest tests/unit -q` | 1838 passed (was 1822) |

## Definition of done

A reader of the reset procedure knows, before running it, whether the
evidence in `raw_capture` will exist afterwards, and if it will, the path
that makes it so is tested.

## What this plan deliberately does not do

- It does not choose object storage or a cloud provider; the export target
  is a path or connection the deployment configures.
- It does not back up silver or gold; those are reproducible by design.
- It does not prune, compact or expire old exports. One directory per run,
  and an operator or a retention policy decides what to keep. A tool that
  deletes evidence is a bigger decision than one that copies it, and it is
  not this plan's.
- It does not carry `control.capture_quarantine` or the slice ledgers; ADR-0006
  says why.
