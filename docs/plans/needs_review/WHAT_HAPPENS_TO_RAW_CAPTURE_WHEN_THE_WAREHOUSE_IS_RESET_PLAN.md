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
- **Status:** Ready for review. All four deliverables are implemented on
  `claude/plans-folder-iteration-4x6itr` and the round-trip test was run on a
  machine session on 2026-09-18. **It did not pass on arrival: it found four
  defects, one of them in `src/`, and the restore path could not have worked
  on any real export.** All four are fixed and the tier is green. See "The
  machine run" -- a reviewer should read that section before the rest.
- **Last updated:** 2026-09-18
- **Next pickup:** none.

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
- [x] The round-trip test passes -- after the four defects that running it
      exposed were fixed. Run 2026-09-18; the whole file is
      `tests/integration/database/test_capture_export_round_trip.py` and all
      three of its tests pass. See "The machine run".
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

### The machine run

Run on 2026-09-18 against the disposable PostGIS 16 container:

```bash
docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres
RUN_INTEGRATION_TESTS=1 TEST_POSTGRES_* ... \
  python -m pytest -m "integration and database" \
  tests/integration/database/test_capture_export_round_trip.py -q
```

**It failed.** Two of the three tests errored on the first run, and fixing
each one exposed the next. Four defects, in the order they surfaced:

1. **`restore_statements` could not send a `jsonb` column.** `src/`, not the
   test. `_json_ready` keeps such a column as the mapping it was -- correct
   for the row file -- and the restore handed that `dict` straight to the
   driver, which raised `can't adapt type 'dict'`. Every real capture carries
   `request_parameters` and `response_headers`, both `jsonb`, so **the restore
   would have failed on the first row of any genuine export.** The one thing
   ADR-0006 exists to guarantee did not work.
2. **The reset simulation could not run.** The block deleted the requests and
   rolled back, but the `DELETE` raises `ForeignKeyViolation` before any
   rollback is reached, because the captures still reference them.
3. **`outcome.status == "PASS"`** -- `RuleOutcome` has no `status`, and its
   vocabulary (`runner.RESULTS`) is lower case. The attribute is `result` and
   the value is `"pass"`.
4. **`capture_id = ANY(%s)`** asked PostgreSQL for `uuid = text` and was
   refused; the array needs `::uuid[]`.

Three of those are the test's own, and the pattern is worth naming: a test
written against a database nobody ran it on is a test written against a
remembered API. The first is not the test's -- it is a product defect that
the unit tier structurally could not see, because its stand-in cursor accepts
whatever it is handed and every fixture row in it was scalar.

**The fix, and its guard.** `_parameter_ready` serialises a `Mapping` or
`list` back to JSON text on the way to the driver. The parameter reaches
PostgreSQL untyped, so the target column decides what it becomes, and every
structured column in the four exported tables is `jsonb` -- which is why text
is sufficient and why the helper's docstring says a `text[]` column would need
different treatment if one is ever added.

Two unit tests now cover it, both confirmed failing without the fix:
`test_a_structured_column_is_handed_over_as_json_text` and
`test_a_json_array_column_is_handed_over_the_same_way`. They belong in the
unit tier rather than only in the integration tier because the defect is in a
pure function, and a `dict` reaching a driver should not need a container to
catch.

**The reset block now asserts what the schema actually does.** A reset is a
`DROP`, not a `DELETE`: while the append-only triggers are on, capture history
cannot be removed from a live warehouse a row at a time. The foreign key
refuses the request and the trigger refuses the capture with SQLSTATE `55000`,
and both refusals are asserted -- which is a stronger statement than the
rolled-back delete that could never run, and it is the reason `restore_captures`
only ever inserts.

**Result.**

```text
test_an_exported_capture_restores_and_verifies       PASSED
test_restoring_the_same_export_twice_changes_nothing PASSED
test_a_corrupted_export_never_reaches_the_database   PASSED
```

The criterion's substance holds on a real warehouse: the restored captures
verify under DQ-SHARED-001, and `_append_only_triggers` reads the same two
triggers after the restore as before it -- nothing was disabled to let the
load through.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/shared tests/unit/tooling -q` | 337 passed (was 321) |
| `RUN_DAG_TESTS=1 python -m pytest -m dag tests/dags -q` | 126 passed, 5 skipped (was 122, 5) |
| `ruff format --check .` / `ruff check .` | clean, 487 files |
| `python -m pytest tests/unit -q` | 1838 passed (was 1822) |

Re-run on the machine session of 2026-09-18, after the four fixes:

| Command | Result |
|---|---|
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_capture_export_round_trip.py -q` | 3 passed (was 2 failed, 1 passed) |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q` | 178 passed, 2 skipped (was 2 failed, 176 passed, 2 skipped) |
| `python -m pytest tests/unit -q` | 1864 passed |
| `ruff format --check .` / `ruff check .` | clean, 493 files |

The whole database tier is run above, not just this file: the fix is in a
function every restore goes through, and the tier is the boundary it sits on.
Both remaining skips are pre-existing and carry their own reasons -- the
Airflow import guard, and the geography guard's positive case wanting a loaded
reference.

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
