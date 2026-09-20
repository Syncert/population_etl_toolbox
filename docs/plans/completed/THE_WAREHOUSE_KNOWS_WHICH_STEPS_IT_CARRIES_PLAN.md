---
id: warehouse-manifest-ledger
branch: claude/warehouse-manifest-ledger
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/shared/test_warehouse_manifest.py tests/unit/quality -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_warehouse_bootstrap.py tests/integration/database/test_raw_schema.py -q
  - ruff format --check . ; ruff check .
---

# The warehouse knows which steps it carries

## Plan status

- **Status:** Ready for review. All four deliverables are implemented and every
  acceptance criterion has been run on a machine session against the pinned
  disposable PostGIS 16 container on 2026-09-18, including the criteria that
  need a real warehouse. One deviation from deliverable 2's wording is
  recorded below under "The Compose initdb path".
- **Last updated:** 2026-09-18
- **Next pickup:** none.

## Why

`sql/bootstrap/warehouse_manifest.json` is the reviewed order in which
forty-odd assets build a warehouse. Nothing records which of them a given
warehouse has received.

- `src/data_ingestion_toolbox/utility/gold_schema.py:86-100` says what
  `control.schema_migration_state` holds: "a content hash of one source's
  gold DDL files ... It records no bootstrap-manifest asset and nothing else
  writes to it."
- The quality inventory declares `DQ-SHARED-004` (BLOCK) unimplemented with
  the reason: "an executor written to compare them today would report every
  one of them missing. Recording them is the prerequisite ... the manifest is
  applied by numbered initdb mounts and by the documented reset, neither of
  which reports back" (`src/data_ingestion_toolbox/quality/inventory.py`).
- The application paths are a `jq | psql -f` loop in
  `docs/reference/BETA_RESET_REINGESTION.md` §3, the initdb mounts in
  `infra/docker/docker-compose.test.yml`, and `apply_sql_files` in
  `tests/support/postgres.py`. No step file opens a transaction (there is no
  `BEGIN;` in `sql/`), so a failure mid-file leaves a half-applied step and
  nothing that says so.

The README compounds it. `sql/migrations/README.md:3` says "Apply these
checked-in SQL files in numeric order when creating a fresh database." The
manifest does not, and cannot: `021` and `023` run in the `glossary-migration`
phase directly after `002`; `013` runs before the reference phase; `015` runs
in the `silver` phase; `003` runs second-to-last. Numeric order would fail
outright, because `004` alters `silver_fred.fact_economic_indicators`, which
is created by `src/data_ingestion_toolbox/fred/DDL/silver_fred.sql`, and `024`
alters `gold_census.rpt_acs_observations`, created by `gold_acs.sql`. The
manifest guard (DB-029, `tests/unit/shared/test_warehouse_manifest.py`)
checks that every file is named, not that the README describes the order.

The question "which revision is this warehouse at?" therefore has no answer
the warehouse can give, and a BLOCK certification rule depends on that answer.

## Deliverables

### 1. One applier that records what it applied

`scripts/apply_warehouse_manifest.py` (reuse the manifest reader already in
`tests/support/postgres.py` rather than writing a second one): for each asset
in manifest order, apply the file inside its own transaction, then upsert
`(component_name = asset id, ddl_hash = sha256 of the file bytes, applied_at)`
into `control.schema_migration_state`. A failed asset leaves no row and a
non-zero exit naming the asset. The existing gold-DDL rows written by
`gold_schema.py` keep their meaning; the test
`test_only_the_gold_bootstrap_writes_the_schema_migration_state` in
`tests/unit/quality/test_rule_automation.py` must be changed deliberately, in
the same commit, to state the new two-writer rule.

### 2. Every path that builds a warehouse uses it

The Compose initdb path calls the applier from its shell hook; the `jq | psql`
loop in `BETA_RESET_REINGESTION.md` §3 is replaced by the one command;
`tests/support/postgres.py` calls the same code so the disposable test
warehouse carries the same ledger rows a deployment does.

### 3. `DQ-SHARED-004` runs

Implement the executor: every manifest asset id has a ledger row whose hash
matches the checked-in file; report missing and drifted assets separately.
When the ledger has no manifest rows at all (a warehouse built before this
plan), the result is `not_applicable` with a note naming this plan, never a
pass. Update `UNIMPLEMENTED_RULES` and the counts in
`docs/reference/DATA_QUALITY_OPERATIONS.md`.

### 4. The README says the true order

Replace "in numeric order" with "in manifest order" and point at the manifest;
extend the DB-029 guard so a README that claims a numeric order fails, and so
each migration's README paragraph names the manifest phase it runs in.

## Acceptance criteria

- [x] After a bootstrap through the applier, `control.schema_migration_state`
      holds one row per manifest asset, and each `ddl_hash` equals the sha256
      of the file at that path. All 43, asserted per asset rather than by
      count (`test_the_ledger_records_every_manifest_asset_at_its_content_hash`).
- [x] A step file made to fail leaves no ledger row for that asset and exits
      non-zero naming it; assets before it are recorded
      (`test_a_failed_asset_records_nothing_and_names_itself`, against a real
      warehouse with a real syntax error).
- [x] Re-running the applier on the same warehouse is a no-op that leaves the
      same rows; `test_warehouse_manifest_is_idempotent` now runs through the
      applier and compares the ledger across two runs.
- [x] `DQ-SHARED-004` is `automated`, with a passing run on the disposable
      warehouse and a `not_applicable` result on a warehouse whose ledger is
      empty -- and a failing run when a row is removed, so the pass means
      something.
- [x] `sql/migrations/README.md` no longer claims numeric order, and the
      manifest guard rejects the claim if it returns. Both new guards were
      confirmed to fail against a deliberately broken README.
- [x] New tests carry `Covers:` labels; `DB-049` added to
      `TESTING_CONTRACT.md` and the applier registered in
      `CI_EVIDENCE_MAP.md` under `postgres-integration`.

## Implementation evidence

### The applier, and where the reader lives

`utility/warehouse_manifest.py` holds both. The plan said to reuse the reader
in `tests/support/postgres.py` rather than write a second one; putting it in
`src/` and having the test support import it satisfies that with the
dependency the right way round, and it is what makes the ledger comparable at
all -- a test warehouse whose rows are written by different code than a
deployment's proves nothing about a deployment's.

`ManifestAsset` carries its own `root`. That is what lets the failure path be
tested honestly: a test builds a small manifest whose last asset does not parse
and applies it for real, rather than mocking a cursor into raising and proving
only that the `except` branch is reachable.

The hash is of the file's **bytes**, not its decoded text. Hashing the string
would make one file hash differently under a different newline convention, and
a Windows checkout would then read as drift against a Linux deployment that
applied the identical step.

### One transaction per asset

No file under `sql/` opens a transaction -- verified, not assumed: there is no
`BEGIN;` in any of the 43. Every asset was also checked for statements that
cannot run inside a transaction block, and the four apparent hits
(`VACUUM`, `CREATE INDEX CONCURRENTLY`) are all in comments, so the shape is
safe for every asset the manifest names.

The DDL and the row that claims it commit together. A row that could outlive a
rolled-back step would be a warehouse claiming DDL it does not have, which is
worse than no ledger.

### The Compose initdb path

Deliverable 2 asks for a shell hook that calls the applier from the Compose
initdb path. **It is deliberately not done, and this is the deviation to
review.**

`postgis:16-3.5-alpine` carries neither Python nor jq -- checked, not assumed;
it has `psql`, `sha256sum`, `sed` and `grep`. A hook there would have to
re-implement both the manifest reading and the hashing in shell, which is the
second opinion about the bootstrap order that deliverable 1 exists to prevent.

It is also not a path that builds a deployment. `docker-compose.yml` mounts
only `001_api_readonly.sql` and `002_app_api.sql` into initdb; its warehouse is
built by `BETA_RESET_REINGESTION.md` §3, which now records. The initdb mounts
in `docker-compose.test.yml` shape a disposable container for its healthcheck
and for the Martin and smoke tiers, and the integration tier's session fixture
applies the manifest through the applier immediately afterwards -- so the tier
that reads the ledger has one.

The consequence worth stating: a container built only from those initdb mounts
and never touched by the fixture carries the DDL and no ledger. `DQ-SHARED-004`
answers `not_applicable` there rather than passing, which is exactly the case
that outcome exists for.

### The rule

`verify_manifest_ledger` reports missing and drifted as separate outcomes,
because one is a step that never ran and the other is a step that ran against a
file that has since changed, and an operator does something different about
each. An empty ledger is `not_applicable` and never a pass: a warehouse built
before the applier carries the DDL and no rows to prove it, which is
indistinguishable here from one never built, and a rule that read nothing must
not certify a publication.

### Two guards restated rather than removed

`test_only_the_gold_bootstrap_writes_the_schema_migration_state` asserted one
writer *so that adding a second would be deliberate*. This plan is that
deliberate act, so it now asserts exactly two, names what each answers, and
still fails on a third -- the reader tells them apart by manifest id, and a new
guard holds the two name sets apart so that filter stays exact.

`test_the_note_names_the_manifest_it_cannot_yet_be_compared_against` guarded a
note that existed only while the rule could not run. The rule runs, so the
executor is what is worth guarding: it must be the registered one and must read
the manifest through the module that owns it.

### The README

It told a reader to apply the steps in numeric order, which fails outright:
`004` alters a relation `silver_fred.sql` creates and `024` alters one created
by `gold_acs.sql`. It now states manifest order, says why numeric order does
not work, and every one of the 26 entries names the phase it runs in.

**Both new guards were confirmed to fail against a broken README, and the
first two attempts at them did not.** The order guard was written with `\b`
inside a raw string, which compiled to a literal backspace and could never
match -- it passed against a README that did claim numeric order. The phase
guard searched for the phase name anywhere in the entry, and
`001_raw_capture_control_foundation.sql` satisfied `foundation` with its own
filename. Both are fixed and both now fail when the thing they guard is
removed. A guard that has not been seen to fail is a guard nobody has tested.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/shared/test_warehouse_manifest.py tests/unit/quality -q` | 65 passed |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_warehouse_bootstrap.py tests/integration/database/test_raw_schema.py -q` | 14 passed |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q` | 184 passed, 2 skipped (was 181, 2) |
| `python -m pytest tests/unit -q` | 1868 passed |
| `ruff format --check .` / `ruff check .` | clean, 495 files |

The whole database tier is run on a warehouse rebuilt from empty, not just the
two files in the `verify` block: the session fixture that builds every test in
that tier now goes through the applier, so the tier is the boundary this change
sits on.

Against an empty PostGIS 16 with no initdb mounts at all,
`python -m scripts.apply_warehouse_manifest` applied and recorded all 43
assets, a second run left the ledger unchanged, and `--check` reported
`all 43 manifest assets recorded and current`.

## Definition of done

An operator can ask a warehouse which manifest steps it carries and at which
content hash, the answer is written by the same code every environment uses,
and the block rule that needs the answer runs.

## What this plan deliberately does not do

- It does not introduce down migrations or an in-place upgrade framework. The
  beta contract (ADR-0001, `sql/migrations/README.md`) keeps rebuild as the
  rollback strategy.
- It does not decide where CDC, FBI and NASS DDL should live; that is
  `source-ddl-under-src`, which may use this ledger for a pre-flight.
