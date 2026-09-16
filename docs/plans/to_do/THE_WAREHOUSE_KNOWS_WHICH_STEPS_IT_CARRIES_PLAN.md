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

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

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

- [ ] After a bootstrap through the applier, `control.schema_migration_state`
      holds one row per manifest asset, and each `ddl_hash` equals the sha256
      of the file at that path.
- [ ] A step file made to fail (a fixture asset with a syntax error appended
      to a copy of the manifest) leaves no ledger row for that asset and
      exits non-zero naming it; assets before it are recorded.
- [ ] Re-running the applier on the same warehouse is a no-op that leaves the
      same rows (the DB idempotency test in `test_raw_schema.py` now runs
      through the applier).
- [ ] `DQ-SHARED-004` is `automated`, with a passing run on the disposable
      warehouse and a `not_applicable` result on a warehouse whose ledger is
      empty.
- [ ] `sql/migrations/README.md` no longer claims numeric order, and the
      manifest guard rejects the claim if it returns.
- [ ] New tests carry `Covers:` labels; add `DB-`/`DQ-` catalog rows as needed
      and register the applier in `docs/reference/CI_EVIDENCE_MAP.md` under
      `postgres-integration`.

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
