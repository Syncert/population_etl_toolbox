---
id: manifest-reapply-populated-warehouse
branch: claude/manifest-reapply-populated-warehouse
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/shared/test_warehouse_manifest.py -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_manifest_reapply_on_populated_warehouse.py tests/integration/database/test_schema_snapshot.py -q
  - ruff format --check . ; ruff check .
---

# Reapplying the manifest to a populated warehouse is tested

## Plan status

- **Status:** Ready for review. All three deliverables are implemented and
  every acceptance criterion was run on a machine session on 2026-09-18,
  including both failing-first proofs.
- **Last updated:** 2026-09-18
- **Next pickup:** none.

## Why

The repository's upgrade path is "re-run the whole manifest".
`docs/reference/BETA_RESET_REINGESTION.md` §4 says reapplying the complete
manifest is supported, §3 says re-running a file against an already-deployed
database *is* the migration, and `sql/migrations/README.md:5` forbids editing
a step other environments depend on. Several steps exist only for that path:

- `024_served_place_name.sql:31-74` runs six `UPDATE ... FROM
  gold_glossary.dim_geo_latest` backfills.
- `026_released_date_is_not_the_refresh_clock.sql:24-46` runs six
  `UPDATE ... SET as_of_date = updated_at::DATE`.
- `025_county_label_is_not_reviewed.sql:43-54` rewrites
  `resolution_method` and `confidence_class`.
- `019_stratum_shape_contract.sql:27-35` and `015_census_pep_historical_series.sql:50-197`
  run `DROP CONSTRAINT IF EXISTS; ADD CONSTRAINT`, which validates every
  existing row; 019's own comment says a warehouse holding a non-array
  stratum "fails this step loudly".

No test exercises any of that against data. `tests/integration/database/conftest.py:24-39`
and `tests/support/postgres.py:94` apply the manifest to a fresh `_test`
database; `test_warehouse_manifest_is_idempotent` in `test_raw_schema.py:63`
reruns it on the *empty* warehouse. Nothing loads rows through the real
pipelines and then reapplies. There is also no schema snapshot: a DDL edit
under `src/` that is not mirrored by a migration (or a migration a phase file
later overwrites) fails nothing.

Drift is already present. `gold_glossary.dim_metric` is defined in three
places (`sql/migrations/003_semantic_policy_extraction.sql:20`,
`sql/gold_contract/002_gold_glossary_schema.sql:234`,
`sql/gold_contract/001_gold_contract_views.sql:11`) and
`gold_glossary.dim_geography` in two (`002:257`, `001:33`). The README's own
paragraph on 026 says a second copy of a sixty-line view body is a copy that
drifts.

## Deliverables

### 1. Reapply-on-populated test

`tests/integration/database/test_manifest_reapply_on_populated_warehouse.py`:
bootstrap; run the existing source fixtures through the real silver and gold
paths (the fixtures `test_acs_gold_refresh.py`, `test_cdc_pipeline.py`,
`test_bls_silver_flow.py` and the NASS/FBI/PEP flow tests already use);
record row counts per served relation and a digest of served rows; reapply
the whole manifest; assert counts and digests unchanged and every
`control.data_quality_result` row intact. Time it and keep it in
`postgres-integration`.

### 2. Schema snapshot

`tests/integration/database/test_schema_snapshot.py`: after bootstrap, render
a deterministic listing from `pg_catalog` (relations, columns with types and
nullability, constraints by name and definition, indexes, view definitions)
and compare with a checked-in snapshot under `tests/sql/`. A change to the
snapshot is a reviewed part of the diff, in the same way the OpenAPI
contract digest is. Provide a regeneration command and document it in
`sql/migrations/README.md`.

### 3. One body per view

Delete the `dim_metric` and `dim_geography` bodies from
`001_gold_contract_views.sql` (they run last and re-create what `002` already
defined); keep `003`'s `CREATE OR REPLACE` because it drops columns first and
must re-create. The snapshot from deliverable 2 proves the result is the
same definition.

## Acceptance criteria

- [x] The reapply test passes, and fails when a migration is edited to change
      a served value -- proven by appending an `UPDATE` to migration `026`,
      which moved the digest while the row count stayed the same.
- [x] The schema snapshot test fails on an unmirrored DDL change (a column
      added under `src/` without regenerating) and passes after regeneration.
- [x] `gold_glossary.dim_metric` and `gold_glossary.dim_geography` each have
      one body; the duplicates in `001_gold_contract_views.sql` are gone and
      the rebuilt schema is byte-identical to the snapshot taken before.
- [x] `TESTING_CONTRACT.md` gains DB-051 and DB-052; `CI_EVIDENCE_MAP.md`
      names the snapshot as an owning path of `postgres-integration`.

## Implementation evidence

### The reapply, against rows

`test_manifest_reapply_on_populated_warehouse.py` seeds a FRED series through
the real transform, digests every served relation, reapplies the whole manifest
through `apply_manifest` -- the same path `scripts/apply_warehouse_manifest.py`
and the reset procedure run, one asset per transaction -- and asserts the
digests are unchanged.

The digest is an order-independent hash of each row's rendered text, summed per
relation, not a row count. That distinction is the whole test: the failure worth
catching is a migration that rewrites a value **in place**, which leaves the
count identical. A second test proves the digest actually has that property by
rewriting one value and asserting it moves, because the first assertion is
worthless if it can only see counts.

**Failing-first.** Appending
`UPDATE silver_fred.fact_economic_indicators SET value = value + 1;` to
migration `026` fails the reapply test with
`(1, '482a3a5b...') -> (1, 'd09e4c1c...')` -- same count, different content,
which is exactly the quiet case.

### The schema snapshot

`tests/support/schema_snapshot.py` renders relations, columns with types and
nullability, constraints, indexes and view bodies from `pg_catalog` into
`tests/sql/warehouse_schema_snapshot.txt` -- 5,700 lines, checked in, compared
on every push, with a bounded diff and the regeneration command in the failure
message.

Read from the catalog rather than the DDL text on purpose: the DDL is the
input, and the question the snapshot answers is what the database ended up
holding after every phase of the manifest ran over it.

**Failing-first.** Adding an unreviewed column to `gold_fred.sql` without
regenerating fails the test naming the column.

**`app_api` is excluded, and the reason is not cosmetic.**
`sql/bootstrap/002_app_api.sql` is not in the warehouse manifest: ADR-0003
makes application storage optional, and `test_api_reader_privileges` applies it
part-way through the tier. Including it made the snapshot depend on which tests
had already run -- the tier passed the file in isolation and failed it in a full
run. A snapshot that depends on execution order is worse than none. What this
one describes is exactly what DB-052 reapplies: the schema the manifest
produces.

### One body per view

`gold_glossary.dim_metric` was defined in three files -- `001_gold_contract_views.sql`,
`002_gold_glossary_schema.sql` and migration `003` -- and `dim_geography` in
two. `001` runs last in manifest order, so its copies silently won.

All three `dim_metric` bodies were compared and are identical, as are both
`dim_geography` bodies; that is the only reason this had cost nothing and the
only reason removing the duplicates is safe. The bodies were deleted from
`001`, the warehouse rebuilt from empty, and the snapshot rendered again:

```text
the warehouse matches the checked-in snapshot
```

Byte-identical, which is the proof the plan asked for. Migration `003` keeps
its `CREATE OR REPLACE` because it drops columns first and must re-create.
`test_every_contract_view_has_exactly_one_body` stops a second copy returning.

### Commands

| Command | Result |
|---|---|
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_manifest_reapply_on_populated_warehouse.py -q` | 2 passed |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_schema_snapshot.py -q` | 2 passed |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q` | 211 passed, 2 skipped (was 207, 2) |
| `python -m pytest tests/unit -q` | 1871 passed |
| `ruff format --check .` / `ruff check .` | clean |

The whole tier is run rather than the two new files, because the snapshot reads
a schema every other test shares -- and running it that way is what found the
`app_api` order dependency.

## Definition of done

The documented upgrade path is exercised against data on every push, and a
schema change that is not reviewed as one is a red job rather than a
surprise on the shared warehouse.

## What this plan deliberately does not do

- It does not introduce down migrations or a migration framework.
- It does not record which steps a warehouse has applied; that is
  `warehouse-manifest-ledger`.
