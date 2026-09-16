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

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

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

- [ ] The reapply test passes, and fails when a migration is edited to
      change a served value (prove failing-first by a throwaway edit).
- [ ] The schema snapshot test fails on an unmirrored DDL change (add a
      column under `src/` without regenerating) and passes after
      regeneration.
- [ ] Each of `gold_glossary.dim_metric`, `gold_glossary.dim_geography`,
      `gold.dim_metric` and `gold.dim_geography` has exactly one body across
      `sql/gold_contract` and the migrations that do not need to re-create it.
- [ ] `TESTING_CONTRACT.md` gains `DB-` rows for both tests;
      `CI_EVIDENCE_MAP.md` names the snapshot file as an owning path of
      `postgres-integration`.

## Definition of done

The documented upgrade path is exercised against data on every push, and a
schema change that is not reviewed as one is a red job rather than a
surprise on the shared warehouse.

## What this plan deliberately does not do

- It does not introduce down migrations or a migration framework.
- It does not record which steps a warehouse has applied; that is
  `warehouse-manifest-ledger`.
