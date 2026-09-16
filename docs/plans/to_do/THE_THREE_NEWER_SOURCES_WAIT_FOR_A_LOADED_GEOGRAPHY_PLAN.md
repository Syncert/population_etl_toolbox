---
id: shared-geography-guard
branch: claude/shared-geography-guard
depends_on: []
parallel_safe: true
complexity: low
verify:
  - RUN_DAG_TESTS=1 python -m pytest -m dag tests/dags -q
  - python -m pytest tests/unit/shared -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k geography -q
  - ruff format --check . ; ruff check .
---

# The three newer sources wait for a loaded geography, not an empty table

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

`docs/reference/BETA_RESET_REINGESTION.md` §5 orders ingestion: the shared
geography reference first, then every source. Three DAGs enforce that order
with a real predicate. `dags/acs_ingest_dag.py:312-323` counts active rows in
`dim_geo_current` and refuses unless `nation == 1`, `states >= 50` and
`counties >= 3000`; `dags/bls_ingest_dag.py` and `dags/pep_ingest_dag.py`
carry the same check.

The three sources added later check something weaker. `dags/cdc_ingest_dag.py:44-49`,
`dags/fbi_ucr_ingest_dag.py:44-49` and `dags/usda_nass_crop_ingest_dag.py:58-63`
each run:

```python
cursor.execute("SELECT to_regclass('silver_ref.dim_geo_entity')")
if cursor.fetchone()[0] is None:
    raise RuntimeError("shared geography reference is not bootstrapped")
```

The table exists, empty, on every fresh bootstrap: the manifest creates it in
its `reference` phase. So the guard passes on exactly the warehouse the
ordering rule exists to protect. A CDC, FBI or NASS run before the geography
DAG resolves every row as `unmapped`, the release is still marked `published`
(`src/data_ingestion_toolbox/cdc/gold_cdc/publisher.py`), and the resolved-
geography serving views from migration 020 then exclude all of it. Nothing
re-resolves an already-published release, so the result is a source that
reports `published` and serves nothing, which is the failure the live smoke
tier was built to notice after the fact rather than prevent.

## Deliverables

### 1. One predicate, in one place

Add a helper under `src/data_ingestion_toolbox/silver_ref/` (for example
`require_shared_geography_loaded(connection)`) that runs the ACS/BLS/PEP
count check and raises with the observed counts. It is the only copy of the
thresholds.

### 2. All six ingestion DAGs call it

Replace the three `to_regclass` guards and the three inline count checks
with a call to the helper. The task names and DAG topology do not change,
so `tests/dags` topology assertions stay valid.

### 3. The rule is written down once

`BETA_RESET_REINGESTION.md` §5 states that every source DAG refuses to run
until the reference carries one nation, fifty states and three thousand
counties, and names the helper.

## Acceptance criteria

- [ ] A DAG unit test asserts that each of the six ingestion DAGs' guard task
      calls the shared helper (patch the helper, run the callable, assert the
      call), and that no DAG file still contains its own `to_regclass` or
      count predicate.
- [ ] An integration test bootstraps the manifest, does not load the
      geography reference, and asserts the helper raises naming
      `nation=0, states=0, counties=0`; after the geography fixture loads, it
      passes.
- [ ] The thresholds appear once in `src/` and are quoted, not restated, in
      `BETA_RESET_REINGESTION.md`.
- [ ] Every new test carries a `Covers:` label; add a `DAG-` catalog row in
      `docs/reference/TESTING_CONTRACT.md` if no existing row states the
      guard, and update `CI_EVIDENCE_MAP.md` if a new owning path is needed.

## Definition of done

Running any source DAG against a bootstrapped-but-empty warehouse fails at its
first task with the counts it saw, for all seven sources in the same words.

## What this plan deliberately does not do

- It does not change what a source does with a row it cannot resolve once the
  reference *is* loaded; CDC, FBI and NASS already record those in the ledger.
- It does not retry or re-resolve releases already published as `unmapped`
  on an existing warehouse. If one exists, the documented remedy is the
  source's replay path.
