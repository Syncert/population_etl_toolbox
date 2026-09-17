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
- **Status:** All three deliverables are implemented on
  `claude/plans-folder-iteration-4x6itr`; the unit and DAG tiers are green.
  **It stays in `in_progress/` for one reason:** the integration assertion has
  never been run, because it needs PostgreSQL. It is written and collects.
- **Last updated:** 2026-09-17
- **Current milestone:** the refusal, on a machine.

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

- [x] A DAG test asserts that each of the six ingestion DAGs' guard task calls
      the shared helper, and that no DAG file still contains its own
      `to_regclass` or count predicate.
- [ ] An integration test bootstraps the manifest, does not load the geography
      reference, and asserts the helper raises naming `nation=0, states=0,
      counties=0`. **Written, never run** -- it needs PostgreSQL.
- [x] The thresholds appear once in `src/` and are quoted, not restated, in
      `BETA_RESET_REINGESTION.md`.
- [x] Every new test carries a `Covers:` label; DAG-020 added, and
      `CI_EVIDENCE_MAP.md` names the helper, the six DAGs and the three tests.

## Implementation evidence

### One predicate

`silver_ref/geography_guard.py` holds the thresholds and the query. Six DAGs
call it; three of them were asking `to_regclass` and three had their own copy
of the counts.

`additional_minimums` exists for exactly one caller. Census PEP serves
place-level estimates and required eighteen thousand places, and folding that
into the shared minimum would have made every other source wait for a grain it
does not use. A caller may raise a threshold and never lower one -- passing
`{"county": 1}` does not let a source opt out of the shared floor, which is
asserted.

The refusal names **every** grain it asked about, including the ones that
answered zero. A message reading `county=0` and saying nothing about `nation`
leaves the reader to guess which half of the predicate failed.

### Two things this found while proving itself

- **The sweep for leftover predicates had to read tokenized code.** Three DAG
  files now carry a docstring explaining what the old `to_regclass` guard did
  and why it was wrong, and a line-based grep flagged that prose. Deleting an
  explanation to satisfy a grep would delete the record of a shipped defect,
  so the test tokenizes comments and strings away and matches the code.
- **`__import__(callable.__module__)` is not the module the callable reads
  from.** Airflow's DagBag loads each DAG file under a synthetic module name,
  and patching the module that name re-imports left the six guard tests
  passing in isolation and failing in the full tier -- against a database
  connection that was never meant to be reached. They patch
  `task.python_callable.__globals__` now, which is what the function actually
  looks up.

### The topology assertion the plan implies, and the one it does not

The guard has to gate the work that resolves a geography, so the test asserts
that nothing named for ingesting, capturing, replaying, publishing,
transforming or loading runs before it. It deliberately does **not** assert
"the guard has no upstream task": `acs_ingest` syncs its dataset list first,
which reads the provider's catalog and resolves nothing. Asserting the
stronger thing would have failed on a DAG this plan says not to re-topologise.

### What a machine session must still do

```bash
docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres
RUN_INTEGRATION_TESTS=1 \
  TEST_POSTGRES_HOST=127.0.0.1 TEST_POSTGRES_PORT=55432 \
  TEST_POSTGRES_USER=population_test TEST_POSTGRES_PASSWORD=population_test \
  TEST_POSTGRES_DATABASE=population_etl_test \
  python -m pytest -m "integration and database" \
  tests/integration/database/test_shared_geography_guard.py -q
```

The refusal test skips rather than passes if the database already carries a
geography reference, so run it against a freshly bootstrapped one -- that is
the state the old guard passed on and the state this has to fail on. Record
the result here and move the plan to `needs_review/`.

### Commands

| Command | Result |
|---|---|
| `RUN_DAG_TESTS=1 python -m pytest -m dag tests/dags -q` | 134 passed, 5 skipped (was 126, 5) |
| `python -m pytest tests/unit/shared -q` | included in the 1850 below |
| `ruff format --check .` / `ruff check .` | clean, 491 files |
| `python -m pytest tests/unit -q` | 1850 passed (was 1844) |

Both DAG-tier guards were verified to fail without what they guard: restoring
the `to_regclass` check in one DAG, and dropping Census PEP's extra grain.

## Definition of done

Running any source DAG against a bootstrapped-but-empty warehouse fails at its
first task with the counts it saw, for all seven sources in the same words.

## What this plan deliberately does not do

- It does not change what a source does with a row it cannot resolve once the
  reference *is* loaded; CDC, FBI and NASS already record those in the ledger.
- It does not retry or re-resolve releases already published as `unmapped`
  on an existing warehouse. If one exists, the documented remedy is the
  source's replay path.
