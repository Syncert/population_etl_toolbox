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

- **Status:** Ready for review. All three deliverables are implemented on
  `claude/plans-folder-iteration-4x6itr`, the unit and DAG tiers are green,
  and the integration refusal was run on a machine session on 2026-09-18
  against a freshly bootstrapped warehouse -- the state the old guard passed
  on. See "The machine run".
- **Last updated:** 2026-09-18
- **Next pickup:** none.

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
- [x] An integration test bootstraps the manifest, does not load the geography
      reference, and asserts the helper raises naming `nation=0, states=0,
      counties=0`. Run 2026-09-18 against a freshly bootstrapped disposable
      PostGIS 16 container; see "The machine run".
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

### The machine run

Run on 2026-09-18 against the disposable PostGIS 16 container from
`docker-compose.test.yml`, brought up fresh so the geography reference really
was absent:

```bash
docker compose -f infra/docker/docker-compose.test.yml down --volumes
docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres
RUN_INTEGRATION_TESTS=1 TEST_POSTGRES_* ... \
  python -m pytest -m "integration and database" \
  tests/integration/database/test_shared_geography_guard.py -v -rs
```

```text
test_a_bootstrapped_but_unloaded_warehouse_is_refused PASSED
test_a_loaded_reference_satisfies_the_guard           SKIPPED
1 passed, 1 skipped
```

The bootstrap was confirmed to be in the exact state the plan is about before
trusting the pass: `silver_ref.dim_geo_entity` exists, and
`SELECT count(*) FROM silver_ref.dim_geo_current WHERE is_active` returns `0`.
That is what the old `to_regclass` guard answered "yes" to, and the new helper
refuses it naming `nation=0, state=0, county=0`. The test's own skip branch --
which fires when a database already carries a reference -- did not fire, so
this is a pass rather than a skip wearing a pass's colours.

**The second test skipped, and that is not this plan's criterion.**
`test_a_loaded_reference_satisfies_the_guard` needs a nation, 50+ states and
3000+ counties, which no fixture in the database tier loads; it skips with
that reason and does so on every tier that runs it, including CI. The
criterion this plan declares is the refusal, and the positive direction is
covered by the DAG tier's assertion that each guard task calls the helper.
Running it for real would need the geography pipeline against the Census API,
which is the `external` tier's business, not this one's.

It was also run as part of the whole database integration tier
(`-m "integration and database" tests/integration/database`), where it behaves
the same way.

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
