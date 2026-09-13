---
id: an-offender-query-orders-by-what-it-wraps
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/quality -q
  - python -m pytest tests/integration/database/test_source_quality_checks.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
---

# An offender query orders by the columns it wraps, and every rule is run once

## Plan status

- **Status:** Implemented; awaiting review. Authored 2026-09-13 by the
  assessment agent, claimed and completed 2026-09-13. It was a present
  defect: `dag-parse` was red on this branch because of it.
- **Last updated:** 2026-09-13
- **Owner surface:** `src/data_ingestion_toolbox/quality/reconciliation.py`,
  `src/data_ingestion_toolbox/quality/sources.py`,
  `tests/integration/database/test_source_quality_checks.py`

## Context

DQ-008 moved every offender rule's `ORDER BY` out of its subquery and onto
the wrapping statement, so that `COUNT(*) OVER ()` measures the whole
offender set and the sample's order is the statement's own contract rather
than a planner's habit. `_shifted` moves *positional* references one place
right to make room for the count column, and fourteen rules write positions.

The fifteenth writes names. `fred_slice_reconciliation` passes
`order_by="dataset.domain, dataset.series_id"`, which names the alias of the
relation *inside* the subquery. The wrapper places that clause outside, where
the only relation in scope is `offender`, and PostgreSQL refuses it:

```text
ERROR:  missing FROM-clause entry for table "dataset" at character 336
STATEMENT:  SELECT COUNT(*) OVER () AS offender_total, offender.*
        FROM ( SELECT dataset.domain, dataset.series_id
                 FROM raw_fred.fred_datasets AS dataset ... ) AS offender
        ORDER BY dataset.domain, dataset.series_id
        LIMIT 20
```

## Findings

- **It is red in CI now.** `dag-parse` failed on `706d658` and `f0a74f8`
  (<https://github.com/Syncert/population_etl_toolbox/actions/runs/34756794982>,
  <https://github.com/Syncert/population_etl_toolbox/actions/runs/34757108860>)
  with exactly that statement in the PostgreSQL service log. Every commit
  since DQ-008 landed (`0a5b8e5`) carries it.
- **Reproduced against a bootstrapped warehouse.** One row in
  `raw_fred.fred_datasets` with no `raw_fred.fred_series` match, then
  `fred_slice_reconciliation(cursor, {})` raises
  `psycopg2.errors.UndefinedTable: missing FROM-clause entry for table
  "dataset"` instead of returning a `fail` outcome with `observed_count=1`.
- **Why three tiers missed it and only the fourth caught it.**
  - `tests/unit/quality` never executes SQL.
  - `tests/integration/database/test_source_quality_checks.py` starts by
    emptying `raw_fred.fred_datasets` (`REQUIRED_EMPTY_RELATIONS`), so the
    FRED rule answers `not_applicable` at `configured == 0` and returns
    **before** `_offenders` is reached. DQ-008's new node proves the exact
    count on the ACS rule only.
  - `postgres-integration` and `coverage` therefore stayed green while the
    plan recorded "141 passed".
  - `tests/dags/test_dag_pipeline_execution.py` runs the real
    `warehouse_data_quality` DAG against a seeded warehouse, reaches the
    FRED rule with rows present, and is the one place the statement ran.
- **The rule that raises takes the run with it.** An executor that raises
  is not a `fail` outcome; it is an errored assessment, and
  `DATA_QUALITY_OPERATIONS.md` says an errored assessment is not promotable.
  A single unmatched FRED dataset row -- the very condition the rule exists
  to report -- makes the whole nightly assessment error instead of
  reporting one failed rule.

## Acceptance criteria

1. `fred_slice_reconciliation` returns a `fail` outcome with an exact
   `observed_count` and bounded, sorted evidence when a dataset has no
   series row. The reproduction above is the failing-first test, in
   `test_source_quality_checks.py`, seeded past `not_applicable`.
2. `_offenders` makes the sixteenth site impossible: an `order_by` that
   qualifies a column with a relation alias (`<name>.<column>`) is refused
   with a message that says what to write instead (positions of the
   wrapped select list, or bare output column names), the same way the
   helper already refuses an embedded `ORDER BY`/`LIMIT`. A unit test in
   `tests/unit/quality` covers the refusal and the two accepted forms.
3. **Every** `_offenders` call site executes against a real PostgreSQL with
   at least one offender present, once, from one parametrized node: every
   executor in `SOURCE_EXECUTORS` and both reconciliation rules that use
   the helper. Seed the smallest offender each rule's predicate describes,
   assert `result == "fail"`, `observed_count >= 1`, and that the evidence
   is sorted. A rule whose offender cannot be seeded from its public tables
   is recorded in the plan by name with the reason, not skipped silently.
   This is the guard DQ-008 should have carried: a wrapper contract proved
   on one rule is proved on none of the others.
4. `dag-parse` is green on the commit that lands this.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `DQ-`
   identifier; DQ-009 at authoring time). DQ-008's row is corrected where
   it claims the wrapper is proved for "every offender rule".

## Non-goals

- Re-opening DQ-008's decision to order on the wrapping statement. That
  reasoning stands; the FRED site simply did not follow it.
- Changing what any rule counts as an offender.

## What changed

- `fred_slice_reconciliation` orders by `1, 2` — the positions of its own
  select list, like every other rule.
- `_offenders` refuses an ordering that qualifies a column with any relation
  but `offender`, naming the three accepted spellings, beside its existing
  refusal of an embedded `ORDER BY`/`LIMIT`. Its docstring now states what an
  ordering may name.
- `tests/unit/quality/test_offender_queries.py` (new) covers the refusal, the
  accepted forms, and — because the run-time guard only fires for a rule a
  suite reaches — a sweep of every `order_by=` literal in the two quality
  modules. Restoring the old FRED ordering fails the sweep by name.
- `tests/integration/database/test_source_quality_checks.py` gains the
  reproduction (criterion 1) and, for criterion 3,
  `test_every_offender_statement_is_one_postgresql_can_run`.

## How criterion 3 was met, and where it was adapted

Seeding one offender per rule means writing a capture, release, geography and
fact chain per source — four of the fifteen rules read `silver_*` fact tables
whose rows cannot be inserted without them. What the defect is about is
whether each statement can *run*, so every `_offenders` call site is handed to
the planner instead: an `_ExplainingCursor` answers each rule's preliminary
count with one row so it proceeds past `not_applicable`, and runs `EXPLAIN`
on each offender statement, which resolves every name, type and scope without
an offender existing. The node asserts at least 19 statements were planned —
the number of call sites — so a rule returning before its own fails it.

The rules whose *fail* outcome is separately seeded from real rows, in the
nodes above it: the ACS, BLS and FRED slice ledgers, the FRED dataset join
(new), USDA NASS's slice ledger, Census PEP's sentinel conformance, the CDC
watermark, the reference resolution accounting, and the publisher registry.
The rest — `pep_release_completeness`, `pep_registry_reconciliation`,
`cdc_suppression_conformance`, `fbi_participation_coverage`,
`fbi_reported_vs_absent`, `nass_suppression_vocabulary`, and the two shared
lineage rules — are proved runnable there, by name, rather than skipped.

## Validation

- `pytest tests/unit` — **1515 passed**.
- `pytest tests/unit/quality` — 46 passed, and **the sweep fails on the old
  ordering**: restoring `order_by="dataset.domain, dataset.series_id"` leaves
  `1 failed, 45 passed`.
- `pytest tests/integration/database/test_source_quality_checks.py -m
  "integration and database"` — **9 passed**. With the old ordering restored
  the run fails on the new nodes: with the run-time guard in place as
  `QualityRunError` naming the relation, and with the guard also removed as
  `psycopg2.errors.UndefinedTable: missing FROM-clause entry for table
  "dataset"` — the CI failure, reproduced locally.
- `ruff format --check .` / `ruff check .` — clean (444 files).
- **`dag-parse` cannot run locally**: Airflow is not installed in this
  environment (`ModuleNotFoundError: No module named 'airflow'`), so
  criterion 4 is cited from CI rather than reported as passing here.
  **It is green on the landing commit `bf76c4a`** — run
  [34762273220](https://github.com/Syncert/population_etl_toolbox/actions/runs/34762273220),
  job "DAG parse tests (Airflow 2.9.3 + Python 3.11)", conclusion `success`,
  the "Run DAG tests" step included. It had failed on `706d658` and
  `f0a74f8` with the `missing FROM-clause entry` statement in the PostgreSQL
  service log.

## Remaining work

- None. Review is the remaining step.
