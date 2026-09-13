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

- **Status:** To do. Investigated and authored 2026-09-13. **Present defect:
  the `dag-parse` tier is red on this branch because of it.**
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

## Validation

To be recorded by the agent that claims this. The `dag-parse` tier needs
Airflow 2.9.3 and a PostGIS service; if it cannot run locally, cite the CI
run on the landing commit rather than reporting it as passing.

## Remaining work

- Everything.
