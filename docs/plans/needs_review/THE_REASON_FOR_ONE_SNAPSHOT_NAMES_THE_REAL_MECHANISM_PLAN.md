---
id: the-reason-for-one-snapshot-names-the-real-mechanism
branch: claude/iterate-plans-improvements-ir885c
depends_on: [one-snapshot-per-request]
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/integration/database -m "integration and database and not slow" -q
  - python -m pytest tests/unit -q
---

# The reason for one snapshot per request names the real mechanism

## Plan status

- **Status:** Needs review. Investigated, authored and implemented
  2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/database.py`,
  `apps/api/services/distribution_service.py`,
  `tests/integration/api/test_request_snapshot.py`,
  `tests/integration/database/test_serving_relation_shape.py`,
  `docs/reference/TESTING_CONTRACT.md`

## Context

Found while reading the warehouse's object kinds for DQ-013. Three places in
the API justify a decision by naming what can commit underneath a read:

- `apps/api/database.py` sets `REPEATABLE READ` on the warehouse engine
  because "a range and its counts taken in two executions let a `REFRESH
  MATERIALIZED VIEW CONCURRENTLY` commit between them" (API-100).
- `apps/api/services/distribution_service.py` evaluates its CTE once because
  "a `REFRESH MATERIALIZED VIEW CONCURRENTLY` committing between them — which
  is what the relation is for — left `min_value` describing rows the counts no
  longer measured" (API-084).
- `tests/integration/api/test_request_snapshot.py` repeats the first.

This warehouse has no materialized view. Not one:

```sql
SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind = 'm' AND n.nspname NOT LIKE 'pg%';   -- 0
```

`gold_bls.mv_bls_latest`, `gold_census.mv_acs_latest`,
`gold_fred.mv_fred_latest` and all three `rpt_*_observations` are ordinary
tables (`relkind = 'r'`); `gold_pep.mv_pep_latest` and `gold.mv_latest_dashboard`
are plain views. `REFRESH MATERIALIZED VIEW` appears nowhere in `src/`,
`sql/`, or `apps/` — only in those comments and the register row quoting
them. What actually rewrites a serving relation is
`refresh_serving_layer_in_year_chunks`, whose own docstring says how:

> Each report/latest pair is committed independently. If a later chunk fails,
> an Airflow retry replans from the unchanged source watermark…

a `DELETE … USING affected_keys` followed by an `INSERT … DISTINCT ON`, per
calendar year, one commit per year.

**The decisions are right and the reason was wrong in the direction that
matters.** An atomic view swap shows a reader either the old set or the new
one. A per-year commit shows a reader without a snapshot some years rebuilt
and others not — a wider window, not a narrower one, so `REPEATABLE READ` and
the single-evaluation CTE matter more than the comments claimed, not less.

And a reason that names a mechanism the source does not contain is a reason
nobody can check. The failure mode is concrete: someone asks whether the
isolation level is still needed, greps for `REFRESH MATERIALIZED VIEW`, finds
nothing, and reads `REPEATABLE READ` as an optimisation to reclaim — undoing
API-100 and API-084 on the strength of a stale comment.

## What was changed

- All three reasons now name the chunked per-year rebuild, and
  `apps/api/database.py` states the consequence in full: the relations are
  tables, the rebuild commits a year at a time, and the window is therefore
  wider than a swap's rather than narrower.
- API-100's register row said "the same materialized latest views a refresh
  rewrites"; it now says what rewrites them.
- `tests/integration/database/test_serving_relation_shape.py` keeps the
  reason checkable. Every relation the observation registry dispatches to is
  a table or a view; the warehouse carries no materialized view, and the
  assertion message names the three files to revisit if it ever does; and
  each serving reserve's two declared procedures exist in the warehouse with
  the report table it rewrites being an ordinary table, which is what a
  per-year commit requires.

## Validation

```text
E  AssertionError: this warehouse gained a materialized view, and three
   reasons in the API describe what commits underneath a read as a chunked
   per-year rebuild of a table (apps/api/database.py,
   apps/api/services/distribution_service.py,
   tests/integration/api/test_request_snapshot.py). Revisit them:
   ['gold_bls.mv_probe']
```

produced by `CREATE MATERIALIZED VIEW gold_bls.mv_probe AS SELECT 1 AS x`,
dropped afterwards. Both nodes pass against the bootstrapped warehouse, and
the behaviour the reasons defend is unchanged — API-100's integration test
still proves a second connection's commit is invisible inside one API
session.

## Deliberately not done

- **Nothing is renamed.** `mv_*_latest` and `rpt_*_observations` are named
  for what they once were, and the names are load-bearing: the API's
  observation registry, the quality inventory, the product-coverage support
  module, and each source's transform list all spell them. A rename is a
  coordinated migration for cosmetics, and the guard plus the corrected
  reasons remove the part that misleads.
- **`gold_pep.mv_pep_latest` keeps its prefix while being a view.** It has no
  refresh procedure because PEP serves a live projection, which is a design
  choice rather than an oversight; the reserve declarations the guard reads
  cover only the three chunked sources, so it is not asserted to be a table.
- **The two plan documents that quote the old reason are left as written.**
  They are the record of what was decided when, and rewriting the record to
  match a later finding is the opposite of keeping one.
