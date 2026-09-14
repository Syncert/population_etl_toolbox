---
id: fred-revision-identity-reaches-gold
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-release-is-the-providers-not-the-refresh-date]
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/shared tests/unit/api -q
  - python -m pytest tests/integration/database/test_fred_silver_flow.py -m "integration and database" -q
---

# FRED's revision identity reaches gold, and the as-released order is the relation's key

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `src/data_ingestion_toolbox/fred/gold_fred/DDL/gold_fred.sql`,
  `apps/api/registry.py`

## Context

Migration 004 added `realtime_start`/`realtime_end` -- FRED's own vintage
window -- to `silver_fred.observation_revision`, indexed for revision lookup.
`gold_fred.fact_fred_observation` throws them away:

```sql
NULL::DATE   AS realtime_start,     -- gold_fred.sql:44
NULL::DATE   AS realtime_end,       -- gold_fred.sql:45
```

while `rpt_fred_observations`, its natural key, its latest-selection index
and `uq_mv_fred_latest` are all built around those columns
(`gold_fred.sql:92-93, 104-105, 125-126`). Every served FRED row collapses
onto the `'0001-01-01'` sentinel, so FRED's `scope=as_released` surface has
no releases in it.

## Findings

- `registry.py` disagrees with itself about the same relation. The
  `ServingContract` for FRED puts the realtime window in `history_order`
  with the comment "FRED's own vintage identity and belongs in the history
  order" (`:190-197`). The `ObservationDispatch` for FRED declares
  `released_order=("observation_date", "geo_id", "as_of_date", "series_id")`
  (`:659`) -- no window -- which breaks the rule stated at `:118-124`: the
  order is the relation's unique key minus pinned columns.
- The moment the columns are carried through, two revisions of one
  observation tie on the neutral order, and `_newest_release_per_period`
  and as-released paging become the defect API-083/API-106 closed for the
  other sources.
- `test_fred_silver_flow.py` asserts silver carries the window; nothing
  asserts gold does.

## Acceptance criteria

1. `gold_fred.fact_fred_observation` projects the silver revision's
   `realtime_start`/`realtime_end`, and the reporting refresh keys on them
   as its DDL already expects.

   **Re-scoped at delivery, and why.** As authored this criterion carried a
   third clause: "an as-released read of a revised observation lists both
   revisions". It cannot be met by carrying the window, and no edit in this
   plan's surface can meet it: `silver_fred.fact_economic_indicators` is
   unique on `(series_id, observation_date)`, so silver holds one row per
   observation and gold has no second revision to project. Serving both
   means gold reads `silver_fred.observation_revision` instead, which is the
   follow-up recorded below rather than a clause this plan quietly dropped.
   The Validation section records the same thing from the evidence side.
2. The FRED dispatch entry's `released_order` includes the window, and a
   static guard in `tests/unit/shared` asserts, for every dispatch entry,
   that `released_order` covers the served relation's unique key minus the
   pinned columns -- derived from the DDL, so the next entry is checked
   without an edit.
3. `a-release-is-the-providers-not-the-refresh-date` decides what
   `as_of_date` means for FRED; this plan does not redefine it, only stops
   the window being lost.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `DB-`
   identifier; DB-038 at authoring time).

## Non-goals

- Backfilling the window for rows ingested before migration 004. Rows that
  never carried it stay at the sentinel and the guide says so.

## Validation

- **Criterion 1, first two clauses.**
  `gold_fred.fact_fred_observation` projects `s.realtime_start` and
  `s.realtime_end` instead of `NULL::DATE`, so
  `uq_rpt_fred_observations_nk`, the latest-selection index and
  `uq_mv_fred_latest` key on the window they were built around rather than on
  the `'0001-01-01'` sentinel they COALESCE to. The reporting refresh needed
  no change: it already inserted the two columns from the fact view.
- **Criterion 1, third clause: not reachable here, and why.** "An as-released
  read of a revised observation lists both revisions" cannot be delivered by
  carrying the window, and the reason is a layer below this plan's owner
  surface: `silver_fred.fact_economic_indicators` is
  `UNIQUE (series_id, observation_date)`. The revision *history* lives in
  `silver_fred.observation_revision` (keyed by capture and observation index,
  indexed `realtime_start DESC`); the fact table holds the current revision,
  and the reporting refresh deletes its window and re-inserts from the fact
  view, so gold mirrors that one row per observation. Serving every revision
  means moving FRED's gold serving from the fact table to the revision table
  -- a change to what `/observations` returns for every FRED series and to
  every row count in the FRED tier, in files this plan does not own. The
  registry already said so, in the comment this plan quotes: "the realtime
  window is FRED's own vintage identity and belongs in the history order even
  though the silver layer serves one window per observation today."
  - What is delivered instead is the whole of the rest: the window a served
    row was published under is now *on* that row, so a consumer can tell
    which FRED vintage a value belongs to, and the keys and orders that were
    built for it are correct rather than degenerate. The follow-up is stated
    under Remaining work with its cost.
- **Criterion 2, and a second defect the guard found.**
  `tests/unit/shared/test_served_paging_order.py` reads every
  `CREATE UNIQUE INDEX` in the bootstrap SQL -- paren-aware, so
  `COALESCE(metric_code, '')` is one key column and not two -- and asserts
  that each dispatch entry's `released_order` **and** `latest_order` covers
  at least one unique key of its relation, minus the pinned `metric_code`.
  Relations served through views are skipped, because their identity is
  declared on the dispatch entry rather than by an index, and a companion
  test asserts the scan really found the two keys it is asked about, so the
  rule cannot pass vacuously.
  - It failed on three entries beyond FRED's `released_order`, and each was
    total only by way of a rule a reader has to know: ACS's `released_order`
    omitted `dataset_code` (pinned implicitly, because `metric_code` is
    `CENSUS_ACS:<dataset_code>:<variable_code>`), and its `latest_order`
    omitted `dataset_code` and `vintage_year` (the latter determined by
    `observation_date`, which is `MAKE_DATE(vintage_year, 1, 1)`). Rather
    than encode those two functional dependencies as exceptions, the orders
    now name the columns: an extra tie-breaker costs nothing, and it makes
    each order provably total against the index instead of total by
    derivation.
  - FRED's `latest_order` was the third, and that one mattered:
    `uq_mv_fred_latest` admits one row per realtime window, and the window
    was NULL for every row, so the order could not tie *until this change
    landed*. Carrying the window without fixing the order would have opened
    the paging defect API-083 and API-106 closed for the other sources. The
    window is now in `latest_order` too.
  - Break-tests, both directions: removing the window from FRED's
    `released_order` fails with `FRED.released_order=(...) covers no unique
    key of gold_fred.rpt_fred_observations`, and adding a column to the DDL's
    index fails it from the other side, naming the new key.
- **Criterion 3.** `as_of_date` is untouched here; DB-039 settled it, and the
  guide's per-source release table (written there) is extended rather than
  rewritten.
- **Criterion 4.** `DB-040` is in `TESTING_CONTRACT.md` (the plan's `DB-038`
  guess was two behind the register), the family range reads
  `DB-001–DB-040`, `AUDITED_COUNTS["DB"]` is 40, and the totals are 407.
- **Integration coverage.**
  `test_fred_silver_flow.py::test_freds_revision_window_reaches_the_served_relation`
  seeds silver with a real window (`2024-06-15` to `9999-12-31`), runs the
  real serving refresh, and asserts all three relations carry it exactly --
  the fact view read through `dim_fred_series`, since it keys the series
  surrogate rather than the id. Break-test: restoring `NULL::DATE` fails with
  `[(None, None)] == [('2024-06-15', '9999-12-31')]`.
- **The guide.** The per-source release table now says FRED publishes a
  revision window, that served rows carry it, and that it tells a consumer
  which vintage a *value* belongs to rather than giving the series' revision
  history -- with a paragraph on what the window means and the plan's
  non-goal stated plainly: a row ingested before the warehouse captured the
  window carries no dates, reads as `0001-01-01` in the relation's keys, and
  is not backfilled.
- **A genuinely fresh warehouse.** Every manifest asset applied in order into
  a new `window_fresh_test` database printed `FRESH BOOTSTRAP OK` and the
  stored view definition carries the window. The database was dropped
  afterwards.
- **Tiers.** `pytest tests/unit` 1591 passed. `pytest tests/integration -m
  "integration and (redis or database) and not slow"` 168 passed, 2 skipped,
  14 deselected. `ruff check .` and `ruff format --check .` clean.

## Follow-up, not in scope

- **Serving every FRED revision, not only the current one.** The window is
  carried now, and the relation still holds one row per observation because
  `silver_fred.fact_economic_indicators` is unique on `(series_id,
  observation_date)`. Doing it properly means the FRED gold serving reads
  `silver_fred.observation_revision` -- which already keeps every revision --
  and that changes what `/observations` returns for every FRED series, every
  row count in the FRED tier, and the `mv_fred_latest` selection rule (one
  row per series *and window*, or one per series). It wants its own plan; the
  keys, the indexes and the paging orders it needs are now all in place and
  correct.

  Recorded here rather than filed as a plan because nothing in this plan's
  surface is left undone by it: it is a change to what `/observations`
  returns for every FRED series, and it wants its own acceptance criteria.

## Remaining work

- None. Review is the remaining step; the follow-up above is a successor,
  not an unfinished part of this plan.
