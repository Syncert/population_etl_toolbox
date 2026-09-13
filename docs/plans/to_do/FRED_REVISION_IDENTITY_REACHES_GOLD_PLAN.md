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

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect.**
- **Last updated:** 2026-09-13
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
   `realtime_start`/`realtime_end`; the reporting refresh keys on them as
   its DDL already expects; an as-released read of a revised observation
   lists both revisions.
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

To be recorded by the agent that claims this.

## Remaining work

- Everything.
