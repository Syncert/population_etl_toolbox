---
id: served-geographies-resolve-in-the-catalog
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/integration/database -m "integration and database and not slow" -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
---

# Every geography a served row names is one the geography catalog resolves

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  divergence; reachable on the next TIGER vintage that retires a county.**
- **Last updated:** 2026-09-13
- **Owner surface:** `sql/gold_contract/002_gold_glossary_schema.sql`,
  the three reporting refreshes under `src/**/gold_*/DDL/`,
  `sql/gold_contract/001_gold_contract_views.sql`

## Context

`gold_glossary.refresh_dim_geo_latest` selects only active geographies and
**deletes** catalog rows that stop being active (`002:131-132, 190-196`).
The three serving refreshes join the same reference with no such predicate
(`gold_acs.sql:322`, `gold_bls.sql:313`, `gold_fred.sql:259`), so a
geography retired in a new vintage vanishes from `/catalog/geographies` and
`/catalog/geographies/{geo_id}` (404) while `rpt_*_observations` keep its
rows with attributes frozen at the last refresh, and `/observations?geo_id=`
still answers them.

A client that resolves geographies through the catalog cannot reach those
rows; one that does not gets rows the catalog will not qualify. API-107
made the geography catalog its own refresh; it did not say what happens to
observations of a geography it no longer lists.

## Findings

- One geography, two names. `gold_glossary.dim_geography.geo_name` is
  `COALESCE(place_name, county_name, state_name, geo_id)` (`002:230`,
  `001:46`); every observation contract view spells
  `COALESCE(county_name, state_name, geo_id)` (`001:57, 88, 130, ...`). A
  place answers under one name on the catalog route and another on the
  observation routes.
- No test asserts that the set of `geo_id` values in the reporting tables
  is contained in `dim_geo_latest`.

## Acceptance criteria

1. A decision, recorded in the plan and in `API_CONSUMER_GUIDE.md`: either
   a retired geography stays resolvable in the catalog with a state that
   says it is retired (the honest answer, mirroring `freshness_state` for
   metrics), or its observations stop being served. The first is expected.
2. An integration guard asserts every distinct `geo_id` in each reporting
   table resolves in `gold_glossary.dim_geo_latest`, failing first with a
   geography flipped inactive between two refreshes.
3. `geo_name` is one expression, defined once and called by the catalog
   and every observation contract view, in the spirit of
   `one-grain-vocabulary-source`.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `DB-`
   identifier; DB-039 at authoring time).

## Non-goals

- Changing how `silver_ref` decides `is_active`. That is the geography
  pipeline's contract.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
