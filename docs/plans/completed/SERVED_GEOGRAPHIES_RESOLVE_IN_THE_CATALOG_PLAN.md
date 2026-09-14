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

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13.)
- **Last updated:** 2026-09-14
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

- **Criterion 1 — the expected answer was taken.** A retired geography stays
  resolvable, with a state that says so. `gold_glossary.dim_geo_latest` gained
  `geography_state` (`current`/`retired`, constrained) and `retired_at`;
  `refresh_dim_geo_latest` no longer DELETEs. `dim_geography` publishes
  `geography_state`, `retired_at`, and `is_active = geography_state =
  'current'` -- the shape `dim_metric` already uses for `freshness_state`, so
  a consumer learns the two the same way. `API_CONSUMER_GUIDE.md` says it,
  beside the paragraph that already warned that "not listed" is a statement
  about the projection.
- **Two details the plan did not specify, decided here.**
  - `retired_at` is `COALESCE(d.retired_at, NOW())`, so the *first* sweep that
    notices a geography is gone is the date, and every later sweep leaves it
    alone. "When did this county go away" has one answer, and the test asserts
    a second refresh does not move it.
  - a reference that lists a geography again un-retires it, and
    `geography_state` had to join the tuple the `ON CONFLICT ... DO UPDATE`
    compares. Without that a geography retired and then restored with
    byte-identical attributes skips the update and stays retired forever --
    which is the likelier real case, because a boundary vintage that
    re-includes an unchanged geography changes none of its attributes. The
    test proves it by restoring exactly the original record.
- **A correction to the plan's Context.** It says
  `/catalog/geographies/{geo_id}` answers 404 for a retired geography. That
  route does not exist -- `apps/api/routers/catalog.py` serves a geography
  *list* and no detail route, and the metric family is the one with a detail
  route. The divergence the plan describes is real through the list route: the
  geography vanished from `/catalog/geographies` entirely while
  `/observations?geo_id=` kept answering its rows. Nothing else in the plan
  depends on the misattribution.
- **Criterion 1, the consequence for the web app.** Deleting the row meant the
  six geography pickers never saw a retired geography; publishing it means
  they would. `ACTIVE_GEOGRAPHIES_ONLY` in `apps/web/lib/observationAccess.ts`
  is the one declaration, and all three components pass it, so the pickers
  behave exactly as they did before while the API stays honest for a client
  resolving a served row. `/catalog/geographies` takes `active_only` exactly
  as `/catalog/metrics` does, and defaults to everything served.
- **Criterion 2.** `test_a_retired_geography_stays_resolvable_and_keeps_naming_its_rows`
  (integration, database) runs the real reference pipeline: it lists a place,
  refreshes the projection, seeds one served row for it, retires it in a new
  vintage, and refreshes again. It then asserts the catalog still resolves it
  as `retired`/`is_active = false` under its own name, and that **every**
  distinct `geo_id` in the three reporting tables resolves in
  `dim_geo_latest`.
  - Break-test: restoring the `DELETE` in `002_gold_glossary_schema.sql` --
    the tier bootstraps from source, so a stubbed procedure is overwritten by
    the fixture and breaking it has to be done in the file -- fails with
    `a retired geography must stay resolvable, and under the same name its
    rows were served with: None`.
  - The containment query was proved to detect an orphan on its own, since the
    break above fails earlier in the test: inserting a reporting row for
    `state:98|place:99999` (no catalog row) makes it return
    `gold_census.rpt_acs_observations | state:98|place:99999`.
  - The test's fixture deletes the catalog rows it leaves behind as well as
    the reporting row. That matters more now than it did: a retired row
    survives by design, so a rerun would otherwise start from a row the
    reference no longer explains, and this tier is asserted repeatable.
- **Criterion 3.** `gold_glossary.geo_name(place_name, county_name,
  state_name, geo_id)` is the one expression, installed by migration 023 in
  the `glossary-migration` phase -- early, for the reason 021 installs
  `geo_grain` there: a view's body resolves at `CREATE VIEW`, so a function the
  contract views call must exist before them. All seven call sites call it: the
  geography catalog (in both files that define that view) and the six
  observation contract views.
  - The six views could not call it with the catalog's arguments until the
    relations they read carried `place_name`. Migration 024 adds it to the
    three reporting tables and the three latest-value tables and backfills
    from the catalog; the table DDL declares it beside `county_name` for a
    fresh bootstrap. What has to hold is that `rpt_*` and `mv_*` agree with
    *each other*, because the latest-value refreshes insert `d.*`
    positionally, and both are altered in the one step.
  - Two static guards, siblings of DB-037's: one fails when any routine the
    bootstrap leaves behind spells a `COALESCE(... geo_id)` name itself
    without calling the function, the other asserts the function is defined
    exactly once and installed before its first caller -- because a
    no-copies rule passes trivially if the function and its callers are all
    deleted.
  - Break-tests: restoring one view's `COALESCE(county_name, state_name,
    geo_id)` fails with `gold_glossary.dim_geography
    (001_gold_contract_views.sql) spells it itself`; renaming the function
    fails with `defined in 0 bootstrap steps`; moving its manifest entry to
    the end fails with `installed at 40, first called at 31`.
  - A frontend guard reads the components and fails when one reads
    `/catalog/geographies` without `ACTIVE_GEOGRAPHIES_ONLY`, or spells
    `active_only` in a geography call itself (it is also a metric filter, so
    the rule looks only at the geography calls). Both break-test.
- **Criterion 4.** `DB-038` is in `TESTING_CONTRACT.md`, the family range reads
  `DB-001–DB-038`, `AUDITED_COUNTS["DB"]` is 38, and the totals are 404.
- **A genuinely fresh warehouse.** Applying every manifest asset in order into
  a new `geoname_fresh_test` database printed `FRESH BOOTSTRAP OK`;
  `geo_name('Springfield city', 'Sangamon County', 'Illinois', ...)` returns
  `Springfield city`, `geo_name(NULL, NULL, NULL, 'state:17|county:167')`
  returns the `geo_id`, `dim_geo_latest` has both new columns, all six serving
  relations have `place_name`, and `gold_census.fact_observation`'s definition
  calls the function. The ordering is load-bearing: dropping the function and
  re-applying the contract views fails with `function
  gold_glossary.geo_name(text, text, text, text) does not exist`. The database
  was dropped afterwards.
- **Tiers.** `pytest tests/unit` 1577 passed. `pytest tests/integration -m
  "integration and (redis or database) and not slow"` 162 passed, 2 skipped,
  14 deselected. Frontend units 377 passed. Browser tier 94 passed against a
  fresh production build. `npx tsc --noEmit`, `eslint`, `ruff check .` and
  `ruff format --check .` clean. The reviewed OpenAPI snapshot was regenerated
  and its diff is additive: the `active_only` query parameter and three
  `GeographyLatest` fields.

## Remaining work

- None. The plan's `DB-039` guess for the identifier was one ahead of the
  register; the row is `DB-038`.
