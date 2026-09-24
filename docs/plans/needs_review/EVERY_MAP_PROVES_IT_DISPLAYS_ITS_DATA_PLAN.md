---
id: every-map-proves-it-displays-its-data
branch: claude/plans-iteration-2026-09-20
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - npm --prefix apps/web run test:browser
  - ./tests/run.ps1 web-maps
  - ./tests/run.ps1 web-smoke
---

# Every explorer map proves it displays its data

## Plan status

- **Status:** **Ready for review.**
  Both tiers are built; every client defect they found is fixed; the two
  USDA NASS defects upstream of the client were decided by syncert on
  2026-09-23 (drop county `998` from the county grain; make the reference
  period a filter) and are implemented as NASS-1 and NASS-2.
- **Last updated:** 2026-09-23
- **Dependencies:** none.
- **Next pickup:** none -- awaiting human review.

## Why

The explorer has repeatedly shown an empty map while the warehouse held the
data. Twice in September 2026 alone:

- the tile worker was not served from the page's origin, so no map drew at all
  (fixed in `bb39f46`);
- the FBI UCR state map drew every state grey while the page itself reported
  "loaded 20902 state records (52 geographies across 402 periods)" -- the
  client's stratification check read `subject_code` (which, at the state
  grain, *is* the state) as 52 series and handed the choropleth nothing
  (fixed as WEB-117).

Nothing caught either. Every existing tier looked at a different layer:

| Tier | What it proves | Why it missed the grey map |
| --- | --- | --- |
| Web unit / browser | the client offers what the API declares, against stubbed data | stubbed rows; headless Chromium there has no WebGL, so MapLibre never draws |
| `viz-coverage.smoke` (WEB-104) | the live API *answers* every screen | the API answered 20,902 rows; the client threw them away |
| `map-wiring.smoke` (WEB-033) | the tile URL template resolves against real Martin | tiles were fine |

What was missing is a check that the map a reader sees shows what the
warehouse holds -- at the model and at the pixels.

## Scope

**In scope**

1. A **data-path sweep** over every source's explorer maps: the explorer's own
   request builder, pager, normalization, map reduction, and choropleth model,
   graded against an oracle computed from the raw rows by the test.
2. A **painted-pixel check** in a real browser with WebGL: each source and
   drawable grain opens the explorer and must paint value-bin colours.
3. One shared definition of "the rows the map colours" (`mapRows`), used by
   the page and graded by the sweep, so the sweep cannot agree with a copy.
4. Fixing every client defect the two checks find.
5. Recording, with evidence, every defect they find upstream of the client.

The two USDA NASS defects the sweep found upstream of the client were first
recorded without a fix, because they are warehouse and API contract changes
(`AGENTS.md`: fix the upstream contract, never compensate in the client). They
were decided on 2026-09-23 and are in scope from then on (NASS-1, NASS-2).

## Design

### Tier 1 -- data-path sweep (`tests/frontend/smoke/map-display.smoke.test.js`)

For every source, every metric (sources with more than `MAP_SWEEP_METRICS`,
default 40, are read at an even deterministic spread; `MAP_SWEEP_ALL=1` reads
all), and every drawable grain the metric declares:

1. build the request with `buildLatestObservationRequest` exactly as the page
   does, read every page with `fetchCollectionPages`, normalize with
   `normalizeObservationRows`;
2. reduce with `mapRows` and model with `buildChoroplethModel`;
3. grade against `tests/frontend/support/mapOracle.js`, which groups the raw
   rows by `geo_id` and period and decides independently:
   - no numeric value at the grain -> **empty** (legitimate);
   - two rows sharing a geography *and* a period -> **declined** is required,
     and the page must name the dimension that separates them;
   - otherwise one series per geography -> the model must colour exactly one
     value per geography whose newest row carries a number -> **coloured**.

   Anything else, or a read that stops before its total, is **FAIL** with the
   metric, grain, row count, and reason.

It also fails if a source with values has no coloured map at all. A tally per
source and grain is printed on every run.

### Tier 2 -- painted pixels (`tests/frontend/live/map-paint.live.spec.js`)

`playwright.live.config.mjs` launches Chromium with SwiftShader WebGL
(`--use-gl=angle --use-angle=swiftshader`) against a running stack; no server
of its own. Subjects are the reviewed matrix's own (source, drawable grain)
pairs from `tests/fixtures/api/viz_coverage.json`, so a source the checkout
says has maps cannot drop out silently. For each, the broadest of the first 12
single-series metrics is opened and graded on:

1. `data-colored-values` equals the oracle computed from the rows **the page
   itself received** (intercepted responses, not a second request);
2. the map canvas carries the legend's value-bin colours over at least 0.05%
   of its area (the grey FBI map measured **0.000%**, the fixed one
   **29.04%**, a correct sparse NASS map 0.73%);
3. the page threw no error.

### Running them

```text
./tests/run.ps1 web-maps          # SMOKE_BASE_URL defaults to http://localhost:3001
make test-web-maps
MAP_SWEEP_SOURCES=FBI_UCR MAP_SWEEP_METRICS=6 SMOKE_BASE_URL=http://localhost:3001 \
  npm --prefix apps/web run test:smoke -- ../../tests/frontend/smoke/map-display.smoke.test.js
SMOKE_BASE_URL=http://localhost:3001 npm --prefix apps/web run test:maps
```

## Work items

- [x] **MAP-1: one definition of the map's rows.** `mapRows()` and
  `seriesDimensionNames()` in `apps/web/lib/observationAccess.ts`; the page
  calls them.
- [x] **MAP-2: data-path sweep** with the independent oracle (WEB-118).
- [x] **MAP-3: painted-pixel tier**, config, `test:maps` script,
  `tests/run.ps1 web-maps`, `make test-web-maps`.
- [x] **MAP-4: prove each tier catches the failure it exists for.**
  - Tier 1, run against the pre-WEB-117 stratification code with
    `MAP_SWEEP_SOURCES=FBI_UCR MAP_SWEEP_METRICS=6`: all six sampled maps
    FAIL, each "one series per geography, declined as stratified by
    subject_code", and the source-level check names `FBI_UCR`.
  - Tier 2's pixel measure on captured canvases: grey FBI map 0.000%, grey
    CDC map 0.000%, fixed FBI map 29.04%.
- [x] **MAP-5: fix the client defects found.**
  - **WEB-117 (FBI):** stratification is judged within a geography, not
    across all rows.
  - **WEB-118 (NASS forecasts):** the first full sweep found 10 USDA NASS
    state maps whose rows share a state and year but differ in
    `reference_period_desc` (`YEAR`, `YEAR - AUG FORECAST`,
    `YEAR - OCT FORECAST`). That dimension is published but not a declared
    filter, and the check only looked at filters, so the map silently
    coloured whichever row arrived last -- possibly a forecast. Stratification
    now groups rows by geography **and** period and names any **published**
    dimension that separates them; the note tells the reader when no declared
    filter can narrow it, rather than pointing at a control that does not
    exist.
    Under an unpinned as-released read, several releases inside one
    geography stay several series whatever periods they cover, so the
    existing as-released contract (`explorer.spec.js`, "as-released
    exploration pins a published release") holds.
- [x] **NASS-1: county `998` is withheld from the county grain.**
  USDA NASS county code `998` is "OTHER (COMBINED) COUNTIES": the counties it
  suppresses for disclosure, combined once per agricultural district. The
  warehouse published them as `state:SS|county:998` without the district, so
  every district in a state collided on one made-up identity. For example,
  `USDA_NASS:soybeans_survey_annual:3e38e231…` had nine different 1990 values
  for `state:05|county:998`, identical in every published dimension. They
  were `unmapped`, and `unmapped` is still served.

  *Decision (syncert, 2026-09-23):* drop it from the county grain. *Assessment
  recorded with it:* the residual is the only place the suppressed counties'
  combined value appears at sub-state level, so it matters for anyone summing
  counties to reconcile against the state. It is not a county, though: it
  cannot be drawn or joined, and NASS publishes the state total itself. It is
  therefore **withheld, not deleted**: the row stays in raw capture and silver
  with its district, so an agricultural-district geography could publish it
  later without re-ingesting.

  *Implementation:* `geography_identity` resolves code `998` to
  `unsupported` (`geo_id` NULL, `geo_source_code = SS998`, `asd_code` and
  `county_name` kept), the path unsupported aggregate levels already take;
  gold and the publisher exclude it. Silver writes are
  `ON CONFLICT DO NOTHING`, so stored rows are rewritten by
  `sql/migrations/029_nass_combined_counties_are_not_counties.sql` (manifest
  and Docker init registered). Tests: unit
  `test_combined_counties_code_is_not_a_county` (both labels; failed first);
  database `test_combined_counties_are_kept_but_never_served_as_a_county`
  (pipeline with two districts' `998` rows, then the pre-fix shape restored,
  migration applied twice). Against the pre-fix identity code the database
  test failed: both rows were served. On the development warehouse, 029 was
  applied alone through `apply_manifest` (the ledger records it; 027 and 028
  were already drifted there and were not re-run): before it, 64,121
  `county:998` facts were in silver and 64,121 in gold; after it (11.2 s),
  0 in gold and 64,121 kept as `unsupported`.
- [x] **NASS-2: `reference_period_desc` is a declared filter.** USDA NASS
  declares it on `/observations` (API-156): bound, never inlined; refused
  with a 422 for sources that do not declare it; OpenAPI and viz-coverage
  snapshots regenerated; `API_CONSUMER_GUIDE.md` names it and why. The
  explorer builds filter controls from declared filters, so the explorer
  offers it with no web change. Live on the development API:
  `hay_survey_annual:3bf8ec4d…` at STATE answers 1,707 rows for `YEAR` and
  898 for `YEAR - AUG FORECAST`.
- [x] **MAP-7: a declined map must be one a reader can narrow.** Declining
  honestly is only half an answer. For every declined map, the sweep now
  re-reads it the way a reader would: it sets each declared filter among the
  separating dimensions to its most common published value, and the narrowed
  map must colour ("narrowed"). A map no declared filter can narrow fails.
  Dimensions that only describe a filter (CDC's `strata` and footnotes, which
  move with `stratum_id`) narrow with it.
- [x] **MAP-6: CI placement.** `test:smoke` includes every `*.smoke.test.js`,
  so the sweep also runs in the composed `web-smoke` stack against seeded
  fixtures. Confirm it passes there (see Evidence) or scope it.

- [x] **MAP-8: the catalog follows a data correction.** After 029 the
  publisher view listed COUNTY for 66 NASS measures, but the catalog still
  listed it for 73, even after a harvest ran: the rewrite moved no
  publication time, and the scheduled harvest only visits a publisher with a
  pending ready event. So seven soybean measures offered a county map with no
  county data. 029 now re-queues the NASS publisher's latest ready event (the
  outbox is unique per watermark, so it is reset to `pending`, not inserted).
  The harvest's content fingerprint then sees the grain change. Test:
  `test_the_catalog_follows_the_combined_counties_rewrite` (old shape
  harvested and its event processed, 029 applied, event pending, harvest
  drops COUNTY); with the re-queue removed it fails on `{'processed'} ==
  {'pending'}`. On the development warehouse, after re-applying 029 and one
  `glossary_harvest` run: catalog 66, publisher 66.
- [x] **MAP-9: a vacant map fails.** The sweep treated "no rows" as a
  legitimate empty map, which is how MAP-8 got past it. It now fails when the
  catalog advertises a grain and the read answers no rows at all. "Rows, but
  no number" stays a legitimate empty map, and the remaining cases were
  checked: the 15 empty ACS maps per grain are the detailed-occupation tables
  (B24114, B24134), which Census publishes only nationally. The API answers
  `null` for every state (`api.census.gov/data/2023/acs/acs5?get=B24114_026E&for=state:55`),
  and the warehouse serves them as `absent` rather than zero.

## Acceptance criteria

1. A map whose rows hold values and that shows none fails a check by name --
   proved by MAP-4.
2. A map that shows one of several competing values without saying so fails a
   check by name -- proved by the NASS forecast finding.
3. The painted canvas, not only the model, is graded for every reviewed
   (source, drawable grain).
4. Every client defect found is fixed with a test that failed first.
5. Every upstream defect found is recorded with a reproducible example and
   the decision it needs.
6. Every declined map can be narrowed to a coloured one with declared
   filters.
7. The whole `verify` block passes.

## Evidence record

All runs on 2026-09-23 against the local development stack
(`http://localhost:3001`) unless noted.

### Final runs (after NASS-1, NASS-2, MAP-7)

| Source / grain | Coloured | Narrowed | Empty | FAIL |
| --- | --- | --- | --- | --- |
| BLS COUNTY | 2 | 0 | 0 | 0 |
| BLS STATE | 4 | 0 | 0 | 0 |
| CDC COUNTY | 12 | 0 | 0 | 0 |
| CDC STATE | 3 | 25 | 0 | 0 |
| CENSUS_ACS COUNTY | 25 | 0 | 15 | 0 |
| CENSUS_ACS STATE | 25 | 0 | 15 | 0 |
| CENSUS_PEP COUNTY | 17 | 0 | 0 | 0 |
| CENSUS_PEP STATE | 17 | 0 | 0 | 0 |
| FBI_UCR STATE | 40 | 0 | 0 | 0 |
| USDA_NASS COUNTY | 28 | 0 | 2 | 0 |
| USDA_NASS STATE | 27 | 10 | 0 | 0 |

**Zero failures.** The 25 CDC state maps colour once `stratum_id` is set; the
10 NASS state maps colour once `reference_period_desc` is set. The two empty
NASS county maps are metrics whose only county-grain rows were the
combined-counties residual, which is now correctly withheld.

| Command | Result |
| --- | --- |
| data-path sweep (`map-display.smoke`), all sources | 4 passed, 0 map failures |
| `npm --prefix apps/web run test:maps` | 11 passed |
| `./tests/run.ps1 web-smoke` (composed CI stack) | 5 files, 27 passed |
| `pytest tests/unit -q` | 2137 passed |
| `RUN_INTEGRATION_TESTS=1 pytest -m "integration and database" tests/integration/database/test_usda_nass_pipeline.py` | 7 passed |
| `npm --prefix apps/web run test:unit` / lint / typecheck | 686 passed / clean / clean |
| `ruff check . ; ruff format --check .` | clean |

### Earlier runs (before NASS-1 and NASS-2)

**Tier 1, full sweep (default budget of 40 metrics per source):**

| Source / grain | Coloured | Declined | Empty | FAIL |
| --- | --- | --- | --- | --- |
| BLS COUNTY | 2 | 0 | 0 | 0 |
| BLS STATE | 4 | 0 | 0 | 0 |
| CDC COUNTY | 12 | 0 | 0 | 0 |
| CDC STATE | 3 | 25 | 0 | 0 |
| CENSUS_ACS COUNTY | 25 | 0 | 15 | 0 |
| CENSUS_ACS STATE | 25 | 0 | 15 | 0 |
| CENSUS_PEP COUNTY | 17 | 0 | 0 | 0 |
| CENSUS_PEP STATE | 17 | 0 | 0 | 0 |
| FBI_UCR STATE | 40 | 0 | 0 | 0 |
| USDA_NASS COUNTY | 6 | 0 | 0 | **24** |
| USDA_NASS STATE | 27 | 10 | 0 | 0 |

Every one of the 24 failures is NASS-1: each failing metric's colliding
geographies are `county:998` and nothing else (checked for all 24). The 10
declined NASS state maps are the forecast case, now declined and naming
`reference_period_desc` (NASS-2). Before the MAP-5 fix, those 10 failed as
"several series share a geography and period, and the map would keep whichever
arrived last". FRED publishes no drawable grain and has no map.

**Tier 2, painted pixels:** `npm run test:maps` -- **11 passed** (BLS, CDC,
CENSUS_ACS, CENSUS_PEP, USDA_NASS at STATE and COUNTY; FBI_UCR at STATE). The
first run failed USDA_NASS COUNTY at a 1% area threshold on a correct sparse map
(traditional corn, ~26 counties, 0.73%); the floor was set to 0.05% and the
subject changed to the broadest candidate.

**CI tier:** `./tests/run.ps1 web-smoke` (composed stack, seeded fixtures) --
5 files, **27 passed**, including the sweep: one coloured county map per
seeded source, no failure.

| Command | Result |
| --- | --- |
| `npm --prefix apps/web run test:unit` | 686 passed |
| `npm --prefix apps/web run lint ; ... typecheck` | clean |
| `npm --prefix apps/web run test:browser` | 165 passed (two earlier full runs each had a different unrelated test time out under dev-server load; those tests pass alone and the final full run is clean) |
| `pytest tests/unit/shared tests/unit/tooling` | 391 passed (catalog register 535 rows) |
