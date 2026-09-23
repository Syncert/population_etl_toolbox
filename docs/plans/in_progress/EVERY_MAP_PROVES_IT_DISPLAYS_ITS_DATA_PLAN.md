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

- **Status:** Claimed and **in progress**. The validation (both tiers) is
  built and running, and the explorer defects it found in the client are
  fixed. Two defects it found upstream of the client -- in the USDA NASS
  warehouse and API contract -- are recorded below with evidence and are not
  fixed here; until they are, the data-path sweep fails on USDA NASS by
  design. The plan stays in `in_progress/` for that reason.
- **Last updated:** 2026-09-23
- **Dependencies:** none.
- **Next pickup:** NASS-1 and NASS-2 below -- a decision on each is needed
  before the sweep can pass on USDA NASS.

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

**Out of scope:** changing the USDA NASS warehouse geography or API contract
(NASS-1, NASS-2 below). Those are warehouse/API contract changes and, per
`AGENTS.md`, are planned upstream first rather than patched in the client.

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
- [ ] **NASS-1 (warehouse, not fixed here): county `998` is not a county.**
  USDA NASS county code `998` means "other (combined) counties" and repeats
  once per agricultural district. The warehouse publishes those rows as
  `state:SS|county:998` without the district, so, for example,
  `USDA_NASS:soybeans_survey_annual:3e38e231…` has nine different 1990 values
  for `state:05|county:998` (Arkansas's nine districts), identical in every
  published dimension. No real county carries that `geo_id`, so the map never
  paints them, but the rows are published at the COUNTY grain under a
  colliding identity -- the `AGENTS.md` invariant "use authoritative geography
  codes". The sweep fails 24 county maps on it. **Decision needed:** publish
  them at an agricultural-district geography, or withhold them from the
  county grain; either is a warehouse change with re-ingestion.
- [ ] **NASS-2 (API contract, not fixed here): no way to choose the final
  value.** After MAP-5 the NASS forecast maps decline honestly, but a reader
  cannot select `reference_period_desc`, because the capability does not
  declare it as a filter. **Decision needed:** declare it as a neutral
  filter, or make the reference period part of the metric identity so a
  forecast and a final value are different metrics.
- [x] **MAP-6: CI placement.** `test:smoke` includes every `*.smoke.test.js`,
  so the sweep also runs in the composed `web-smoke` stack against seeded
  fixtures. Confirm it passes there (see Evidence) or scope it.

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
6. The whole `verify` block passes, except the sweep's USDA NASS failures that
   NASS-1/NASS-2 own.

## Evidence record

All runs on 2026-09-23 against the local development stack
(`http://localhost:3001`) unless noted.

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
