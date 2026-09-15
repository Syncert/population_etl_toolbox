---
id: web-visualization-coverage
branch: claude/analytics-api-data-coverage-29xn1n
depends_on:
  - deployment-observability
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit
  - python -m pytest -o addopts='' tests/integration/api -m "integration and not external"
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - ruff format --check . ; ruff check .
---

# Every web visualization, every source, and a reviewed answer for each cell

## Plan status

- **Status:** Accepted. The matrix, both offline halves, the live sweep and
  the three defects the audit found are implemented and validated. Delivered
  as `27bf583` on `claude/analytics-api-data-coverage-29xn1n` and merged to
  `main` in `765f1c0` (PR #59); accepted by the repository owner on
  2026-09-15.
- **Last updated:** 2026-09-15
- **Current milestone:** complete.

## Why

The question this plan answers was asked directly: *robust testing that
guarantees and warns when the API is or is not serving something that will
ultimately break web analytics viz — all web viz should be serving all
available types of data from all of the available data sources.*

The audit found the repository's coverage evidence to be unusually strong and
to run entirely along **one axis**: does the warehouse publish what the
catalog advertises, and does the API serve what the catalog publishes.
`tests/integration/api/test_catalog_serving_agreement.py` walks that
exhaustively, `/health/content` reports it per source, DB-044 proves the
fixture corpus reaches every grain each pipeline can publish, and the
live-stack smoke tier replays it against a deployment.

None of it asks the question a reader's blank screen answers. A source can
publish current measures, answer `/observations` with rows, satisfy every
agreement sweep and report `serving` — and still have nothing to draw in the
explorer's map, the workbench's heatmap, the comparison scatter or the
distribution histogram. The presentations rest on **capabilities**, not on
rows: a route that must be declared, a parameter the source must actually
accept, a grain the tile boundary must be able to draw. When one of those
goes away the screen goes blank and nothing anywhere in the stack reports an
error.

Three real defects were sitting in that gap, and the matrix found all three
before any of them was looked for.

### Finding 1 — two served routes were declared for nobody

`/comparison/correlation` and `/comparison/matrix` are served, tested, and
documented. Neither appeared in `ANALYSIS_NEUTRAL_PATHS`, so neither appeared
in any source's `observation_routes`, so a client discovering the API could
not learn they exist. The workbench calls both, and gated them on
`/comparison/preflight` being declared — an inference about one route standing
in for a contract about another. Restricting either route for one source would
have left the panel offered and the answer empty.

The browser tier had already written the contract the API does not serve:
`tests/frontend/browser/workbench.spec.js` declares both paths in its
capability fixture. WEB-043 checks a fixture's *parameters* against the served
OpenAPI document; nothing checked that a capability fixture's *routes* are
routes the capability resource would publish.

### Finding 2 — three screens sent a reduction three sources refuse

`newest_per_geography` and `newest_release_per_period` are declared by
`/observations` for every source, because a route declares one parameter set.
Whether a *source* reduces to one value per geography is a different fact, and
`reduction_refusal` answers it with a 422 naming the strata a reduction would
collapse (API-118).

The client read only the parameter. `supportsNewestPerGeography` and
`supportsSettledHistory` were both true for all seven sources, so the
explorer's settled trend, the workbench's cross-sectional scatter and its
geography-by-period heatmap each sent a request the API had already said it
would not answer — for CDC, FBI UCR and USDA NASS. Nine cells. The live sweep
printed all nine as `EMPTY` the first time it ran, against a real warehouse.

### Finding 3 — FRED has no map, and nothing said so

`gold_fred.fact_fred_observation` writes `'NATIONAL'` as a literal: FRED is
national by construction, and the boundary draws no national polygon. The
explorer already declines a map for a non-drawable grain per *metric*
(`viewModes`), which is correct, but nothing stated the source-level fact or
tested it, so "FRED is never mappable" and "this FRED measure happens to have
no county rows" were indistinguishable — and only the second is a content
problem someone could fix.

## Deliverables

### 1. The reviewed visualization matrix (API-138)

`tests/support/viz_coverage.py` declares **fifteen presentations** a reader
can select, each with the page it lives on, the web modules that build its
requests, and the exact capabilities it needs:

| Surface | Page |
| --- | --- |
| `explorer.map` | `/explore` |
| `explorer.trend` | `/explore` |
| `explorer.table` | `/explore` |
| `explorer.export` | `/explore` |
| `explorer.quality` | `/explore`, `/quality` |
| `explorer.distribution` | `/explore` |
| `explorer.as_released` | `/explore` |
| `explorer.settled_history` | `/explore` |
| `comparison.workspace` | `/compare` |
| `workbench.series` | `/workbench` |
| `workbench.cross_section` | `/workbench` |
| `workbench.heatmap` | `/workbench` |
| `workbench.correlation` | `/workbench` |
| `workbench.matrix` | `/workbench` |
| `profiles.product` | `/profiles` |

Evaluated against the capability payload `/catalog/capabilities` actually
serves — built by the same function, from the same served OpenAPI document —
each of the 15 × 7 cells is either served or carries a reviewed decline.

Current coverage: **83 of 105 cells served, 22 declined**, and every decline
rests on one of three named policies.

Decisions worth reviewing:

- **`REVIEWED_DECLINES` is written out, not derived.** A derived table agrees
  with whatever the registry currently says, so flipping `analysis_ready` on a
  source would move the cell and its expectation together and report green —
  the shape of the silent skip DB-043 closed in the catalog sweeps. Written
  out, the same flip fails naming the source and the four screens it empties.
  The table is then checked *against* the registry in both directions, so it
  cannot record a policy the API does not hold.
- **Three policies, not a list of exceptions.** The aligned analysis routes,
  the per-geography reduction, and a source with no drawable grain. A surface
  in none of them has no way to serve six of seven sources and still pass —
  which is what makes "all web viz, all sources" a bound rather than a hope.
- **The drawable grains are parsed from `apps/web/lib/tileGrains.ts`.** That
  file's own comment records that three places used to answer the question and
  did not agree; a fourth copy here would restart it.
- **`ADVERTISED_GEO_GRAINS` moved to `tests/support/source_grains.py`.** Two
  tiers now ask what a source's pipeline can publish — DB-044 and this matrix
  — and two copies would be two answers to the first question a map asks.
- **A snapshot, because two languages cannot import one declaration.**
  `tests/fixtures/api/viz_coverage.json`, regenerated with
  `python -m tests.support.regenerate_viz_coverage`, exactly as the reviewed
  OpenAPI snapshot is.

### 2. The two correlation routes are declared (API-138)

`ANALYSIS_NEUTRAL_PATHS` now carries `/comparison/correlation` and
`/comparison/matrix`. Both refuse an incomparable pair with the rules
`/comparison` refuses it by, and both reduce each side to one value per
geography — which is the reduction `analysis_ready` governs — so they belong
beside the routes they were already declined alongside.

`ExplorerSource` gains `servesCorrelation` and `servesMatrix`, read from the
declarations; `correlationEligibility` takes a `declaredRoutes` map and
refuses a route the capability entry does not publish, naming it. A source
with no entry declares neither: an unknown capability is not a capability.

### 3. A source says whether it reduces (API-139)

`publishes_aligned_reduction` on `/catalog/capabilities` and
`/catalog/metrics/{metric_code}`, derived from the dispatch entry's
`analysis_ready` — the same declaration `reduction_refusal` is read from, so
the published capability and the served behaviour cannot disagree.

`supportsNewestPerGeography` and `supportsSettledHistory` now require both
halves: the route's parameter *and* the source's own declaration. The three
screens that send a reduction stop sending it to the three sources that refuse
it, and say so from the capability rather than from a 422.

Additive to the OpenAPI contract: one field on each of two schemas.

### 4. The web half of the matrix, and the fixture guard (WEB-103)

`tests/frontend/unit/viz-coverage.test.js` runs `buildExplorerSources` over
the reviewed capability payload and grades every surface's real offer
predicate — `describeViewModes`, `servesHistory`, `servesAsReleased`,
`buildSettledHistoryRequest`, `buildSettledSurfaceRequest`,
`correlationEligibility`, the `serves*` flags — against the matrix, cell for
cell, in both directions. Each request the client builds must also carry every
parameter its surface needs.

A mismatch is a blank screen either way, and the failure message says which:

```
BLS: the API serves the workbench's correlation matrix and the web does not
offer it, so a reader is told data is missing that is published
```

Decisions worth reviewing:

- **Every surface must have a web predicate.** Without that assertion, a
  surface added to the matrix would simply not be graded here and the web half
  would silently stop covering it.
- **Nothing is mocked.** The one shared artifact is the snapshot.
- **A fixture cannot claim a reduction the API refuses.** WEB-043 checks a
  fixture's route *parameters* against the served OpenAPI document, which is
  exactly why `workbench.spec.js` could declare two routes no source
  published and why a fixture could claim CDC reduces. The guard in
  `served-contract-fixtures.test.js` now reads every frontend fixture's
  `publishes_aligned_reduction` against the matrix: `true` where the API says
  false watches a screen send a request that 422s, and `false` where it says
  true hides a working presentation.

### 5. The live sweep, and the coverage table (WEB-104)

`tests/frontend/smoke/viz-coverage.smoke.test.js` asks the deployment for
every cell the matrix says is served, using the request the application's own
module builds, and prints a table on every run:

```
visualization coverage against http://127.0.0.1:38000
  explorer.map BLS: draws
  ...
  explorer.settled_history FRED: EMPTY
47 probes, 3 empty, 0 not exercised
```

Decisions worth reviewing:

- **A report first, a bound second.** The question an operator asks is "what
  can this deployment actually show", and the answer is a table, not a
  pass/fail they infer.
- **`EMPTY` and `NOT EXERCISED` are different facts and bound separately.** A
  request that was made and answered nothing means the API declared a screen
  servable and it is not — always a defect, bound by `SMOKE_REQUIRE_ALL_VIZ`,
  on by default for a deployment. A cell that could not be probed at all means
  the warehouse holds no current measure at a grain that screen needs — a
  content state an operator may be mid-way through, bound separately by
  `SMOKE_REQUIRE_FULL_VIZ_SAMPLE`, opt-in.
- **Two things are asserted bound or not,** because neither depends on how
  much the warehouse holds: the deployed capability payload must declare what
  this checkout's matrix says it declares (so a deployment on an older API is
  named rather than quietly grading fewer screens), and the sweep must not be
  vacuous. The first caught a stale process during this plan's own validation,
  which is the best evidence it works.
- **`frontend-smoke` sets both bounds.** Validated: the seeded stack answers
  47 of 47 probes with 0 empty and 0 unexercised.
- **The geography a probe asks a history of is read, not composed.** A
  `geo_id` this file spelled would be this file's idea of the deployment's
  coverage, and a trend answering nothing would then mean "the seed does not
  hold that county" rather than "this screen is empty".

## Acceptance criteria

- [x] Every presentation the web offers is declared with the routes,
      parameters, filters, dimensions, reduction and grain it needs.
- [x] Every source × presentation cell is served or carries a reviewed
      decline, asserted in both directions.
- [x] Every declared route exists in the served OpenAPI document and accepts
      every parameter its surface sends.
- [x] Every decline rests on a named policy bound to the registry declaration
      that decides it.
- [x] Every presentation no policy restricts reaches all seven sources.
- [x] The client offers exactly the cells the API declares, and its requests
      carry what each surface needs.
- [x] A live deployment is asked for every served cell, prints the coverage
      table, and fails an empty answer under a bound CI sets.
- [x] The two correlation routes are declared, and the client reads the
      declaration instead of inferring it.
- [x] A source publishes whether it reduces to one value per geography, and
      the three screens that send a reduction read it.
- [x] `TESTING_CONTRACT.md`, `CI_EVIDENCE_MAP.md`, `API_CONSUMER_GUIDE.md`,
      the catalog evidence counts, and both reviewed snapshots are updated
      with the implementation.

## Validation evidence (2026-09-15)

Run on Linux with Python 3.11, a local PostgreSQL 16 + PostGIS 3.4 cluster
bootstrapped from the compose stack's own 45 initdb files in order, plus
`tests/sql/frontend_smoke_seed.sql`, and a live `uvicorn` process against it.

| Check | Command | Result |
| --- | --- | --- |
| Unit tier | `pytest tests/unit` | **1744 passed** |
| Matrix, unit | `pytest tests/unit/api/test_viz_coverage.py` | **10 passed** |
| API integration tier | `pytest -o addopts='' tests/integration/api -m "integration and not external"` | **79 passed, 4 skipped** |
| Database integration tier | `pytest -o addopts='' tests/integration/database -m "integration and not external and not e2e and not redis and not martin"` | **164 passed, 1 skipped** |
| Frontend unit | `npm --prefix apps/web run test:unit` | **557 passed (35 files)** |
| Matrix, frontend | the `viz-coverage` file alone | **25 passed** |
| Browser tier | `CI=1 PLAYWRIGHT_CHROMIUM_EXECUTABLE=… npm --prefix apps/web run test:browser` | **114 passed** |
| Frontend lint / typecheck / build | `npm --prefix apps/web run lint` / `run typecheck` / `run build` | **Passed** |
| Ruff | `ruff format --check .` and `ruff check .` | **Passed** |
| OpenAPI snapshot | `python -m tests.support.regenerate_openapi_contract` | Additive only: one field on each of `SourceCapability` and `MetricCapability` (42 operations, 69 schemas, unchanged) |
| Live sweep, clean | `SMOKE_BASE_URL=… SMOKE_REQUIRE_ALL_VIZ=1 SMOKE_REQUIRE_FULL_VIZ_SAMPLE=1 npx vitest run --config vitest.smoke.config.mjs viz-coverage` | **5 passed; 47 probes, 0 empty, 0 not exercised** |

Mutation checks, to establish that each guard fails for the right reason and
names the right thing:

| Mutation | Result |
| --- | --- |
| `/comparison/matrix` removed from `ANALYSIS_NEUTRAL_PATHS` | `test_a_declining_cell_is_one_the_registry_reviewed` fails naming all four sources and the screen; `test_every_surface_serves_at_least_one_source` fails naming `workbench.matrix` |
| `analysis_ready` flipped on one dispatch entry | `test_an_analysis_decline_is_the_registrys_own_refusal` fails: *"the reviewed table declines [...] and the dispatch registry declines [...]; an aligned analysis is offered or refused per source, not per screen"* |
| `NATIONAL` added to `DRAWABLE_TILE_GRAINS` | the reviewed snapshot fails, naming the file to regenerate and why not to |
| `servesMatrix` hard-coded false in the client | the frontend half fails four times: *"BLS: the API serves the workbench's correlation matrix and the web does not offer it, so a reader is told data is missing that is published"* |
| `gold_fred.rpt_fred_observations` emptied on the live warehouse | the live sweep prints three `EMPTY` cells and fails under the bound, naming the surface, the source and the exact request: `/observations {"metric_code":"FRED:SMOKE_UNRATE","scope":"as_released","limit":5,"geo_level":"NATIONAL"}` |
| the API restarted on pre-API-139 code | the drift assertion fails naming twelve flag disagreements between the deployment and this checkout |

The nine-cell reduction defect (Finding 2) is recorded as found rather than
asserted: the live sweep's **first** run, before `publishes_aligned_reduction`
existed, printed `explorer.settled_history`, `workbench.cross_section` and
`workbench.heatmap` as `EMPTY` for CDC, FBI UCR and USDA NASS. The 422 body
was the dispatch entry's own analysis refusal.

## Checks not run in this environment

- `frontend-smoke` and `deployment-smoke` as Compose tiers: no container
  runtime is available in this session. The visualization sweep was instead
  run against a live `uvicorn` process on a warehouse built from the same 45
  initdb files that job's Postgres mounts, plus the same smoke seed, in both
  bound modes and in the deliberately-broken mode above.
- Nothing: the browser tier ran here against the production build, using the
  sandbox's pre-installed Chromium through the config's own
  `PLAYWRIGHT_CHROMIUM_EXECUTABLE` seam.
- `live-deployment-smoke` cannot run until an operator sets
  `DEPLOYMENT_SMOKE_BASE_URL`, unchanged from the deployment-observability
  plan.
- Local PostGIS is 3.4; CI pins 3.5. Nothing here touches geometry.

## Follow-on work this plan deliberately does not do

- **A capability fixture's *routes* are still unchecked against the capability
  resource.** WEB-043 checks a fixture's parameters against the served OpenAPI
  document, which is why `workbench.spec.js` could declare two routes no
  source published. The matrix catches the class at the source level, and the
  new guard catches `publishes_aligned_reduction` at the fixture; checking
  every fixture's *route list* against `/catalog/capabilities`' own output per
  source would close the rest of it.
- **The workbench's analysis-refusal sentence is still its own.** The API
  publishes that sentence in four served responses and the registry holds it
  once; the client writes a fifth wording because no capability field carries
  it. Publishing the refusal on the capability entry would end that, and is a
  larger change than this plan's scope.
- **`explorer.quality` and `explorer.export` are graded by declaration only.**
  Both are reachable for every source by construction — a catalog read and a
  declared dimension list — so a live probe would prove nothing the offline
  tiers do not.
