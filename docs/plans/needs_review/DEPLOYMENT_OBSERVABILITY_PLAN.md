---
id: deployment-observability
branch: claude/web-viz-metrics-checks-qttkzz
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit
  - python -m pytest -o addopts='' tests/integration/api -m "integration and not external"
  - npm --prefix apps/web run test:unit
  - ruff format --check . ; ruff check .
---

# Deployment observability: content-aware health and a job that watches it

## Plan status

- **Status:** Ready for review. Both deliverables are implemented, validated,
  and documented. One operator action is required before the scheduled job can
  be green: setting `DEPLOYMENT_SMOKE_BASE_URL` (see *Operator setup* below).
- **Last updated:** 2026-09-14
- **Current milestone:** delivered.

## Why

The question this plan answers was asked directly: *are there robust checks in
place to ensure that metrics are being served properly from the API?*

The audit's answer was yes for the code and no for the deployment. The
repository already carries unusually strong evidence — the per-source
end-to-end tier with exact expected values (`tests/e2e`), the catalog/serving
agreement sweeps built after Census ACS published 4,447 codes the serving
layer had never heard of (`tests/integration/api/test_catalog_serving_agreement.py`),
and a live-stack smoke tier that runs the explorer's own modules unmocked
against a real API, Martin, and proxy (`tests/frontend/smoke/live-stack.smoke.test.js`).

Every one of them grades a stack CI built seconds earlier from seeded
fixtures. None of them can see the deployment, and the deployment has no
signal of its own that would notice the failure that matters most:

- `/health/ready` runs `SELECT 1` (`apps/api/routers/health.py`). An API
  pointed at an empty or half-loaded warehouse reports itself **ready**,
  answers `total: 0` for every metric, and draws a blank chart on every
  screen, with no error anywhere in the stack.
- Nothing polls the deployed origin. There is no scheduled synthetic and no
  monitoring configuration under `infra/`.

So a deployment could be entirely broken for readers while sixteen required
CI jobs stayed green. Closing that is a precondition for the stated goal of
making the web platform the dominant focus: the frontend cannot be debugged
against a backend whose content state is unobservable.

## Deliverables

### 1. `GET /api/v1/health/content` (API-137)

A content-aware resource reporting, per source the observation registry
declares, whether the warehouse publishes a measure a client could ask for.

- `apps/api/services/content_health.py` — one grouped scan of
  `gold_glossary.dim_metric_catalog` left-joined to
  `gold_glossary.publisher_harvest_state`, and the pure grading rule over it.
- `apps/api/schemas/health.py` — `SourceContent`, `ContentHealthResponse`.
- `apps/api/routers/health.py` — the route, on a **separate** `content_router`.
- `apps/api/services/metric_freshness.py` — the freshness vocabulary
  (`FRESHNESS_CURRENT`, `FRESHNESS_STALE`, `PUBLISHED_FRESHNESS_STATES`) read
  from one place rather than re-spelled in the query.

Decisions worth reviewing:

- **`metrics_current` alone decides a source's word.** BLS carries 13,261
  retired codes against 63 active ones; a rule reading "the catalog has rows"
  would call a source whose every measure has retired healthy. That is exactly
  the reading that makes an empty screen look like a working deployment.
- **A separate router, not another route on `health.router`.**
  `apps/api/ratelimit.py` derives its exempt paths from every route the health
  routers serve (API-101, because the web app calls health on each page load).
  Inheriting that exemption would publish an unauthenticated, unmetered
  grouped scan. The content resource is a warehouse read and is metered,
  declared (`422/429/503`), and uncached as one.
- **Always `200`, including `empty`.** A report you cannot read in the state
  you need it is not a report.
- **Readiness deliberately unchanged.** An empty warehouse is a content
  problem, not an unservable process; failing readiness on it would take the
  API out of the load balancer for a condition no restart can fix, turning a
  blank dashboard into a total outage.
- **Read from the catalog, not the fact tables.** One grouped scan of a
  modest relation is cheap enough to poll; `COUNT(*)` over seven gold fact
  tables is not. The catalog is also what decides what a client can ask for,
  so an observation it does not publish is unreachable however many rows sit
  behind it.
- **Three states, not a boolean.** `empty` (never loaded) and `degraded`
  (stopped part-way) differ in what an operator does about them.

### 2. `live-deployment-smoke` (ENV-019) and its tier assertions (WEB-102)

- `.github/workflows/live-deployment-smoke.yml` — daily schedule and manual
  dispatch, running the **existing** live-stack smoke tier against the real
  origin. No new tier: that tier already walks the live catalog, asks each
  active metric through the access shape the explorer picks, and checks that
  observed geographies intersect the tile layer. Pointed at a deployment it
  becomes the check the audit recommended first.
- `tests/frontend/smoke/content-health.smoke.test.js` — reads
  `/health/content` from the deployed origin and cross-checks it against
  `/catalog/metrics?active_only=true`, which migration 003 defines as the same
  predicate (`freshness_state = 'current'`), so the agreement check is an
  equality rather than an approximation.
- `tests/support/deployment_smoke.py` — validates the target before the tier
  starts, so a misconfigured origin fails as a configuration error rather than
  as a connection error indistinguishable from the outage the job reports.

Decisions worth reviewing:

- **No push or pull-request trigger.** A red run means the deployment needs
  attention, not that a branch is broken. A job no branch can fix must never
  block a merge.
- **Fails loudly while unconfigured.** A scheduled observer that skips when
  `SMOKE_BASE_URL` is unset reports the same green as one that checked a
  healthy deployment — the failure mode `SMOKE_REQUIRED` was added to the tier
  to end (WEB-027). The failure message says exactly what to set, and says to
  disable the workflow if there is no deployment to watch yet.
- **`runs-on` is configurable** (`DEPLOYMENT_SMOKE_RUNNER`). This repository's
  deployment path is `scripts/deploy_stack.ps1` over
  `infra/docker/docker-compose.yml`, which usually is not reachable from a
  GitHub-hosted runner. A job hard-wired to `ubuntu-latest` would be
  permanently red for the deployment shape the repository actually ships.
- **`SMOKE_REQUIRE_ALL_SOURCES` is opt-outable, not opt-in.** It defaults to
  `1`, mirroring `E2E_REQUIRE_ALL_PRODUCTS`. It is a setting rather than a
  bare assertion because the tier runs against two stacks: this job's real
  deployment, and `frontend-smoke`'s Compose stack. Deliverable 3 below made
  the second one satisfy it too, so both now run with the bound on.

### 3. One measure per source in the live-stack seed (WEB-027)

`tests/sql/frontend_smoke_seed.sql` published a single ACS measure for a
single county, so `frontend-smoke`'s "every active catalog metric answers"
loop iterated one metric of one source — a check over a seventh of the surface
its own summary line names. Six of seven sources could have stopped answering
entirely with that tier green.

The seed now publishes one measure per registered source: Census ACS, FRED,
BLS, Census PEP, CDC, FBI UCR and USDA NASS. `frontend-smoke` runs with
`SMOKE_REQUIRE_ALL_SOURCES=1`, so the breadth is now enforced rather than
merely present.

Decisions worth reviewing:

- **No catalog row is written by hand.** Every one is selected from that
  source's own `gold_<source>.metric_publisher` view, composed exactly as
  `glossary/harvest.py` composes it (`source_code || ':' || source_object_key`).
  A hand-written catalog row carries a hand-written `physical_lineage`, and
  that is the field the neutral resource resolves serving rows through: a
  fixture spelling it independently keeps testing whatever shape was true the
  day it was written, which is the drift ARC-005 and DB-034 both ended. The
  seed writes the data and lets the publisher say what it means — so a source
  whose publisher stops yielding a row publishes no catalog row, and the tier
  reports a source with no metrics instead of passing on an invented lineage.
- **No measure may declare the `STATE` grain.** `spatialGrains` returns
  `['STATE', 'COUNTY']` and the tile-join test picks the first source
  publishing at the first of those grains. The Martin seed draws one county
  polygon and no state, so a STATE measure would be chosen, decode zero
  features, and fail a geography test for a reason purely about this seed.
  Every county-grain measure uses that county's `geo_id` for the same reason.
  Both rules are stated at the top of the file, because neither is visible
  from the insert that would violate them.
- **The silver chains are seeded, not the gold views.** CDC, FBI UCR, USDA
  NASS and Census PEP serve through views over silver, so the seed writes
  silver rows and lets the real gold contract project them — including FBI's
  participation row, which its fact table's foreign key requires, and PEP's
  observation revision, which its as-released surface reads.

## Operator setup

The scheduled job is red until one repository variable is set:

| Variable | Required | Meaning |
| --- | --- | --- |
| `DEPLOYMENT_SMOKE_BASE_URL` | Yes | The deployment origin serving `/api/v1` and `/tiles` through the proxy a browser uses. An origin, no path. |
| `DEPLOYMENT_SMOKE_RUNNER` | If private | A self-hosted runner label that can reach the deployment. |
| `DEPLOYMENT_SMOKE_REQUIRE_ALL_SOURCES` | No | `0` to accept a partially loaded deployment. Defaults to `1`. |

A variable rather than a secret: an origin is not a credential, and a masked
value would render every failure message as `***`.

If there is no deployment to watch yet, disable the workflow rather than
leaving it unconfigured.

## Acceptance criteria

- [x] A served resource reports per-source content and names silent sources.
- [x] It is metered, uncached, and answers `200` in every content state.
- [x] Readiness behaviour is unchanged.
- [x] A scheduled job runs the live-stack tier against a deployed origin and
      cannot pass by skipping.
- [x] The deployed content report is checked against the deployed catalog.
- [x] The live-stack seed publishes one measure per registered source, with
      every catalog row derived from that source's own publisher view, and
      `frontend-smoke` grades against all of them.
- [x] `TESTING_CONTRACT.md`, `CI_EVIDENCE_MAP.md`, `API_CONSUMER_GUIDE.md`,
      the CI evidence manifest, and the reviewed OpenAPI snapshot are updated
      together with the implementation.

## Validation evidence (2026-09-14)

Run on Linux with Python 3.11, a local PostgreSQL 16 + PostGIS 3.4 cluster,
and Redis 7.

| Check | Command | Result |
| --- | --- | --- |
| Unit tier | `pytest tests/unit` | **1730 passed** |
| API integration tier | `pytest -o addopts='' tests/integration/api -m "integration and not external"` | **80 passed** |
| Content report, unit | `pytest tests/unit/api/test_content_health.py` | **9 passed** |
| Content report, integration | `pytest -o addopts='' tests/integration/api/test_content_health_contract.py` | **4 passed** |
| Scheduled job contract | `pytest tests/unit/deployment/test_live_deployment_smoke.py` | **7 passed** |
| Frontend unit | `npm --prefix apps/web run test:unit` | **528 passed (34 files)** |
| Frontend lint / typecheck | `npm --prefix apps/web run lint` / `run typecheck` | **Passed** |
| Ruff | `ruff format --check .` and `ruff check .` | **Passed** |
| OpenAPI snapshot | `python -m tests.support.regenerate_openapi_contract` | Additive only: one operation, two schemas (42 operations, 69 schemas) |
| Widened seed applies | the compose stack's 45 initdb files, in order, then the seed | **0 failures**; applying the seed a second time changes no row count, so it is idempotent |
| Widened seed publishes | `SELECT … FROM gold_glossary.dim_metric_catalog` | **8 metrics across all 7 registered sources**, every `physical_lineage` publisher-derived, no measure declaring `STATE` |
| Every seeded metric answers | the tier's own loop (`buildExplorerSources` + `buildLatestObservationRequest` + `apiFetch`) against a live API on that stack | **8 of 8 answered `total=1`**, up from 2; every source resolved to the neutral `/observations` shape |
| Served geographies join the boundary | `/observations` for each county-grain measure | every one serves `state:55\|county:025`, the one polygon the Martin seed draws |
| All-sources bound | `SMOKE_REQUIRE_ALL_SOURCES=1 npm run test:smoke` (content-health file) | **5 passed**, including the bound that was skipped before the seed grew |

Mutation check, to establish the tests fail for the right reason: changing the
grading rule from `current > 0` to `total > 0` turned
`test_a_source_whose_measures_have_all_retired_is_reported_empty` red
(`assert 'serving' == 'empty'`) and one unit test red; restoring it returned
both to green.

End-to-end demonstration against a live process (uvicorn on a real
bootstrapped warehouse):

```
GET /health/ready          -> {"status":"ready","database":"ok","cache":"disabled"}
GET /api/v1/health/content -> {"status":"empty","silent_sources":[all seven]}
```

That is the gap this plan closes, shown on one running API. The smoke tier
then failed with the intended message, named all seven silent sources, and
went green after one measure was seeded — with `SMOKE_REQUIRE_ALL_SOURCES=1`
still failing and naming the six sources that remained silent.

## Checks not run in this environment

- `frontend-smoke` and `deployment-smoke` Compose tiers: no container runtime
  is available in this session. The content-health smoke file was instead run
  against a live uvicorn process on a real bootstrapped warehouse, in both
  grading modes, with the results above.
- Playwright browser tier: not exercised; no browser-tier file changed.
- `live-deployment-smoke` itself cannot run until an operator sets
  `DEPLOYMENT_SMOKE_BASE_URL`. Its structure, ordering, and refusal behaviour
  are covered by `tests/unit/deployment/test_live_deployment_smoke.py`.
- Local PostGIS is 3.4; CI pins 3.5. Nothing here touches geometry.
- One caveat found while validating the widened seed, recorded because it
  looks alarming and is not: running `tests/integration/api` against a
  database that also carries the compose stack's smoke seed fails two
  agreement sweeps. The two are not designed to share a database — the smoke
  seed is mounted only by `docker-compose.test.yml` (so `frontend-smoke`,
  `martin-integration`, `deployment-smoke`), while `api-integration`
  bootstraps its own schema through `bootstrapped_postgres` and never sees it
  — and the suite's CDC and NASS fixtures run a real `harvest_publisher`,
  which retires catalog rows their cleanup then removes the silver rows for.
  On a CI-shaped database the tier passes 80/80, and the seeded codes answer
  `total=1` to the sweep's own query when the sweep's fixtures have not run.
  Combining the two locally is what produced the failure, not the seed.

## Follow-on work this plan deliberately does not do

1. **The catalog/serving sweeps are as wide as their fixtures.**
   `test_every_registered_source_answers_each_current_catalog_code` loops the
   registered sources but does `if not codes: continue`, so sources without a
   seed fixture in that suite are silently skipped. Per-source coverage does
   exist in `tests/e2e`; the cross-cutting sweep is narrower than it reads.
2. **Staleness is not alerted on.** `freshness_state` distinguishes `stale`
   from `current`, and the content report now counts both, but nothing fails
   when a source drifts to stale. The counts are the input a future check
   would need.
