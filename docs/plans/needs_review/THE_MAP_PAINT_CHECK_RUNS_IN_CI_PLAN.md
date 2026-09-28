---
id: map-paint-check-in-ci
branch: codex/analytics-backlog-2026-09-28
depends_on:
  - every-map-proves-it-displays-its-data
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - ./tests/run.ps1 web-smoke
---

# The map paint check runs in CI

## Plan status

- **Status:** Ready for review on `codex/analytics-backlog-2026-09-28`.
- **Last updated:** 2026-09-28
- **Dependencies:** `every-map-proves-it-displays-its-data` (in
  `needs_review/`), which built the tier.
- **Next pickup:** Human review. The workflow is committed locally; the first
  remote GitHub Actions result requires a push, which this request did not
  authorize.

## Why

The painted-pixel tier (`tests/frontend/live/map-paint.live.spec.js`,
`npm run test:maps`, WEB-118) is the only check that sees a map's pixels. It
catches what no other tier can: a tile worker served from the wrong origin
(the empty-map bug fixed in `bb39f46`), a join key the tiles do not carry,
or a paint expression MapLibre refuses. Today it runs only by hand, against a
developer's local stack (`./tests/run.ps1 web-maps`), so a regression in any of
those reaches `main` unseen.

## What is already there

`.github/workflows/frontend-smoke.yml` already composes
`postgres martin api proxy web` from `docker-compose.test.yml` +
`docker-compose.smoke.yml`, seeds the warehouse, and serves the web container
at `WEB_SMOKE_URL=http://127.0.0.1:33100` on one origin with `/api/v1` and
`/tiles`. It also installs Chromium for the vitals step. The paint tier needs
exactly that.

## The design problem to settle first

The paint tier takes its subjects from the reviewed matrix
(`tests/fixtures/api/viz_coverage.json`, `advertised_geo_grains`), which lists
STATE and COUNTY for most sources. The seeded smoke warehouse holds only
county rows. The map sweep's `web-smoke` run on 2026-09-23 read one coloured
COUNTY map per seeded source and nothing at STATE. So the tier, unchanged,
would fail every STATE test in CI with "the catalog lists no STATE metric".

Pick one of the following and record why:

- **(a)** In CI, take subjects from what the deployment's catalog advertises
  (`/catalog/metrics` `valid_geo_grains`), not from the matrix, and assert
  separately that every source the matrix names has at least one painted
  grain. This keeps the tier honest about the deployment in front of it.
- **(b)** Seed one STATE row per source in the smoke fixtures so the matrix
  is paintable there too. This is stronger, but it costs fixture work in
  seven sources.

## Work items

- [x] **MPC-1: choose (a) or (b)** with a failing-first run of the tier
  against the smoke stack that shows the current failure.
- [x] **MPC-2: add the step** to `frontend-smoke.yml` after the vitals step:
  `SMOKE_BASE_URL=${WEB_SMOKE_URL} npm run test:maps`. Chromium must run
  with the SwiftShader flags `playwright.live.config.mjs` already sets, and
  the job timeout (25 min) must be re-measured.
- [x] **MPC-3: prove the step fails on a broken map.** Break the tile worker
  origin (revert `bb39f46` locally) or the join key, show the CI step red,
  and restore it.
- [x] **MPC-4: register it.** Record the job in `CI_EVIDENCE_MAP.md` against
  WEB-118, and update the WEB-118 row in `TESTING_CONTRACT.md` to name the CI
  owner.

## Acceptance criteria

1. Every push runs the paint tier against a composed stack.
2. A map that paints nothing fails that job by name.
3. The tier cannot pass by skipping (`SMOKE_REQUIRED=1`).
4. The CI evidence map and the testing contract name the job.

## Decision and implementation evidence

Chose **(a)**. Before the change, the composed smoke stack failed the ACS
`STATE` pixel test because its active catalog published no state metric. The
test now derives grains from that catalog and fails when a reviewed spatial
source has no drawable grain. The reviewed matrix's FRED entry is national
only, so it has no polygon map and is excluded explicitly. Six spatial
sources remain required. The NASS smoke row had a county `geo_id` but null
`state_fips`/`county_fips`; the fixture now supplies its authoritative `55` and
`025`, allowing a state-scoped browser view to fit that small county.

The live check selects a state present in the candidate's rows and masks
legend and MapLibre controls when capturing pixels. Without the mask, an empty
map passed on its own legend swatch. With it, a temporary broken
`buildChoroplethMatchExpression` join key made `CENSUS_ACS COUNTY` fail at
**0 painted pixels**, while the page still reported a coloured row. The join
key was restored, and the healthy six-source sweep passed. With
`SMOKE_REQUIRED=1` and no base URL, the tier failed by name rather than
skipping. The earlier attempted broken worker URL did not remove these
GeoJSON-backed polygons and was not used as passing regression evidence.

The `frontend-smoke` workflow now runs `test:maps` after browser vitals with
the web container's origin. Its pull-request path filter covers the live test,
subject helper, and reviewed matrix. `tests/run.ps1 web-smoke` composes the
web container and runs the same tier locally. The paint sweep took **25.7 s**
on the composed stack, well within the existing 25-minute job timeout.

## Validation

- `npm run test:unit`: **703 passed**; `npm run lint`: passed.
- `python -u -m pytest tests/unit/tooling/test_plan_environments.py
  tests/unit/shared/test_ci_evidence_manifest.py -q --tb=short --maxfail=1`:
  **12 passed**; `tests/unit/shared/test_catalog_evidence.py`: **2 passed**.
- `./tests/run.ps1 web-smoke`: **27 smoke tests and 6 map-paint tests passed**;
  the composed API and web images built, and teardown completed.
- `SMOKE_REQUIRED=1` with no `SMOKE_BASE_URL`: the named ACS test failed as
  expected. The temporary bad join-key build failed the named ACS pixel test
  at 0%; the healthy build passed it after restoration.
- `git -c core.whitespace=cr-at-eol diff --check`: passed.
- After the plan move, `tests/unit/tooling/test_plan_environments.py` and
  `test_plan_dispatcher_graph.py`: **23 passed**; `ruff check .` and workflow
  YAML parsing passed.
- GitHub Actions on the remote branch has **not run**: no push was requested.
