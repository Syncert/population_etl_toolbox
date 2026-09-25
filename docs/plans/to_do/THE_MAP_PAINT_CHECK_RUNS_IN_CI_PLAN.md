---
id: map-paint-check-in-ci
branch: claude/map-paint-check-in-ci
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

- **Status:** Unclaimed.
- **Last updated:** 2026-09-24
- **Dependencies:** `every-map-proves-it-displays-its-data` (in
  `needs_review/`), which built the tier.
- **Next pickup:** MPC-1.

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

- [ ] **MPC-1: choose (a) or (b)** with a failing-first run of the tier
  against the smoke stack that shows the current failure.
- [ ] **MPC-2: add the step** to `frontend-smoke.yml` after the vitals step:
  `SMOKE_BASE_URL=${WEB_SMOKE_URL} npm run test:maps`. Chromium must run
  with the SwiftShader flags `playwright.live.config.mjs` already sets, and
  the job timeout (25 min) must be re-measured.
- [ ] **MPC-3: prove the step fails on a broken map.** Break the tile worker
  origin (revert `bb39f46` locally) or the join key, show the CI step red,
  and restore it.
- [ ] **MPC-4: register it.** Record the job in `CI_EVIDENCE_MAP.md` against
  WEB-118, and update the WEB-118 row in `TESTING_CONTRACT.md` to name the CI
  owner.

## Acceptance criteria

1. Every push runs the paint tier against a composed stack.
2. A map that paints nothing fails that job by name.
3. The tier cannot pass by skipping (`SMOKE_REQUIRED=1`).
4. The CI evidence map and the testing contract name the job.
