---
id: the-browser-tier-grades-the-build
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# The browser tier grades the build it just made, not a server compiling on demand

## Plan status

- **Status:** Implemented; awaiting review. Authored 2026-09-13 by the
  assessment agent; claimed and completed 2026-09-13. It was a present
  defect: the `frontend` job was flaky on this branch because of it.
  Register row **WEB-068** (WEB-062, suggested at authoring time, had been
  taken).
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/playwright.config.mjs`,
  `.github/workflows/frontend.yml`

## Context

`frontend.yml` runs `npm run build`, `check:bundle`, and `check:csp`, then
`npm run test:browser`. The Playwright `webServer` is:

```js
command: "node ./node_modules/next/dist/bin/next dev -p 3100",
```

So the tier that was just graded on its production build starts a
**development** server and tests that instead. A dev server compiles each
route the first time a browser asks for it, and the suite's `expect`
timeout is ten seconds.

## Findings

- On `706d658` the job failed
  (<https://github.com/Syncert/population_etl_toolbox/actions/runs/34756794906>):

  ```text
  articles.spec.js:83 › a stored composition this build cannot read is not
  reported as nothing composed
  Expected: "unreadable"   Received: "loading"
  24 × locator resolved to <main data-state="loading" ...>
  ```

  The page never hydrated within the window. The next push, `f0a74f8`,
  changed nothing on that route and passed. Neither the spec nor
  `ComposedArticle.tsx` has a race: the effect that reads storage sets
  `loaded` synchronously once it runs. What varies between runs is how long
  the dev server takes to compile `/articles` on first hit under a loaded
  runner.
- The same log carries dozens of `Failed to proxy http://localhost:8000/...
  ECONNREFUSED` lines: the dev server's rewrite forwards every request a
  spec did not route to an API that is not there. That is noise today and
  a cost per request; it is also a sign the tier's server is not the one
  a deployment runs.
- `check:csp` proves "the CSP nonce survives the production build", and
  `csp-nonce.spec.js` then exercises the nonce against a dev server whose
  middleware and headers are not that build's.
- Locally, `reuseExistingServer: !process.env.CI` already makes the config
  behave differently in CI; the CI branch is the one to change.

## Acceptance criteria

1. In CI the browser tier serves the output of the build the job already
   made (`next start` on `.next`, or equivalent), not `next dev`. The local
   developer path is unchanged.
2. The flaky spec is left as it is. A retry or a longer timeout is not the
   fix; the fix is a server that does not compile during the assertion.
3. A static guard reads `playwright.config.mjs` and fails when the CI
   command names `next dev` (beside the existing route-bundle and CSP
   checks, or in `tests/frontend/unit`).
4. The `frontend` job's displayed name, `CI_EVIDENCE_MAP.md`, and
   `TESTING_CONTRACT.md` WEB-007/WEB-008 say what the tier now grades.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `WEB-`
   identifier; WEB-062 at authoring time).

## Non-goals

- Serving a real API to the browser tier. The specs route their own
  responses by design; the live-stack tier is `frontend-smoke`.
- Changing the smoke tier's server.

## What changed

- `playwright.config.mjs`'s `webServer.command` is `next start -p 3100`
  under `process.env.CI` and `next dev -p 3100` otherwise, with the flake
  and its reasoning recorded beside it. `package.json` already had the
  `start` script on the same port.
- `tests/frontend/unit/browser-tier-server.test.js` (new) reads the config
  and fails if the CI half names `next dev`, if the served port and the
  suite's `baseURL` disagree, or if the workflow browses before it builds.
  Read from the file because the symptom of a revert is an occasional red
  job months later rather than a failing test now.
- `TESTING_CONTRACT.md`'s WEB-007 and WEB-008 rows and
  `CI_EVIDENCE_MAP.md`'s browser row say what the tier grades. The job's
  displayed name already reads "Frontend lint, typecheck, unit, build, and
  browser", which is now accurate rather than aspirational — the build is
  the artifact the browser step serves.

## Validation

- `npm --prefix apps/web run test:unit` — **354 passed** (351 before: +3).
- `npm run build` then **`CI=1 npx playwright test`** — **84 passed in
  54.6s**, against the production build. The same suite takes 2.1 minutes
  against `next dev`, so serving the build is both the right artifact and
  less than half the wall-clock: the difference is the per-route compile
  this plan is about.
- `pytest tests/unit` — 1520 passed, the register guards included.
- The flaky spec is untouched, as criterion 2 requires. No retry and no
  timeout change.

## Remaining work

- None. Review is the remaining step.
