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

- **Status:** To do. Investigated and authored 2026-09-13. **Present defect:
  the `frontend` job is flaky on this branch because of it.**
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

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
