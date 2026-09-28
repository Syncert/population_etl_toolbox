---
id: browser-tier-cold-dev-server
branch: codex/analytics-backlog-2026-09-28
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:browser
---

# The browser tier holds under a cold dev server

## Plan status

- **Status:** Ready for review on `codex/analytics-backlog-2026-09-28`.
- **Last updated:** 2026-09-28
- **Decision:** Build before each local browser run and serve the production build with `next start`, as CI does. Keep the ten-second assertion timeout.

## Why

On 2026-09-23 two consecutive full runs of `npm run test:browser` on the
Windows development host each failed one test, and a different one each time:

- `explorer.spec.js` "as-released exploration pins a published release" and
  `profiles.spec.js` "the community profile reads a place", in one run;
- `data-quality.spec.js` "per-metric quality shows the publisher's own
  fields" and `evidence-packet-account.spec.js` "an opened account packet
  carries the API's verdict", in the next.

Each timed out on a `toHaveAttribute` at the 10 s expect timeout while the web
server logged `ECONNRESET` / `aborted`. All four passed when run alone, and a
third full run passed 165/165. Locally `playwright.config.mjs` starts
`next dev`, which compiles each route on first request; CI starts
`next start` against a build. So the local tier is exposed to compile time
that CI is not, and a red local run does not mean what it should.

"Flake" is not a root cause. This plan finds the one.

## Work items

- [x] **BT-1: reproduce and attribute.** Run the full tier five times under
  `next dev` and five under `next start` (build first), recording which tests
  fail and the server log around each failure. Show whether the failures are
  first-compile latency of the route under test.
- [x] **BT-2: fix the cause, not the timeout.** Likely either run the local
  tier against `next start` like CI (a build step, one command), or warm each
  route before its first test. Raising the expect timeout is not a fix: it
  hides a compile stall and a real hang alike.
- [x] **BT-3: evidence.** Ten consecutive clean local runs, and the
  `ECONNRESET` noise gone from the log or explained.

## Acceptance criteria

1. The local browser tier passes ten consecutive runs without a retry.
2. Local and CI run the web server the same way, or the difference is
   documented with its reason.
3. No expect timeout is raised to get there.

## Implementation and validation

- The plan already records two failing local `next dev` runs from 2026-09-23. On 2026-09-28, three more unchanged-tree dev runs produced 168/169 (`page.goto: net::ERR_ABORTED` on the community profile), 168/169 (navigation did not complete on the unknown-route test), and 169/169. All three logged `ECONNRESET`/aborted server errors. The failures moved between first navigations to different routes and each failed test passed when isolated; the development server's cold/concurrent route work is the distinguishing condition, though the logs do not expose an individual compile duration.
- Five full `CI=1` production-server runs on the same build passed 169/169 (the first before the plan was claimed, followed by four measured runs of 39–43 seconds).
- `pretest:browser` now builds on local runs; Playwright always starts `next start` and refuses to reuse a running server. `WEB-068` static evidence and the testing/CI reference now state this shared mode.
- The formerly failing community profile test passed through the new local command. Ten consecutive full local `npm --prefix apps/web run test:browser` runs each passed 169/169 in 58–70 seconds including build, with **zero** `ECONNRESET` or `ERR_ABORTED` log signatures across all ten. No retry or assertion timeout increase was used.
- `npm --prefix apps/web run test:unit -- browser-tier-server.test.js`: 3 passed; `npm --prefix apps/web run lint`: passed; `python -u -m pytest tests/unit/tooling/test_plan_environments.py -q`: 5 passed.
