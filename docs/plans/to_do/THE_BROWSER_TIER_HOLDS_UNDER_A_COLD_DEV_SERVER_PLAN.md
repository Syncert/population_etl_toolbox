---
id: browser-tier-cold-dev-server
branch: claude/browser-tier-cold-dev-server
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:browser
---

# The browser tier holds under a cold dev server

## Plan status

- **Status:** Unclaimed.
- **Last updated:** 2026-09-24
- **Next pickup:** BT-1.

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

- [ ] **BT-1: reproduce and attribute.** Run the full tier five times under
  `next dev` and five under `next start` (build first), recording which tests
  fail and the server log around each failure. Show whether the failures are
  first-compile latency of the route under test.
- [ ] **BT-2: fix the cause, not the timeout.** Likely either run the local
  tier against `next start` like CI (a build step, one command), or warm each
  route before its first test. Raising the expect timeout is not a fix: it
  hides a compile stall and a real hang alike.
- [ ] **BT-3: evidence.** Ten consecutive clean local runs, and the
  `ECONNRESET` noise gone from the log or explained.

## Acceptance criteria

1. The local browser tier passes ten consecutive runs without a retry.
2. Local and CI run the web server the same way, or the difference is
   documented with its reason.
3. No expect timeout is raised to get there.
