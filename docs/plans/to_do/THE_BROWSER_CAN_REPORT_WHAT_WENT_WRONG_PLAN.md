---
id: client-error-and-vitals-reporting
branch: claude/client-error-and-vitals-reporting
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build ; npm --prefix apps/web run check:csp
  - npm --prefix apps/web run test:browser
---

# The browser can report what went wrong

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit. The
  first deliverable is a choice of sink; the recommended one needs no API
  change.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

`docs/plans/completed/DEPLOYMENT_OBSERVABILITY_PLAN.md` says plainly that
"the frontend cannot be debugged" and delivered server-side smoke tiers in
response. Nothing since has given the browser a voice: `useReportWebVitals`,
`window.onerror`, `unhandledrejection` and `securitypolicyviolation` appear
nowhere under `apps/web/{app,components,lib}`, and `apps/web/middleware.ts`
sets no `report-to` or `report-uri` on the Content-Security-Policy. The CSP
browser spec proves zero violations in CI on a build CI just made; a
violation in production, a chunk blocked under the nonce policy, a MapLibre
WebGL failure or a hydration mismatch produces no signal anywhere.

The CSP fixes the sink: `connect-src 'self'` (`middleware.ts`), so a report
must go to the same origin. `next.config.mjs` rewrites `/api/*` and
`/tiles/*` to the API and Martin, so a Next route handler must sit
elsewhere.

## Deliverables

### 1. The sink

Recommended: a Next route handler at a path outside the rewritten prefixes
(for example `/_report`) that validates a small, closed report shape, writes
one structured line to the web container's stdout, and answers `204`. It
holds no state. If the reviewer prefers the API as the sink, that is an
upstream contract addition with its own route, catalog row and snapshot
change, and it lands first.

### 2. Three reporters, one payload discipline

`useReportWebVitals` in the layout; a `window.onerror`/`unhandledrejection`
hook registered once; `report-to` (and `report-uri` for older agents) on
the CSP pointing at the sink. Every payload carries route path (not query),
metric or error name, message, and the app's build id, and never a token,
a query string, a value, a saved-analysis name or an id.

### 3. The deployment reads it

The Compose smoke job asserts that the web container log contains one
vitals line after the browser tier's first navigation, so the path is
proven on every push.

## Acceptance criteria

- [ ] A unit test asserts the payload builder strips query strings and
      refuses any field outside the closed shape.
- [ ] A browser test triggers a synthetic error and a synthetic CSP
      violation and asserts one request each to the sink from the
      served-request log, with the payload shape.
- [ ] `check:csp` passes with the `report-to` directive present.
- [ ] The Compose smoke job sees the vitals line in the web container log.
- [ ] `TESTING_CONTRACT.md` gains `WEB-` and `DEPLOY-` rows.

## Definition of done

A production regression a reader sees is a line an operator can read, and
the line carries nothing the privacy boundary keeps off the wire.

## What this plan deliberately does not do

- It does not add a third-party error service or any external endpoint; the
  CSP forbids it and the plan does not widen the CSP.
- It does not sample, batch or persist reports; stdout is the store.
