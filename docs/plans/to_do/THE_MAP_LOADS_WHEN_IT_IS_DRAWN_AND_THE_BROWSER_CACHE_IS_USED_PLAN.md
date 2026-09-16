---
id: map-bundle-and-browser-cache
branch: claude/map-bundle-and-browser-cache
depends_on: []
parallel_safe: false
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# The map loads when it is drawn, and the browser cache is used

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

Two routes cost four times the others and every request bypasses a cache
the API spent two plans making honest.

**MapLibre is statically bundled.** `apps/web/components/useMapLibre.ts` and
`components/SourceExplorerPage.tsx` import `maplibre-gl` at module top; no
file under `apps/web` uses `next/dynamic` or a dynamic `import()`.
`scripts/bundle-budgets.json` records `/explore` at 1.69 MB and `/compare`
at 1.67 MB against roughly 0.4–0.5 MB for every other route. The map is
unmounted when WebGL is unavailable (`useMapLibre.ts`), but its bytes still
ship and parse, and the explorer's default tab is the map, so it is a
first-paint cost on the platform's main screen.

**The client forces `cache: "no-store"`.** `apps/web/lib/api/client.ts:216`
sends every request with `cache: "no-store"`. The API answers
`Cache-Control: public, max-age=<ttl>` on every public analytical `GET`
(`apps/api/middleware.py`; `response-cache-coverage` and
`a-failure-is-never-publicly-cacheable`), and the CSP nonce forces dynamic
rendering (`middleware.ts`), so the browser HTTP cache is the only cheap
layer left, and it is switched off. Full catalog reads of 3,144 geographies
are repeated independently by the explorer, the workbench, the comparison
workspace and the profile with no in-flight de-duplication in `client.ts`.

`TESTING_CONTRACT.md` WEB-009 pins `no-store` ("Requests are constructed
against `/api/v1` with `no-store`"), so the second change is a documented
contract change, recorded there, not a quiet edit.

## Deliverables

### 1. The map is a dynamic import

`ChoroplethMap` and the explorer's map panel load through `next/dynamic`
with `ssr: false` and a stated loading element that keeps the status row
and the table alternative visible. `bundle-budgets.json` is lowered for
`/explore` and `/compare` to the measured post-change size, so a regression
back to static import fails the budget check.

### 2. Public reads use the browser cache

`apiFetch` sends `cache: "default"` for a public `GET` (no token, no body)
and keeps `no-store` for every token-bearing or writing request. WEB-009 is
rewritten to say so, and `tests/frontend/unit/api-client.test.js` asserts
the mode by request kind.

### 3. In-flight de-duplication for catalog pages

A small in-flight map in `client.ts` keyed by path and query for
`fetchAllPages` of `/catalog/*`, so two screens mounting in one navigation
share one request. It holds promises, not responses, and clears on settle.

## Acceptance criteria

- [ ] `/explore` and `/compare` first-load JavaScript drops below the new
      budgets, and the budget check fails if `maplibre-gl` returns to a
      static import (proven by reverting once).
- [ ] With WebGL unavailable, no MapLibre chunk is requested, asserted in
      the browser tier from the served-request log
      (`tests/frontend/support/servedRequests.js`).
- [ ] A public `GET` is sent with `cache: "default"`, a token-bearing request
      with `no-store`, asserted in the unit tier; the browser tier asserts a
      repeated catalog page in one navigation reaches the server once.
- [ ] Every existing browser spec passes against the dynamic map, including
      the keyboard and CSP specs.
- [ ] `TESTING_CONTRACT.md` WEB-009 is rewritten and a `WEB-` row added for
      the bundle split.

## Definition of done

A reader who never opens the map never downloads it, and a reader who opens
three screens on the same catalog downloads it once.

## What this plan deliberately does not do

- It does not add a client-side data store or cache responses in memory
  beyond the in-flight promise map.
- It does not change the API's cache headers or TTLs.
