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

- **Status:** Complete on `claude/plans-folder-iteration-4x6itr`, awaiting
  review. All three deliverables landed; `/explore` and `/compare` each drop
  about 990 kB of first-load JavaScript.
- **Last updated:** 2026-09-17
- **Current milestone:** none; every tier is green.

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

- [x] `/explore` and `/compare` first-load JavaScript drops below the new
      budgets, and the budget check fails if `maplibre-gl` returns to a
      static import (proven by reverting once).
- [~] With WebGL unavailable, no MapLibre chunk is requested, asserted in the
      browser tier from the served-request log. **Met against the gate the
      code actually has.** Nothing in `apps/web` probes WebGL: the map is
      withheld when the tile boundary publishes no geometry for the selected
      grain (`lib/viewModes.ts`, WEB-029), and that is the condition the test
      drives. The claim proven is the one that matters -- a selection with no
      map fetches no map -- and it is read from the requests the page made,
      not from the log helper, which records *parameter* complaints rather
      than a request list.
- [~] A public `GET` is sent with `cache: "default"`, a token-bearing request
      with `no-store`, asserted in the unit tier; the browser tier asserts a
      repeated catalog page in one navigation reaches the server once.
      **First half met in both tiers; second half is a standing guard.** No
      route in this application mounts two screens that read the same catalog
      page, so the browser assertion is not currently load-bearing -- and it
      is marked as such in the spec rather than left to look like a proof.
      The de-duplication itself is proven in the unit tier, where two
      overlapping reads are arranged deterministically and the guard was
      verified to fail without it. See "What this tier cannot see" below.
- [x] Every existing browser spec passes against the dynamic map, including
      the keyboard and CSP specs: 150 passed, 0 failed.
- [x] `TESTING_CONTRACT.md` WEB-009 is rewritten and WEB-113 added for the
      bundle split.

## Implementation evidence

### What it cost before, and now

Measured by `scripts/check-bundle-budget.mjs` on a production build, before
and after, with nothing else changed:

| Route | Before | After | Budget |
|---|---|---|---|
| `/compare/page` | 1452.9 kB | 459.0 kB | 1635 -> 528 kB |
| `/explore/page` | 1471.1 kB | 477.7 kB | 1648 -> 550 kB |

Every other route sits between 342 kB and 474 kB, so the two map routes were
paying roughly a megabyte each and now sit among the rest. The new budgets
are the file's own formula -- measured x 1.15, rounded to a kilobyte -- not a
number chosen to pass.

### Where the split actually is

`useMapLibre` reaches the library through `import()` inside its effect. That
is what removes the bytes: the hook is the only value import of `maplibre-gl`
in the application, and `SourceExplorerPage` had the other one, which is now
types and a plain corner pair in place of `LngLatBounds`.

The comparison map additionally loads through `next/dynamic` with
`ssr: false`, which the plan asks for. The explorer's map panel does **not**:
it is ~120 lines welded into a 2,400-line client component by a dozen effects
over the page's own state, and extracting it is a refactor of the platform's
main screen with real regression risk -- for no further bytes, because the
hook's `import()` has already moved all of them. The reverting proof shows
the asymmetry plainly: restoring the static import inside the hook pushes
`/explore` to 1511 kB and fails the budget, while `/compare` stays at 459 kB,
because `next/dynamic` keeps it out of that route's first load either way.
The two mechanisms guard different routes, and both are guarded.

### The cache mode

`requestCacheMode` decides by the kind of request rather than by the caller:
a `GET` carrying neither a token nor a body is a public read and is sent
`default`, so the browser applies the `Cache-Control: public, max-age=<ttl>`
the API already answers with. Everything else keeps `no-store` -- a
token-bearing read is somebody's own library, served `private, no-store`, and
a write has nothing to reuse. The TTL stays the API's to decide; this client
only stopped refusing it.

### The de-duplication

An in-flight map in `client.ts`, keyed by the built path, page size, page
bound and transport identity. It holds promises and deletes on settle, so it
caches nothing: two reads that overlap in time share one request, and a read
a minute later makes its own. Three kinds are excluded for stated reasons --
anything outside `/catalog/`, anything carrying an `AbortSignal` (one
caller's cancellation would reject another caller's promise), and anything
carrying a token.

### What this tier cannot see

Two honest gaps, both recorded rather than papered over:

1. **Cache reuse is not observable in the browser tier.** Every response
   there is fulfilled by `page.route`, and an intercepted response is never
   written to Chromium's HTTP cache. Measured directly: the same
   `/catalog/geographies?...geo_level=STATE...` URL, answered with
   `public, max-age=300`, reached the handler on both of two navigations. So
   the tier asserts what the client *sends* -- the cache mode, read by
   patching `window.fetch` before the app loads -- and the reuse itself is
   the browser's own documented behaviour given those headers.
2. **No page duplicates a catalog read today.** A probe across `/explore`,
   `/compare`, `/profiles`, `/workbench`, `/quality`, `/catalog` and `/`,
   with de-duplication disabled, found no repeated catalog URL in any single
   navigation: each route mounts one screen, and `/profiles`' two geography
   reads use different `geo_level` values. The plan's premise -- four screens
   reading the catalog independently -- is true across navigations, where the
   HTTP cache is what helps, not within one. The de-duplication is kept as
   the cheap safeguard it is, proven in the unit tier and labelled in the
   browser tier as a standing guard.

### A fifth provider fact, found in passing

The map tooltip told a reader that "ACS1 publishes county estimates only for
areas meeting its population threshold" -- the same Census rule WEB-112
deleted from three other places, worded without the number its grep looked
for. It is deleted, and `provider-facts.test.js` gains a pattern that matches
the *claim* rather than the literal. The WEB-112 plan records it too; a grep
for a number is only as good as the wording someone happened to use.

### Commands

| Command | Result |
|---|---|
| `npm --prefix apps/web run test:unit` | 603 passed, 40 files (was 595) |
| `npm --prefix apps/web run test:browser` | 150 passed, 0 failed (4.6m) |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web run build` + `check:bundle` | every route within budget |
| `npm --prefix apps/web run check:csp` | passed |
| `python -m pytest tests/unit -q` | 1807 passed |

Each new guard was verified to fail without the change it guards: the budget
check against a restored static import (+961 kB on `/explore`), the browser
map test against the same (no new chunk on mount), the browser cache-mode
test against `no-store`, and the unit de-duplication test against a disabled
key.

## Definition of done

A reader who never opens the map never downloads it, and a reader who opens
three screens on the same catalog downloads it once.

## What this plan deliberately does not do

- It does not add a client-side data store or cache responses in memory
  beyond the in-flight promise map.
- It does not change the API's cache headers or TTLs.
- It does not extract the explorer's map panel into its own component. The
  bytes are already out; what remains would be a refactor of the platform's
  main screen for tidiness, and it belongs to a plan that says so.
