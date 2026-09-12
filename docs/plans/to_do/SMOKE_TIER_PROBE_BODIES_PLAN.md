---
id: smoke-tier-probe-bodies
branch: fix/smoke-tier-probe-bodies
depends_on: []
parallel_safe: true
complexity: low
verify:
  - ./tests/run.ps1 web-unit
  - ./tests/run.ps1 web-smoke
---

# The smoke tier exits clean

## Plan status

- **Status:** To do. Filed 2026-09-12 from the catalog-grain-vocabulary
  work, which ran the live-stack smoke tier and found its process exiting
  non-zero with every test passing.
- **Last updated:** 2026-09-12
- **Owner surface:** `apps/web/lib/tiles.js`,
  `tests/frontend/smoke/live-stack.smoke.test.js`, `apps/web/vitest.smoke.config.mjs`
- **Depends on:** nothing open.

## The defect

Running `SMOKE_BASE_URL=http://localhost:3001 npm --prefix apps/web run test:smoke`
against the development stack on 2026-09-12: six of six tests pass, and
Vitest reports three unhandled errors and exits non-zero:

```
AssertionError: The expression evaluated to a falsy value:
  assert(!this.paused)
 ❯ Parser.finish node:internal/deps/undici/undici:7388:9
 ❯ Socket.onHttpSocketEnd node:internal/deps/undici/undici:7827:34
```

raised "while `a real tile decodes to features carrying the join key` was
running". Four such errors appeared on the first run of the day, before any
change to the tier or the stack, so this is not a regression of anything
merged today.

The assertion is inside Node's bundled undici and fires when an HTTP socket
ends while the response parser is paused — the shape produced by a `fetch`
whose body is never read or cancelled. `discoverTileMetadata` probes
candidate tile templates and TileJSON URLs, deciding on status and
`content-type` and moving on; `loadPreviewTileFeatures` may do the same for
a tile it rejects. In a browser an abandoned body is the browser's problem;
in Node 24 it is this.

The consequence is the exact failure this tier was built to end: **a red
process with green tests is indistinguishable, in CI, from a red process
with red tests.** `frontend-smoke` cannot be trusted as a gate until the
exit code means what it says.

## Objective

The smoke tier's process exit code is the tests' verdict and nothing else.

## Scope

- Every probe in `tiles.js` consumes or cancels the body of a response it
  does not use (`await response.arrayBuffer()` for a small probe, or
  `response.body?.cancel()`), so no socket ends with the parser paused.
- The smoke tier asserts on itself: an `afterAll` (or a Vitest `onUnhandledError`
  hook) that fails the run if any unhandled error was recorded, so the
  condition is a test failure with a name rather than a footnote after a
  green summary. `--dangerouslyIgnoreUnhandledErrors` is the wrong fix and
  is not an option.
- A unit test over `tiles.js` with a fake `fetch` that records whether each
  response's body was consumed or cancelled, so the hygiene does not depend
  on the smoke tier being run.

## Acceptance

- `npm --prefix apps/web run test:smoke` against a live stack exits 0 with
  zero unhandled errors.
- A deliberately abandoned body in a probe fails the new unit test.
- `TESTING_CONTRACT.md` WEB-027's pass metric names the clean exit.

## Non-goals

Changing what discovery probes or how it chooses a layer; the tile-catalog
shapes WEB-026 covers.
