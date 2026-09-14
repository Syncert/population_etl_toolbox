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

- **Status:** Accepted 2026-09-14 (Ready for review. Claimed and delivered 2026-09-12. The tier's exit code is now its tests' verdict on the origin it declares, the condition has a named test in every smoke file, and the runner composes that origin itself (`./tests/run.ps1 web-smoke`).)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/lib/tiles.js`,
  `tests/frontend/smoke/live-stack.smoke.test.js`,
  `tests/frontend/smoke/map-wiring.smoke.test.js`,
  `tests/frontend/smoke/unhandledErrors.js`,
  `apps/web/vitest.smoke.config.mjs`, `tests/run.ps1`
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

The consequence is the exact failure this tier was built to end: **a red
process with green tests is indistinguishable, in CI, from a red process
with red tests.** `frontend-smoke` cannot be trusted as a gate until the exit
code means what it says.

## What the cause turned out to be

This plan was filed on the hypothesis that an abandoned response body was the
cause: `discoverTileMetadata` probes candidate tile templates and decides on
status and `content-type`, and a `fetch` whose body is never read leaves the
parser paused when the socket ends. **Measurement refuted that as the cause of
the observed failure**, and the correction matters more than the hypothesis
did.

One whole-world tile (`/tiles/counties/0/0/0`, 864,711 bytes) fetched six
times per cell, Node 24.20.0, 2026-09-12:

| origin | body read fully | body cancelled | body abandoned |
| --- | --- | --- | --- |
| `next dev` rewrite origin (`localhost:3001`, `connection: close`) | **6 asserts** | 0 | 0 |
| composed nginx proxy (`127.0.0.1:33001`, keep-alive) | 0 | 0 | 0 |

The trigger is the origin closing the socket under a large response, not the
client's handling of the body — and reading it through `arrayBuffer()`,
through `body`'s async iterator, and through a reader all assert equally
(four errors each, same script). A small JSON body over the same closing
origin never asserts. The failing request was therefore the one that reads the
most bytes, which is exactly the tile read the original report named.

`next dev`'s rewrite origin answers `connection: close` on every response; the
composed proxy answers `keep-alive`. The tier's origin is part of its
contract, not a convenience, and the runner now composes it.

The body hygiene in `tiles.js` is kept on its own merits — an unread body
holds its connection until the response is collected, and discovery probes up
to six whole-world tiles per candidate layer before it draws anything — but it
is documented as hygiene, not as the fix.

## Objective

The smoke tier's process exit code is the tests' verdict and nothing else.

## Delivery

| Item | Where | Evidence |
| --- | --- | --- |
| Every probe releases the body it does not read | `apps/web/lib/tiles.js` (`releaseBody`, four call sites) | Rejected discovery endpoints, rejected TileJSON, the content-type-only tile sample, and a rejected preview tile are cancelled; the two bodies discovery does read are consumed untouched |
| The hygiene is pinned without a live stack | `tests/frontend/unit/tile-discovery.test.js` | Fake `fetch` records `body.cancel()` per probe; a bodiless response (a 304, a HEAD) is not an error |
| The tier asserts on itself, in every file | `tests/frontend/smoke/unhandledErrors.js`, called last in both smoke files | `uncaughtException` and `unhandledRejection` recorded from module load, reported as a named failing test |
| The tier declares its origin | `apps/web/vitest.smoke.config.mjs`, `tests/run.ps1` (`web-smoke`) | `./tests/run.ps1 web-smoke` composes Postgres, Martin, the real API, and the proxy, runs against `127.0.0.1:33001` with `SMOKE_REQUIRED=1`, and tears the stack down |
| The tier grades the working tree | `tests/run.ps1` (`web-smoke`, `up --build`) | See "A stale image graded as a client defect" below |
| The pass metric names the clean exit | `docs/reference/TESTING_CONTRACT.md` WEB-026, WEB-027 | WEB-027 requires the run's exit code to be the tests' verdict; WEB-026 requires released probe bodies |

### Why the guard lives in every file rather than once

Declared in one file it guards one file. Run on its own against the closing
origin, `map-wiring.smoke.test.js` reported **two tests passed, one unhandled
error, exit 1, and no failure anywhere in the report** — the exact condition
this plan exists to end, in the one file that had no guard.
`--dangerouslyIgnoreUnhandledErrors` is the opposite fix: it keeps the green
summary and drops the red exit code.

### A stale image graded as a client defect

The first full run of `./tests/run.ps1 web-smoke` failed two WEB-027 tests:
`CENSUS_ACS:acs5:B01003_001_SMOKE` and `…_MARTIN_TEST` each answered 0 rows
from `/observations`, and no observed geography reached the tile layer. The
rows were in `gold_census.mv_acs_latest`, and the registry in the working tree
selects them with `metric_code_column="metric_code"`. The running container did
not: `population-etl-api:smoke` had been built four days earlier and still
carried `lineage_key_column="metric_code"` with `lineage_key_prefix="ACS:"` —
the pre-ARC-005 identity — so it queried
`metric_code = 'ACS:acs5:B01003_001_SMOKE'` and matched nothing.

Compose reuses an image by name, so the tier was grading four-day-old code. CI
builds the image on every run and was never affected. `web-smoke` now passes
`--build`, and the failures did not reproduce afterwards.

**Corrected 2026-09-12, after this plan merged.** An earlier revision claimed
this also answered the open question `WEB_ANALYTICS_FIRST_WAVE_PLAN.md` was
held in `in_progress/` for. It does not: that plan's two WEB-027 failures were
recorded against the development stack, where they were four retired LAUCN
series demanded to answer and a national series joined against a county
boundary. They are explained and closed in that plan's WEB-034 delivery
record, and the code fix reached `main` in `a74c43f`. Two failures on the
composed fixture stack, two on the development stack, one stale image and one
contract misreading — the same two test names, and nothing else in common.

## Acceptance

- [x] `npm --prefix apps/web run test:smoke` against a live stack exits 0 with
      zero unhandled errors.
- [x] A deliberately abandoned body in a probe fails the new unit test.
- [x] `TESTING_CONTRACT.md` WEB-027's pass metric names the clean exit.

## Validation

| Check | Command | Result |
| --- | --- | --- |
| Web unit tier | `./tests/run.ps1 web-unit` | 20 files, 220 tests passed |
| Live-stack tier, composed origin | `$env:TEST_POSTGRES_HOST_PORT="55532"; ./tests/run.ps1 web-smoke` | 2 files, **10 tests passed, exit 0**, stack torn down; both unhandled-error guards green |
| Live-stack tier, dev origin | `SMOKE_BASE_URL=http://localhost:3001 npm --prefix apps/web run test:smoke` | 1 failed of 10 — the guard names the closing origin's unhandled error instead of leaving it to the exit code |
| The unit test catches the regression | `releaseBody(sampleTileResponse)` commented out, focused vitest run | `a rejected discovery endpoint and every tile-sample probe are released` failed; restored, 12 passed |
| Lint | `npm --prefix apps/web run lint` | clean (`--max-warnings=0`) |
| Runner parses | `[PSParser]::Tokenize` over `tests/run.ps1` | parsed |

`TEST_POSTGRES_HOST_PORT=55532` is this host's workaround, not the tier's:
Windows reserves 55384–55483, which contains the compose default 55432. CI
uses the default.

## Non-goals

Changing what discovery probes or how it chooses a layer; the tile-catalog
shapes WEB-026 covers. Making the tier pass against an origin that answers
`connection: close` — the undici assertion there is not the client's to fix,
and the guard reporting it is the correct outcome.
