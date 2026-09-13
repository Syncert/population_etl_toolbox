---
id: the-published-contract-declares-its-failures
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api/test_openapi_contract.py tests/unit/api/test_consumer_guide.py -q
---

# The published OpenAPI contract declares the failures the guide promises

## Plan status

- **Status:** Needs review. Implemented 2026-09-13 as catalog row API-121.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/` route declarations,
  `tests/fixtures/api/openapi_contract.json`,
  `tests/unit/api/test_openapi_contract.py`

## Context

The guide opens: "Everything here is served by the checked-in application
and pinned by the reviewed OpenAPI snapshot ... so a change to anything
below appears in review as a snapshot diff." Its Errors table promises
401, 404, 409, 413, 422, 429 and 503, and "Every error body is
`{"detail": "..."}`" -- a string.

The reviewed snapshot declares, across all 39 operations, exactly the
statuses `200`, `201`, `204`, `422`. The one error schema is FastAPI's
`HTTPValidationError` with `detail: array`. Every failure the API raises by
hand sends a string `detail`: the 404 in `routers/observations.py:157`, the
strict-parameter 422 in `dependencies.py:92-98`, the 503 at `:24`, the 429
in `ratelimit.py:252`, the 413 in `middleware.py:207`, the 409 in
`routers/saved_analysis.py:150-157`, the 401 in `auth.py:66-71`.

## Findings

- A client generated from `/openapi.json` types `422.detail` as an array
  and has no branch for 404 or 429. The guide's Errors table can change
  without a snapshot diff, which is the opposite of what the guide says.
- Nothing compares the guide's Errors table against the served document.

## Acceptance criteria

1. Every route declares the failure statuses it can answer, with one shared
   error schema whose `detail` is a string, in the served document and the
   snapshot.
2. `test_consumer_guide.py` reads the Errors table and asserts each status
   it names is declared by at least one route, and that the declared error
   schema is the string form.
3. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `API-`
   identifier; API-115 at authoring time).

## Non-goals

- Changing any status or body. This plan makes the document say what the
  API already does.

## Validation

- `apps/api/schemas/errors.py` declares `ErrorDetail` (`detail: str`) plus
  `HTTPValidationError`/`ValidationError` under the names and with the exact
  fields FastAPI generated, so the snapshot's schema entries for the latter
  two are byte-identical to before. They are declared rather than inherited
  because a route that declares its own 422 gets no generated default, and
  every route now declares one.
- `apps/api/failures.py` is the one statement of what each group can refuse
  with. Each description is imported from where the refusal is raised
  (`SERVICE_UNAVAILABLE_DETAIL`, `REQUEST_TOO_LARGE_DETAIL`,
  `RATE_LIMITED_DETAIL`), so a reworded refusal cannot leave the contract
  describing the old one.
- Declarations follow the application, not a blanket:
  - application-wide 422, because the strict-parameter dependency (API-093)
    is applied to every route and refuses an undeclared parameter before any
    of them runs;
  - `WAREHOUSE_READ_FAILURES` (422, 429, 503) for the public analytical
    routers, `PRIVATE_STORE_FAILURES` (401, 422, 429, 503) for the two
    user-owned stores, and 422 alone for the versioned health resource, which
    reads nothing and is exempt from the limiter;
  - per route: 404 where an identifier is resolved (the metric capability,
    `/observations`, `/observations/releases`, both comparison routes,
    `/distribution/bins`, and every by-id route of both private stores), 409
    where a name is held unique or a version is written against, 413 where a
    body is parsed;
  - `/health/ready` declares both of its 503 bodies: its own readiness
    report, and the sanitized refusal when no session could be opened.
- Three guards keep the declarations true, and none of them is a list beside
  the code:
  - `test_every_status_a_router_raises_is_declared_by_it` reads each router
    module's own source with `ast`, collects every status handed to
    `HTTPException`, and asserts the module's served operations declare it.
    It walks the routers the application factory mounts rather than
    `app.routes`, because this FastAPI version wraps an included router in an
    opaque object and walking the app finds only its documentation routes.
  - `test_shared_failures_are_declared_where_they_apply` asserts the
    middleware's and dependencies' failures are declared for exactly the
    routes they reach: 422 everywhere, 401 iff the path is a private store,
    429 iff the limiter meters it, 413 iff the method accepts a body.
  - `test_every_declared_failure_carries_the_body_it_answers` asserts every
    4xx/5xx response has a description and the `ErrorDetail` schema, with the
    two-body union for 422 and for the readiness probe.
- `test_consumer_guide.py::test_guide_errors_table_is_the_declared_contract`
  parses the guide's Errors table and asserts the promised set and the
  declared set are *equal* — so the contract can neither hide a promised
  failure nor invent one — and that `ErrorDetail.detail` is a string.
- Corrected: `test_the_guide_describes_both_shapes_of_a_refused_request`
  asserted the contract declared exactly one 422 shape
  (`application/json:HTTPValidationError`). That was the defect: the API
  answers two and the document named one. It now asserts the union.
- Snapshot regenerated: 39 operations (unchanged), 57 schemas (`ErrorDetail`
  added), and 158 inserted lines of response declarations. No status, body,
  or route changed — the plan's non-goal held.
- Break-test: dropping the group declarations from `include_router` and the
  `{**CONFLICT, **BODY_LIMIT}` from one write route leaves `3 failed, 17
  passed` across the two files (the snapshot, the shared-failure guard, and
  the guide-table guard), and `475 passed` with them restored.
- Tiers: `pytest tests/unit` 1556 passed; `pytest tests/integration -m
  "integration and (redis or database) and not slow"` 157 passed, 2 skipped,
  14 deselected; `ruff format --check .` and `ruff check .` clean.

## Remaining work

- None.
