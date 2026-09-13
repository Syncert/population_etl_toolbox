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

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  divergence between the guide and the snapshot it says pins it.**
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

To be recorded by the agent that claims this.

## Remaining work

- Everything.
