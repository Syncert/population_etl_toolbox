---
id: a-failure-is-never-publicly-cacheable
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api/test_cache_middleware.py tests/unit/api/test_cache_coverage.py tests/unit/api/test_operational_hardening.py -q
  - python -m pytest tests/integration/redis -m "integration and api and redis" -q
---

# A failure is never decorated as publicly cacheable

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect.**
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/middleware.py`

## Context

The response cache refuses to *store* anything but a 200
(`middleware.py:397`), then replays every buffered message through
`_decorate_miss`, which appends `cache-control: public, max-age=<ttl>` and
`x-cache: MISS` to any `http.response.start` with no status check
(`:342-355`). Because the rate limiter sits inside the cache in the
middleware stack (`main.py:160-173`), its 429 is decorated too; so are the
404, 422 and sanitized 503 bodies.

The guide scopes `Cache-Control: public, max-age=<ttl>` to successful
public analytical GETs, tells clients to honour `Retry-After` on 429, and
sanitizes the 503 so it cannot be used to probe deployment state. A shared
cache that honours the header serves one client's 429 to everyone for the
TTL and pins an outage.

## Findings

- `test_cache_middleware.py::test_ineligible_response_is_not_stored[error-response]`
  passes for the wrong reason: it sends a 503 and asserts only that Redis
  saw no write. The response it receives already carries
  `cache-control: public, max-age=30`; the test never reads headers.

## Acceptance criteria

1. A non-200 on a cacheable path carries `Cache-Control: no-store` and no
   `x-cache` header; a 200 is unchanged.
2. The existing test reads the headers, failing first; a second case covers
   the 429 built inside the cache.
3. `API_CONSUMER_GUIDE.md`'s caching section says a failure is never
   cacheable.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `API-`
   identifier; API-111 at authoring time).

## Non-goals

- Reordering the middleware stack. The decoration is the defect.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
