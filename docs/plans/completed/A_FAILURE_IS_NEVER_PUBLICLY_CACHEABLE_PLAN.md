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

- **Status:** Accepted 2026-09-14 (Implemented; awaiting review. Authored 2026-09-13 by the assessment agent; claimed and completed 2026-09-13. It was a present defect. Register row **API-115** (API-112, suggested at authoring time, had been taken).)
- **Last updated:** 2026-09-14
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
   identifier; API-112 at authoring time).

## Non-goals

- Reordering the middleware stack. The decoration is the defect.

## What changed

- `_decorate_miss` labels a `200` as it always did and gives every other
  status `Cache-Control: no-store` and no `x-cache`. The headers are
  *replaced* rather than appended, so one response can never carry two
  contradictory `cache-control` values.
- The `x-cache` omission is deliberate, and stated in the code: a label
  belongs to a response the cache could have answered, and `MISS` on a 503
  says the cache looked and did not have it — which invites a client to
  retry for a hit that can never arrive.
- `test_ineligible_response_is_not_stored` now reads the headers, and splits
  on status: the two ineligible-by-size/emptiness cases are still served
  responses that say so, and the failure case is `no-store` with no label. A
  second node sweeps 404, 422, 429, 500 and 503.
- `API_CONSUMER_GUIDE.md`'s caching section states the rule, naming the
  statuses and why a shared cache cannot serve one client's refusal to
  another.

## Validation

- `pytest tests/unit` — **1520 passed**.
- **The tests fail on the old behaviour.** Decorating every status again
  (`if True:` in place of `if status == 200:`) leaves `6 failed, 4 passed`
  in `test_cache_middleware.py`. `apps/api/middleware.py` was restored
  byte-for-byte afterwards.
- `pytest tests/integration/api/test_middleware_order_behaviour.py -m
  "integration and redis"` — 3 passed, the new node included: on the shipped
  `create_app(Settings())` with real Redis and a budget of one, the 429 the
  limiter raises *inside* the cache answers `no-store`, carries no `x-cache`,
  and keeps its `Retry-After`.
- `ruff format --check .` / `ruff check .` — clean (444 files).
- `python -m tests.support.catalog_evidence` renders API-115 `FULL`.

## Remaining work

- None. Review is the remaining step.
