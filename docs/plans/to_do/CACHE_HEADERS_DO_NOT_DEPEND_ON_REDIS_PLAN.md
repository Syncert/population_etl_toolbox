---
id: cache-headers-without-redis
branch: claude/cache-headers-without-redis
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api/test_cache_middleware.py tests/unit/api/test_consumer_guide.py -q
  - ruff format --check . ; ruff check .
---

# The cache-control contract holds when there is no Redis

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

`docs/reference/API_CONSUMER_GUIDE.md`, caching section, states without
qualification that every public analytical `GET` answers
`Cache-Control: public, max-age=<ttl>` on a `200` and `Cache-Control:
no-store` on every other status, and that "a failure is never cacheable".
API-115 (`a-failure-is-never-publicly-cacheable`) was written to make the
second half true, because a shared cache honouring a `public` header on a
`429` or `503` pins one client's refusal on every client.

Both halves hold only when Redis is configured.
`apps/api/middleware.py`, `RedisResponseCacheMiddleware._is_cacheable`,
begins `bool(self.redis_url) and ...`, and `__call__` passes the request
straight through when it is false. The `no-store` decoration for failures
lives on the cacheable branch (`_decorate_miss`), so a Redis-less API sends
no `Cache-Control` at all. RFC 9111 lets a shared cache treat a `404` as
heuristically cacheable, which is exactly the case API-115 closed.

A Redis-less deployment is a supported shape. `infra/docker/docker-compose.smoke.yml:56`
leaves `REDIS_URL` unset on purpose, `docker-compose.yml` and the external
stack make it optional, and readiness never gates on it (API's stated
policy: Redis is an optimisation the API must survive without).
`tests/unit/api/test_cache_middleware.py` exercises the configured path only.

## Deliverables

### 1. Decoration is decided by the path, storage by Redis

Split `_is_cacheable` into "is this a cache target" (method and path) and
"can we store" (Redis configured and reachable). Every response on a target
path is decorated: `no-store` for a non-`200`, `public, max-age=<ttl>` for a
`200` with `x-cache: BYPASS` (or another documented label) when no store
exists, `MISS`/`HIT` as today when one does. Nothing about what is stored
changes.

### 2. The guide says what a Redis-less deployment answers

One sentence in the caching section naming the label, so a client seeing it
knows the header is honest and the shared cache is absent.

## Acceptance criteria

- [ ] With `redis_url=""`, a `404`, `422`, `429` and sanitized `503` on a
      cacheable path each answer `Cache-Control: no-store` and no `x-cache`
      header, asserted by a parametrised unit test beside API-115's.
- [ ] With `redis_url=""`, a `200` on a cacheable path answers
      `public, max-age=<ttl>` with the documented label; a `200` on a
      non-cacheable path answers as today (no cache headers).
- [ ] The Redis-configured behaviour is unchanged: the existing
      `test_cache_middleware.py` and the `redis-integration` tier pass.
- [ ] The smoke tier, which runs without Redis, asserts the headers on one
      failure and one success.
- [ ] `TESTING_CONTRACT.md` API-115 is extended or a new `API-` row added,
      with `CI_EVIDENCE_MAP.md` unchanged in ownership.

## Definition of done

The header a client reads describes the response, not the presence of a
store behind the API.

## What this plan deliberately does not do

- It does not add an in-process cache for the Redis-less case.
- It does not change TTLs, cacheable prefixes, or the key digest.
