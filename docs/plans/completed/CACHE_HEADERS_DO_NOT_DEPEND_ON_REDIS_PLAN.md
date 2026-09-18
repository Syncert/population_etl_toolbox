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

- **Status:** Ready for review. Implemented 2026-09-16 on
  `claude/plans-folder-iteration-4x6itr`.
- **Last updated:** 2026-09-16
- **Current milestone:** complete.

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

- [x] With `redis_url=""`, a `404`, `422`, `429` and sanitized `503` on a
      cacheable path each answer `Cache-Control: no-store` and no `x-cache`
      header, asserted by a parametrised unit test beside API-115's.
      `test_no_failure_is_publicly_cacheable_without_a_store` parametrises
      404, 422, 429, 500 and 503 — the same five statuses API-115's own
      parametrisation carries.
- [x] With `redis_url=""`, a `200` on a cacheable path answers
      `public, max-age=<ttl>` with the documented label; a `200` on a
      non-cacheable path answers as today (no cache headers).
      `test_success_without_a_store_is_cacheable_and_labelled_bypass` and
      `test_non_target_without_a_store_carries_no_cache_headers`.
- [x] The Redis-configured behaviour is unchanged: the existing
      `test_cache_middleware.py` and the `redis-integration` tier pass.
- [x] The smoke tier, which runs without Redis, asserts the headers on one
      failure and one success.
- [x] `TESTING_CONTRACT.md` API-115 is extended or a new `API-` row added,
      with `CI_EVIDENCE_MAP.md` unchanged in ownership. API-140 was added;
      the map is untouched, because both owners already exist (`api-unit`
      for the unit assertions, `frontend-smoke` for the live-stack ones).

## Implementation evidence

### What changed

- `apps/api/middleware.py`: `_is_cacheable` is split into `_is_cache_target`
  (type, method, path) and `_has_store` (Redis configured). `__call__` passes a
  non-target straight through as before; a target with no store now runs the
  application with a send wrapper that decorates the response and stores
  nothing. The nested `_decorate_miss` became the method `_decorate(message,
  label)` so both branches share one statement of the header rule — the only
  difference between them is the label a `200` carries.
- The label for a storeless `200` is `x-cache: BYPASS`, distinct from `MISS`
  because no later request can turn it into a `HIT`; a `MISS` from a
  deployment with no cache would be a false promise.
- `tests/unit/api/test_cache_middleware.py`: three tests built on a
  `redis_url=""` middleware, beside API-115's configured-path ones.
- `tests/frontend/smoke/live-stack.smoke.test.js`: the live-stack tier, whose
  stack leaves `REDIS_URL` unset, reads the headers off one success
  (`/catalog/metrics?limit=1`) and one failure (an unpublished metric code's
  404) through the real API and the proxy.
- `docs/reference/API_CONSUMER_GUIDE.md`: one caching bullet stating that the
  headers do not depend on the deployment having a cache, and what `BYPASS`
  means to a client.
- `docs/reference/TESTING_CONTRACT.md`: new row API-140, with the API range,
  the catalog total and the prose row count moved to 473; `AUDITED_COUNTS`
  in `tests/support/catalog_evidence.py` follows.

### The gap, before the fix

The three storeless tests were written first. Six of them failed with
`KeyError: 'cache-control'` — a Redis-less API answered no cache header at
all on a cacheable path, for every status — while the fourth
(non-target, no headers) passed, which is what separated "decoration is
missing" from "decoration is wrong".

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/api/test_cache_middleware.py tests/unit/api/test_consumer_guide.py -q` | 30 passed |
| `python -m pytest tests/unit -q` | 1800 passed |
| `TEST_REDIS_URL=redis://127.0.0.1:6379/15 RUN_INTEGRATION_TESTS=1 python -m pytest -o addopts='' tests/integration/redis -m integration -q` | 6 passed (local `redis-server` 7, database 15) |
| `npm --prefix apps/web run lint` | clean |
| `ruff format --check . ; ruff check .` | 477 files formatted; all checks passed |

The live-stack smoke tier itself was not run in this container: it needs the
`docker-compose.smoke.yml` stack and there is no Docker daemon here
(`docker info` fails). Its execution is owned by the `frontend-smoke`
workflow, which runs on push to `claude/**`, so the assertions run against a
real Redis-less stack on this branch. The conclusion that is unverified
locally is only that the two headers survive the real application and the
nginx in front of it; the header values themselves are asserted
deterministically in the unit tier.

## Definition of done

The header a client reads describes the response, not the presence of a
store behind the API.

## What this plan deliberately does not do

- It does not add an in-process cache for the Redis-less case.
- It does not change TTLs, cacheable prefixes, or the key digest.
