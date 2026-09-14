---
id: response-cache-coverage
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - pytest -m "unit and api" tests/unit/api
  - ruff check .
---

# The response cache covers the resources it claims to

## Plan status

- **Status:** Accepted 2026-09-14 (Ready for review. Authored, claimed, and delivered 2026-09-13 from an investigation of the API's caching middleware.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/middleware.py`, `apps/api/main.py`
- **Depends on:** nothing.
- **Next pickup:** none.

## Context

`RedisResponseCacheMiddleware` decides what to cache by prefix match:

```python
CACHEABLE_SUFFIXES = ("/catalog/", "/observations/", "/distribution/", "/comparison")
```

Matched against the 31 versioned paths the application actually serves, that
list reaches 8 of them and misses 13 public analytical GETs:

| Uncached today | Why it was missed |
| --- | --- |
| `/api/v1/observations` | The prefix is `/observations/` **with** a trailing slash; the neutral resource is mounted at `/observations` **without** one |
| `/api/v1/{bls,census,fred,pep}/observations/{latest,timeseries}` | Source-scoped paths begin with the source segment, which no prefix names |
| `/api/v1/cdc/observations` | same |
| `/api/v1/usda-nass/{observations,series,measures,source-notes}` | same |

The one the guide tells clients to prefer is in that list.
`API_CONSUMER_GUIDE.md` says "New work should use `/observations`" and calls
the cached pair legacy, so the documented path is the uncached one — and
`apps/web` reads the source-scoped routes, so the frontend's entire data path
misses the cache as well.

Two contracts are broken by this, not one:

1. **Every request reaches PostgreSQL.** The rate limiter sits inside the
   cache deliberately, "so a cache hit costs no budget and the limits meter
   exactly the requests that reach the database" — for these routes every
   request both queries the warehouse and spends budget.
2. **The published contract is false for them.** The guide promises
   "Cacheable public analytical GETs answer with `x-cache: HIT|MISS` and
   `Cache-Control: public, max-age=<ttl>`". These answer with neither.

The root cause is not the missing entries; it is that cache eligibility is a
hand-written list of path fragments that nothing checks against the served
contract. API-063 asserts the private resources stay *out* of that list.
Nothing asserts the public ones are *in* it, so the list could only ever be
wrong in the direction it was wrong.

## Objective

Cache eligibility is derived from the routers the application mounts, and a
served public GET that is in neither the cacheable set nor the declared
private set fails the suite.

## Acceptance criteria

1. Cache targets are built from the route paths of a declared tuple of
   public, cacheable routers — exact paths, plus a prefix only where a path
   carries a parameter — rather than from hand-written fragments.
2. All 21 public analytical GET paths are cacheable: the catalog, the neutral
   observation resource and its releases, the legacy pair, distribution,
   both comparison routes, all eight source-scoped observation routes, CDC,
   and all four USDA NASS routes.
3. The authenticated, user-scoped resources (ADR-0003 saved analysis, ADR-0004
   evidence packets) remain outside the cache, and `/api/v1/health` and the
   unversioned probes stay uncached — a probe answer must reflect now.
4. A guard test partitions every served path into cacheable, declared
   private, and declared uncacheable, and fails when a path lands in none —
   so the next router added must be classified before it merges.
5. Nothing about cache identity, TTL, the freshness epoch, the body bound, or
   the degrade-to-MISS behavior changes.
6. The consumer guide states which resources are cached, and the behavior is
   a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Caching anything that is not a public GET.
- Changing the TTL, the freshness window, or the cache key.
- Per-route TTLs.
- Making the health resource cacheable.

## Evidence

### The gap, established first

The served contract was enumerated against the prefix list before anything
changed: 8 of the 31 paths the application serves matched it, and 13 public
analytical GETs did not — `/api/v1/observations`, the eight source-scoped
observation routes, `/api/v1/cdc/observations`, and the four USDA NASS
routes. `tests/unit/api/test_cache_coverage.py` states that as four tests,
which failed on import before `PUBLIC_CACHE_TARGETS` existed.

### What changed

- `apps/api/middleware.py` gains `CacheTargets` and `build_cache_targets`,
  which read the GET routes of the routers handed to them and produce exact
  paths plus one prefix per parameterised template, ending at the separator
  before the parameter. `CACHEABLE_SUFFIXES`/`CACHEABLE_PREFIXES` are gone.
- `apps/api/main.py` splits `PUBLIC_ROUTERS` into `CACHEABLE_ROUTERS` and
  `PRIVATE_ROUTERS` and builds `PUBLIC_CACHE_TARGETS` from the first. The
  health resource is in neither, deliberately: a probe answer must describe
  now. `PUBLIC_ROUTERS` is still the mounting order and still contains every
  router, so nothing about what is served changed.
- The middleware caches nothing when it is handed no targets. A caller that
  forgets loses an optimization rather than caching a resource nobody
  classified — the safe direction for a mistake in a shared cache.
- API-063's two assertions now ask `PUBLIC_CACHE_TARGETS.covers(path)`
  instead of matching a prefix tuple, so they keep testing the same property
  against the real decision procedure.

### Bounds this does not change

The cache still stores only a `200` with a non-empty body under
`MAX_CACHE_BODY_BYTES` (2 MB), so a 5,000-row observation page over that
bound streams through as a MISS exactly as before. Redis runs with
`--maxmemory 128mb --maxmemory-policy allkeys-lru` in the compose stack, so
the wider working set evicts rather than grows; nothing here changes the TTL,
the freshness epoch, the key, or the degrade-to-MISS behavior.

### Commands

| Command | Result |
| --- | --- |
| `pytest tests/unit/api/test_cache_coverage.py -q` | 4 passed |
| `pytest -m "unit and api" tests/unit/api -q` | 305 passed |
| `pytest tests/unit -q` | 1370 passed |
| `python -m tests.support.catalog_evidence` | 297-row register renders; API-076 is `FULL` |
| `ruff check .` | All checks passed |
| `ruff format --check .` | 440 files already formatted |

### Not run

`make test-integration` (the Redis tier) needs a Redis service, and this
environment has no Docker daemon. `tests/integration/redis/test_response_cache.py`
constructs the middleware directly and was updated to pass
`PUBLIC_CACHE_TARGETS`; its paths (`/api/v1/catalog/metrics`,
`/api/v1/catalog/sources`) are covered by the real target set, which the unit
guard asserts. It was collected (`--collect-only`) to prove the module still
imports and parametrises. `tests/integration/api/test_cache_real_services.py`
and `tests/resilience/test_redis_outage.py` exercise the built application
and needed no change.

## Remaining work

None.
