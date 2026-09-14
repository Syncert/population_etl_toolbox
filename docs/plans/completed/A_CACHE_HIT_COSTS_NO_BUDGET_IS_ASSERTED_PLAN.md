---
id: a-cache-hit-costs-no-budget-is-asserted
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/api -m "integration and (redis or database) and not slow" -q
---

# "A cache hit costs no budget" is a promise nothing would fail on

## Plan status

- **Status:** Accepted 2026-09-14 (Implemented; awaiting review. Claimed and completed 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `tests/unit/api/`, `tests/integration/api/`

## Context

The consumer guide tells clients two things about the rate limiter:

> Cache hits cost no budget.

> **Request bodies are bounded.** … Over the bound answers `413` … before
> any parsing.

Both are true only because of the order the middleware is installed in, and
`create_app` says so:

> Middleware executes outermost-last-added: telemetry wraps everything …
> then security headers … then the cache, and innermost the rate limiter,
> **so a cache hit costs no budget** and the limits meter exactly the
> requests that reach the database. Innermost of all: a body over the bound
> is refused before any router parses it, is never a cacheable response, and
> **still spends budget**.

Nothing asserts any of it. The existing suite reads `user_middleware` once —
to check the `trusted_proxies` kwarg the limiter was given (API-075) — and
never looks at the order. So installing a middleware in the wrong place, or
moving the cache inside the limiter, would leave every cached read spending
the analytical budget, and no test would fail. The symptom reaches a client
as a `429` on a read the guide says is free, which is invisible until
someone is throttled.

This is the same shape as API-095 and API-106: a promise the guide makes to
clients, true today, that nothing would catch losing.

## Acceptance criteria

1. The order is asserted from the built application, outermost first, with
   each comparison standing for the promise it keeps, so a reorder fails in
   the cheap tier.
2. The behaviour is asserted too, against a real cache: a second identical
   read answers `x-cache: HIT` under a budget of one request per minute, and
   a *third* read of a different query is refused `429` — without that last
   step a passing test cannot tell "the hit was free" from "the limiter was
   never on".
3. A body over the bound is shown to spend budget: the over-bound request
   answers `413`, and the request after it is refused, which is what the
   comment claims and the guide implies by refusing "before any parsing".
4. Each is read off the shipped application — `create_app(Settings())` — not
   a hand-built stack, because a hand-built stack in the right order proves
   nothing about the one that ships.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-114).

## Non-goals

- Changing any middleware or its order. Nothing here is wrong; it is
  unguarded.
- Asserting the cache's own contents or TTL behaviour, which
  `test_cache_middleware.py` and `test_cache_real_services.py` already own.

## What changed

Nothing in the application. Two guards, and the register row is numbered
**API-114** — the next free id, not API-115; the family is contiguous and the
hygiene guard says so.

- `tests/unit/api/test_operational_hardening.py` —
  `test_the_middleware_order_keeps_the_promises_it_claims` reads
  `create_app(Settings()).user_middleware` (Starlette inserts each addition
  at the front, so the list runs outermost to innermost) and asserts four
  relations, each written as the promise it keeps rather than as a position.
- `tests/integration/api/test_middleware_order_behaviour.py` (new) asserts
  the behaviour against real Redis and the shipped application, with the
  catalog read stubbed through `get_db_session_dep` — the module is about
  the middleware, and `relation_is_absent` already promises a `bind`-less
  double is not evidence of absence. A long freshness window means the
  publication epoch is read once, so nothing here needs a database.

## Validation

- `pytest tests/unit` — **1507 passed** (1506 before: +1).
- `pytest tests/integration -m "integration and (redis or database) and not
  slow"` — **147 passed**, 2 skipped, 14 deselected (145 before: +2).
- **Both guards fail on a reorder.** Swapping the `RateLimitMiddleware` and
  `RedisResponseCacheMiddleware` additions in `create_app` — which puts the
  limiter outside the cache — gives `1 failed, 1 passed` in the new
  integration module and fails the order guard. The body-bound node
  correctly keeps passing under that swap: it is about a different pair.
  `apps/api/main.py` was restored byte-for-byte afterwards.
- `ruff format --check .` / `ruff check .` — clean (443 files).
- `python -m tests.support.catalog_evidence` renders API-114 `FULL`; the
  register is 374 rows.

## Remaining work

- None. Review is the remaining step.
