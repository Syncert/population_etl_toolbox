---
id: health-is-not-analysis
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
---

# A health check does not spend the analysis budget

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/ratelimit.py`, `apps/api/main.py`

## Context

The rate limiter states its own classification rule:

> Two token buckets per client: `catalog` for the inexpensive discovery reads
> and `analysis` for **everything that reaches observation or analysis SQL**.

and its own exemption:

> Never limited: the deployment probes and documentation.

`_EXEMPT_PATHS` names five literal paths — `/health`, `/health/ready`,
`/docs`, `/openapi.json`, `/redoc` — and matches them exactly. The versioned
health resource, `/api/v1/health`, is not among them. It falls through to
`_classify`, which has no fragment for it, so it bills the **analysis**
bucket: the budget that exists to protect the expensive queries.

It reaches no SQL at all. Its handler returns a constant:

```python
@router.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    return HealthResponse(status="ok", service="data-ingestion-toolbox-api")
```

So it is misclassified by the module's own definition, and by the same
definition as the unprefixed probe that is already exempt — the two handlers
are the same three lines.

The consequence is not theoretical. `apps/web` calls it on every page load
(`checking /api/v1/health`), so each load spends an analysis token before it
asks for any data — and under a tight budget the health check is the request
that gets the `429`, which the explorer then presents as an unhealthy API.
Rate limiting would make the application report the thing it protects as
broken.

## Acceptance criteria

1. The versioned health resource is exempt, exactly as the unprefixed probe
   is.
2. The exemption is derived from the routers that serve those paths, not from
   a literal list, so a health route added later is exempt by construction
   and one that moves does not leave a stale entry behind.
3. Nothing else changes class: the catalog reads stay `catalog`, everything
   reaching observation or analysis SQL stays `analysis`, and
   `/health/ready` — which does touch the database — keeps the exemption a
   probe needs.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-101).

## Non-goals

- Exempting anything else that happens to be cheap. The split is by what a
  route reaches, and every other public route reaches the warehouse.
- Making the classification configurable. A deployment choosing which routes
  are free is a way to configure the protection away.

## Validation

**Failing first.** Two nodes: one behavioural — ten `/api/v1/health` requests
against a budget of one analytical token, then two `/api/v1/observations` to
prove the budget is still whole — and one structural, asserting the exempt
set covers every path the health routers serve.

Both failed before: the first because the eleventh request never arrived (the
second one already answered `429`), the second because `EXEMPT_PATHS` did not
exist and the literal it replaced named only the unprefixed probe.

**Derived, not listed.** The exempt paths are now the health routers' own —
`probe_router`'s paths as served, `router`'s under every declared API prefix
— plus the three documentation paths. The structural test reads the routers
the same way, so it cannot pass by restating the implementation's list: a
health route added later is exempt by construction, and one that moves leaves
no stale entry behind.

**Nothing else moved.** The pre-existing cost-class test still holds:
`/api/v1/catalog/metrics` bills `catalog`, `/api/v1/observations` bills
`analysis`, an exhausted catalog budget does not spend the analytical one,
and `/health` is unlimited. `/health/ready` — the one probe that does touch
the database — keeps its exemption, which is what a readiness probe needs.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1465 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 135 passed, 2 skipped |
| Lint | `ruff format --check .`, `ruff check .` | clean |

**Register.** 351 rows.

### Not made cacheable

Exempting a path from the limiter does not add it to `CACHEABLE_ROUTERS`, and
this one deliberately stays out: "a probe answer must describe now, not the
last five minutes".

## Remaining work

- None.
