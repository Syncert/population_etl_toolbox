---
id: releases-total-order
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
---

# The releases listing pages a total order, and the guide says so

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/services/neutral_observations_service.py`,
  `docs/reference/API_CONSUMER_GUIDE.md`

## Context

The consumer guide makes one promise about every read that pages:

> Each of those reads pages a **total order**, so two consecutive pages can
> neither repeat a row nor skip one

Every paged read in this API keeps it — `/catalog/metrics` orders by
`metric_code`, `/catalog/geographies` by `geo_id`, the account collections by
`(name, id)`, `/comparison` by `geo_id` after a one-row-per-geography
reduction, and each observation route by its source's declared key. One does
not:

```sql
SELECT {release_expression} AS release, ... FROM {released_relation}
WHERE ... GROUP BY 1
ORDER BY MAX({release_order_expression}) DESC
LIMIT :limit OFFSET :offset
```

`/observations/releases` orders by the release *ordering* expression alone.
It is total today only by coincidence of the registry: every dispatch entry's
`release_order_expression` happens to be its `release_expression` with a cast
(`as_of_date` and `as_of_date::TEXT`, `release_watermark::BIGINT` and
`release_watermark`), so one group cannot share an ordering value with
another. Nothing declares that, nothing checks it, and a source whose release
identity is a name ordered by a date — the obvious next shape — breaks paging
here the moment two releases land on one date.

The guide's ordering table has a second, smaller gap: it lists the legacy
pair and the source-scoped pair and omits both `/observations`, the resource
it tells clients to prefer, and `/observations/releases` itself. A client
reading it cannot learn what the primary resource orders by.

## Acceptance criteria

1. `/observations/releases` orders by a key that is total by construction,
   not by a property of the current registry.
2. Nothing else about the listing changes: the same releases, newest first,
   with the same counts.
3. The guide's ordering table names every observation-family read that pages,
   and a read that pages without a row there is a test failure.
4. The check is derived from the served document and the reviewed registry,
   never from a list of routes written beside it.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-095).

## Non-goals

- Changing what orders the releases: `release_order_expression` descending is
  the right primary key for "newest first", and API-081 is why a client must
  not re-derive it from the identity's spelling.
- Auditing the non-observation collections. They are already total, and
  saying so in a guide section about observations would misplace it.

## Validation

**Failing first**, both halves, each naming what it found rather than what it
expected.

`test_every_source_lists_its_releases_in_a_total_order`, swept over the
registry:

```
AssertionError: BLS orders its releases by 'MAX(as_of_date) DESC', whose
tie-break does not name the release identity 'as_of_date::TEXT'; two releases
sharing an ordering value could then repeat or skip across a page boundary
```

`test_every_paged_observation_read_declares_what_orders_it`, after the two
rows the plan named were added:

```
AssertionError: these paged observation reads name no ordering in the
consumer guide: ['/cdc/observations', '/usda-nass/observations']
```

That second failure is the guard earning its place: the plan had found two
undocumented reads by reading the guide, and the guard found two more by
reading the served document. Both of those orders were already total — CDC
closes with `observation_sk`, NASS with the same after `year` — so this is a
documentation gap, not a paging one, and it is now in the table with the
reason each surrogate key is there.

**The fix.** One term: `ORDER BY MAX(release_order_expression) DESC,
release_expression DESC`. The identity is the `GROUP BY` key, so no two rows
of the listing can tie on it and the order is total whatever the first term
does. Nothing else about the listing changes.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1452 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 133 passed, 2 skipped |
| End-to-end | `E2E_REQUIRE_ALL_PRODUCTS=1 pytest tests/e2e -m e2e`, fresh database | 9 passed |
| Lint | `ruff format --check .`, `ruff check .` | clean |

**Register.** 342 rows.

### What the audit found elsewhere

Every other paged read in the API was already total, which is why this plan
is one line of SQL and four table rows:

| Read | Ordered by | Total |
|---|---|---|
| `/catalog/metrics` | `metric_code` | unique |
| `/catalog/geographies` | `geo_id` | unique |
| `/analysis-configurations` | `name, configuration_id` | unique |
| `/evidence-packets` | `name, packet_id` | unique |
| `/comparison` | `geo_id`, after a one-row-per-geography reduction | unique |
| `/cdc/observations` | … `observation_sk` | unique |
| `/usda-nass/observations` | … `observation_sk` | unique |
| the observation pairs | the source's declared key | declared |
| `/observations/releases` | `MAX(release_order_expression)` | **by coincidence** |

## Remaining work

- None.
