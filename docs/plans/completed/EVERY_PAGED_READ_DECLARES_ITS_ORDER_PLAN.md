---
id: every-paged-read-declares-its-order
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - pytest tests/unit/api/test_consumer_guide.py
---

# Every paged read declares its order, not only the observation reads

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `docs/reference/API_CONSUMER_GUIDE.md`

## Context

The guide makes a blanket promise:

> Pagination is `limit`/`offset` with **documented deterministic ordering per
> resource**. `offset` is bounded; page with filters rather than deep offsets.

API-095 made it true for the observation reads, and its guard says so in its
own name — `test_every_paged_observation_read_declares_what_orders_it` — and
in its filter: `if get is None or "observations" not in path: continue`.

Twenty served GETs take `limit` and `offset`. The guard sweeps the fourteen
with `observations` in the path. Six are outside it, and none of them appears
in the ordering table:

| Read | Its actual ORDER BY |
|---|---|
| `/catalog/metrics` | `metric_code` |
| `/catalog/geographies` | `geo_id` |
| `/comparison` | `geo_id` |
| `/usda-nass/series` | `product_id, short_desc, geo_id, series_id` |
| `/analysis-configurations` | `name, configuration_id` |
| `/evidence-packets` | `name, packet_id` |

**Every one of those is already a total order**, which was checked rather
than assumed:

- `dim_metric_catalog.metric_code` is `NOT NULL UNIQUE`; `dim_geo_latest.geo_id`
  is the primary key.
- `/comparison` reduces each side with
  `ROW_NUMBER() OVER (PARTITION BY geo_id …) WHERE recency_rank = 1` and joins
  `USING (geo_id)`, so `joined` holds one row per geography.
- `gold_nass.crop_series.series_id` is an MD5 over the exact tuple the view
  groups by, unique per row by construction (API-080 says so in the query's
  own comment).
- The two private reads close on their table's surrogate key.

So this is not a present miscount. It is the promise being true by
coincidence for six resources, with nothing documenting it for a client and
nothing failing if an `ORDER BY` were narrowed — which is precisely what
API-095 found for `/observations/releases`: "its order was total only by
coincidence of the registry".

## Acceptance criteria

1. Every paged read the API serves names what orders it, in the guide, for a
   client to page against.
2. The guard sweeps **every** paged GET, not the ones whose path happens to
   contain `observations`. A resource that grows `limit` and `offset` without
   a documented order fails.
3. The private reads are covered too. They page the caller's own rows, and a
   repeated or skipped row is no more acceptable there.
4. No route's SQL changes. Each order is already total; this documents and
   pins them.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-106).

## Non-goals

- Changing any `ORDER BY`. Each was checked and each is total.
- Documenting ordering for the unpaged reads (`/catalog/sources`,
  `/catalog/capabilities`, `/catalog/freshness`, `/usda-nass/measures`,
  `/usda-nass/source-notes`). They answer whole collections, so a client has
  no page boundary to be stable across; the promise is about paging.

## Validation

**Failing first**, by widening the guard before writing the documentation:

```
FAILED tests/unit/api/test_consumer_guide.py::test_every_paged_read_declares_what_orders_it
```

**One guard, both rows.** The observation-scoped test was widened rather than
duplicated, and now reads `Covers: API-095, API-106`: API-095's claim is the
subset, and a second node asserting the subset beside a node asserting the
whole would be two declarations of one rule. The floor
(`len(paged) >= 20`) is there because the sweep is derived from the served
document — a change that stopped matching would otherwise make the assertion
pass by examining nothing, which is the failure mode of every derived guard
in this suite.

**The parser needed no change.** `_ordering_table_reads` already scans the
whole guide for `| `/path` |` rows rather than one section, so a second table
elsewhere is picked up; verified by printing what it matched before and after
(eight rows, then fourteen). Its regex requires the backticked cell to start
with `/`, so the route-listing table's `GET /api/v1/…` cells do not satisfy
the guard by accident.

**Every order was checked, not assumed** — the plan's table above records
each one against the DDL or the query: a `NOT NULL UNIQUE` column, a primary
key, a pre-join `ROW_NUMBER() … WHERE recency_rank = 1`, an MD5 digest over
the grouping tuple, and two surrogate keys. No `ORDER BY` changed, which is
why the unit tier is unchanged at 1488 rather than showing a behavioural
delta.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1488 passed |
| Unit, this file | `pytest tests/unit/api/test_consumer_guide.py` | 7 passed |
| Lint / format | `ruff format --check .`, `ruff check .` | clean |

**Register.** 361 rows.

## Remaining work

- None.
