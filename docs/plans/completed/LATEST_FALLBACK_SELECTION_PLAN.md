---
id: latest-fallback-selection
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - reduction-tie-determinism
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/unit/shared -q
---

# The durable fallback picks a geography's newest row, not any of them

## Plan status

- **Status:** Accepted 2026-09-14 (Complete, awaiting review. Authored, claimed, and delivered 2026-09-13 as catalog row API-086.)
- **Last updated:** 2026-09-14
- **Owner surface:** `src/data_ingestion_toolbox/sql/observation_queries.py`
- **Depends on:** API-083, which closed the same gap in the three reductions
  the neutral resource and the analysis routes use. This is the fourth.
- **Next pickup:** none.

## Context

`/observations/latest` reads `gold.v_metric_latest_by_geo`, and when that
answers nothing it falls back to the durable as-published history and reduces
it itself (API-027: an empty page there means "not refreshed yet", not "no
such data"). The reduction is:

```sql
ROW_NUMBER() OVER (PARTITION BY geo_id ORDER BY observation_date DESC)
```

`observation_date` alone is not an order over that view, and this module says
so twelve lines further down, about the very same relation:

> `observation_date` alone is not a total order over the union: the
> as-published relations behind it hold one row per release of a period, so
> an ACS metric published under two vintages ties on its observation date and
> PostgreSQL promises nothing about which of the two a page boundary keeps.

The paging order learned that. The reduction did not. An ACS metric's newest
period is published under `acs1` and `acs5`, and under more than one vintage,
so the group `ROW_NUMBER` picks 1 from holds several rows and the value a
geography gets is whichever the plan produced. Two identical requests can
answer two different published values, and nothing reports it — each is a
real row, just not the same one.

This is the same defect API-083 closed in the neutral resource's
`newest_per_geography`, in its settled history, and in the `ranked_latest_cte`
the comparison and distribution routes share. It is the one reduction those
three did not cover, because it reads the cross-source union rather than a
dispatched relation.

## What "newest" means here, and what it cannot mean

Each source's own refresh procedure declares how it picks the row that lands
in its latest relation, and the three do not agree:

| Source | The rule its refresh declares |
|---|---|
| BLS | `observation_date DESC, updated_at DESC` |
| FRED | `observation_date DESC, realtime_start DESC, realtime_end DESC, updated_at DESC` |
| Census ACS | `observation_date DESC, updated_at DESC, acs1 before acs5, vintage_year DESC` |

One static `ORDER BY` over a cross-source union cannot be all three, and
restating them here would put a fourth copy of three rules in a fourth place
— which is what the registry exists to prevent.

What the union *does* publish is a release identity, and this module already
declares it: `_TIMESERIES_ORDER` pages the same view by `observation_date`,
then `as_of_date`, `dataset_code`, `vintage_year`, because those are what the
underlying unique indexes key a period's rows by once a metric and a
geography are pinned. Reading that order for recency instead of for paging
gives the fallback a total order derived from a declaration already in this
file: newest period, newest release, `acs1` before `acs5` (which is what
`dataset_code` ascending spells), newest vintage.

That agrees with each source's rule wherever the rules agree, and it differs
from them in one stated way: it ranks on `as_of_date`, the *published*
release date, where each refresh procedure ranks on `updated_at`, the
warehouse's own row-update time. The published identity is the better key for
a serving route to name, and the difference is written down here rather than
left to be discovered.

## Acceptance criteria

1. The fallback's `ROW_NUMBER` ranks on a total order over the union, so one
   geography resolves to the same published row on every request.
2. The order is the reverse-recency reading of the order this module already
   declares for the same view, not a new rule restated from the refresh
   procedures.
3. A row that records no release identity does not outrank one that does.
4. Paging, projection, filters, and the count are unchanged: the fallback
   still answers the same set of geographies, in the same order, with the
   same columns.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Removing or narrowing the fallback, which API-027 established for a reason.
- Making the legacy route dispatch per source. It reads the union by
  definition, and the neutral resource is where per-source rules belong.

## What was built

`_LATEST_SELECTION_ORDER` sits beside `_TIMESERIES_ORDER` in the same module,
carrying the reasoning above, and the fallback's `ROW_NUMBER` ranks on it:

```
observation_date DESC, as_of_date DESC NULLS LAST,
dataset_code ASC, vintage_year DESC NULLS LAST
```

A test pins the two declarations to the same four columns, so the selection
order cannot drift from the paging order this view is keyed by without CI
saying so. Another pins the `NULLS LAST` on both nullable columns: `DESC`
sorts nulls first in PostgreSQL, so without it a row carrying no release date
or no vintage would have won every tie — the opposite of the rule.

The consumer guide gained a short paragraph under "Paging a history, and what
orders it" stating the rule and pointing at `newest_per_geography=true` as
the same question on the neutral resource.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| Query builders | `python -m pytest tests/unit/api/test_sql_query_builders.py -q` | 18 passed |
| Whole unit tier | `python -m pytest tests/unit -q` | 1408 passed |
| Register | `python -m tests.support.catalog_evidence` | 319 rows; API-086 is `FULL` |
| Lint | `ruff check src/data_ingestion_toolbox/sql tests/unit/api` | clean |
| Format | `ruff format --check` on the changed files | already formatted |

Both new tests were confirmed failing-first (`_LATEST_SELECTION_ORDER` did
not exist, and the ranking read `ORDER BY observation_date DESC`).

### Against a real database

The rendered query was run on a live PostgreSQL 16 against a view shaped like
`gold.v_metric_timeseries_by_geo`, holding one geography whose newest period
is published five ways:

| Period | `as_of_date` | `dataset_code` | `vintage_year` | value |
|---|---|---|---|---|
| 2023 | 2024-09-01 | acs5 | 2023 | 100 |
| 2023 | 2025-09-01 | acs1 | 2023 | **111** |
| 2023 | 2025-09-01 | acs5 | 2024 | 222 |
| 2023 | *(none)* | acs1 | *(none)* | 999 |
| 2019 | 2020-09-01 | acs5 | 2019 | 50 |

The fallback answered `111`: the newest period (2019 excluded), its newest
published release (2024-09-01 excluded), `acs1` ahead of `acs5` — which is
the preference the ACS refresh procedure declares — and the row recording no
release identity did not win despite `DESC`. Under the order being replaced,
the tie group `ROW_NUMBER` chose from held **four** rows, confirmed by
counting it on the same fixture.

Not run, and why: the Docker-gated tiers (`make test-integration`,
`make test-e2e`, `make test-compose-smoke`) need a container runtime this
environment does not provide. The integration tier is where this runs against
the real union view; the ad-hoc cluster above covers the ordering semantics.

## Acceptance criteria, as delivered

1. **Met.** Four columns, total over the view once a metric and a geography
   are pinned.
2. **Met.** `test_latest_fallback_ranks_a_total_order_over_the_union` asserts
   the selection order's columns are the declared paging order's columns.
3. **Met.** `test_latest_fallback_prefers_a_recorded_release_to_a_missing_one`,
   and confirmed on a live database.
4. **Met.** The two pre-existing fallback tests (projection, ranked shape,
   count) pass unmodified; only the window's `ORDER BY` changed.
5. **Met.** `API-086` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `api-unit` CI job, with `AUDITED_COUNTS["API"]` raised to 86.

## Remaining work

- None.
