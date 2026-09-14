---
id: distribution-period-honesty
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - comparison-uncertainty-caveat
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
  - npm --prefix apps/web run test:browser
---

# A distribution says which period its bins describe

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/services/distribution_service.py`,
  `apps/api/schemas/analysis.py`, `apps/web/components/SourceExplorerPage.tsx`

## Context

`/distribution/bins` and `/comparison` reduce through the same CTE:

```sql
ROW_NUMBER() OVER (
    PARTITION BY geo_id
    ORDER BY period_start DESC, …
) AS recency_rank
…
WHERE recency_rank = 1
```

Each geography contributes its own newest period, which means two
geographies in one answer can be describing two different years. The
comparison route treats that as load-bearing and publishes `period_a` and
`period_b` on every row — the guide calls them "the periods actually
combined", and the web workspace has a whole note for the mismatch.

The distribution route publishes no period at all. Its response carries
`total`, `bin_count`, `min_value`, `max_value`, `units`, `geo_level` and the
bins, and nothing says which period any of it is from. A histogram whose bars
mix 2023 and 2019 county estimates is indistinguishable from one that does
not.

That answer is not only a chart. The explorer feeds it to the **map legend**,
so the bins decide the colour scale a choropleth is painted with — and the
client cannot correct for it, because the bins are computed over every
geography the metric publishes while the client holds one page of rows.

## Acceptance criteria

1. `/distribution/bins` publishes the period its bins describe: one period
   when every binned row came from the same one, and an explicit statement
   that they differ when they do not.
2. Both facts are computed in the same statement as the bins, from the same
   reduced rows, so the period cannot describe a different set than the
   counts.
3. Unpublished stays unpublished: an answer with no rows reports no period
   rather than inventing one.
4. The fields are additive — no existing field changes meaning — and the
   reviewed OpenAPI snapshot is regenerated deliberately.
5. The explorer says which period the legend's scale describes, and says
   when it mixes periods, where the distribution is presented.
6. Two `TESTING_CONTRACT.md` catalog rows: API-097 for the answer, WEB-054
   for the surface.

## Non-goals

- Refusing a mixed-period distribution. Each geography's newest value is a
  legitimate map, and it is what the explorer asks for; the defect is not
  saying so.
- Splitting the bins by period. That is a different analysis, and one the
  caller can already build from `/observations`.
- Adding a `caveats` array to the distribution response. The uncertainty note
  API-096 added to the comparison belongs here too, and it is its own plan.

## Validation

**Failing first**, on both sides.

API-097 — three nodes in `tests/unit/api/test_distribution.py`, each on a
different answer shape: one period, mixed periods, nothing published. All
three failed before the change; the mixed-period one is the point, and it
asserts `period is None` as well as `periods_differ is True`, because naming
the earliest or the latest would label a whole histogram with a period most
of it is not from.

WEB-054 — with the explorer reverted and the specs in place, two browser
nodes fail on the status the legend does not carry:

```
Error: expect(locator).toContainText(expected) failed   (× 2)
2 failed
20 passed
```

**Measured in the one statement.** `published` now carries `period_start`
beside `value`, and `stats` adds `COUNT(DISTINCT period_start)`,
`MIN(period_start)` and `MAX(period_start)` — the same reasoning API-084
applied to the range: a period read in a second statement could describe a
different set than the bins it labels.

**Additive.** `python -m tests.support.regenerate_openapi_contract` changed
exactly two lines of the reviewed snapshot:

```diff
         "min_value": "number | null",
+        "period": "string | null",
+        "periods_differ": "boolean",
```

**The fixtures were part of the defect, again.** The browser stub for
`/distribution/bins` published neither field, so the tier modelled an API
that never says which period painted the map — the failure mode WEB-043
exists to name. It now carries both.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1457 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 133 passed, 2 skipped |
| Frontend units | `npm --prefix apps/web run test:unit` | 289 passed |
| Frontend browser | `npm --prefix apps/web run test:browser` | 75 passed |
| Frontend lint / typecheck | `run lint`, `npx tsc --noEmit` | clean |
| Lint | `ruff format --check .`, `ruff check .` | clean |

**Register.** 346 rows.

One full browser run reported a single failure and the rerun reported none;
the failing node was outside the two specs this plan touches and its
explorer-only run passed 22/22 both times, so it is recorded here as a flake
rather than as evidence of anything.

## Remaining work

- None. The uncertainty caveat API-096 added to the comparison belongs on
  this response too, and is named under Non-goals: the distribution has no
  `caveats` field, so it is an additive contract change with its own plan.
