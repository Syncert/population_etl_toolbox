---
id: distribution-one-snapshot
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - reduction-tie-determinism
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/unit/shared -q
---

# A distribution's range and its bins describe one reading

## Plan status

- **Status:** Complete, awaiting review. Authored, claimed, and delivered
  2026-09-13 as catalog row API-084.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/distribution_service.py`
- **Depends on:** API-083, which made the reduction under this route's CTE
  deterministic. This closes the other half: two readings of it.
- **Next pickup:** none.

## Context

`list_distribution_bins` builds one CTE and executes it twice:

```python
stats_row = db.execute(stats_query, params).mappings().one()   # COUNT, MIN, MAX
...
bins_rows = db.execute(bins_query, {...}).mappings().all()     # width_bucket
```

The session runs at the database's default isolation, so each statement takes
its own snapshot. Between the two, the ETL can commit a
`REFRESH MATERIALIZED VIEW CONCURRENTLY` over the very relation the CTE
reads — that is what the relation is for. The response is then assembled from
two different readings of the warehouse:

- `total`, `min_value`, and `max_value` come from the first.
- Every `count` comes from the second.

A value the second reading published below the first reading's `min_value`
gets `width_bucket(...) = 0`. The result is clamped on the high side only —
`LEAST(width_bucket(...), :bin_count)` — so bin 0 survives into `counts`,
and `items` is assembled over `range(1, bin_count + 1)`, which never asks for
it. The geography vanishes from the bins while `total` still counts it. A
value above the first reading's `max_value` is worse than lost: it is clamped
into the last bin, whose `upper_bound` this response reports as the maximum
it was not.

Either way `sum(count) != total`, which is precisely what this route's own
tests assert must hold, and nothing in the response says a word about it.
API-079 made every bin the caller asked for appear so that a histogram could
be drawn without rebuilding the gaps; a histogram whose bars do not add up to
the stated total is the same defect one layer down.

The fix is not a clamp on the low side. Counting a value into bin 1 because
it fell below a range computed from stale data would report a number that
belongs to neither reading. One statement is the fix: a CTE referenced more
than once is evaluated once, so the range and the bins are measured over the
same rows, and a value outside the range becomes impossible rather than
handled.

## Acceptance criteria

1. The range and the bin counts are measured in one statement over one
   evaluation of the reduction, so a refresh between them cannot exist.
2. `sum(item.count) == total` for every non-degenerate answer, and the
   response's `min_value`/`max_value` bound every value that was counted.
3. The degenerate answers are unchanged: no published values stays
   `total: 0`, `items: []`, null bounds; a single distinct value stays the
   one bin closing on itself.
4. `width_bucket` is never asked for a range whose bounds are equal, which
   the database rejects outright.
5. Every bin the caller asked for is still reported, including the empty ones
   (API-079), with the boundaries it reports today.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing the binning rule, the boundaries, or the API-derived labelling.
- Raising the route's isolation level, which would fix this one read by
  changing every other read's semantics.

## What was built

One statement. `published` (the reduction's non-null values) is referenced by
both `stats` and `binned`, so PostgreSQL materializes it and evaluates it
once; the range and the counts are then two projections of the same rows,
and a refresh landing between them is not a state that exists.

`width_bucket` rejects a range whose bounds are equal, and `stats` can
produce one. The bounds handed to it are therefore
`CASE WHEN max = min THEN min + 1 ELSE max END`: with one distinct value
every value equals the lower bound and falls in bin 1, and the degenerate
branch in Python replaces the bins with the single bin closing on itself
anyway. Guarding the *bounds* rather than wrapping the call in a `CASE`
keeps the function off a conditional evaluation whose short-circuiting the
PostgreSQL documentation explicitly warns not to rely on.

`stats` is an aggregate over a possibly empty set, which is still exactly one
row, so `stats LEFT JOIN binned ON TRUE` answers the range even when nothing
was published — that row carries a null `bin_index`, which the reader skips.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| Distribution unit | `python -m pytest tests/unit/api/test_distribution.py -q` | 15 passed |
| Whole unit tier | `python -m pytest tests/unit -q` | 1402 passed |
| Register | `python -m tests.support.catalog_evidence` | 317 rows; API-084 is `FULL` |
| Lint | `ruff check apps/api tests/unit/api` | clean |
| Format | `ruff format --check` on the changed files | already formatted |

Both new tests were confirmed failing-first: before the change the route
issued two serving statements, and the assertion reported
`the range and the bins must be measured in one statement: 2 were issued`.

### Against a real database

The unit tier asserts rendered SQL, so the three premises this change rests
on were checked against a live PostgreSQL 16 started for the purpose (outside
the repository, in `/var/tmp`), not taken from documentation:

| Claim | Checked | Result |
|---|---|---|
| A value below the low bound is bucket 0 | `width_bucket(1.0, 10.0, 100.0, 5)` | `0` — and `items` never asks for bin 0, which is how the row disappeared |
| A value at the high bound is bucket n+1 | `width_bucket(100.0, 10.0, 100.0, 5)` | `6`, which `LEAST(..., 5)` clamps, as today |
| Equal bounds are rejected | `width_bucket(7.5, 7.5, 7.5, 5)` | `ERROR: lower bound cannot equal upper bound` |

Then the served statement itself, over a fixture table shaped like a latest
relation (one geography with two periods, one with a null value, one at the
maximum):

- The full shape answered `total 3, min 10, max 100, bins {1: 2, 5: 1}` —
  the reduction kept each geography's newest period, the null was excluded
  rather than counted as zero, and the bin counts sum to the total.
- Nothing published answered exactly one row: `total 0`, null bounds, null
  `bin_index`.
- One distinct value answered `total 4, min = max = 7.5, bin 1 = 4` with no
  error, which is the bounds guard doing its job.
- `EXPLAIN` shows `CTE published` materialized once, with `stats` and
  `binned` both scanning it — one evaluation, as intended.

Not run, and why: the Docker-gated tiers (`make test-integration`,
`make test-e2e`, `make test-compose-smoke`) need a container runtime this
environment does not provide. The ad-hoc cluster above covers the database
semantics those tiers would exercise for this route; it does not cover the
warehouse's own relations, which is what the integration tier is for.

## Acceptance criteria, as delivered

1. **Met.** One `db.execute` for the serving read, asserted by
   `test_range_and_bins_are_measured_in_one_statement`.
2. **Met.** Asserted by `test_every_counted_value_is_inside_the_reported_range`
   and, structurally, by the range being measured over the rows that are
   binned.
3. **Met.** `test_degenerate_distributions_are_unchanged` passes unmodified.
4. **Met.** The bounds guard, confirmed against a live database above.
5. **Met.** `test_every_bin_asked_for_is_reported` (API-079) passes
   unmodified.
6. **Met.** `API-084` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `api-unit` CI job, with `AUDITED_COUNTS["API"]` raised to 84.

## Remaining work

- None.
