---
id: bins-are-the-ones-the-api-measured
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The legend's bins are the bins the API measured

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/lib/explorerViewModel.ts`

## Context

`/distribution/bins` publishes each bin whole — `bin_index`, `lower_bound`,
`upper_bound`, `count` — and says why:

> Every bin the caller asked for, including the ones nothing falls into
> (API-079). `GROUP BY` returns no row for an empty bin, and an absent bin
> and a bin holding zero geographies are different statements: the second is
> a fact this query measured, and **reporting it as the first makes every
> consumer rebuild the gaps from min/max.**

`distributionBins` rebuilds the gaps from min/max. It reads `min_value`,
`max_value` and `bin_count`, recomputes every boundary itself, and fills
counts from a map with `|| 0` — discarding `lower_bound` and `upper_bound`
entirely. Proved by feeding it bins whose published bounds are unmistakable
(`999+i`, `9990+i`): the model comes back with `0.1 – 0.22`, `0.22 – 0.34`,
… recomputed from min/max, and the published numbers appear nowhere.

That is a second declaration of the API's binning rule. The reachable
consequence is the degenerate answer the API documents — "one distinct value
stays the single bin closing on itself". Fed the response the API actually
sends for a metric where every geography published the same value:

```
API:    bin_count 5, min 4.2, max 4.2, items [ {1, 4.2, 4.2, count 51} ]
client: 5 bins, all [4.2, 4.2], counts [51, 0, 0, 0, 0]
```

Four bins claiming zero geographies in a range that is one point, invented by
the client out of `bin_count`. And the map disagrees with that legend:
`colorForDistributionValue(4.2, bins)` tests `value < bin.upperBound`, false
for every bin whose upper bound is 4.2, so it falls through to the last bin
and returns the **fifth** colour. Every geography is drawn in the fifth
colour while the legend attributes all 51 of them to the first.

The legend already has the right case for this — `apiBins.length === 1` reads
"All numeric values" — so the one-bin answer was anticipated. The bin model
just never produces it, because it counts bins from `bin_count` instead of
from the bins it was given.

## Acceptance criteria

1. The model's bins are the API's `items`: its `bin_index`, its
   `lower_bound`, its `upper_bound`, its `count`. Nothing is recomputed from
   `min_value`/`max_value`/`bin_count`.
2. The degenerate answer renders as one bin holding every value, and the
   colour the map gives that value is that bin's colour. Legend and map agree.
3. A response whose `items` do not cover `bin_index` 1..N contiguously is
   refused (no bins) rather than gap-filled with zeros. An absent bin is not
   a bin holding nothing, which is API-079's own statement read from the
   other side.
4. The palette guard stays: more bins than the palette has colours renders no
   bins rather than a legend that cannot be coloured.
5. Existing fixtures that model the old shape are corrected to the real
   response, not worked around.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row (WEB-057).

## Non-goals

- Changing the API. It already publishes the bins; the client was not reading
  them.
- The log scale. `buildLogChoroplethModel` deliberately bins in log space
  itself and says so, and it reports `usesDistribution: false`; it is a
  different presentation, not a second copy of the API's bins.

## Validation

**Failing first**, four of the five new nodes (the fifth pins the palette
guard, which already held):

```
FAILED the published bounds are the bounds the legend shows
FAILED one distinct value is one bin, and the map colours it that bin
FAILED a gap in the published bins is refused, never filled with zeros
FAILED a bin with no published bounds is refused
```

The first feeds bins whose bounds are deliberately *not* the equal-width
split of the reported range, because a model that recomputes the rule cannot
tell reading the answer from agreeing with it. The second is the reachable
one: it asserts that the colour the map gives a value is the colour beside
the count in the legend, which is what the old model got wrong.

**Six fixtures were unfaithful, and that is the finding's other half.** Two
frontend unit fixtures and four browser stubs published `items` carrying only
`bin_index` and `count` — no bounds at all — and one of the unit fixtures
reported only its non-empty bins, which API-079 explicitly does not do. So
every test of this model was written against a response the API does not
send, and the recomputation had nothing to contradict it. They were corrected
to the real shape rather than worked around; the six browser failures on the
first run after the change are that drift, caught.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Frontend units | `npm --prefix apps/web run test:unit` | **300 passed** (was 295) |
| Frontend browser | `npm --prefix apps/web run test:browser` | 77 passed (2.0m) |
| Frontend lint / typecheck | `run lint`, `npx tsc --noEmit` | clean |
| Unit | `pytest tests/unit` | 1488 passed |
| Lint / format | `ruff format --check .`, `ruff check .` | clean |

**The declaration that hid it.** `DistributionBin` in `lib/api/types.ts`
named `bin_index` and `count` and nothing else, behind an index signature —
so `lower_bound` and `upper_bound`, both *required* fields of the served
schema, were invisible to every reader of the interface while still
typechecking when accessed. They are now declared, which is why the
recomputation was plausible to write in the first place.

A general fixture-completeness guard — "a fixture literal naming fields of a
served schema must name every field that schema requires" — was considered
and declined: matching object literals to schemas by key overlap is a
heuristic, and a false alarm in a guard is worse than the gap. The
behavioural node covers it exactly where it counts: a bins fixture that omits
the published bounds now makes the model return no bins, which is how the six
browser stubs announced themselves.

**Register.** 357 rows.

## Remaining work

- None.
