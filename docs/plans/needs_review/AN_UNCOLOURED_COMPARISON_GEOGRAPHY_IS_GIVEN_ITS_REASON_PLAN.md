---
id: an-uncoloured-comparison-geography-is-given-its-reason
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-withheld-value-is-not-no-observation]
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
---

# An uncoloured comparison geography is given the reason that applies to it

## Plan status

- **Status:** Needs review. Investigated, authored and implemented
  2026-09-13. **Found while checking whether WEB-078's legend would read
  correctly on the comparison map.**
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/comparison.ts`

## Context

WEB-078 gave the choropleth a legend row naming why a geography carries no
number, in the words the row's `value_status` supplies. The comparison map
feeds that model through `comparisonMapRows`, which supplied one phrase for
every case:

```ts
value_status: usable ? null : "not published on both sides",
```

That phrase is the wrong statement about the row it lands on.
`/comparison`'s SQL joins its two sides on geography — `FROM side_a JOIN
side_b USING (geo_id)` — so every row in the answer *is* on both sides. A
geography published by only one side is not in the answer at all, and the
route reports that separately and honestly as `geographies_a` and
`geographies_b` beside `total` (API-087).

What the route does refuse **inside** the answer is a ratio:

```sql
CASE WHEN side_b.value IS NULL OR side_b.value = 0 THEN NULL
     ELSE side_a.value / side_b.value END AS ratio
```

and of those two conditions only the second can happen today: the aligned
analysis routes accept `analysis_ready` sources, which are exactly the four
that serve published numbers only (API-127). A zero is an ordinary published
value for a count in a small county. So the case a reader actually meets —
a county whose ratio is undefined because the denominator is zero — was
reported as a county one of the two publishers had never published.

## What was changed

`uncolouredReason(row, field)` states the reason that applies:

- a side that published no number → `one side published no number`
- a `ratio` whose `value_b` is zero → `the denominator is zero`
- neither → `not derived for this geography`

The first case is kept deliberately even though it is unreachable today: a
source that publishes a value state becoming analysis-ready would make it
reachable, and a single phrase is exactly how this went wrong the first time.

## Validation

`tests/frontend/unit/comparison.test.js`:

- a zero denominator is named as one, and the same geography's *difference*
  is still published and still coloured
- a side that published no number says so
- the existing map-rows test now asserts the reason that applies to its
  fixture row (whose `value_a` is null) rather than the blanket phrase

With one phrase restored for every case, three tests fail:

```text
→ expected 'not on both sides' to be 'one side published no number'
→ expected 'not on both sides' to be 'the denominator is zero'
```

## Deliberately not done

- **No API change.** `/comparison` could publish each side's value state,
  and there would be nothing to report: its four sources serve published
  numbers only. The row already carries `value_a` and `value_b`, which is
  everything this reason needs.
- **The comparison table is unchanged.** It shows each side's published value
  and the derived columns as they are, so a blank ratio beside a zero
  denominator is visible in the row itself; the map is the surface that had
  to put it in words, because a colour cannot.
