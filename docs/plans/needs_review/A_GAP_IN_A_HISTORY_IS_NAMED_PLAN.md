---
id: a-gap-in-a-history-is-named
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-client-discovers-whether-a-row-can-be-null]
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A gap in a history from a source that serves only numbers is named

## Plan status

- **Status:** Needs review. Investigated, authored and implemented
  2026-09-13. **The web half of API-127, plus the test-harness defect found
  while writing its tests.**
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/components/TimeSeriesChart.js`,
  `apps/web/lib/explorerSources.ts`, `apps/web/lib/api/types.ts`,
  `apps/web/components/SourceExplorerPage.tsx`, `tests/frontend/setup.js`

## Context

API-127 established that four of the seven sources serve only the periods
they published a number for: a period BLS or FRED published without one is
absent from the series rather than present and marked.

`TimeSeriesChart` was already careful about the case it could see. WEB-042
made the horizontal axis time rather than position in the list, so a missing
period opens as spacing instead of a slope across one ordinary interval, and
a counted note says how many periods "published no value and are not
plotted". But that count is `rows.length - series.length` — the rows the API
*sent* with no value. For those four sources it is always zero, so the chart
said nothing at all, and the only evidence of a missing month was a wider
span that reads exactly like an interval the measure moved across.

So a reader had to already know the source's publication shape to read the
chart correctly, which is what the capability now makes unnecessary.

## What was changed

- `ExplorerSource` carries `publishesValueStatus` from the capability's own
  `publishes_value_status`, and the offline fallback declares `false` — which
  is factually right for the source it stands in for (Census ACS).
- `TimeSeriesChart` takes it. Where the source publishes no value state and
  the plotted dates leave an interval at least **half again** the median
  cadence, the chart names it: a period without a published value, not a
  period the measure moved across, and not a zero. Half again rather than
  twice, because calendar months are 28 to 31 days — a skipped month is
  1.94× a 31-day neighbour and would slip under a doubling threshold, while
  February beside January is 1.11× and stays well under this one.
- A source that does publish a value state keeps the counted note only:
  saying both would describe one period twice, and the count names how many.
  A two-point series has one interval and no cadence to compare it against.
  An unknown source claims nothing.

## The harness defect this uncovered

The first draft of the chart tests failed in a way the component could not
explain: a test that passed `publishesValueStatus` **true** found the gap
note in the document. It was the previous test's note.

`@testing-library/react` registers its auto-cleanup only when a global
`afterEach` exists, and this project runs vitest without `globals` — every
test file imports `describe`/`test` explicitly. So nothing unmounted between
tests: one document accumulated every render in a file, and
`screen.getByTestId` could resolve a node an earlier test rendered. A test
asserting that its own render produced something would pass when the render
produced nothing at all.

`tests/frontend/setup.js` now calls `cleanup()` in its `afterEach`. Every one
of the 383 existing frontend tests still passes, so nothing was relying on
the accumulation — which is exactly why nothing had noticed.

## Validation

`tests/frontend/unit/timeseries-chart.test.jsx`:

- a regular monthly cadence is not reported as a gap
- February beside January is not what trips the threshold (the skipped April
  in the same series is)
- a skipped month, and a skipped year in an annual history, are named
- a two-point series carries no cadence
- a source that publishes a value state shows the counted note and not this
  one, and an unpublished period it did send is counted rather than plotted

`tests/frontend/unit/render-isolation.test.jsx` holds the harness: the first
test renders a marker, the second asserts the document no longer holds it.
With `cleanup()` removed, that test fails and so do five of the chart tests:

```text
× component test isolation > the next test does not see the previous test's render
× a history from a source that serves only published numbers > a skipped month is named …
× a history from a source that publishes a value state > the gap note is not shown …
Tests  6 failed | 386 passed
```

## Deliberately not done

- **The chart does not reconstruct the expected calendar.** A metric's
  `valid_time_grains` would let it name *which* periods are missing, but that
  means deciding what a source's cadence implies about every period between
  two dates — revisions, irregular series, a publication calendar the
  warehouse does not carry. The median of the plotted intervals is the
  series' own evidence about itself, and the claim is scoped to what that
  supports: there is a wider span, and for this source a wider span is a
  period without a published value.
- **No marker on the line itself.** A dashed segment across the gap would say
  the same thing more visibly, and it is a design change to the chart rather
  than a statement it was missing; the note is the honest minimum.
