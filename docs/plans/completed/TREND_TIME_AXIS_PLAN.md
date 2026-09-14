---
id: trend-time-axis
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A trend's horizontal axis is time

## Plan status

- **Status:** Accepted 2026-09-14 (Ready for review. Authored, claimed, and delivered 2026-09-13 from an investigation of the history chart.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/components/TimeSeriesChart.js`
- **Depends on:** nothing.
- **Next pickup:** none.

## Context

`TimeSeriesChart` is careful about the thing it is careful about. It refuses
to coerce an unpublished value, and says so in its own comment:

```js
// `Number(null)` and `Number("")` are both 0, and 0 is finite -- so a
// period the source suppressed or never published would join the line at
// zero and the trend would describe a different series than the one the
// source published.
```

Having dropped those periods, it then places every remaining point by its
**index**:

```js
const x = paddingX + (index / (series.length - 1)) * chartWidth;
```

So the gap it was careful not to fill with a zero is closed instead. A series
missing 1980 is drawn with 1979 and 1981 adjacent and evenly spaced, and the
line between them slopes as though the measure moved over one ordinary
interval. The chart labels only the first and last dates, so nothing on
screen reveals the distortion.

This is not hypothetical for this warehouse. `API_CONSUMER_GUIDE.md` spends a
paragraph on exactly such a gap:

> **July 1980 is absent, and left absent.** … the Bureau published no July
> 1980 county estimate in these products. The gap is reported rather than
> interpolated.

The API reports the gap. The chart closes it. And the same happens whenever
the reader drops a suppressed period — the note says "N periods … are not
plotted", and the line is then drawn as if those periods did not exist rather
than as if their values were unknown.

## Objective

A point's horizontal position is its date, so an interval the data does not
cover looks like one.

## Acceptance criteria

1. Where every plotted point carries a parseable date and the series spans
   more than an instant, `x` is proportional to the point's position in time
   between the first and last dates.
2. A gap in the series therefore shows as a gap: two points a decade apart
   are a decade apart on the axis.
3. Where a date cannot be parsed, or every point shares one date, the chart
   still renders — falling back to the even spacing it uses today rather
   than failing or dropping the point.
4. A single point still renders centred, and everything else about the
   component is unchanged: the unpublished-value rejection, the note that
   counts dropped periods, the accessible label, the min/max labels, and the
   per-point tooltips.
5. The behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Drawing a broken line, a gap marker, or a distinct style across a missing
  interval. Spacing the points truthfully is this plan; annotating absence is
  a design question it does not answer.
- Axis ticks, gridlines, or a date scale beyond the two labels already shown.
- Changing which points are plotted.

## Evidence

### The gap, established first

`a gap in the series is a gap on the axis` renders 1979, 1981, 1982 and reads
the `cx` of each plotted circle. It failed first: the three points were
equally spaced, so a two-year interval and a one-year interval measured the
same on screen. It now asserts the first interval is twice the second.

Two companion tests guard what must not change: an evenly spaced series still
comes out evenly spaced, and a series whose dates cannot be parsed — or whose
dates are all identical — still renders both points with finite positions
rather than failing or dropping one.

### What changed

Eleven lines. Each point's `x` is `(t - tFirst) / span` across the plot area
when every date parses and the span is more than an instant; otherwise the
even index spacing the component used before. Nothing else moved: the
unpublished-value rejection, the note counting dropped periods, the
accessible label, the min/max labels, the tooltips, and the single-point
centring are as they were.

### What this deliberately does not do

It spaces the points truthfully; it does not annotate absence. A broken line,
a gap marker, or a distinct style across a missing interval is a design
question, and inventing one here would be this plan answering it by accident.
The dropped-period note continues to state how many periods published no
value.

### Commands

| Command | Result |
| --- | --- |
| `npm --prefix apps/web run test:unit` | 242 passed |
| `npm --prefix apps/web run test:browser` | 61 passed (Chromium) |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web run build` / `check:bundle` | succeeded; every route within budget |
| `python -m tests.support.catalog_evidence` | 308-row register renders; WEB-042 is `FULL` |

### Not run

No smoke tier is implicated: this is geometry over rows already in hand, and
the unit tier reads the rendered SVG directly.

## Remaining work

None.
