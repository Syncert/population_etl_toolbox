---
id: trend-charts-a-phone-can-read
depends_on: [place-page-money-and-percent]
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# Trend charts a phone can read

## Status

Built (2026-11-15), awaiting review. Fourth fix from the website UX review
(`docs/product/WEBSITE_UX_REVIEW_2026_11.md`, findings D1, D2, D4). Web
only: no load, source, API route or warehouse object changes, and no figure
changes.

Evidence: `tests/frontend/unit/trend-axis.test.js` (WEB-180) and the trend
assertions in the place browser specs.

## Why

A place page's trend chart labelled its axis with the data's own minimum,
middle and maximum ("99.66", "150.1", "200.53"). On a phone the 640-unit
drawing was scaled to about 300 px, so its 11-unit axis text was about
5 px tall and the lines thinned with it. "Where the money goes" printed
"0.0%" beside $17.8 million.

## What

- `roundTicks` in `apps/web/components/PlaceTrend.tsx`: two to four round
  reference values inside the drawn range ("100, 150, 200"), each with a
  gridline. The lines are drawn to the published values as before; only
  the reference lines moved.
- Axis values are written short ("340M"); every digit stays in the
  "Values drawn" table under the chart.
- The drawing is taller (640 by 300 units, at most 720 px wide). At phone
  width the stylesheet draws axis text at 22 units, about 10 px on a
  390 px screen, and the left margin is sized for that text. Line weight no
  longer scales down with the drawing.
- `shareText` in `apps/web/lib/whereMoneyGoes.ts`: a positive share below
  0.05% reads "under 0.1%". A share of exactly zero still reads "0.0%".

## Not in scope

The index chart's two-sentence explanation (D3) and the one-measure map's
repeated legend (D5) are left for a later pass. A hover read-out on the
lines is not added: the chart is server-rendered and every drawn value is
in its table.
