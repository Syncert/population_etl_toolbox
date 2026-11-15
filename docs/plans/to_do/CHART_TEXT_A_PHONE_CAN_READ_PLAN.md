---
id: chart-text-a-phone-can-read
depends_on: [compare-and-sources-pages-for-a-reader]
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Text inside every place-page chart reads at 10 px on a phone

## Status

Built (2026-11-15), awaiting review. Follow-up to the website UX review
(`docs/product/WEBSITE_UX_REVIEW_2026_11.md`, findings D1, G1, G2) from the
live check of 64ecfb6a at 390 px, which measured chart text at 5.1 px on
the public money lines, 7.3 px on the age charts and 9.2 px on the trend
charts, one "±0.0%" margin, and three controls under the size floor. Web
only: no figure, load, API route or warehouse object changes.

Evidence: `tests/frontend/browser/reading-sizes.spec.js` and
`tests/frontend/unit/place-depth.test.js` (WEB-184).

## Why

A drawing laid out 680 units wide and scaled to a 330 px card draws
11-unit text at about 5 px. Enlarging the text inside the wide layout made
labels collide with bars; the layout itself has to be a phone's width.

## What

- `lib/useNarrowScreen.ts`: true at 640 px and under, after first paint.
  The age pyramid, the age-share difference bars, the cohort chart and the
  public money lines lay themselves out about 350 units wide when it is
  true, so their text is drawn near its written size.
- On a phone the public money lines end in the value alone, in the line's
  colour; the legend above names the colours and the table has every year.
- Trend chart axis text is 26 units on a phone (was 22).
- Trend chart axis text is 14 units on a wide screen (was 11): the live
  check of 371324b6 at 1440 px measured it at 8.6 px in the record cards.
- A margin that would round to "±0.0%" reads "± under 0.1%"; a published
  margin of zero reads "±0%".
- The wordmark badge and the "draft" mark are 12 px; a tick box on a
  place page is 24 px square.

## Not in scope

The explorer's charts (`LineChart`, `BarChart`, `ScatterChart`, heatmaps)
keep their layout: the explorer is an analyst tool reviewed under F3.
