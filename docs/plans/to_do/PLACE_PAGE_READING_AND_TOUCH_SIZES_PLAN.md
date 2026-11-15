---
id: place-page-reading-and-touch-sizes
depends_on: [place-page-notes-said-once]
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Reading and touch sizes on a phone place page

## Status

Built (2026-11-15), awaiting review. Sixth fix from the website UX review
(`docs/product/WEBSITE_UX_REVIEW_2026_11.md`, findings G1, G2). Stylesheet
only: no figure, load, API route or warehouse object changes.

Evidence: `tests/frontend/browser/reading-sizes.spec.js` (WEB-182).

## Why

On a 390 px phone the live Dane County page drew about 200 pieces of text
under 12 px (level-strip labels, sparkline end labels, header links) and
about 290 links and toggles under 24 px tall ("compare" links, chapter
links, "Values drawn" toggles).

## What

Rules at the end of `apps/web/app/styles/shell-narrow.css`, so they hold
over each component's own size:

- Header links, section kickers, segmented-control buttons, sparkline end
  labels, level-strip labels and the public-money track labels are
  0.75rem (12 px).
- A link on a place page carries 6 px of vertical padding. On an inline
  link that enlarges what a finger can hit without moving the line of text
  it sits in. A link in a table is an inline block at least 24 px tall.
- A disclosure toggle (`summary`) on a place page is at least 24 px tall.

## Not in scope

The wordmark's badge is a logo. Text inside a chart is sized by the chart
(`TREND_CHARTS_A_PHONE_CAN_READ_PLAN.md`). The explorer, catalog, profiles
and use-case pages keep their sizes; the review measured the place page.
