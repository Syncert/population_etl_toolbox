---
id: place-page-notes-said-once
depends_on: [trend-charts-a-phone-can-read]
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# A place page says each note once

## Status

Built (2026-11-15), awaiting review. Fifth fix from the website UX review
(`docs/product/WEBSITE_UX_REVIEW_2026_11.md`, findings C3, B8, B9). Web
only: no load, source, API route or warehouse object changes, and no figure
changes.

Evidence: `tests/frontend/browser/place-notes-once.spec.js` (WEB-181).

## Why

The five-year-estimate note was printed in full under every survey trend
in a chapter, and "Where the money goes" printed its place-of-performance
note under each spending level. Basis lines ("Survey estimate (American
Community Survey)") were bold green, the colour of a link. A card wrote
"confidence lower 88532" beside "±2,947".

## What

- Within one chapter, a note two trend charts share is shown under the
  first chart that is drawn; each later chart links to it ("The notes
  under an earlier chart in this chapter apply here too."). A note about a
  chart's own margin-of-error bars stays with each chart that draws them.
  A chart with nothing drawn does not hold the note.
- In "Where the money goes", a note that two or more levels carry is shown
  once, under all the levels. A note only one level carries stays with it.
- `.place-basis` is body ink at weight 600, not the link colour.
- A card's uncertainty line formats each bound as every other number on
  the page ("confidence lower 88,532"). The fields keep the source's own
  names and none is merged or dropped.

## Not in scope

The same note repeated across different chapters stays: a reader who
opens one chapter by its link should not have to find another.
