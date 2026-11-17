---
id: site-shell-and-home-for-residents
depends_on: [place-page-plain-words]
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# The header, chapter links and home page leave the page to a resident

## Status

Built (2026-11-15), awaiting review. Second fix from the website UX review
(`docs/product/WEBSITE_UX_REVIEW_2026_11.md`, findings A3, A4, C2 and the
sticky bar from C1, E1, E2, E3, F1, F2). Web only: no load, source, API
route or warehouse object changes, and no figure changes.

Evidence: `tests/frontend/browser/site-shell.spec.js` (WEB-178),
`tests/frontend/browser/home-page.spec.js` and
`tests/frontend/unit/catalog-view-model.test.js` (WEB-112, WEB-072).

## Why

On a 390 px phone the header was 148 px tall, wrapped to three rows and
stayed stuck over every page. A county page ran to 52,000 px on a phone
with its chapter links only at the top. "Find your place" opened the
nation's page, which had no search. Below the search, the home page turned
to analysts: a count of "population matches", a featured analysis whose
link opened an empty page, a raw catalog name as a headline, and the
source names printed twice.

## What

- `apps/web/app/styles/shell-narrow.css` (loaded last): below 1,250 px the
  header scrolls away with the page; below 900 px the brand and "Sign in"
  share one row and the navigation takes one more; below 520 px "Where the
  numbers come from" reads "Sources" so the navigation is one row. A menu
  opens across the header, inside the screen.
- A place page's chapter links stay at the top of the screen (under the
  header where it is sticky). On a phone they are one row that scrolls
  sideways inside the bar. A link lands its chapter below the bar.
- The chapter links include "Cost of living" when that card is on the
  page.
- The nation's page carries the home page's place search (its own chunk,
  `PlaceFinder.tsx`).
- The home page keeps the search, the map and the status line. Under them:
  one row of analyst tools, then the sources, counted and named once each,
  with a link to "Where the numbers come from". The signal strip, featured
  analysis, national snapshot and path grid are removed; none of them
  showed a figure the API answered for the reader's place.
- The map's legend says what a click does ("Select a county to open its
  page").
- The Tools menu is no longer marked as the current section on the home
  page.
- The explorer's source tabs wrap inside the page width.

## Not in scope

The compare workspace's 130 px overflow on a phone (F4) is left for the
explorer wording pass. Page length and folding (C1, C4, C5), "DRAFT"
badges (B7), the site name (E4) and the "Use cases" entry (F5) are Nick's
decisions; see the review's triage.
