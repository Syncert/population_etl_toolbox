---
id: compare-and-sources-pages-for-a-reader
depends_on: [place-page-reading-and-touch-sizes]
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# The compare and sources pages fit a phone and speak in words

## Status

Built (2026-11-15), awaiting review. Seventh fix from the website UX review
(`docs/product/WEBSITE_UX_REVIEW_2026_11.md`, findings F4, F8 and the
intro sentence from F3). Web only: no figure, load, API route or warehouse
object changes.

Evidence: `tests/frontend/browser/comparison.spec.js` and
`tests/frontend/browser/find-your-place.spec.js` (WEB-183).

## Why

The compare workspace was wider than a phone screen: a select holding a
long measure name set the width of its grid column, and the rule table had
no scroll frame. "Where the numbers come from" wrote dates as "11/6/2026"
and coverage as codes ("census_division, provider_area"). The explorer
introduced itself with "validate data availability for this MVP".

## What

- `.grid` and the single-column `.selector-grid` use `minmax(0, 1fr)`, so
  a column never grows to its longest option. The compare workspace's rule
  table sits in a `.table-wrap` and scrolls inside it.
- The sources page writes dates as "November 6, 2026" and names every
  coverage level in words (census tracts, metro areas, census regions,
  census divisions, the publisher's own areas); an unknown code is shown
  with spaces in place of underscores.
- The explorer's intro says what the page does.

## Not in scope

The explorer's source tabs as warehouse codes and its tile-status line
(the rest of F3) are an analyst-tool rewrite with their own tests. The
failed `auth/refresh` request for readers who are not signed in (F7) needs
a server-side hint, since the page cannot see the refresh cookie.
