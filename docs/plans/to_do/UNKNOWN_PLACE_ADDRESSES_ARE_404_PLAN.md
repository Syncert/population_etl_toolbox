---
id: unknown-place-addresses-are-404
depends_on: [chart-text-a-phone-can-read]
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# An address that names no place answers 404, and the bare compare address starts a comparison

## Status

Built (2026-11-16), awaiting review. Finding F6 of the website UX review
(`docs/product/WEBSITE_UX_REVIEW_2026_11.md`), confirmed on the live site
at 35723711: `/us/<made-up>` answered HTTP 200 with a "not found" page and
`/compare/places` answered 404. Web only: no figure, load, API route or
warehouse object changes.

Evidence: `tests/frontend/unit/place-exists.test.js` and
`tests/frontend/browser/cross-level-compare.spec.js` (WEB-185).

## Why

A place page resolved its address in the browser, so the server answered
200 before anyone knew whether the place exists, and a search engine
indexes a 200. `/compare/places` is an address a reader reaches by trimming
a compare link; it named no pair and said only "not found".

## What

- `lib/placeExists.ts`: the web server asks the geography catalog the
  questions the page asks, with the page's own resolvers
  (`resolveStateSegment`, `resolveCountySegment`, `resolveCitySegment`),
  and the state, county/city and metro routes answer `notFound()` when the
  catalog says the address names nothing. Reads are cached for an hour and
  time out at 1.5 s.
- Only a positive "no" is a 404. If the API is down, slow, cut short or
  answers something unexpected, the page renders as before and resolves
  the address in the browser (the same rule as `lib/metroRedirect.ts`).
- `/compare/places` with neither `a` nor `b` shows "Compare two places"
  with the place search and says where the compare links are. A malformed
  pair is still a 404.

## Not in scope

The tract route (`/us/<state>/<county>/<tract>`) keeps resolving in the
browser: a tract list per county is a larger read than this check should
add to every page.
