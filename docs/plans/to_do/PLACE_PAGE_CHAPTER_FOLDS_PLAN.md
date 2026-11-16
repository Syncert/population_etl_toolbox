---
id: place-page-chapter-folds
depends_on: [unknown-place-addresses-are-404]
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
  - python -m pytest tests/unit/deployment
---

# A place page's chapters show their headlines and fold their detail

## Status

Built (2026-11-16), awaiting review. Findings C1, C4 and C5 of the website
UX review (`docs/product/WEBSITE_UX_REVIEW_2026_11.md`). Nick chose "Fold
detail, headlines stay open" on 2026-11-16. Web only, plus one deployment
setting: no figure, load, API route or warehouse object changes.

Evidence: `tests/frontend/browser/place-folds.spec.js` (WEB-186) and
`tests/unit/deployment/test_stack_configuration_contracts.py`.

## Why

A county page ran to about 34,000 px on a phone: every chapter drew its
headline cards, then its charts over time, its table of further figures
and its special sections (age and sex, migration, public money). A reader
looking for one figure scrolled past all of it.

## What

- Each chapter keeps its heading, headline cards and its period and source
  line open. Its charts, further figures and special sections sit in one
  `<details class="place-chapter-more">` under "More about <chapter>".
  Nothing is removed and nothing is read later: the folded content is
  rendered as before, so the browser's find and a link both reach it.
- `components/PlaceFolds.tsx` (its own chunk): one switch, "Open every
  chapter's detail", remembered in this browser (`localStorage`
  `place-detail`); an address or an in-page link that names something
  inside a fold opens that fold and scrolls to it, also when the target is
  drawn after the page arrives; printing opens every fold.
- The not-found view moved to `components/PlaceNotFound.tsx`, its own
  chunk, which keeps the place routes inside their bundle budget (763 bytes
  to spare after this change).
- The browser fixtures stand for a reader who chose "open everything", so
  the existing tests still read the whole page; `installPlaceFixtures(page,
  { folded: true })` is the first visit.
- Deployment: the web container's root is read-only, so Next could not
  write `.next/cache` (seen live after the place routes began caching
  their catalog check: `ENOENT mkdir '/app/.next/cache'`). The base and
  external compose files mount a 128 MB tmpfs there, owned by the image's
  runtime user. The web service must be recreated, not only rebuilt.

## Not in scope

"What the record shows", "Cost of living", "Starting a family" and
"Climate averages" are sections of their own, not chapters, and stay open.
Folding them is a separate call.
