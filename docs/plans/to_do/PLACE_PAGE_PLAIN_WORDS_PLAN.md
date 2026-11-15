---
id: place-page-plain-words
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# A place page says its periods, sources and loading state in a reader's words

## Status

Built (2026-11-15), awaiting review. First fix from the website UX review
(`docs/product/WEBSITE_UX_REVIEW_2026_11.md`, findings A1, A2, B1, B2, B4,
B5). Web only: no load, source, API route or warehouse object changes, and
no figure changes.

Evidence: `tests/frontend/unit/plain-period.test.js`,
`tests/frontend/unit/distinctive.test.js` and the nation and county tests in
`tests/frontend/browser/places.spec.js` (WEB-177).

## Why

The live Dane County page printed 206 database dates ("2020-01-01 –
2024-12-31"), named sources by warehouse code ("CENSUS_SAIPE_SAHIE"), and
opened with three developer status pills ("Place: state:55|county:025").
A resident had to translate each before reading a figure.

## What

- `apps/web/lib/plainPeriod.ts`: a served period rewritten as "2024",
  "2020–2024", "September 2026", "January–March 2026" or "July 1, 2025".
  Only whole years and whole months are shortened; any other span keeps
  both of its dates, in words. Nothing is rounded or reinterpreted.
- Shown on headline cards (subtitle, caption, the "not published for ..."
  cell), chapter footers, depth tiles and industry headings, the tract
  release picker, Starting a family, Groceries and gas, derived values,
  sparkline change lines and the standout rows. `data-period` attributes
  keep the published dates for tests and tools.
- Chapter footers name each source by the display name its capability
  publishes; the code is shown only when none is published.
- One status line replaces the three pills: "Loading the published
  figures…" while reads are in flight, a plain failure line if a read
  fails, and nothing once the page has settled. The three technical lines
  stay in the page, hidden, for diagnosis and the browser tests.
- A card whose catalog name equals its heading no longer prints it twice
  (the name stays for screen readers and hover).
- Among the lowest, a standout row says "Lower than 70 of 71 Wisconsin
  counties" (served `siblings_with_value` minus `siblings_below` minus
  `siblings_tied`), not "Higher than 1 of 71".
- The index chart names its base as the axis does ("1971 = 100").
- "What stands out" moved to its own chunk (`PlaceStandout.tsx`), so the
  place-page bundle is 0.8 kB smaller than before this change.

## Not in scope

Money and percentages written as "$89,975" and "9.9%" (review B3), the
chapter bar and page length (C1, C2), chart axes (D1 to D3) and the
"DRAFT" badges (B7, Nick's decision). See the review's triage.
