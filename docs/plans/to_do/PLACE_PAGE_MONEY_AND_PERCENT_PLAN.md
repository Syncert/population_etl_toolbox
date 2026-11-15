---
id: place-page-money-and-percent
depends_on: [place-page-plain-words]
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# Money and percentages are written the way a reader writes them

## Status

Built (2026-11-15), awaiting review. Third fix from the website UX review
(`docs/product/WEBSITE_UX_REVIEW_2026_11.md`, finding B3). Web only: no
load, source, API route or warehouse object changes, and no figure changes.

Evidence: `tests/frontend/unit/value-with-unit.test.js` (WEB-179), and the
place, batch and per-capita browser specs.

## Why

A place page wrote "89,975 Dollars", "9.9 percent", "1,631 dollars per
week" and "590,375 persons", while Starting a family on the same page wrote
"$394,800" and "$1,486 a week". One figure, two notations.

## What

- `valueWithUnit` in `apps/web/lib/explorerViewModel.ts`: one rule for a
  formatted number and its unit. "Dollars" and "US dollars" become a
  leading "$" ("$89,975", "-$412"), keeping what a price is per
  ("$1,631 per week"). "percent" becomes "%", keeping what it is a share
  of ("4.1% of personal income"). "persons" reads "people". Every other
  unit follows the number as published ("thousands of US dollars",
  "percentage points", "Years", "per 1,000 people").
- The number is the one already formatted; nothing is rounded or scaled.
- Used by headline card rows, member-county tables, depth tiles, derived
  values, the standout rows, and both compare views.
- A card's unit line still names the unit ("2024 · Dollars"): the earlier
  UAT decision that a card states its unit where the survey serves none
  stands.

## Not in scope

Large dollar figures in thousands ("51,263,648 Thousands of chained 2017
dollars", review B10) need a rounding rule and are Nick's decision. The
explorer, profiles and use-case tables keep their analyst notation.
