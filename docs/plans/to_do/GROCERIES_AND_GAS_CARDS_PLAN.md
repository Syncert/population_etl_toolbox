---
id: groceries-and-gas-cards
depends_on:
  - grocery-and-gasoline-prices
  - place-pages
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Groceries and gas cards on place pages

## Status

To do. Split out of
[`GROCERY_AND_GASOLINE_PRICES_PLAN.md`](../needs_review/GROCERY_AND_GASOLINE_PRICES_PLAN.md)
deliverable 9 on 2026-10-07: the warehouse and API half shipped there; the
cards need the county, state and nation pages from `place-pages`, which is
not on `main` yet.

## Why

A resident's first cost-of-living questions are what food and gas cost
where they live. The API now answers them for the areas the providers
publish; a county page has to show the containing area's figure, labelled
as that area, never as the county's own.

## Work items

- [ ] **GC-1: containing areas.** For a county, find the area each measure
  is published for through the shared reference's `contains` edges: its
  CBSA (BEA price parities, `METRO`), its state (EIA, BEA), its Census
  division and region (BLS), and the nation. Never by name.
- [ ] **GC-2: the card.** A "Groceries and gas" block on county, state and
  nation pages: the containing area's regular gasoline price (EIA where EIA
  publishes the area, else BLS's average price for the region), its
  food-at-home change over the year (BLS CPI percent change, never an index
  level across areas), and its price parity (BEA, nation = 100), each with
  the area's name and the source.
- [ ] **GC-3: honesty.** A figure from a larger area says so ("Midwest
  region", "Wisconsin"); a refused or missing figure shows its reason; the
  card names that no source publishes county grocery or gas prices.

## Acceptance criteria

- Every number on the card names the area and source it describes.
- No CPI index level is compared across areas.
- Unit, browser and build gates pass.
