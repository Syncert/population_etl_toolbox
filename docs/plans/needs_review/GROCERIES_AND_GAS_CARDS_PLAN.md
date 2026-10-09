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

Ready for review (2026-10-09) on `claude/project-thread-u7eazc`, stacked on
`feat/collapse-8-scouted-sources`. Split out of
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

- [x] **GC-1: containing areas.** For a county, find the area each measure
  is published for through the shared reference's `contains` edges: its
  CBSA (BEA price parities, `METRO`), its state (EIA, BEA), its Census
  division and region (BLS), and the nation. Never by name.
- [x] **GC-2: the card.** A "Groceries and gas" block on county, state and
  nation pages: the containing area's regular gasoline price (EIA where EIA
  publishes the area, else BLS's average price for the region), its
  food-at-home change over the year (BLS CPI percent change, never an index
  level across areas), and its price parity (BEA, nation = 100), each with
  the area's name and the source.
- [x] **GC-3: honesty.** A figure from a larger area says so ("Midwest
  region", "Wisconsin"); a refused or missing figure shows its reason; the
  card names that no source publishes county grocery or gas prices.

## Acceptance criteria

- Every number on the card names the area and source it describes.
- No CPI index level is compared across areas.
- Unit, browser and build gates pass.

## Implementation evidence (2026-10-09)

- **GC-1.** `apps/web/lib/groceriesAndGas.ts` `containingAreas` reads the
  county's state, metro (`METRO`, `cbsa:<5 digits>`), division, region and
  nation from the page's `/catalog/geographies/{geo_id}/related` `part_of`
  rows, by level and code. `readingsFor` builds each identity from those
  codes (`BLS:CUUR0<region><division>0SAF11`, `BLS:APU0<region>0074714`,
  `BEA:MARPP:1`/`BEA:SARPP:1`, `EIA:EPMR`).
- **GC-2.** `apps/web/components/GroceriesAndGas.tsx`, on county, state and
  nation pages: gas (EIA for the state, else BLS's regional average, else
  EIA's nation), food at home as the area's own change from the same month a
  year earlier (`changeOverYear`, no index level shown), and BEA parity for
  the metro, else the state, with the nation 100 by definition.
- **GC-3.** Each line names its area ("Midwest Region figure") and source,
  lists why each more local area did not answer, and the card says no
  source publishes county grocery or gas prices.
- **Upstream fixes found on the way.** (1) `bls_ingest` expanded its silver
  transform over a hard-coded five-program list, so `ap` average prices
  stopped at `silver_bls.observation_revision` and never reached gold; it now
  expands over `CONFIG.programs` (`tests/unit/bls/test_silver_transform_programs.py`,
  ETL-078). (2) `silver_ref.dim_geo_current` gave only tracts and ZCTAs an
  `area_name`, so the catalog named regions, divisions, metros and provider
  areas by their ids; they now carry their names
  (`tests/integration/database/test_area_geography.py`, ETL-075; guide
  `geo_name` paragraph updated).
- Catalog: WEB-143 added; total 621, `AUDITED_COUNTS["WEB"] = 143`.

### Validation

- `npx vitest run tests/frontend/unit/groceries-and-gas.test.js`: 13 passed.
- `npm --prefix apps/web run test:unit`: 852 passed, 1 failed. The failure is
  `explainers.test.js` "refuses HTML", which also fails on the unchanged
  `dev` checkout on this Windows host: the test edits the explainer source by
  matching `"## Short answer
"`, and the checkout has CRLF line endings.
  Not caused by this change.
- `npm --prefix apps/web run lint`: clean. `npm --prefix apps/web run build`: built.
- `npx playwright test places.spec` (includes compare-places): 26 passed,
  including the two new WEB-143 tests (axe clean, no horizontal scroll at 390px).
- `pytest tests/unit/bls tests/unit/shared`: passed (531 + hygiene 385).
- `pytest -m integration tests/integration/database/test_area_geography.py
  test_sub_county_geography.py test_reference_dimensions.py`: 10 passed
  against the disposable test database.

### Not yet shown on the development warehouse

- BLS prices: a `bls_ingest` run with the fix (`bls_prices_reload_20261009`)
  is queued behind the September run, which is waiting out BLS's daily
  request quota.
- Area names: the `silver_ref` DDL and `gold_glossary.refresh_dim_geo_latest`
  must be re-applied; that waits for the ACS load, which holds the reference
  view while it runs.
- The API answers 503 for observation reads while the ACS load saturates the
  warehouse, so live screenshots wait for it.
