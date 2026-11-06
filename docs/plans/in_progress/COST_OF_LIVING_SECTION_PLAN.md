---
id: cost-of-living-section
depends_on:
  - groceries-and-gas-cards
parallel_safe: false
complexity: high
verify:
  - python -m pytest tests/unit/bls tests/unit/semantics tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "bls" -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every profile has a "Cost of living" section with everyday prices and the minutes of work they take

## Status

In progress. Slice 1 of
[`EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md`](../to_do/EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md).
Nick approved all four slices and the minutes-of-work measure on
2026-11-06 18:15Z ("I'm for all of this ... would also have to include
plans to incorporate it into the existing displays (maybe as an additional
section to the profiles that exist)").

Two changes from the first draft, made while building:

- **EIA residential electricity by state moved to slice 2**
  (`home-heating-fuel-prices`). The EIA adapter is built around one weekly
  gasoline route; generalising it once for electricity, natural gas, heating
  oil, propane and diesel together is less risk than doing it twice. Until
  then the electricity line shows BLS's regional price per kilowatt-hour.
- **Minutes of work is its own PR in this plan.** It divides a regional or
  national price by a county's or state's wage, which the derived-ratio
  service (ADR-0009) cannot express today: its pairings share one geography
  and period. It needs a `time_cost` kind in the API, specified below.

`parallel_safe: false`: it changes the place page that every web plan
touches.

## Why

The site showed one small "Groceries and gas" card (gasoline, food-price
change, overall price level). The warehouse already holds dollar prices for
eggs, milk, bread, ground beef, chicken, bananas and coffee back to 1990
(nation and the four regions; live check 2026-11-06), and the ACS rent
people pay where they live. None of it was shown as a price a person
recognises.

## Work items

- [x] **CL-1. BLS Average Price items.** `72610` (electricity per kWh) and
  `72620` (utility piped gas per therm) added to `AVERAGE_PRICE_ITEMS`
  (`src/data_ingestion_toolbox/bls/price_series.py`). Areas stay the
  intersection with BLS's own `ap.series`; a series BLS stopped is not
  requested. History reaches the warehouse through the loader's
  never-loaded-period detection on the scheduled run (LOADER RULE), not a
  manual run; unchanged series write nothing (CHURN RULE). The gold unit is
  read from the item's title with a word boundary, so "Electricity per KWH"
  (no comma before "per") is "U.S. dollars per KWH", not "per unit".
- [x] **CL-2. The "Cost of living" section.** `components/GroceriesAndGas.tsx`
  and `lib/groceriesAndGas.ts`, loaded as their own chunk:
  - One table of everyday prices in a fixed order (eggs, whole milk, white
    bread, ground beef, chicken breast, electricity), each with its dollar
    price, its change from the same month a year earlier and a sparkline of
    its own history. The area and source are stated once in the caption;
    an item answered by another area (the nation, where the region has no
    series) names that area on its row. An item with no series says "Not
    loaded into this site yet."
  - Then the existing lines (regular gasoline, food at home over the year,
    overall price level) and a new **median rent** line: the place's own
    ACS median gross rent, so county, metro, state and nation each show
    their own figure.
  - The section says once that no source publishes county or city grocery
    prices. No CPI index level is shown across areas.
  - Plain names for every new metric in `docs/semantics/metric_definitions.json`.
- [ ] **CL-3. Minutes of work (derived), its own PR.** A `time_cost` kind in
  `docs/semantics/derived_ratios.json`, `semantics/derived_ratios.py` and
  `apps/api/services/derived_ratio_service.py`: a price divided by an
  hourly wage, `QCEW average weekly wage / 40`, for the page's own county or
  state (the nation: the QCEW U.S. total), matching the price's calendar
  year to the wage's (the newest wage year when the price's year has none,
  said so). The entry names both inputs and both areas ("Midwest price,
  Dane County wage") and is `approved` (Nick, 2026-11-06 18:15Z). The
  section gains one "Price / Minutes of work" toggle; where no wage is
  loaded the toggle is absent and the section says why.

## Acceptance criteria

- [x] Electricity per kWh and utility gas per therm are selected from BLS's
  own series list for the nation, a region and a metro; a series BLS stopped
  is not requested (`tests/unit/bls/test_price_series.py`).
- [x] The section renders the everyday prices on county, metro, state and
  nation pages with each figure's area named; the nation's own figure is
  used where a region has none; rent is the place's own; place pages stay
  inside the bundle budget (`tests/frontend/unit/groceries-and-gas.test.js`,
  `tests/frontend/browser/places.spec.js`).
- [x] No horizontal scroll at 390 px; no accessibility violations.
- [ ] Minutes of work is served only from an `approved` `time_cost` entry and
  names both areas (CL-3).
- [ ] After deploy, OFFICE_LAB confirms `72610`/`72620` rows from the
  scheduled BLS run.

## Evidence (2026-11-06, cloud session)

- `python -m pytest tests/unit -q`: 3,217 passed.
- `npm --prefix apps/web run test:unit`: 1,071 passed; lint and typecheck
  clean; `npm run build` and `check:bundle` within every budget (`/us`
  484.6 kB of 495 kB).
- `playwright test places.spec metro`: 33 passed.
- Database integration tiers run in CI (no PostGIS in this session).
