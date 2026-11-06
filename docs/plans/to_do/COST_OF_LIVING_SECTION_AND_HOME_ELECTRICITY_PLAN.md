---
id: cost-of-living-section
depends_on:
  - groceries-and-gas-cards
parallel_safe: false
complexity: high
verify:
  - python -m pytest tests/unit/bls tests/unit/eia tests/unit/semantics tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "bls or eia" -q
  - python -m pytest tests/dags -m dag -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every profile has a "Cost of living" section with everyday prices and the minutes of work they take

## Status

To do. Slice 1 of
[`EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md`](EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md).
Nick approved all four slices and the minutes-of-work measure on
2026-11-06 18:15Z ("I'm for all of this ... would also have to include
plans to incorporate it into the existing displays (maybe as an additional
section to the profiles that exist)").

`parallel_safe: false`: it changes the EIA adapter's configuration shape,
which slice 2 (`home-heating-fuel-prices`) builds on, and the place page
that every web plan touches.

## Why

The site shows one small "Groceries and gas" card (gasoline, food-price
change, overall price level). The warehouse already holds dollar prices for
eggs, milk, bread, ground beef, chicken, bananas and coffee back to 1990
(nation and the four regions; live check 2026-11-06), and the HUD rent and
ACS rent people pay where they live. None of it is shown as a price a
person recognises.

## Data work

1. **BLS Average Price items.** Add `72610` (electricity per kWh) and
   `72620` (utility piped gas per therm) to `AVERAGE_PRICE_ITEMS`
   (`src/data_ingestion_toolbox/bls/price_series.py`). Areas stay the
   intersection with BLS's own `ap.series`. History reaches the warehouse
   through the loader's never-loaded-period detection on the scheduled run
   (LOADER RULE), not a manual run. About 75 series: two requests a run.
2. **EIA residential electricity by state.** Generalise `eia/config.py` from
   one `GASOLINE_ROUTE` to a route registry (route, facets, frequency,
   history start, refresh window, citation). Gasoline becomes the first
   entry, unchanged in behaviour; add `electricity/retail-sales`, sector
   `RES`, data `price` (cents per kWh), monthly, from 2001-01, for every
   state and the nation. Silver keeps `(series_id, period, capture_id)`;
   `fact_retail_price` gains a `frequency` column (`weekly`, `monthly`) and
   `period_start` replaces `week_start` through a migration that renames,
   never rewrites, existing rows.
3. **CHURN RULE.** A page whose bytes match the last published capture
   replays nothing (`utility/unchanged.py`). A revision row is written only
   when the value differs from the previous reading of the same period; gold
   refreshes only the series whose rows changed. A rerun over unchanged
   bytes writes zero silver and gold rows (test).
4. **Minutes of work (derived).** A new `time_cost` kind in
   `docs/semantics/derived_ratios.json` and
   `semantics/derived_ratios.py`: price divided by the place's average
   hourly wage, QCEW average weekly wage over 40, same calendar year,
   county or state grain (nation: QCEW U.S. total). It names both inputs and
   both areas. Status `approved` (Nick, 2026-11-06 18:15Z). Pre-computed in
   gold, never in the browser. Where the wage year is not loaded the line
   shows the price only and says why.

## Display: the "Cost of living" section

- A new chapter on county, state, metro and nation profiles, between
  "Work and Money" and "Housing", replacing the `GroceriesAndGas` card
  (`apps/web/components/PlacePage.tsx`). Loaded lazily when the reader
  scrolls to it, so place pages stay inside their bundle budget
  (`apps/web/scripts/bundle-budgets.json`).
- **Eight lines, the same order everywhere:** eggs (dozen), milk (gallon),
  bread (pound), ground beef (pound), chicken breast (pound), regular
  gasoline (gallon), electricity (kWh), 2-bedroom rent (HUD Fair Market
  Rent, month). Later slices add natural gas, heating oil or propane, a
  week of groceries and childcare as further lines in the same section.
- Each line: the dollar price, a "Price / Minutes of work" toggle for the
  whole section (one control, not one per line), the change from the same
  month a year ago, and a five-year same-month sparkline (`lib/sparkline.ts`).
- **Area chain.** Each line takes the most local area that publishes it,
  through the reference's `part_of` rows (`containingAreas` in
  `lib/groceriesAndGas.ts`, extended): county, then metro, state,
  division, region, nation. The area's name sits beside the number
  ("Midwest price", "Wisconsin average"). The section states once, at the
  top, that no source publishes county grocery prices.
- Missing lines say "Not loaded into this site yet" or "Not published for
  this area", never zero. Plain names for every measure in the label
  registry. No CPI index level is shown across areas.

## Acceptance criteria

- Electricity per kWh and utility gas per therm replay offline from fixtures
  for the nation, a region and a metro; an item BLS does not publish for an
  area is not configured.
- EIA residential electricity replays offline for the nation and a state;
  gasoline replays exactly as before; a missing month has no value; the key
  is in no fixture, log or capture.
- A rerun over unchanged bytes writes zero silver and gold rows.
- Minutes of work is served only from an `approved` `time_cost` entry and
  names both areas.
- The section renders eight lines on county, state, metro and nation pages
  with each line's area named; place pages stay inside the bundle budget.
- Unit, integration, DAG, lint, typecheck, build and browser tiers pass,
  with evidence recorded here. After deploy, OFFICE_LAB confirms the new
  BLS items have rows and EIA electricity runs 2001 onwards.
