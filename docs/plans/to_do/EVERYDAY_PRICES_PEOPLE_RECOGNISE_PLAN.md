---
id: everyday-prices-people-recognise
depends_on:
  - groceries-and-gas-cards
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/bls tests/unit/eia tests/unit/api -q
  - ruff format --check . ; ruff check .
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every place shows what everyday things cost there, in prices and hours of work people recognise

## Status

To do (scoping). Written 2026-11-06 after Nick asked: "Does the EIA have
other consumer prices such as eggs etc? Trying to figure out more cost of
living statistics to incorporate that are relatable to people."

This plan scopes the sources and recommends a first slice. It builds no
loader until Nick picks the slice. Slice 1 is the recommendation; slices 2
to 4 are written so each can become its own plan and PR.

## The short answer

**EIA publishes energy prices only.** Retail gasoline and diesel (weekly),
residential electricity (monthly, by state), residential natural gas
(monthly, by state), and heating oil and propane (weekly, winter only, by
state). It has no eggs, milk or any other grocery.

**Grocery prices in dollars come from BLS Average Price data (`ap`).** BLS
publishes dollar prices for about 70 items, including eggs, milk, bread,
ground beef, chicken, bananas, coffee, gasoline, electricity per kWh and
utility gas per therm, for the nation, the four Census regions and a set of
metro areas. No official source publishes grocery prices for a county or a
city outside those metros.

## What the warehouse already has

Most of the plumbing exists. The gap is mostly in what the site shows and
how it frames it, not in loaders.

| Already built | What it holds | Where | Shown on the site? |
| --- | --- | --- | --- |
| BLS Average Price (`ap`) | Eggs, whole milk, white bread, ground beef, chicken breast, bananas, ground coffee, regular and all-types gasoline; nation, regions, divisions, 23 metros, from BLS's own `ap.series` | `bls/price_series.py` (`AVERAGE_PRICE_ITEMS`), `silver_bls.fact_labor_statistics` (`program = 'ap'`) | **No.** Only gasoline is read by the "Groceries and gas" card |
| BLS CPI (`cu`) subnational | Food at home, gasoline, food, energy indexes for the same areas | `CPI_PRICE_ITEMS` | Yes: food-at-home change over a year |
| EIA weekly retail gasoline | Regular, midgrade, premium, all grades; nation, PADDs, 9 states, 10 cities, from 2015 | `eia/`, `silver_eia.fact_retail_price` (75,864 rows on 11-06) | Yes: gasoline line on the card |
| BEA Regional Price Parities | Overall price level vs the nation (= 100) for states, metros, nonmetro parts of states; goods, housing, utilities, other | `bea` package (`SARPP`, `MARPP`, `PARPP`) | Yes: "Overall price level" |
| HUD Fair Market Rents | Rent for 0 to 4 bedroom units by county / FMR area, each fiscal year | `hud_fmr_il/` | Not on the price card |
| ACS housing costs | Median gross rent (B25064), rent as share of income (B25070), home value (B25077), owner costs share (B25091) | `census_acs/config.py` | Yes, elsewhere (housing pages, "Check the record") |
| FHFA house price index | County annual index | `fhfa_hpi/` | Index only |
| FRED | National CPI-U all items (`CPIAUCSL`) and others | `fred/config.py` | National only |
| BLS QCEW wages | County average weekly wage | `bls_qcew/` | Jobs pages; history still loading |

The card that exists today is `apps/web/components/GroceriesAndGas.tsx`
(three lines: regular gasoline, food-at-home change, overall price level),
built by `GROCERIES_AND_GAS_CARDS_PLAN.md`.

**Live coverage (OFFICE_LAB, 2026-11-06,
`/mnt/project-files/cost_of_living/live_price_coverage.sql`, read-only):**

| Program | Items | Areas | Months |
| --- | --- | --- | --- |
| BLS `ap` groceries | bread `702111`, bananas `711211`, coffee `717311`, milk `709112`, ground beef `FC1101`, chicken `FF1101` | 5 (nation + 4 regions) | from 1990 (milk 1995-07, beef 1998, chicken 2006) to 2026-09 |
| BLS `ap` eggs `708111` | 1 | 4 (one of the five areas has no egg series) | 1990-01 to 2026-09 |
| BLS `ap` gasoline `74714`, `7471A` | 2 | 37 | 1990-01 to 2026-09 |
| BLS `cu` food at home, food, energy, gasoline | 4 | 37 | 1990-01 to 2026-09 |
| BLS `cu` national extras (`SA0`, `SA0L1E`, `SAH1`, `SAM`, `SEHA`, `SEHC`) | 6 | 1 | 1990-01 to 2026-09 |
| EIA gasoline, four grades | 4 | nation, 9 states, 19 PADD/city areas | 2015-01-05 to 2026-11-02 |

So the grocery prices are loaded and current, at nation and region grain
only: BLS publishes no metro egg or milk series in the loaded set. Slice 1
needs no grocery loader work; a county page shows its Census region's price.
Which area lacks eggs is checked when the card is built.

## Candidate series

Grain is the finest area the publisher prints. "Cloud" means a cloud
session can reach the source for development; "OFFICE_LAB" means a live
read-only check from OFFICE_LAB is needed before the contract is trusted. History depths, area counts and item
codes not already in the code are from publisher pages and FRED mirrors
(for example [APU000072610](https://fred.stlouisfed.org/data/APU000072610));
confirm each against the publisher's own metadata at implementation.

### Everyday items

| Series | Grain | Frequency | History | Access | Reach | Status |
| --- | --- | --- | --- | --- | --- | --- |
| BLS AP grocery items (eggs, milk, bread, beef, chicken, bananas, coffee) | Nation, 4 regions; metros for some items | Monthly (nation, regions); metros vary | Most items from 1980 or earlier | BLS API v2, `BLS_API_KEY` | Cloud for `download.bls.gov` metadata (worked in Oct), OFFICE_LAB for volume | **Loaded in code** |
| BLS AP electricity per kWh (`72610`) | Nation, regions, many metros | Monthly | From 1978 | Same | Same | **New: two item codes** |
| BLS AP utility (piped) gas per therm (`72620`) | Nation, regions, many metros | Monthly | From 1978 | Same | Same | **New: two item codes** |
| BLS AP more staples (e.g. rice, potatoes, cheese, ground chuck, orange juice, tomatoes) | Nation, regions | Monthly | Varies by item | Same | Same | Optional, read item list from `ap.item` |
| BLS CPI subnational categories: shelter, rent of primary residence, electricity, motor fuel, medical care | Nation, regions, divisions, metros | Monthly or bimonthly | Decades | Same | Same | Optional: change-over-year only |
| USDA Thrifty Food Plan cost of food (family of four, weekly and monthly) | Nation (plus Alaska and Hawaii) | Monthly report | Decades | PDF reports on `fns.usda.gov` | OFFICE_LAB | New; PDF parsing |
| USDA ERS Food Price Outlook | Nation | Monthly | Forecast plus recent years | ERS xlsx | OFFICE_LAB | Not recommended: national forecasts, not prices |

### Energy (EIA, key `EIA_API_KEY` already in `stack.env`)

| Series | Grain | Frequency | History | Route | Status |
| --- | --- | --- | --- | --- | --- |
| Retail gasoline | Nation, PADDs, 9 states, 10 cities | Weekly | 2015 in warehouse (1990s at EIA) | `petroleum/pri/gnd` | **Loaded** |
| Retail diesel | Same | Weekly | Same | `petroleum/pri/gnd` (product facet) | New facet on existing adapter |
| Residential electricity price (cents per kWh) and average monthly bill inputs | **Every state** | Monthly | 2001 | `electricity/retail-sales` | **New route** |
| Residential natural gas price ($ per thousand cubic feet) | Every state | Monthly | 1980s | `natural-gas/pri/sum` | New route |
| Residential heating oil and propane | About 38 states | Weekly, October to March | 1990s | `petroleum/pri/wfr` | New route; winter gaps are `not_published`, never zero |

Electricity by state is the most local energy price available anywhere,
and it is the line people check on their own bill.

### Housing, care and other big costs

| Series | Grain | Frequency | History | Access | Reach | Status |
| --- | --- | --- | --- | --- | --- | --- |
| HUD Fair Market Rent, 2-bedroom | County / FMR area | Annual (fiscal year) | 1983+ at HUD; warehouse years per registry | HUD User API (token) | OFFICE_LAB | **Loaded**, not on the card |
| ACS median gross rent and rent burden | County, place, tract | Annual (1-year and 5-year) | 2009+ | Census API | OFFICE_LAB | **Loaded** |
| DOL Women's Bureau National Database of Childcare Prices | **County**, by age group and care type | Annual | 2008 to 2022 (check for a newer release) | xlsx on `dol.gov` | OFFICE_LAB | New; strong candidate |
| CMS marketplace benchmark premium (second-lowest silver plan) | County | Annual | 2014+ | Healthcare.gov landscape files | OFFICE_LAB | Later |
| Zillow Observed Rent Index (ZORI) | Metro, county, city, ZIP where listings suffice | Monthly | 2015+ | CSV download | Cloud | **Not recommended yet**: Zillow's terms of use restrict reuse; needs a license read before any load |
| MIT Living Wage, EPI Family Budget | County | Annual | Varies | Web | n/a | **Not recommended**: model outputs under their own licences, not publisher statistics |

## Framing: hours of work, not index points

The design rule is to minimise cognitive load. A price people recognise
beats an index, and a price measured in their own time beats a price.

- **Dollar prices, never index levels across areas.** "A dozen eggs: $3.12
  in the Midwest" is read in one glance. CPI index levels are not
  comparable across areas (each area has its own base) and are never shown
  side by side; CPI is used only for "up 4% from a year ago".
- **Minutes of work.** "A dozen eggs takes 6 minutes of work at Dane
  County's average wage." Computed as price divided by the area's average
  hourly wage (QCEW average weekly wage / 40). This is a derived measure,
  so it follows the derived-ratio approval path used for the T-6 income
  ratios: Nick approves it once before it is served. The label names both
  inputs and both areas ("Midwest price, Dane County wage").
- **A small fixed set, same order everywhere.** Eggs, milk, bread, ground
  beef, chicken, gasoline, electricity, rent. Eight lines a person can scan
  top to bottom, the same on every page, so comparing two places is
  reading the same row twice.
- **One trend per line.** A sparkline of the last five years in the same
  calendar month, with today's value labelled; no axis clutter. Monthly
  series compare the same calendar month, per the existing rule, and are
  never rolled up to annual.
- **Say whose number it is.** A county shows its region's or metro's egg
  price labelled as that area, as the current card already does; electricity
  and natural gas are state figures; rent is the county's own.
- **No invented basket.** "A week of groceries" uses the USDA Thrifty Food
  Plan's published cost for a family of four (slice 3), never a sum of BLS
  items this project chose. A homemade basket would be a number nobody
  else publishes.

## Slices

### Slice 1 (recommended): "Everyday prices" card from data that is already loaded, plus home energy

1. **OFFICE_LAB check: done 2026-11-06** (table above). Grocery `ap` rows
   are current through 2026-09 at nation and region grain.
2. **BLS AP config:** add `72610` (electricity per kWh) and `72620`
   (utility gas per therm) to `AVERAGE_PRICE_ITEMS`. The area set stays the
   intersection with BLS's own `ap.series`. About 60 new series, which is
   two requests per run at 50 series a request, well inside the
   500-a-day key quota; history comes from the loader's existing
   never-loaded-period detection, not a manual run.
3. **EIA residential electricity by state:** a second route in the `eia`
   adapter (`electricity/retail-sales`, residential sector, price). Same
   key, same capture-first path, monthly slices.
4. **Card:** rename the "Groceries and gas" card to "Everyday prices" and
   show eggs, milk, bread, ground beef, chicken, gasoline, electricity and
   2-bedroom rent (HUD FMR, already loaded), each with its area, its
   change from the same month a year ago, and a five-year sparkline.
5. **Minutes of work:** a toggle between "Price" and "Minutes of work",
   served only after Nick approves the derived measure.
6. **Glossary and labels:** plain names in the label registry for every new
   measure; empty lines say "Not loaded into this site yet".

### Slice 2: rest of home energy

Natural gas by state (`natural-gas/pri/sum`), heating oil and propane
(`petroleum/pri/wfr`, winter only) and diesel on the existing gasoline
route. Same adapter, three routes.

### Slice 3: a week of groceries

USDA Thrifty Food Plan monthly cost reports: parse the PDF tables for the
family-of-four weekly and monthly cost, nation only (plus Alaska and
Hawaii). Shown as "A thrifty week of groceries for a family of four:
$X, or N hours of work here". Needs an OFFICE_LAB check that the PDF
layout is stable across years before any parser is trusted.

### Slice 4: childcare

DOL National Database of Childcare Prices, county grain, by child age and
centre or home care. The single largest monthly cost many families face,
and one of the few published at county grain. New adapter; OFFICE_LAB
reads the xlsx first.

## Loader rules every slice follows

- **LOADER RULE.** Each loader keeps a registry of the periods the
  publisher offers (BLS months per series, EIA months or weeks per route,
  USDA report months, NDCP study years). A scheduled run compares the
  registry with what is loaded and loads every period never loaded. No
  manual history runs.
- **CHURN RULE.** A re-read whose bytes match the last published capture
  replays nothing (`utility/unchanged.py`). A changed read writes only the
  rows whose value changed; a revision row is written only when the value
  differs from the previous reading of the same period (the EIA revision
  repeats found on 11-06, 4,176 rows, are the case `#307` fixes). Gold
  refreshes skip unchanged partitions. Reference and metadata rows are
  upserted only when they differ.
- **Missing is not zero.** An off-month for a bimonthly BLS metro, a
  summer week for heating oil, a suppressed childcare cell: no value, a
  reason, never `0`. API null means missing.
- **Storage.** New series are small (slice 1 adds well under 1 GB). No
  duplicate captures.

## Acceptance criteria (slice 1)

- Electricity per kWh and utility gas per therm replay offline from
  fixtures into gold for the nation, a region and a metro; an item BLS does
  not publish for an area is not configured.
- EIA residential electricity replays offline for the nation and a state;
  a month with no value is missing, not zero; the key appears in no
  fixture, log or capture.
- A rerun over unchanged source bytes writes zero silver and gold rows.
- The card shows eight lines on county, state and nation pages, each
  naming its area and source; no CPI index level is compared across
  areas; minutes of work appear only after approval.
- Unit, lint, typecheck, build and browser tiers pass, with evidence
  recorded here.

## Open questions for Nick

1. Which slice first (slice 1 recommended).
2. Approve "minutes of work at the area's average wage" as a derived
   measure (yes / no / different wage source, such as ACS median earnings).
