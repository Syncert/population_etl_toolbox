---
id: w8-zcta-towns-and-geography-gaps
depends_on: [acs-place-tract-history-backfill]
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/census -q
  - ruff check .
---

# W-8: ZIP code areas, New England towns, and the recorded geography gaps

## Status

To do, after v1.0. Recorded 2026-11-01 when the Wave W history loads
finished on OFFICE_LAB (tract 2009-2024 complete; metro 2024 back to 2015
loaded, 2014-2005 in progress). Nothing here has been built. This note
collects the geography gaps that Wave W left open, so each one has a single
place to start from. It replaces the one-line mentions in the backfill and
map-tile plans.

## What the warehouse holds now (measured 2026-11-01)

- **ACS grains served:** nation, state, county, place, tract and metro
  (CBSA). The backfill driver accepts `place`, `tract`, `metro` and `area`
  only (`census_acs/backfill.py`, `BACKFILL_GEO_LEVELS`).
- **Boundaries** (`silver_ref.dim_geo_geometry_version`): one vintage each.
  County, place, state and tract are 2025, metro is 2023, ZCTA is 2020.
- **ZCTA identities** load in `silver_ref` (`SUB_COUNTY_GEO_TYPES` includes
  `zcta`; `ZCTA_BOUNDARY_VINTAGE = 2020`), but no source publishes data at
  ZCTA grain. So there is no ZCTA layer in the tiles
  (`PLACE_AND_METRO_MAP_TILES_PLAN.md`).
- **County subdivisions** (New England towns, Midwest townships) are not
  loaded at all, neither as identities nor as data.

## The gaps, in the order worth doing them

### 1. ZCTA grain for ACS 5-year

- **Why:** "my ZIP code" is the most common way people name a place, and
  ZCTAs cover the rural areas that place (city and town) grain leaves out.
- **Source:** ACS 5-year publishes `zip code tabulation area` from the
  2011 release, nationally, not nested in a state. Only the 2020 ZCTA
  boundaries are held, and the Census Bureau redraws them once a decade:
  releases 2011-2019 sit on the 2010 ZCTAs, and 2020 onward on the 2020
  ZCTAs.
- **Shape:** add `zcta` to `BACKFILL_GEO_LEVELS` with a narrow table list,
  as the tract grain has (`tract_tables`), and with its own fingerprint so
  widening the list later reloads, rather than skips, the years already done
  (the 2026-10-31 tract lesson). Serve it per state through the ZCTA-to-state
  relationship file, not nationally, so the publish runs on
  `ACS_SERVING_WORKERS` like tract and place.
- **Size (projection, not measured):** about 33,800 ZCTAs, against 85,200
  tracts. With the tract table list, a ZCTA year would be roughly 40% of a
  tract year: about 20 GiB and under an hour on the parallel path. That is
  about 300 GiB for 2011-2024.
- **Needs:** the 2010 ZCTA boundaries for releases before 2020 (TIGER
  `ZCTA5CE10`), or a caveat that those years draw on 2020 shapes, as places do
  now.

### 2. New England towns (county subdivisions)

- **Why:** in Connecticut, Maine, Massachusetts, New Hampshire, Rhode Island
  and Vermont the town is the unit of local government, and most towns are
  not places. A Massachusetts town of 20,000 people has no page today.
- **Source:** ACS publishes `county subdivision` for every state. Limit the
  first load to the six New England states (about 1,500 towns), where the
  geography means something to a reader. Midwest townships can follow.
- **Shape:** a new `cousub` level in `silver_ref` (identities and 2025
  boundaries from TIGER `COUSUB`), then a backfill grain like tract.
  Connecticut needs its 2022 change, below, handled first.

### 3. Historical place boundaries for map vintages

- **Why:** a city map for 2015 draws on 2025 city boundaries, with a caveat
  (Nick's decision, 2026-10-31). A city that annexed land since then is drawn
  larger than the area it was measured on.
- **Source:** TIGER place shapefiles, one per year, 2010-2024.
- **Shape:** load each year's place geometry as its own
  `dim_geo_geometry_version`. The tile server already accepts `?vintage=`
  and returns 204 for a vintage it lacks
  (`PLACE_AND_METRO_MAP_TILES_PLAN.md`, verify step 6). The caveat goes away
  for any year with its own boundary.

### 4. Connecticut's 2022 county change

- In 2022 Connecticut's eight counties were replaced by nine planning
  regions as county equivalents. ACS 2022 onward publishes the regions; 2021
  and earlier publish the old counties.
- **Gaps it leaves:** Connecticut metros before 2024 do not match the
  current delineation, and Connecticut tracts were recoded in 2022, so the
  tract decade crosswalk does not link them across the change.
- **Shape:** a planning-region-to-county relationship in `silver_ref`, and
  a recode table for the 2022 Connecticut tracts (the Census Bureau publishes
  one). Until then, comparisons across 2021 and 2022 in Connecticut are
  refused rather than joined.

### 5. Puerto Rico 1990 long form

- The 1990 loader (`census_acs.load_1990`) reads STF1A and STF3A. Puerto
  Rico's 1990 long-form data were published separately, so Puerto Rico has
  1990 short-form figures only.
- Low priority. Record it as a known gap on the source page until there is a
  reader asking for it.

## Not in scope

- Block groups and blocks. Too small for the margins of error ACS publishes
  at that grain, and too many rows for the serving layer as it stands
  (`THE_WEBSITE_READS_A_SERVING_LAYER_NOT_THE_WAREHOUSE_PLAN.md`).
- School districts, congressional districts and other administrative
  geographies.

## Verify

- `python -m pytest tests/unit/census -q` and `ruff check .` for any code a
  stage adds.
- For each new grain: the ledger shows every slice `success` for every
  year, a sample page renders with the new level in "Compare with", and the
  tile layer for the new level answers inside the tile byte budget.
