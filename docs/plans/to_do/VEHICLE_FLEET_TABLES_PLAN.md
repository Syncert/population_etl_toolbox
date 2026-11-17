---
id: vehicle-fleet-tables
depends_on:
  - focus-counties
  - acs-environment-profile-tables
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/census tests/unit/semantics -q
  - python -m pytest tests/dags -m dag -q
  - ruff format --check . ; ruff check .
---

# How many cars each Dane tract has, who parks them where, and what kind they are

## Status

Requested by Nick on 2026-11-17 (survival game thread "Represent real local
cars in the world"). Implemented on branch `claude/project-thread-ykufvr` (one
pull request to `dev`); see "Implementation notes" at the end. It stays in
`to_do/` until the load on the warehouse shows the definition of done.

## Why

The survival game parks cars in its real-place maps. Today every car park gets
the same fixed mix of eight body types and nothing is parked on streets or in
driveways. The game wants the parked fleet to follow the place: how many cars
a block has, whether they sit in a driveway (owners) or on the kerb (renters),
and the car / pickup-and-SUV split. Later, when the game covers the whole of
Dane County, the same numbers decide the drivable fleet.

What is already coming:

- **B08201** (household size by vehicles available) reaches Dane's tracts
  through `focus_counties` in the environment-profile plan
  (`acs-environment-profile-tables`). It gives cars per household. Nothing more
  is needed for that.

What is missing:

1. **B25044 (tenure by vehicles available)** is not a Dane tract table. It
   splits vehicles-per-household by owner and renter, which is what decides
   driveway versus kerb parking on the isthmus (mostly renters).
2. **B25046 (aggregate number of vehicles available)** is not a Dane tract
   table. B08201 tops out at "4 or more", so it cannot give a car count; the
   aggregate can.
3. **Body-type mix by county is not loaded at all.** ACS does not record what
   kind of vehicle people own. WisDOT's DMV Report 28, "Registered by County
   and city/village/town" (calendar and fiscal year, PDF), counts registered
   vehicles per county and municipality by WisDOT vehicle type: AUTO
   (passenger vehicles), TRUK (pick-ups, vans, most SUVs, heavy trucks), CYCL
   (motorcycles, mopeds), TRLR (trailers), AUCY (autocycles). Index page:
   https://wisconsindot.gov/Pages/about-wisdot/newsroom/statistics/veh-info.aspx
   (e.g. rpt-28-cal-25.pdf). Monthly new registrations by county
   (`nvr-<mon><yy>.pdf`) add AUTO / TRUCK / BUS / MOTOR HOME / CYCL columns.
4. **A portable, every-state equivalent.** The game must work in any US
   county. FHWA Highway Statistics table MV-1 (state motor-vehicle
   registrations by type: automobiles, buses, trucks, motorcycles) is
   nationwide and public. It is the fallback where a state publishes no county
   counts.

No public source found gives registrations by **make or model** per county.
WisDOT's bulk record files go through a contractor (Tyler Technologies) and
are out of scope. The game fills make and model from its own body-type tables.

## What changes

- `AcsConfig.curated_tables`: confirm `B25044` and `B25046` are curated (add
  them if not), with metric definitions `CENSUS_ACS:B25044_*` (total, owner and
  renter by 0 / 1 / 2 / 3 / 4 / 5+ vehicles) and `CENSUS_ACS:B25046_001`
  (aggregate vehicles; owner and renter lines too).
- `AcsConfig.focus_counties`, Dane County (55025): `tables` gains `B25044` and
  `B25046` (beside B08201 from the environment-profile plan).
- New source, **WisDOT vehicle registrations by county** (state source,
  Wisconsin only to start): parse Report 28 (calendar year) into one row per
  county and municipality, year and vehicle type. Raw PDF kept in raw capture.
  The PDF text layout is messy: pin column positions with a test against one
  known county row (Dane) and the statewide total.
- New source, **FHWA MV-1** (all states, yearly): registrations by state and
  type, public and private split as published.
- Tests:
  - `tests/unit/census/test_focus_counties.py`: Dane's table set gains the two tables.
  - a parser test for Report 28 on a saved page (Dane's row, the state sum).
  - a parser test for MV-1 (Wisconsin's row matches the published total).

## What the game reads

For each tract that touches the play area, newest ACS 5-year: B08201 lines
1-6, B25044 all lines, B25046 lines 1-3. For the county: the newest Report 28
AUTO / TRUK / CYCL counts for Dane County and the City of Madison; for the
state, MV-1. The game pins them under `Data/population/raw/` at build time.

## Size

Two small tables for about 107 tracts and 16 years: under 30,000 estimates.
Report 28 is a few thousand rows a year; MV-1 is 51 rows by a few types a
year. Negligible.

## Not in scope

- Make and model registrations (no public source).
- Other states' county registration reports. Each is its own source when a
  county there becomes a map; MV-1 covers them meanwhile.
- Serving any of it through the API; the game reads a pinned summary.

## Definition of done

- The verify commands pass.
- After the next load: B25044 and B25046 have values for Dane's tracts in the
  newest 5-year year, and a tract's B25044 total agrees with its B25003
  occupied-unit count.
- Report 28 rows exist for Dane County and its sum over types matches the
  report's Dane total; MV-1 has a Wisconsin row for the newest year.

## Implementation notes

- **ACS.** `B25044` and `B25046` are curated after `B25040` and listed in
  `added_tables`, so the next scheduled ACS run backfills them; Dane's focus
  tables gain both. Curating them for every county, place and metro, as every
  curated table is, adds about 11 million estimates (about 14 GB at the
  measured ~1,300 bytes per fact), beyond the tract-only estimate under
  "Size". This follows the `B25040` precedent. Eighteen metric definitions
  (`B25044_001`-`015`, `B25046_001`-`003`).
- **New source `VEHICLE_REGISTRATIONS`** (`vehicle_registrations` package,
  DAG `vehicle_registrations_ingest`, monthly on the 5th at 15:00 UTC):
  WisDOT's two vehicle-information pages, every calendar-year Report 28 they
  link, and FHWA MV-1 for each year from 2015, each captured before parsing.
  An MV-1 year answering 404 is recorded as not posted. Report 28 is read
  from `pypdf` layout text; a line is kept only when its counts add to its
  printed Sum, and printed totals that disagree are recorded on the
  document, the lines kept as printed. MV-1 is found by its header; FHWA's
  stored decimals (2022 and 2023 estimates) are kept; blanks and dashes are
  not reported, never zero. Silver tables follow the CHURN RULE.
- **Serving.** "Serving any of it through the API" is out of scope, so no
  source-specific route exists. The repository's product-coverage contract
  still requires every product to be discoverable through the provider-neutral
  catalog, so the source has a metric publisher and an observation dispatch:
  `/api/v1/catalog/metrics` and `/api/v1/observations` answer it, and a
  municipality line stays in `gold_vehicle_registrations.wisdot_registration_latest`
  only (it is not a geography the warehouse serves).
- **Deploy.** `pypdf` joins the `airflow` extra, so the scheduler image is
  rebuilt (`make deploy-up`); the manifest gains the silver, gold and
  publisher files; the API is rebuilt for the dispatch. The new DAG arrives
  paused: unpause it, then trigger `glossary_harvest`.
