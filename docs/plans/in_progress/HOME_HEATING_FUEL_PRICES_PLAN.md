---
id: home-heating-fuel-prices
depends_on:
  - cost-of-living-section
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/eia tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "eia" -q
  - python -m pytest tests/dags -m dag -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every profile shows what it costs to heat a home and fill a diesel tank there

## Status

In progress: built, pending review and the OFFICE_LAB speed check of
`gold_eia.observation_latest` before merge. Slice 2 of
[`EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md`](../to_do/EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md),
approved by Nick 2026-11-06 18:15Z. Residential electricity by state moved
here from `cost-of-living-section` on 2026-11-06, so every new EIA route is
built on one generalisation of the adapter.

## Data work

**EIA route registry.** Generalise `eia/config.py` and `eia/capture.py`
from one `GASOLINE_ROUTE` to a registry of routes (route, facets, data
column, frequency, history start, refresh window, unit, citation). Gasoline
becomes the first entry and keeps its request parameters exactly, so its
capture fingerprints and replay are unchanged. The monthly and seasonal
routes land in a new `silver_eia.fact_energy_price` keyed by
`(series_id, period_start, capture_id)` with a `frequency` column; the
75,864 gasoline rows in `fact_retail_price` are not moved or rewritten.
`control.eia_read` gains a `route` column (default the gasoline route), so
each route plans its own window.

Four entries beside gasoline; no new adapter.

| Entry | Route | Grain | Frequency | From |
| --- | --- | --- | --- | --- |
| Residential electricity, cents per kWh | `electricity/retail-sales`, sector `RES`, data `price` | Every state, nation | Monthly | 2001-01 |
| Residential natural gas, $ per thousand cubic feet | `natural-gas/pri/sum`, process `PRS` | Every state, nation | Monthly | 1973-01 (EIA's route metadata) |
| Residential heating oil and propane, $ per gallon | `petroleum/pri/wfr` | About 38 states, PADDs, nation | Weekly, October to March | 1990-10-01 (EIA's route metadata) |
| Retail diesel, $ per gallon | `petroleum/pri/gnd`, product diesel | As gasoline | Weekly | 2015 (warehouse start) |

- **LOADER RULE.** Each entry's period registry is read from the route's
  own facet metadata; a scheduled run loads every period never loaded.
- **Seasonal gaps.** A summer week for heating oil or propane has no row and
  is `not_published`, never zero or missing. A data-quality rule warns if a
  value appears in a month EIA does not survey.
- **CHURN RULE.** Unchanged bytes replay nothing; revision rows only for
  changed values; gold only for changed series.

## Display

The electricity line switches from BLS's regional price to EIA's state
price where EIA publishes the state (cents per kWh, the state named).
Two more lines in the "Cost of living" section: "Natural gas for heating"
(state) and, from October to March, "Heating oil" or "Propane" (state where
EIA publishes it, else PADD), each with its area and same-month change.
Diesel goes on the gasoline line as a second figure, not a new line. Minutes
of work applies per unit (per thousand cubic feet is restated per therm,
with the conversion named in the glossary).

## Acceptance criteria

- Gasoline replays exactly as before the registry (same request parameters
  and fingerprints). Each new route replays offline for the nation and a
  state; a summer heating
  oil week has no row; the key is in no fixture, log or capture.
- A rerun over unchanged bytes writes zero rows.
- The section shows the new lines with their areas; unit, DAG, build and
  browser tiers pass with evidence recorded here.

## As built (2026-11-06)

- **Registry.** `eia/routes.py` holds five entries; gasoline is the first
  and its request, recorded parameters, parser contract and source revision
  are byte-identical to before (unit test). Each route's `history_start` is
  the `startPeriod` EIA's route metadata states (unit test against the
  captured metadata), and later reads go back 14 months or 8 weeks from the
  route's newest published period. The DAG runs one task per route after
  gasoline, through the same `eia_api` pool.
- **Silver.** `silver_eia.energy_price_revision` and
  `silver_eia.fact_energy_price`; `control.eia_read.route` with a CHECK of
  the registered keys. Electricity's Census divisions and the `wfr` wholesale
  rows (`PWR`) are set aside with their reason. `outside_survey_season` marks
  a heating fuel value in April to September; `DQ-EIA-005` warns on it.
  `DQ-EIA-002` counts energy revisions too.
- **Gold.** `observation_revision` is gasoline's branch unchanged plus an
  energy branch, with `route`, `frequency` and `outside_survey_season`
  appended. `observation_latest` is "no newer published read of the same
  series and period" (an anti-join into each fact table) instead of
  `DISTINCT ON`, so a read pinned to one metric and area reaches each fact
  table's index. The publisher states each metric's own unit, time grain,
  source and citation.
- **Display.** Electricity is EIA's state price where published, named on
  its row; minutes of work for electricity still divide BLS's region price
  (the one price with a wage pairing) and name it. New lines: natural gas for
  heating (state, restated a therm at 10.37 therms a thousand cubic feet,
  said on the line), heating oil and propane (state, else the state's PADD
  from its FIPS code, else the nation; same week a year earlier). Diesel is a
  second figure on the gasoline line.

### Decisions taken while building

- **Heating oil and propane lines show all year**, dated, with "EIA surveys
  heating oil and propane prices weekly from October to March" beside them,
  rather than disappearing from April to September. In summer the reader
  sees the last surveyed week of March and its date, not an empty line.
- **Minutes of work for EIA prices are deferred.** A `time_cost` ratio is
  defined over BLS's listed average-price areas; extending it to EIA's state
  and PADD prices needs a numerator per state, a change to the derived-ratio
  validator and service, and an ADR-0009 amendment. Follow-up, not in this PR.
- **Diesel falls back state, PADD, nation**, as heating fuels do.

### Evidence

- Unit: `python -m pytest tests/unit -q` passes, including
  `tests/unit/eia/test_eia_energy_routes.py` (registry, requests, parser on
  real EIA bytes) and the quality inventory.
- Database (local PostgreSQL 16, outside the CI harness): the four
  `tests/integration/database/test_eia_energy_replay.py` tests pass:
  electricity 6 facts, natural gas 6, winter heating fuels 12 (12 wholesale
  rows set aside), summer heating fuels 0, diesel 3; a rerun writes 0 facts
  with `repeated_row_count` 6; a revised month is a second row and latest
  serves one; a July value warns under `DQ-EIA-005`. Gasoline's
  `test_a_second_reading_stores_only_the_weeks_it_revises` passes on the new
  views. CI runs the tier on the full manifest.
- Web: `npm run test:unit` 1,082 passed; `npm run build`; bundle budget
  within every route's limit; `places.spec.js` 35 passed in Chromium,
  including WEB-170.
- Speed gate: pending OFFICE_LAB `EXPLAIN ANALYZE` of
  `gold_eia.observation_latest` by `metric_key` and `geo_id`.
