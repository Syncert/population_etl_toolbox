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

To do. Slice 2 of
[`EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md`](EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md),
approved by Nick 2026-11-06 18:15Z. Builds on the EIA route registry from
`cost-of-living-section`.

## Data work

Three entries in the EIA route registry; no new adapter.

| Entry | Route | Grain | Frequency | From |
| --- | --- | --- | --- | --- |
| Residential natural gas, $ per thousand cubic feet | `natural-gas/pri/sum`, process `PRS` | Every state, nation | Monthly | 1989 (confirm at implementation) |
| Residential heating oil and propane, $ per gallon | `petroleum/pri/wfr` | About 38 states, PADDs, nation | Weekly, October to March | 1990s (confirm) |
| Retail diesel, $ per gallon | `petroleum/pri/gnd`, product diesel | As gasoline | Weekly | 2015 (warehouse start) |

- **LOADER RULE.** Each entry's period registry is read from the route's
  own facet metadata; a scheduled run loads every period never loaded.
- **Seasonal gaps.** A summer week for heating oil or propane has no row and
  is `not_published`, never zero or missing. A data-quality rule warns if a
  value appears in a month EIA does not survey.
- **CHURN RULE.** Unchanged bytes replay nothing; revision rows only for
  changed values; gold only for changed series.

## Display

Two more lines in the "Cost of living" section: "Natural gas for heating"
(state) and, from October to March, "Heating oil" or "Propane" (state where
EIA publishes it, else PADD), each with its area and same-month change.
Diesel goes on the gasoline line as a second figure, not a new line. Minutes
of work applies per unit (per thousand cubic feet is restated per therm,
with the conversion named in the glossary).

## Acceptance criteria

- Each route replays offline for the nation and a state; a summer heating
  oil week has no row; the key is in no fixture, log or capture.
- A rerun over unchanged bytes writes zero rows.
- The section shows the new lines with their areas; unit, DAG, build and
  browser tiers pass with evidence recorded here.
