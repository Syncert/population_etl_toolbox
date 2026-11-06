---
id: county-childcare-prices
depends_on:
  - cost-of-living-section
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/dol_childcare tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "childcare" -q
  - python -m pytest tests/dags -m dag -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every county shows what childcare costs there, by age and type of care

## Status

To do. Slice 4 of
[`EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md`](EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md),
approved by Nick 2026-11-06 18:15Z.

## Why

Childcare is the largest monthly cost many families face and one of the
few prices published for every county.

## Source

- U.S. Department of Labor Women's Bureau, National Database of Childcare
  Prices (NDCP), xlsx on `dol.gov`, with a technical guide. County grain,
  study years 2008 to 2022 at last check; median weekly price by care type
  (centre, family home) and child age group (infant, toddler, preschool,
  school age), plus price as a share of family income.
- **OFFICE_LAB check first:** download the current release, record its
  years, column layout and any newer release, and confirm the terms (public
  domain, cite DOL). Some county values are imputed by the Women's Bureau;
  the technical guide says which, and the flag is kept.

## Data work

- New adapter `dol_childcare`. Registry of releases and study years; a
  scheduled run loads every year never loaded (LOADER RULE). A new release
  that restates old years is kept beside the earlier one as a vintage.
- Capture the workbook bytes; an unchanged file replays nothing; a changed
  file writes only rows whose value changed (CHURN RULE).
- Silver by (county, study year, care type, age group, measure) with an
  `imputed` flag and suppressed cells as `provider_missing`, never zero.
  About 1.7 million rows and well under 1 GB.
- Counties resolve by FIPS through the shared reference; an unresolved code
  is ledgered.

## Display

- County page: a "Childcare" line in the "Cost of living" section with the
  centre-based infant price per week and its minutes-of-work figure, and a
  small sorted bar of the four age groups for centre and home care, all on
  one scale. Imputed values are marked once in a footnote.
- State and nation: the median of their counties is **not** computed here;
  the line says "Published for counties" and links to the state's county
  map. The county map layer uses the quantile bands rule.

## Acceptance criteria

- A fixture workbook replays offline into gold for two counties and two
  years, with an imputed value flagged and a suppressed cell missing.
- A rerun over the unchanged file writes zero rows.
- The county line and age-group bars render; state and nation show the
  "Published for counties" note; unit, DAG, build and browser tiers pass,
  with evidence recorded here.
