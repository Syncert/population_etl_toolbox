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

Built (2026-11-08), awaiting review. Slice 4 of
[`EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md`](EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md),
approved by Nick 2026-11-06 18:15Z. Decisions made while building:

- **One release.** `NDCP2022.xlsx` holds study years 2008 to 2022 and
  restates every year of the earlier `nationaldatabaseofchildcareprices.xlsx`
  (2008 to 2018), so only `NDCP2022` is registered: keeping both would store
  2008-2018 twice (storage rule). A later release is added to the registry
  and loads on the next monthly run.
- **Eight prices**, the Women's Bureau's own median weekly full-time
  aggregates (`MCINFANT` ... `MFCCSA`); the 75th percentiles and the 370
  county characteristics are not loaded. A price's imputation code is its
  age bands' five positions combined, kept beside the price.
- **Rows stored only when they change.** A repeated price is counted, not
  stored (CHURN RULE), so a monthly re-read of the unchanged 92 MB workbook
  replays nothing and a restated release adds only the prices it changed.
- **Minutes of work are not shown yet.** Dividing a weekly childcare price
  by a wage is a new kind of figure; it waits for Nick's yes.
- **Not built:** the state-level files the Women's Bureau also publishes
  (`NDCP2022-state-level-estimates-and-rankings.xlsx`,
  `NDCP-State-Level-Estimates-2018-2023.xlsx`, which reaches 2023) could be a
  follow-up for a state figure that is the publisher's own.

Evidence: `tests/unit/dol_childcare` (ETL-123),
`tests/integration/database/test_dol_childcare_capture_replay.py` and
`tests/e2e/test_dol_childcare_pipeline.py` (DB-073),
`tests/frontend/unit/childcare.test.js` and the childcare test in
`tests/frontend/browser/places.spec.js` (WEB-171). Operations:
[`DOL_CHILDCARE_PIPELINE_OPERATIONS.md`](../../user-guides/DOL_CHILDCARE_PIPELINE_OPERATIONS.md).

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

- County page: a "Childcare" block in the "Cost of living" section leading
  with the centre-based infant price per week, then a table of the four age
  groups by care type, each price with a bar on one scale for the whole
  table (built; minutes of work deferred, see Status). Imputed values are
  marked and footnoted once with the reasons their code gives.
- State and nation: the median of their counties is **not** computed here;
  the block says the prices are published for counties only (built). The county map layer uses the quantile bands rule.

## Acceptance criteria

- A fixture workbook replays offline into gold for two counties and two
  years, with an imputed value flagged and a suppressed cell missing.
- A rerun over the unchanged file writes zero rows.
- The county line and age-group bars render; state and nation show the
  "Published for counties" note; unit, DAG, build and browser tiers pass,
  with evidence recorded here.
