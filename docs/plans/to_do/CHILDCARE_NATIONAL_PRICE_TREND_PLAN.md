---
id: childcare-national-trend
depends_on:
  - county-childcare-prices
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/bls -q
  - ruff format --check . ; ruff check .
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# A county's childcare card says how childcare prices have moved nationally since its study year

## Status

Built (2026-11-11), awaiting review. Nick picked option 2 of the childcare
recency options on 2026-11-10 ("2"): add a newer, true national figure
beside the county's price, never scale or estimate the county's price.

Deployed 2026-11-11 (#351). Follow-up: each index value is shown exactly as
stored (Dane: 407.8 ÷ 338.2), with no decimals added (true-to-source).

Evidence: `tests/unit/bls/test_childcare_price_index.py` (ETL-125),
`tests/frontend/unit/childcare.test.js` and the national-change test in
`tests/frontend/browser/places.spec.js` (WEB-174).

## Why

The Women's Bureau's National Database of Childcare Prices has no release
schedule; its newest study year is 2022, and people care most about recent
years. The county price cannot be brought forward without estimating it,
which the no-extrapolation rule forbids. The Bureau of Labor Statistics
publishes a monthly national price index for day care and preschool to the
current month, so the card can say, as its own labelled line, how childcare
prices have moved nationally since the study year. Same pattern as the
grocery card's region figure: a larger area's own number, named as such.

## Source

- **Series:** `CUUR0000SEEB03`, Consumer Price Index for All Urban
  Consumers, day care and preschool, U.S. city average, not seasonally
  adjusted, monthly. BLS publishes it for the nation only.
- **Access:** the keyed BLS API v2 (`api.bls.gov`) through the existing
  `bls_ingest` DAG, program `cu`. The series is one more curated id in the
  program's existing 50-series chunk, so it costs no extra request against
  the 500-a-day key budget beyond the one-off history re-read below.
  download.bls.gov is not used.
- **Unit:** the gold transform names it from `cu.series`' base period
  (stored list; download.bls.gov's 403 keeps it as stored, #340).

## Data work

- `bls/config.py`: `CUUR0000SEEB03` added to `curated_by_program["cu"]`.
- The program's series fingerprint changes, so the next run re-plans `cu`'s
  history band (1990 to three years back, two 20-year requests) once, then
  skips it again; the rolling window is re-read every run as before.
- No new table, view or DDL; the series is served as `BLS:CUUR0000SEEB03`
  for `us:1` like the other national CPI series.
- `docs/semantics/metric_definitions.json`: a draft definition for the new
  metric (the nation's change, not any county's price).

## Display

- **Childcare card (Cost of living, county pages):** above the table,
  "Nationally, prices for day care and preschool rose X% from <month>
  <study year> to <newest month>." Below the table, the arithmetic: the
  series, both published index values, the division, and that the county's
  prices are the Women's Bureau's for the study year, as published, not
  adjusted by it.
- **The change** is the newest month with a value divided by the same month
  of the study year: two published values, a like-for-like month. A base
  month BLS did not publish is said, and no other month stands in for it.
  Nothing is said when no month after the study year is published.
- **Starting a family:** the infant care row repeats the sentence (taken from
  the card's read, not read again); the footer names the CPI among sources.
- **Wording:** "rose", "fell" or "did not change"; no subjective words.
  A catalog without the series says "not loaded into this site yet".

## Deploy (OFFICE_LAB)

1. `git merge --ff-only` to the merged dev.
2. Python-only change for the scheduler (config list): no restart needed.
   Rebuild web with `--no-deps`. No gold view or index changes, so no
   manifest `--check` is needed.
3. Trigger `bls_ingest` (or wait for the 1st of the month), which loads the
   new series; then, so the new metric gets its `dim_metric` row without
   waiting, run in the scheduler
   `emit_latest_publisher_ready(publisher_schema='gold_bls', requeue=True)`
   and trigger `glossary_harvest`.
4. Check: `/api/v1/catalog/metrics/BLS:CUUR0000SEEB03` answers 200, and Dane
   County's card shows the national line.

## Acceptance criteria

- The county's dollar prices and their hours of work are unchanged.
- The national change is two published values divided, named as the
  nation's, with its series and arithmetic shown.
- No regional, state or county childcare index is requested or estimated.
