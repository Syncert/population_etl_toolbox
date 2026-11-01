---
id: web-city-history
depends_on:
  - acs-place-grain
  - acs-place-tract-history-backfill
  - place-pages
  - web-plain-labels-everywhere
parallel_safe: true
complexity: medium
verify:
  - python -m pytest -q tests/unit/api/test_history_batch.py tests/unit/api/test_openapi_contract.py
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# A city page reads its own history beside its county, state and the nation

## Status

In progress, 2026-11-01: built and tested against fixtures (D-3 of the v1.0
build-out, branch `claude/project-thread-uefxof`). Waiting for the live check
on the office warehouse after merge. Catalog rows: API-187, WEB-153.

## Why

City history is now in the warehouse. ACS 5-year place data is loaded for
2009–2024, every slice. ACS 1-year place data is loaded for 2005–2024: each
slice is either loaded or "empty", because the Bureau published no 1-year
place rows for it, and 2020 has no standard 1-year release. Decennial 2000
SF1/SF3 is loaded, and 1990 for Madison only.

City pages already used the shared chapter contract, with three-level
cards. What they could not show was history. Austin's median household
income, rent and home value from 2009 to 2024 beside Travis County, Texas
and the U.S., with margins of error, appeared nowhere. A city page also drew
a chart that misled. The Work and Money trend is the unemployment rate, and
BLS does not publish it for cities. So the page drew the county's line first
and labelled it "(this page)".

## What a city page shows now

- **Work and Money:** median household income as a trend, for the city, its
  county (the one holding most of the city's area, from the reference's
  overlap weights), its state and the nation.
- **Housing:** median gross rent and median home value as trends for the same
  four places. The monthly permits trend stays for counties.
- **Margins of error:** each published margin is a bar through its point on a
  level chart, and a `±` beside the value in the table. The Bureau's coded
  margins ("not computed", "open-ended median") read as words and draw no
  bar. The chart says the margins are at 90 percent confidence and that two
  values whose bars overlap may not differ.
- **Five-year periods:** each point is labelled by its first and last year,
  "2020–2024", not by a date range. The chart says neighbouring points share
  four of their five years, and that the Bureau advises comparing periods
  that do not overlap. Dollar measures say each point is in its own period's
  dollars, so part of any rise is inflation.
- **Years not published:** the Bureau's curated tables for a place grow over
  time. Madison has 33 tables in 2009 and 48 from 2020. So a city's line can
  start years after its county's. That period's cell reads "Not published that
  year". The chart names the places whose lines start, end or skip, and says
  nothing is filled in. A period published without a number reads
  "Published without a number". Neither is presented as a gap in the
  warehouse.
- **A trend the city does not publish draws no chart.** Before, a county line
  posed as the city's. It is the same rule for every page: a trend is drawn
  only where the chapter's own place publishes it.

### The seven chapters on a city page

| Chapter | On a city page |
|---|---|
| People | PEP population estimate and its trend, ACS median age, depth counts, age and sex |
| Work and Money | ACS income and depth; median household income trend. BEA, BLS, QCEW and SAIPE cards say "Not published at city or town grain" |
| Housing | ACS rent and home value with trends, and the housing stock depth. Permits say "Not published at city or town grain" |
| Health | ACS health insurance depth. The CDC PLACES and SAHIE cards say they are not published for cities |
| Safety | Omitted with its reason. The FBI publishes agencies, states and the nation, not places, and ADR-0012's computed rate is for counties |
| Land and Farms | Omitted with its reason. NASS publishes counties and states |
| Change | Omitted with its reason. PEP publishes components of change for counties, not places |

All seven appear in the same order on every page. A chapter with no city
source is named in the footer with its reason, never shown empty. Rates for
city agencies (Safety) and city components of change are not in v1.

## API: one request per chapter for its trend lines (API-187)

`GET /api/v1/observations/history/batch?metric_codes=…&geo_ids=…` takes at
most 12 codes and 8 places. Each item is exactly what the single settled read
answers:
`/observations?metric_code=&scope=as_released&newest_release_per_period=true&geo_id=&limit=1000`.
The metrics one released relation holds are one `UNION ALL`, each branch the
single read's reduction with the place bound as `= ANY`. Each pair is bounded
at 1,000 rows, and `total` states the rest. A city page's trend lines were
about sixteen requests (four trends for four places), six in flight. A
chapter's lines from a source with a settled history (ACS among them) are now
one request. If a batch fails, the page falls back to the single reads.

It reads gold relations only (`gold_census.rpt_acs_observations` for ACS),
scoped to the named places, never a whole grain.

**Index DDL: none.** Each ACS branch is an index probe on the existing
`ix_rpt_acs_observations_metric_geo_date (metric_code, geo_id,
observation_date)` in each year partition, the same plan the single read
already uses. Nothing to pre-build on OFFICE_LAB.

## Evidence (cloud container, 2026-11-01)

- `pytest tests/unit/api/test_history_batch.py`: 9 passed. The OpenAPI
  snapshot was regenerated and reviewed: one operation and two schemas added.
- `tests/integration/api/test_dispatch_expressions_execute.py::test_the_history_batch_executes_for_its_source`
  runs against the warehouse schema in `postgres-integration`.
- `npm run test:unit`: 964 passed. `lint` and `typecheck` are clean.
  `check:bundle` passes; `/us/[state]/[county]` is 473.6 of 487 kB.
- `test:browser`: `city-history.spec.js` has 3 passed, plus the full suite
  (see the PR).

## Live check (OFFICE_LAB, after merge and an api + web rebuild)

1. `/us/texas/austin-city`. Work and Money shows a median household income
   trend with four lines: Austin city, Travis County, Texas, United States.
   Its periods run from 2005–2009 to 2020–2024, with bars, and the table
   shows `±` values.
2. Housing on the same page shows rent and home value trends with the same
   four lines.
3. `/us/wisconsin/madison-city`. Any measure whose Madison line starts later
   than Dane County's reads "Not published that year" for those periods.
4. In the browser network panel, the city page sends
   `/observations/history/batch` once per chapter with ACS trends, and no
   single `newest_release_per_period=true` reads for ACS codes.
5. Time one batch for Austin's three ACS trends × four places. Expect well
   under a second, as the single reads it replaces each took.
