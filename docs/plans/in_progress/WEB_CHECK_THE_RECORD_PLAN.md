---
id: web-check-the-record
depends_on:
  - web-city-history
  - web-per-capita-toggle
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# Every place page says what the record shows

## Status

In progress, 2026-11-01: built and tested against fixtures (D-9 of the v1.0
build-out, branch `claude/project-thread-l8lizs`). Waiting for the live check
on the office warehouse after merge. Catalog row: WEB-156. No API change and
no DDL.

## Why

The build-out's definition of done says a reader can "read what the record
shows for the common questions". ADR-0011 (accepted 2026-10-26) fixed the
wording rules; nothing on a place page answered the questions yet.

## What a place page shows now

A "What the record shows" section after the chapters (linked from the
chapter rail as `#record`) asks four fixed questions. Each answer is one or
more sentences generated from templates over published numbers, a chart, the
sources (identities kept in a `data-provenance` record), the caveats, and a
link to the rows it was made from.

| Question | County | State | Nation | City | Metro |
|---|---|---|---|---|---|
| Has violent crime gone up here in ten years? | ADR-0012 monthly rate from agency reports | FBI's own rate and coverage | FBI's own rate and coverage | Sent to its county's page | "Not published or computed for metro areas" |
| Are rents rising faster than incomes? | ACS rent and income | same | same | same | same |
| Is the population growing or shrinking, and why? | PEP estimate + components | same | same | PEP estimate; components on its county's page | PEP estimate where published |
| How does unemployment here compare with the state? | LAUS, county vs state | LAUS, state vs nation ("with the nation") | "No larger area" | Sent to its county's page | LAUS, metro vs principal state |

### The wording rules (ADR-0011), as built

- Only "rose", "fell", "was unchanged", "was higher than", "was lower than"
  and "was not distinguishable from". `FORBIDDEN_WORDS` lists degree, cause
  and judgement words; unit tests and the browser test check every
  generated sentence against them.
- A survey change is "rose" or "fell" only when the two 90 percent intervals
  do not overlap. A value with a Bureau-coded margin gets no verb, and the
  sentence says why.
- The span is the newest period against the earliest no more than ten years
  before it. ACS five-year periods compared share no year (the Bureau's
  advice).
- Crime (county): the newest month with a rate against the same calendar
  month up to ten years before. The rate stays monthly (never rolled up into
  a year). Each month states how many agencies reported and whom the rate
  covers. A coverage difference over 10 percentage points adds "may reflect
  which agencies reported". A refused month reads as its reason, never zero.
- Crime (state, nation): the FBI's published rate and the coverage the FBI
  published for each year.
- Rents and incomes: two series from the same dataset and periods, each
  change stated, then the two percent changes compared using the Bureau's
  ratio margin for each. Neither is divided by the other.
- Population: the PEP estimate's change, then the components the program
  publishes for its newest year (births, deaths, net domestic and net
  international migration). Nothing else is attributed.
- Unemployment: the newest period both places publish, on the same
  seasonal-adjustment basis, compared on the published values (LAUS
  publishes no margin).

### Reads

Everything comes from what the chapters already read: their trend histories
(including the history batch, API-189) and newest values. The one new read is
on county pages only: `GET /api/v1/observations/derived?ratio_id=violent_crime_county_agency_rate&geo_id=<county>&scope=as_released&newest_release_per_period=true&limit=300`.
It reads `gold_fbi.county_agency_rate` filtered by the county's `geo_id`,
product and measure, on the existing `fbi_county_agency_rate_geo_idx
(geo_id, product_id, measure_id, period_start DESC)`. It fires once the
county is resolved, never with a placeholder. **Index DDL: none.**

## Not done

- A separate gallery page. The four question cards on each place page are the
  gallery. A standalone `/record` index can follow if wanted.
- City crime: the FBI publishes agencies, and ADR-0012's rate is for counties,
  so a city's crime question links to its county (as the Safety chapter
  already omits it for cities).

## Evidence (cloud container, 2026-11-01)

- `vitest tests/frontend/unit/check-the-record.test.js`: 28 passed.
- `test:browser` for `check-the-record.spec.js`, `places.spec.js`,
  `city-history.spec.js`, `per-capita-toggle.spec.js`, `place-batch.spec.js`:
  47 passed. Full suites are in the PR.
- `lint`, `typecheck` clean; `check:bundle`: every route within budget. The
  section is loaded on demand, like the trend charts.
- Screenshots (fixture data): `/mnt/project-files/screenshots/check-the-record/`.

## Live check (OFFICE_LAB, after merge and a web rebuild)

1. `/us/wisconsin/dane-county#record`. Four cards. Crime compares a recent
   month with the same month ten years before (Return A is loaded for
   2015–2025), with agency counts and coverage for both months.
2. In the network panel, one
   `/observations/derived?...newest_release_per_period=true` request for
   `state:55|county:025`. Time it (expect well under a second; it is an index
   probe per county). Run `EXPLAIN (ANALYZE, BUFFERS)` on
   `SELECT DISTINCT ON (geo_id, period_start) * FROM gold_fbi.county_agency_rate WHERE product_id = 'return_a_violent_crime' AND measure_id = 'V:offense:absolute_total' AND geo_id = 'state:55|county:025' ORDER BY geo_id, period_start, release_key DESC`
   if it is not. Measured on the current api before merge (2026-11-01,
   during a DQ run): 200 in 0.38 s cold, 13 ms warm.
3. `/us/texas/austin-city#record`. Rents and incomes compare 2010–2014 (or
   the earliest period within ten years that shares no year) with 2020–2024.
   Crime and unemployment link to Travis County's page.
4. `/us/wisconsin#record` and `/us#record`. Crime uses the FBI's rate with
   its coverage for each year. The state compares unemployment with the
   nation; the nation says there is no larger area.
5. Read every sentence on those pages against its chart. None should hold a
   word from `FORBIDDEN_WORDS` in `apps/web/lib/checkTheRecord.ts`.
