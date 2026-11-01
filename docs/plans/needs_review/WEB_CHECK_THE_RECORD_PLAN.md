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

Needs review, 2026-11-01: merged as #245 (c37b8cca), catalog row WEB-156, no
API change and no DDL. The first live check (dev 346786ec) found the state
and national crime spans comparing different months, fixed by #256
(95d7f006). The second (dev 3748996) confirmed that fix and found two display
faults, fixed in the PR that moved this plan here: see "Live check results".
Left for review: after the web rebuild, read each population and
unemployment card's sentences (whole-card captures) against its chart.

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
| How does unemployment here compare with the state? | LAUS, county vs state | LAUS has no national series, so the card says so beside the state's own chart | "No larger area" | Sent to its county's page | LAUS, metro vs principal state |

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
  publishes no margin). LAUS publishes no national series (the nation's
  rate is the Current Population Survey's, `LNS14000000`), so a state's card
  says its rate is not compared with a national rate, and its chart is
  titled with the state alone.

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
- A state against the nation's CPS rate. BLS itself sets state LAUS rates
  beside the CPS national rate, but that is a second series and a second
  read on the state page; a follow-up can add it with a caveat naming both
  surveys.
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

## Live check results

### 2026-11-01, OFFICE_LAB at dev 346786ec

Screenshots: `/mnt/project-files/record-dane.png`, `record-austin.png`,
`record-wisconsin.png`, `record-us.png` (the crime and rent cards).

- **Dane County, crime:** "fell from 20.3 per 100,000 … in December 2015 to
  13 … in December 2025, a change of -7.3 per 100,000 (-35.9 percent)".
  Same month, ten years, both months' agency counts and coverage (98 % and
  93 %: under the 10-point flag, so no "may reflect reporting" line). The
  chart's monthly line ends lower than it starts. Correct.
- **Rents and incomes:** Dane (+53.3 % rent higher than +44.4 % income),
  Austin (+70.8 % against +69.6 %, not distinguishable), Wisconsin (+40.8 %
  lower than +46.9 %), US (+53.6 % higher than +51.0 %). Each sentence's
  values, periods and percents match the arithmetic and the rising chart.
  Correct.
- **Austin, crime:** sends the reader to Travis County's answer. Correct.
- **Wisconsin and US, crime: mismatch.** The FBI's published state and
  national rate is monthly, and the span chose the newest month (June 2023)
  against the earliest month within ten years (January 2013). January
  against June reads the season as a change: the US chart shows a strong
  seasonal pattern. **Fixed:** a monthly series now compares the same
  calendar month (`chooseSpan`, unit test "compares a monthly series with
  the same month of an earlier year"), as the county rate already did.
- **Not yet read:** the population and unemployment cards (below the
  screenshots' fold).

Recheck after the follow-up merges and a web rebuild: `/us/wisconsin#record`
and `/us#record` crime compares June 2013 with June 2023 (or the newest
month's same month), and screenshots of the population and unemployment
cards for Dane, Austin, Wisconsin and the US.

### 2026-11-01, OFFICE_LAB at dev 3748996 (after #256)

- **Wisconsin and US, crime:** "rose from 25.6 per 100,000 residents in June
  2013 to 26.8 in June 2023"; "fell from 36.7 in June 2013 to 33.1 in June
  2023". Same calendar month ten years apart. Dane still December 2015 to
  December 2025. Correct.
- **Austin, unemployment:** "BLS publishes the unemployment rate for
  counties, metro areas, states and the nation, not for cities and towns",
  with the link to Travis County's answer. Correct.
- **US, unemployment:** "The nation has no larger area to compare its
  unemployment rate with." Correct.
- **Dane, unemployment chart:** Dane's monthly line under Wisconsin's from
  1990 to 2026, the 2020 spike in both. Matches its title.
- **Population charts** (Dane 1971–2025, Austin 2020–2025, Wisconsin,
  US): each line rises, as their estimates do.
- **Fault: the value labels were cut off.** The US chart read "1,784,857"
  and "6,826,000" for 341,784,857 and 206,826,000: the left margin fit about
  seven characters. `PlaceTrend` (every chapter's trend, not only these
  cards) now widens its margin to the longest value label.
- **Fault: Wisconsin's unemployment chart promised a line it cannot draw.**
  Titled "Wisconsin and United States" with one line: LAUS publishes no
  national series. The title now names only the places drawn, and the card
  says the state's rate is not compared with a national rate (unit test
  "says LAUS has no national series instead of promising a line it cannot
  draw").
- **Cards with one sentence** stretched to their row's height; the grid now
  aligns them to the top.
- **Not yet read:** the population and unemployment sentences on Dane,
  Austin, Wisconsin and the US (the captures were of the charts alone).

Recheck after the web rebuild: capture each whole card
(`[data-testid=record-population]`, `[data-testid=record-unemployment]`) on
`/us/wisconsin/dane-county#record`, `/us/texas/austin-city#record`,
`/us/wisconsin#record`, `/us#record`, and the US population chart's value
labels in full.
