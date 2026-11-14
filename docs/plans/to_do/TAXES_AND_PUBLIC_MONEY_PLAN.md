---
id: taxes-and-public-money
depends_on:
  - elections-and-economic-performance
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/bea tests/unit/census_acs tests/unit/irs_migration tests/unit/irs_soi tests/unit/census_govfin tests/unit/api tests/unit/shared -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "bea or irs or govfin" -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every place shows what it pays in tax, what it receives from government, and how that compares with the nation

## Status

To do. Written 2026-11-05 after Nick's request ("more information on the
taxes of different county states and national levels ... economic
ramifications of policies") and his choice of **option B** the same day.

Discovery proposal with sources, options, indicators, caveats and mockups
(invented values): <https://claude.ai/artifact/BJ5y3sdnDkRPhDVvickfUx>
(private to Nick). Mockup images: `/mnt/project-files/taxes/01-03*.png`.

It is not part of v1. Build PRs go through the dev merge queue as the
elections and display PRs did (merged into dev 2026-11-04). The
governor-term columns (T-6) join `gold_elections.seat_term`, so T-6 follows
E-2 of [`ELECTIONS_AND_ECONOMIC_PERFORMANCE_PLAN.md`](../in_progress/ELECTIONS_AND_ECONOMIC_PERFORMANCE_PLAN.md)
(PR #287). T-1 to T-5 do not depend on it and may run in parallel.

## Rules

[ADR-0014](../../decisions/0014-economic-performance-by-party-and-seat.md)
applies unchanged: each measure on its own, a place's change net of the
nation's, peer groups, no composite score or rank, comparisons not causes.
T-6 adds a short fiscal section to it:

- Fiscal years are mapped to the calendar year they end in, and pages say so
  (46 states end June 30).
- County-area government finance is shown only for Census of Governments
  years (ending in 2 and 7); between them Census surveys a sample.
- Where a tax is collected is not who bears it (tourist sales taxes,
  severance taxes, business property tax); the wording says "collected in".
- Transfer reliance reads one way round: payments respond to age and income,
  they do not cause them.
- Tax and transfer changes are drawn in neutral grey, never good or bad
  colours.

## Sources

| Source | Holds | Grain | Years | Licence |
| --- | --- | --- | --- | --- |
| BEA CAINC35 (formerly CA35) | Transfer receipts by program: Social Security, Medicare, public assistance medical care (mostly Medicaid), SSI, EITC, SNAP, unemployment insurance, veterans | County, metro, state | 1969–2024 | Public domain |
| BEA SAINC50, SAINC51 | Personal current taxes, disposable personal income | State | 1948 onward | Public domain |
| BEA CAINC4 line 46 | Dividends, interest and rent | County | 1969–2024 | Public domain |
| ACS B25103 | Median real estate taxes paid | Tract, place, county, state | 5-year 2009 onward | Public domain |
| IRS SOI migration (existing source) | County-to-county returns, people, AGI | County pair | 1990–2023 | US government work |
| [IRS SOI county and state income](https://www.irs.gov/statistics/soi-tax-stats-county-data) | Returns, AGI, wages, income tax, EITC, by AGI band | County (1989–2023), state (1996–2023) | Yearly, late February, 3-year lag | US government work |
| [Census of Governments, finance](https://www.census.gov/data/datasets/2022/econ/local/public-use-datasets.html) | Revenue by tax type, spending, debt per government (Individual Unit Files) | Each government, rolled up to county area | 2002, 2007, 2012, 2017, 2022 | Public domain |
| Census Annual Survey of State and Local Government Finances | Same items | State, state and local combined | Yearly | Public domain |
| [Census QTAX](https://www.census.gov/programs-surveys/qtax/about.html) | State tax revenue by more than 25 types | State, quarterly | 1962 onward | Public domain (Census API `timeseries/eits/qtax`) |

Tax Foundation (CC BY-NC 4.0) and Lincoln Institute FiSC are cross-checks
only, never loaded.

## Deliverables

Each item is one PR, merged in order through the merge queue.

- **T-1. BEA additions.** Add CAINC35, SAINC50, SAINC51 and CAINC4 line 46
  to `src/data_ingestion_toolbox/bea/registry.py`. Derived rates approved as
  provider-first products (ADR-0009): transfer receipts ÷ personal income,
  per program; personal current taxes ÷ personal income; disposable income
  per person, deflated by the national PCE index and, as a second view, by
  SARPP regional price parities. DQ: CAINC35 programs sum to CAINC4 line 47
  within 0.5 percent. Catalog ids; `warehouse_inventory.py` entry updated.
- **T-2. ACS property tax.** Add B25103 to `census_acs/config.py`; derived
  rate median real estate taxes ÷ median home value (B25077). Backfill run on
  OFFICE_LAB across tract partitions (ACS slice rules: >5M-row slices split).
- **T-3. IRS migration back to 1990.** Register year pairs 1990–1991 to
  2017–2018 so the scheduled run loads them (loader rule). DQ unchanged.
- **T-4. IRS SOI income source.** `src/data_ingestion_toolbox/irs_soi/` on
  the source-adapter template: registry of county files 1989–2023 (zips
  before 2011, per-year pages after) and state files 1996–2023; about 20
  curated items by AGI band; capture, silver, gold in the neutral
  observation shape; DAG `irs_soi_ingest`, monthly, loading any registered
  year not yet loaded. Derived rates: income tax ÷ AGI; EITC returns ÷ all
  returns. DQ: county sums within 1 percent of the state total; AGI-band
  shares sum to 100. User guide under `docs/user-guides/`.
- **T-5. Census government finance source.**
  `src/data_ingestion_toolbox/census_govfin/`: annual state finance, Census
  of Governments Individual Unit Files 2002–2022 rolled up to county area
  (a government spanning counties sits in its home county, and the page says
  so), QTAX from the Census API. Measures: tax revenue by type, own-source
  revenue, intergovernmental aid, debt. Derived: state and local tax ÷
  personal income, per person; revenue mix shares. DQ: unit roll-ups within
  1 percent of Census's published state totals. Loader rule as T-4.
- **T-6. API, web and ADR.** Fiscal measures served as ordinary metrics and
  map measures (reads scoped to one place or one grain-year; EXPLAIN on
  OFFICE_LAB before merge; OpenAPI snapshot). Web: a "Taxes and public money"
  group in the Work and money chapter (stacked payments-by-program bar
  against state and nation; federal tax rate trend against the nation;
  property tax load; a "where income comes from" bar of wages,
  dividends-interest-rent and payments), loaded as its own chunk; the
  governor-term table gains tax burden, payments share and tax mix columns
  joined on `gold_elections.seat_term`; transfer reliance and the federal
  tax rate on `/map` with the one-hue scale (#286); tax burden offered as a
  peer group on the lean page (E-3). Glossary entries for adjusted gross
  income, transfer receipts, fiscal year and effective tax rate. ADR-0014
  fiscal section.

## Sizing

| Item | Size |
| --- | --- |
| T-1 | 1–2 days |
| T-2 | 1 day plus the backfill run |
| T-3 | 1 day |
| T-4 | 3–4 days |
| T-5 | 4–5 days |
| T-6 | 5–6 days |

About 3.5 weeks of build. Backfills run on OFFICE_LAB alongside and are not
counted. Data size: curated SOI items about 25 million rows; Census unit
files about 20 million rows before roll-up; BEA and ACS additions under 2
million. Under 10 GB in all.

## Performance (prevention list applied)

- New silver facts get `(metric_code, geo_id)` indexes; ANALYZE after loads.
- County-area roll-ups and derived rates are pre-computed at publish, never
  per request.
- Index DDL pre-built CONCURRENTLY on OFFICE_LAB before deploy.

## Acceptance criteria

- Dane County shows payments by program for 2023 summing to the CAINC4 total,
  and a federal tax rate series 1989–2023 against the nation.
- Wisconsin's state and local tax burden for fiscal 2022 matches Census's
  published state total divided by BEA personal income, within rounding.
- Dane County's 2022 county-area tax revenue equals the sum of its
  governments' unit-file amounts, and the page names the census-year rule.
- The governor-term table shows the three fiscal columns in neutral grey with
  the ADR-0014 wording.
- No view combines measures into a score or rank.

## Tests

Unit tests for registries, parsers, the unit-file county roll-up, derived
rates and fiscal-year mapping; database integration for the new gold
relations; API contract tests; browser tests for the new group, map
measures and term-table columns with fixtures under `tests/frontend/support/`.

## Open questions

- QCEW history back to 2014 should be loaded before T-6 (the term table's jobs
  column uses it); it is scheduled on the 20th.
- Option C (federal spending, SNAP and Medicaid enrolment, opportunity zones,
  minimum wage, policy-event marks, border-county pairs) is recorded for
  later; if taken, start with the Medicaid expansion and minimum wage event
  files.

## Checkpoint

Next pickup: T-1. Read `bea/registry.py` table definitions, add CAINC35 with
its line codes, and write the failing parser test for one county row.

T-4 done (ETL-117, EXT-032): `irs_soi` loads county files 1989–2023 and
Historic Table 2 state files 2020–2023 through DAG `irs_soi_ingest`; guide
`docs/user-guides/IRS_SOI_PIPELINE_OPERATIONS.md`. Found while building it:

- The 1989–2009 county files carry six items only (returns, exemptions,
  AGI, wages, dividends, interest received) and no income tax, credits or
  AGI bands. The county federal tax rate and EITC share therefore start in
  2010. The acceptance criterion "federal tax rate series 1989–2023" holds
  for AGI and returns only; T-6 should show the rate from 2010 and say why.
- Historic Table 2 is CSV only from tax year 2020; 1996–2019 are Excel
  workbooks with their own layouts and are not registered. The nation's
  series starts in 2020 until they are.
- The 2012–2021 county band schemes are registered as eight stubs from the
  2023 guide and checked only by the parser's exact-stub rule; the first
  OFFICE_LAB run confirms them (a mismatch is refused, never served).


T-6 built (ETL-119, API-194, WEB-168; rule DQ-ELEC-006), pending live
checks on OFFICE_LAB:

- Gold views `gold_elections.governor_tenure`, `governor_term_fiscal`,
  `governor_term_tax_mix` and `governor_fiscal_measure_definition`
  (`gold_elections/DDL/governor_fiscal.sql`). Consecutive terms of one
  holder and party are one tenure; each measure's change runs from the year
  before the start to the last full year, minus the nation's change over
  the same years; a tenure still serving ends on the newest year both
  publish. A missing year is a stated reason, never a zero.
- Lean groups gain the peer type `tax_burden` (state quarter of state and
  local tax burden in the election year).
- Routes `GET /public-money` (one nation, state or county with its state
  and the nation) and `GET /governor-terms` (one state). The Work and Money
  chapter shows them on county, state and nation pages; state pages add
  the governor table. Two ratios added: `dividends_interest_rent_share` and
  `transfer_receipts_share`.
- The governor table carries tax and payment columns only. The economy
  columns from the ADR-0014 mockup belong to E-4/E-5, which are not built.
- Live check: `/mnt/project-files/taxes/t6/t6_explain_check.sql` (rolls
  back) times every new read and the forced lean-group refresh.

T-6 live follow-up (2026-11-06, OFFICE_LAB deploy of dev 8ec6ca90):

- `/public-money` and `/governor-terms` answered 503: `api_reader` had no
  USAGE on `gold_elections`, `gold_census_govfin` or `gold_irs_soi`, created
  after the read-only grants last ran at initdb. The grants are now the
  manifest's last asset, re-applied on every `--only-flagged` deploy, and
  `--check` fails on any ungranted gold schema (DB-072).
- Tax mix totals read 72 to 97 percent: the annual survey has no "other
  taxes" aggregate, so "other" is now the total less property, general sales
  and income, and a year missing a named group has no mix.
- Walker and Doyle tax burden (`start_year_not_published`) and every
  payments share: read-only diagnostics in
  `/mnt/project-files/taxes/t6/t6_followup_diagnostics.sql`.
