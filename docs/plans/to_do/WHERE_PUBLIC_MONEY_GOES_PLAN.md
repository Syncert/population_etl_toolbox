---
id: where-public-money-goes
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/census_govfin tests/unit/treasury_mts tests/unit/usaspending tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "govfin or treasury_mts or usaspending" -q
  - python -m pytest tests/dags -m dag -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every place shows what its federal, state and local governments spend money on

## Status

To do. Requested by Nick on 2026-11-12 (14:09Z, 14:27Z, 14:28Z): "Are there
publicly available records of what taxpayer money is spent on?", then "find
and source for federal state and local. plan it out fully and implement",
with federal and county the levels he called most interesting, and a size
estimate against the 8 TB drive.

## The three layers

Each layer of government raises and spends its own money, and each passes
some of it down:

| Layer | Raises money mostly from | Spends mostly on | Passes money to |
| --- | --- | --- | --- |
| Federal | income and payroll taxes | Social Security, Medicare, health, defense, interest | states, local governments, people |
| State | income and sales taxes | universities, Medicaid, prisons, highways | school districts, counties, cities |
| Local (county, city, town, school and special districts) | property tax, fees | schools, police, fire, roads, parks, utilities | other local governments |

A county page therefore carries two different figures, never added together:

1. **What the local governments in the county spend**, by category (Census).
2. **Federal money spent in the county**, by agency and kind of award
   (USAspending).

## What is already loaded (checked on OFFICE_LAB, 2026-11-12, dev 7d5c306d)

| Source | Held today | Gap for this ask |
| --- | --- | --- |
| `census_govfin` `cog_units` (Census of Governments unit files 2002, 2012, 2017, 2022) | Every item code of every government in `silver_census_govfin.unit_amount` (7.5 M records, 1.5 GB), including the spending families: 2022 E 379,744 records / 38 codes, F 120,007 / 38, I 51,707 / 5, M 13,608 / 25 (2022 has no G family; 2002-2017 do) | Only revenue, aid and debt measures are built; no spending measure |
| `census_govfin` `asslgf` (`timeseries/govslocalfin`, 2017-2024, nation and states) | Captured responses hold every `AGG_DESC` (Wisconsin 2022: 225 codes, 1,155 rows) | Only 14 revenue codes are mapped; spending codes unread |
| `census_govfin` revenue measures | Own-source revenue, taxes, aid from federal, state and local governments, per county area and state | Already answers "how much of local money comes from local taxes"; served on the "Taxes and public money" chapter |
| BEA CAINC35 / SAINC35 | Federal transfer payments to people by program, county and state | People's receipts, not government spending; kept as is |

Dane County area, 2022, local governments (thousands of dollars, as
stored): elementary and secondary education current spending E12 1,185,484
(16 governments); police E62 186,419; electric utility E92 508,875; welfare
E79 272,927; interest on general debt I89 132,111.

## Sources checked (2026-11-12, live reads on OFFICE_LAB; all HTTP 200)

1. **Census Finance aggregate lines** (`Finance_Aggregate_Lines_2017.xlsx`,
   inside the 2017 unit-file zip already captured): Census's own definition
   of every published line as a list of item codes, sheets `State_Local` and
   `State`. Spending lines used here: line 56 Direct expenditure; lines
   69-112 Direct general expenditure by function (education with higher,
   elementary and secondary, other; libraries; public welfare; hospitals;
   health; social insurance administration; veterans' services; highways;
   airports; parking; ports; police; fire; correction; protective
   inspection; natural resources; parks and recreation; housing and
   community development; sewerage; solid waste; financial administration;
   judicial and legal; general public buildings; other governmental
   administration; interest on general debt; miscellaneous commercial
   activities; other and unallocable); lines 113-118 utility spending (water,
   electric, gas, transit); line 119 liquor stores; lines 120-124 insurance
   trust spending (unemployment, employee retirement, workers'
   compensation, other). Line 55 intergovernmental spending (M, Q, S items)
   is payments to other governments and is not part of a place's direct
   spending.
2. **Census `timeseries/govslocalfin`** (already registered): carries an
   `AGG_DESC_LABEL` attribute (variables/AGG_DESC.json), so spending codes
   are mapped by Census's own label and checked against the aggregate-line
   sums of the unit file, not matched by hand. 2024 is the newest year
   (2024 annual unit file, 4.27 MB, modified 2026-08-04). Annual years
   survey a sample of local governments, so county-area figures exist only
   for Census of Governments years.
3. **Treasury Monthly Treasury Statement, Table 9** (Fiscal Data API,
   `/v1/accounting/mts/mts_table_9`, no key): receipts and outlays by budget
   function, current month and fiscal year to date, record dates
   2015-03-31 to 2026-09-30. 19 outlay functions (National Defense,
   International Affairs, General Science Space and Technology, Energy,
   Natural Resources and Environment, Agriculture, Commerce and Housing
   Credit, Transportation, Community and Regional Development, Education
   Training Employment and Social Services, Health, Medicare, Income
   Security, Social Security, Veterans Benefits and Services, Administration
   of Justice, General Government, Net Interest, Undistributed Offsetting
   Receipts) and the total. FY2026 total outlays $7,395,799,983,313.23;
   Net Interest $1,051,753,969,311.11.
4. **USAspending.gov API v2** (no key): `search/spending_by_geography`
   returns every county (3,234 for FY2024, 3,230 for FY2008) or state in one
   response of about 414 KB, by place of performance, with filters for
   awarding agency and award type; FY2008 onward. `spending_by_category`
   gives one place's split by agency or assistance listing. Dane County
   FY2024 all awards $53,610,988,361.00, of which Medicare Hospital
   Insurance (93.773) $25,185,837,616.35: payments are recorded at the
   place of performance the agency reports, which for Medicare is where the
   claims are processed, so a county figure can include money paid to people
   elsewhere. Shown as published with that note.
5. **BEA NIPA Table 3.16** (bulk `NipaDataA.txt`, 12.1 MB, 2026-11-04):
   all governments combined by function, nation only, 1959-2025. Not used:
   Treasury and Census give each layer separately and more currently.

Considered and left out: USAspending award-level bulk files (hundreds of GB;
the geography endpoint gives the same county totals); OMB historical tables
(whitehouse.gov files, refused automated reads in 2026-11 research); Census
Consolidated Federal Funds Report (ended FY2010).

## Rules that shape the design

- **True to source.** Category names and amounts as Census, Treasury and
  USAspending publish them; Census lines are Census's item-code lists, not a
  grouping of our own. Federal amounts are Treasury outlays (cash paid) and
  USAspending obligations (money committed); the two are never mixed.
- **No extrapolation.** County-area local spending exists only for 2002,
  2012, 2017 and 2022; other years show "Census counts every local
  government only in years ending in 2 and 7", never an estimate. 2007 is
  not registered (recorded gap in the source registry).
- **Separate figures.** Local government spending and federal money spent
  in a county are separate measures, separate cards, never summed.
- **Shares.** A category's share is the category over the same source's
  total for the same place, level and year (ADR-0009 pairing). "All other"
  is the total minus the named lines, as `tax_other` is.
- **Partial coverage.** A share shown when the total is missing or zero is
  withheld with its reason. USAspending's place-of-performance note travels
  with every county and state federal figure.
- **Churn rule.** Replays write only facts whose value changed; a rerun on
  unchanged captures writes zero rows.
- **Lineage.** One lineage row per fact, pointing at its capture.
- **Automation.** Every source is fetched by a scheduled DAG through the
  official API or bulk file; nothing hand-sourced.
- **No subjective words.**

## Slice 1: Local and state spending by category, Census of Governments years (extend `census_govfin`, no new download)

- Registry: one spending measure per aggregate line above, defined by its
  item-code list (codes read from the aggregate-lines file and pinned in the
  registry with a test that every listed code maps to exactly the lines the
  file says), plus `spending_direct_total` (line 56) and
  `spending_direct_general_total` (line 66).
- Replay the stored unit-file captures under a new parser contract version so
  the roll-up computes the new measures for every county area and state; the
  existing measures' facts are unchanged (churn rule), so only new facts are
  written.
- Gold `gold_census_govfin.spending_mix`: each line's share of direct
  spending, same dataset, level, year and place.
- DQ: county-area roll-up of each spending line matches Census's
  `statetypepu` state totals within the documented tolerance
  (DQ-GOVFIN-003 extended); function lines sum to direct general spending.
- Size: about 35 measures x (3,143 county areas + 51 states + nation) x 4
  editions, about 0.45 M facts, about 0.2 GB with indexes.

## Slice 2: State and local spending by category every year 2017-2024 (extend `asslgf`)

- Map govslocalfin spending `AGG_DESC` codes to the slice 1 measures by
  `AGG_DESC_LABEL`, verified against the unit-file aggregate-line sums for
  the 2017 and 2022 census years (the same check that matched LF0008). The
  codes are pinned in the registry (one code per measure), so requests stay
  unchanged and stored captures keep their hashes.
- Measures registered after a capture was published reach it through
  `replay_new_measures` (newest published capture per period, primary-key
  conflicts skipped, so unchanged rows are never rewritten); the next
  scheduled run picks up new years.
- Served for the nation and each state at state, local and combined levels.
- Size: about 35 measures x 52 places x 3 levels x 8 years, about 44 k
  facts, negligible.

## Slice 3: Federal spending by category, nation (new adapter `treasury_mts`)

- Fiscal Data API Table 9, paged (`page[size]` up to 10,000), every record
  date from 2015-03-31; a monthly DAG loads record dates never loaded and
  replays revised months (Treasury restates the prior month).
- Silver `silver_treasury_mts.statement_line` (record date, line code,
  classification id, description, parent, current month, fiscal year to date,
  prior fiscal year to date, each beside its published text) as published;
  `null` stays missing. Control is per page (`control.treasury_mts_page`):
  the whole table is one page of about 4,600 rows.
- Gold `gold_treasury_mts.outlays_by_function`: fiscal-year totals read from
  the September record's fiscal-year-to-date column (never summed from
  months), current fiscal year to date from the newest record, monthly values
  as published; share of total outlays per function.
- Size: about 140 record dates x about 60 lines, under 10 k rows, under 0.05
  GB.

## Slice 4: Federal money spent in each state and county (new adapter `usaspending`)

- For each fiscal year 2008 onward: the year's awarding-agency list
  (`spending_by_category/awarding_agency`, paged), then `spending_by_geography`
  (place of performance) for the state and county layers, cut by all awards,
  each award type group (contracts, grants, direct payments, loans, other
  assistance) and each listed agency. About 19 years x 2 layers x (1 + 5 +
  about 80 agencies), about 3,200 requests, paced at one a second.
  `control.usaspending_request` holds one row per request: a monthly run
  re-reads the current and prior fiscal years (agencies report within 30
  days, defense about 90), and `{"history": true}` reads, oldest year first,
  only the requests never captured, stopping at `max_requests` (1,500) so the
  next run resumes.
- Silver `silver_usaspending.geography_amount` (fiscal year, layer, cut,
  key, shape code, amount beside its published text); replaced and
  withdrawn rows kept in `geography_amount_revision`; the year's agencies in
  `agency_year`. An empty shape code (no recorded place) is kept `unsupported`
  and never served.
- Gold `gold_usaspending.observation_latest` / `observation_revision`
  (all awards and each group, state and county), `awards_by_award_type` and
  `awards_by_agency` with shares of the place's all-award total for the same
  year (or the reason it is not published), and the place-of-performance
  note on every row.
- Size (measured about 320 bytes per silver row with indexes): about 2.2 M
  rows, about 0.7 GB, plus about 0.25 GB of captures; monthly refresh
  revisions up to about 1.2 GB a year.
- Built (2026-11-12): ETL-132, DQ-USASPENDING-001 to -004; unit, database
  and e2e tests replay the fiscal 2024 state answer, Wisconsin county grants
  and the agency list offline.

## Slice 5: "Where the money goes" on the place page and API

- API `/where-money-goes?geo_id=` returning, for the place: federal outlays
  by function (nation only, Treasury), state and local spending by Census
  line (state and nation, yearly 2017-2024; county area, census years), and
  federal awards by agency and type (state and county). Each block states its
  source, year, level and note.
- Web: one card per layer with a share bar of the largest lines and "all
  other", dollars and shares, the layer's year, and the "separate figures"
  line on county pages. Local revenue split (own taxes, state aid, federal
  aid) linked from the existing "Taxes and public money" chapter.
- Speed gate: every read under 500 ms (BEGIN / EXPLAIN ANALYZE / ROLLBACK).

## Order and size

| Slice | New rows | Disk | New download |
| --- | --- | --- | --- |
| 1 Census of Governments spending | ~0.45 M facts | ~0.2 GB | none (stored) |
| 2 Annual state and local spending | ~44 k facts | <0.05 GB | none (stored; labels added next run) |
| 3 Treasury outlays by function | <10 k | <0.05 GB | ~5 MB |
| 4 USAspending county and state | ~2.2 M | ~1 GB, then up to ~1.2 GB a year | ~0.25 GB |
| 5 API and web | views | ~0 | none |

Total about 3-4 GB, outside the census schemas. Drive H on 2026-11-12:
3,584.9 GB used, 4,043.1 GB free (about 7.6 TB); the Docker disk file is
3,581.6 GB, so if its 6 TB cap still applies that is the binding limit at
about 2.4 TB of headroom. One PR per slice, in the order above.

## Acceptance criteria

- Fixture replays offline into gold: two county areas with spending lines
  from E, F, G and I families, a 2022 edition without G items, a line with a
  missing amount shown as missing; a govslocalfin capture with labels; a
  Treasury page with a restated month and a `null` amount; a USAspending
  county and state response with a negative obligation kept as published.
- A rerun over unchanged captures writes zero rows.
- Dane County area 2022 elementary and secondary education spending equals
  the sum of E12, F12 and G12 over its local governments (1,405,622 thousand
  from E12 and F12 as stored on 2026-11-12).
- Every gold read on the place page passes the speed gate on OFFICE_LAB.
- DQ checks pass on the full load; the card renders for Dane County,
  Wisconsin and the nation; unit, DAG, build and browser tiers pass, with
  evidence recorded here.
