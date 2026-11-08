# Operating the BEA regional accounts pipeline

The `bea_regional_ingest` DAG captures and publishes the Bureau of Economic
Analysis's regional economic accounts for the nation, every state and every
county: personal income in total and per capita, its major components,
earnings by industry, and county gross domestic product. Every figure keeps
its dollar basis, so a chained-dollar series is never read as current
dollars.

## What is registered

`src/data_ingestion_toolbox/bea/registry.py` names every table and line the
pipeline loads. Each table is one zip at
`https://apps.bea.gov/regional/zip/<TABLE>.zip`. These bulk files need no
credential, unlike the BEA API, so the adapter holds no key.

| Table | What | Lines | Basis |
| --- | --- | --- | --- |
| `CAINC1` | Personal income summary, from 1969 | 1 personal income, 2 population, 3 per capita personal income | current dollars, persons, per-capita current dollars |
| `CAINC4` | Personal income by major component, from 1969 | 35 earnings by place of work, 46 dividends, interest and rent, 47 personal current transfer receipts, 50 wages and salaries | current dollars |
| `CAINC35` | Personal current transfer receipts by program, from 1969 | 1000 all; 2000 from government and its programs 2100 to 2700 (2110 Social Security, 2210 Medicare, 2220 public assistance medical care, mostly Medicaid, 2310 SSI, 2320 EITC, 2330 SNAP, 2400 unemployment insurance, 2500 veterans' benefits, and their subtotals); 3000 from nonprofits; 4000 from businesses; 5000 refundable tax credits | current dollars |
| `CAINC5N` | Earnings by NAICS industry, from 2001 | 81 farm earnings and the NAICS sector lines 100 to 2000 | current dollars |
| `CAGDP1` | GDP summary, from 2001 | 1 real GDP, 3 current-dollar GDP | chained 2017 dollars, current dollars |
| `CAGDP2` | GDP by industry, from 2001 | 1 all industries and the NAICS sector lines | current dollars |
| `SARPP` | Regional price parities by state, from 2008 | 1 all items, 2 goods, 3 housing, 4 utilities, 5 other services | price level, nation = 100 |
| `MARPP` | Regional price parities by metropolitan area, from 2008 | the same five lines | price level, nation = 100 |
| `PARPP` | Regional price parities by state metropolitan and nonmetropolitan portion, from 2008 | the same five lines | price level, nation = 100 |
| `SAINC35` | Transfer receipts by program by state, from 1929, read from `SAINC.zip` | the `CAINC35` lines, plus 2221 Medicaid and 5200 the Additional Child Tax Credit | current dollars |
| `SAINC50` | Personal current taxes by state, from 1948, read from `SAINC.zip` | 10 personal income; 60 personal current taxes; 70 federal (80 income taxes net of refunds, 81 gross, 82 refunds); 120 state (130 income, 150 motor vehicle licences, 160 other); 180 local (190, 200, 210); 230 state and local personal property taxes | current dollars |
| `SAINC51` | Disposable personal income by state, from 1948, read from `SAINC.zip` | 51 disposable personal income (millions), 53 per capita disposable personal income | current dollars, per-capita current dollars |
| `MAIRPD` | Implicit regional price deflators by metropolitan area, from 2008, read from `MARPP.zip` | 1 implicit regional price deflator | price index, 2017 = 100 |

Each zip holds one every-area CSV,
`<TABLE>__ALL_AREAS_<first>_<last>.csv` (the price parity zips:
`SARPP_STATE_`, `MARPP_MSA_`, `PARPP_PORT_`; the deflator `MAIRPD_MSA_`
inside `MARPP.zip`; `SAINC35__ALL_AREAS_`, `SAINC50__ALL_AREAS_` and
`SAINC51__ALL_AREAS_` inside `SAINC.zip`, which also holds BEA's other state
tables and each table only ever reads its own member), with one column per year and a
footer that states the release (`Last updated: February 5, 2026`). A file
without that footer is refused whole: its rows are quarantined as
`release_date_missing` and nothing is published from it.

Cells BEA does not publish are codes, not numbers. Each keeps its status
and carries no value:

| Code | `value_status` |
| --- | --- |
| `(D)` | `withheld`, to avoid disclosing an individual business |
| `(NA)` | `not_available` |
| `(NM)` | `not_meaningful` |
| `(L)` | `below_threshold` |

Each of these is counted out of scope, not loaded:

- lines not registered above;
- BEA regions (`91000` to `98000`);
- BEA's combined areas: the Virginia independent cities merged with their
  surrounding county (`51901` to `51958`) and Kalawao merged into Maui
  (`15901`). They are BEA's own geography, not counties. Their cells are
  still read, to sum metro areas from (below).

## Transfer receipts, personal taxes and after-tax income

Added for the taxes-and-public-money plan (T-1). What these figures are and
are not:

- Transfer receipts are counted where the recipients live. Medical benefits
  (Medicare, Medicaid) are income of the people whose care they pay for.
- Refundable tax credits (line 5000) overlap the programs: the EITC is line
  2320 and inside line 5000. Never add 5000 to the program lines.
- `SAINC50` personal current taxes are income, licence and personal property
  taxes. Sales, excise and business property taxes are not in them.
- `SAINC50` also repeats disposable income, population and per capita lines
  that `SAINC51` and `CAINC1` hold; those repeats are counted, not loaded.
- BEA publishes the county program detail later than the county totals. The
  `CAINC35.zip` read on 2026-11-05 ended in 2022 (released November 16, 2023)
  while `CAINC4` ran to 2024 (February 5, 2026). The state table `SAINC35`
  runs to 2025 (September 30, 2026). The weekly run picks up a new county
  file as soon as BEA posts one.

Rates divide by personal income of the same program and release, as
ADR-0009 requires (`docs/semantics/derived_ratios.json`):
`transfer_share_<program>_county` divides `CAINC35` by `CAINC1:1`,
`transfer_share_<program>_state` divides `SAINC35` by `SAINC50:10`, and
`<level>_share_of_income` divides `SAINC50` lines 60, 70, 120 and 180 by
`SAINC50:10`. While the county program file and the county income file come
from different releases, a county share on `/observations/derived` says
`different_period` rather than dividing one vintage by another. The
"Payments from government and others" card on `/public-money` instead pairs
the county files by year (Nick's call, 2026-11-08): the newest year both
files publish, which is the last year of `CAINC35`, divided by that year's
personal income from the newer `CAINC1`. The card names both release dates.

`gold_bea.disposable_income_real` serves `SAINC51:53` per state and for the
nation in current dollars and in 2017 dollars two ways: by the national PCE
price index (the mean of FRED PCEPI's twelve months; a year with fewer has no
figure, and the annual index itself is never served), and by the PCE index
and the state's all-items price parity (`SARPP:1`, 2008 on). Each row names
both release dates.

```sql
SELECT geo_id, year, current_dollars, national_prices_2017_dollars,
       regional_prices_2017_dollars
FROM gold_bea.disposable_income_real
WHERE geo_id IN ('us:1', 'state:55') AND year >= 2020
ORDER BY geo_id, year;
```

## Metro areas summed from counties

BEA publishes no metro rows for its county income and GDP tables: the bulk
zips hold none, and the Regional API refuses `GeoFips=MSA` for them
(checked 2026-10-30). BEA builds its own metro figures by adding up each
metro's counties, so every county table's replay does the same, in the same
transaction, for each CBSA of the newest delineation the reference holds
(`silver_ref`'s `census_cbsa_delineation` relationships), applied to every
year as BEA applies the current delineation to its whole history.

- Current-dollar and persons lines are summed. Per capita personal income
  (`CAINC1:3`) is the metro's summed income over its summed population, in
  whole dollars. Chained-dollar lines (`CAGDP1:1`) and price lines are never
  derived.
- A county BEA folds into a combined area counts through that area when all
  of the area's counties are in the metro (Charlottesville's metro holds
  Albemarle and Charlottesville city, so it counts `51901`). A combined area
  split across a metro's edge leaves that metro with no figure.
- A metro has a value only when every member area has a valid one; otherwise
  it is `not_available` with no number, and `value_source` says how many
  members had none. A partial sum is never served.
- Derived rows carry `derivation` (`sum_of_member_counties` or
  `ratio_of_member_county_sums`) through `gold_bea.observation_revision`;
  it is NULL on every figure BEA published.

Run `silver_ref` before the first BEA run on a new warehouse: with no
delineation loaded, a county table adds no metro rows.

The price parity tables describe other areas. `MARPP` rows are CBSAs by OMB
code (`cbsa:<code>`, OMB bulletin 23-01, the delineation the shared
reference loads), and `PARPP` rows and the nation's nonmetropolitan portion
(`00999`) are BEA's own areas (`area:bea:<code>`), which the `silver_ref`
DAG loads from the same two zips (`load_provider_areas`). Run `silver_ref`
first: a parity whose area the reference does not hold is served
`unmapped`. In `PARPP`, `0.000` is BEA's mark for a portion with no counties
(Delaware, the District of Columbia, New Jersey and Rhode Island have no
nonmetropolitan county): it is `not_meaningful` with no value.

## Schedule and scope

The DAG runs at 14:00 UTC every Wednesday. BEA releases county personal
income each November and county GDP each December, and revises earlier years
with every release. A run that finds the same bytes as the table's last
published capture records that table `unchanged` in
`control.bea_table_capture` and adds no observation. A county table is
replayed anyway when a CBSA delineation was loaded after its last
publication, so its metro areas follow the new delineation; a parser
contract change also replays. A new release is kept beside the one it revised, and
`observation_latest` serves the newest release date.

Every run reads every year of every registered table, so a fresh warehouse
needs no separate history run. Each table is one mapped task in the
one-slot `bea_files` pool. `CAINC5N` and `CAGDP2` are the largest zips, at
tens of megabytes each.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_bea_schema`, re-applies the package's DDL before any capture.
Create the pool if deployment automation has not:

```text
airflow pools set bea_files 1 'BEA regional bulk files (serialized)'
```

## What a run does

1. Commits one capture per table and records it in
   `control.bea_table_capture`; an `unchanged` table stops here.
2. Replays each capture into `silver_bea.observation_revision`: one row per
   registered line, geography and year. Rows whose table, geography code or
   column count cannot be read go to `observation_quarantine`. It writes the
   `silver_ref.geography_resolution` ledger and conforms
   `silver_bea.fact_observation`. The run commits only if every in-scope row
   is accounted for.
3. Marks the table `published`, which exposes it through
   `gold_bea.observation_revision` and `observation_latest`, and signals the
   glossary harvest. Each `<table>:<line>` is one metric, for example
   `BEA:CAINC1:3`.

## Checks after a run

```sql
SELECT table_code, release_date, status, captured_row_count, in_scope_row_count
FROM control.bea_table_capture ORDER BY created_at DESC;

SELECT dollar_basis, value_status, COUNT(*)
FROM gold_bea.observation_latest GROUP BY dollar_basis, value_status;
```

## Quality rules

- `DQ-BEA-001` (uniqueness) and `DQ-BEA-003` (a provider code carries no
  number) are enforced by the fact's key and named CHECK constraints.
- `DQ-BEA-002` is the table-ledger reconciliation and runs in the daily
  sweep.
- `DQ-BEA-004` (year continuity) is declared and not yet executed.
- `DQ-BEA-005` fails a price parity table whose national all-items parity is
  not 100 in a published year; it runs in the weekly sweep.
- `DQ-BEA-006` warns where a total's parts differ from it by more than 0.5
  percent (or three thousand dollars, BEA's rounding) in the newest release of
  `CAINC35`, `SAINC35` or `SAINC50`: 1000 = 2000 + 3000 + 4000, 2000 = 2100 to
  2700, and `SAINC50` 60 = 70 + 120 + 180 + 230. Weekly sweep.
- `DQ-BEA-007` warns where `CAINC35` line 1000 and `CAINC4` line 47 differ by
  more than 0.5 percent for the same geography, year and release date. It is
  `not_applicable` while the two files come from different releases. Weekly
  sweep.
