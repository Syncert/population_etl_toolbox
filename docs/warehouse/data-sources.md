# Data sources

What the warehouse loads, at which geographic levels, for which years, how often, and where each source lands.
Everything here is what the repository declares (each source's `registry.py`/`config.py`, its DAG schedule, and its
operations guide under `docs/user-guides/`). The **status** column and each source's loaded years are a read-only count of
the internal warehouse with [`coverage.sql`](coverage.sql), taken 2026-10-31 17:05 UTC. **Loaded** means the declared years
are there; **Partly loaded** means years or levels are still missing.

Sources added or extended from 2026-11-03 to 2026-11-07 (taxes and public money, elections, cost of living, the FBI
Return A master files and the full permits history) were not in that count: their **status** is from the operator's
checks on the live warehouse up to 2026-11-07 and says so. Each section's **Shown on** line names the site pages that
read it. What the new sources add up to, and the measures derived from them, is summarised in
[What was added in November 2026](#added-2026-11).

Interactive version: [Warehouse Source Map](https://claude.ai/artifact/R8x3zkZjRAXFcPEYNr45s7). Table structures: [erd.md](erd.md).

**Live version (admins):** the site's `/admin/warehouse` page shows this map and the schema atlas read from the
warehouse each time it opens: grains and metric counts from `gold_glossary.dim_metric_catalog`, years per grain
measured from each source's serving relations (kept until a new release lands), and every table and view from `pg_catalog`. The
descriptions it shows come from `apps/api/services/warehouse_inventory.py`; a unit test fails if a served source has
no entry there, so add one with every new source. Turn the page on by listing operator labels or sign-in emails in
`API_ADMIN_ACCOUNTS`. This file was last brought up to date on 2026-11-07; `erd.md` was regenerated from the DDL the same day.

Marks: ● the publisher's own figure · ◐ built by the warehouse (summed, averaged or placed from finer records) · ◌ some programs or areas only.

| Source | Publisher | Nation | State | County | Metro | Place | Tract | Other | Years | Refresh | Status |
| --- | --- | :-: | :-: | :-: | :-: | :-: | :-: | :-: | --- | --- | --- |
| [American Community Survey](#census_acs) | U.S. Census Bureau | ● | ● | ● | ● | ● | ● |  | 2005–2024 | Monthly check | Partly loaded |
| [Census 2000 and 1990](#census_decennial) | U.S. Census Bureau | ● | ● | ● |  | ● | ● |  | 1990–2000 | One-time history load | Partly loaded |
| [Population Estimates Program](#census_pep) | U.S. Census Bureau | ● | ● | ● |  | ● |  | ● | 1970–2025 | Monthly check | Loaded |
| [SOI county-to-county migration](#irs_migration) | IRS Statistics of Income |  |  | ● |  |  |  |  | 1990–2023 | Monthly | Loaded |
| [BLS labor and prices (LAUS, CPS, CES, CPI, JOLTS, average prices, Consumer Expenditure)](#bls) | Bureau of Labor Statistics | ● | ● | ● | ● | ◌ |  | ◌ | 1990–2026 | Monthly | Loaded |
| [SOI individual income tax statistics by county and state](#irs_soi) | IRS Statistics of Income | ● | ● | ● |  |  |  |  | 1989–2023 | Monthly | Loaded |
| [Quarterly Census of Employment and Wages](#bls_qcew) | Bureau of Labor Statistics | ● | ● | ● |  |  |  |  | 2014–2025 | Monthly | Partly loaded |
| [County Business Patterns](#census_cbp) | U.S. Census Bureau | ● | ● | ● |  |  |  |  | 2016–2023 | Monthly | Loaded |
| [Regional economic accounts](#bea) | Bureau of Economic Analysis | ● | ● | ● | ◐ |  |  | ● | 1969–2024 | Weekly | Loaded |
| [SAIPE and SAHIE](#census_saipe_sahie) | U.S. Census Bureau | ● | ● | ● |  |  |  |  | 1989–2024 | Monthly | Partly loaded |
| [2020 Census counts](#census_decennial) | U.S. Census Bureau | ● | ● | ● |  |  | ● |  | 2020 | On demand | Not loaded yet |
| [LEHD Origin-Destination Employment Statistics](#census_lodes) | U.S. Census Bureau |  | ◐ | ◐ |  |  |  |  | 2002–2023 | Monthly | Partly loaded |
| [Building Permits Survey](#census_bps) | U.S. Census Bureau | ● | ● | ● |  | ● |  |  | 2000–2026 | Monthly | Loaded |
| [House Price Index (annual, all transactions)](#fhfa_hpi) | Federal Housing Finance Agency |  |  | ● |  |  |  |  | 1975–2025 | Monthly | Loaded |
| [Fair Market Rents and income limits](#hud_fmr_il) | HUD |  |  | ● |  |  |  |  | 2026–2027 | Monthly | Loaded |
| [Gasoline, diesel and home energy prices](#eia) | Energy Information Administration | ● | ◌ |  |  |  |  | ● | 1973–2026 | Weekly | Partly loaded |
| [County childcare prices](#dol_childcare) | DOL Women's Bureau |  |  | ● |  |  |  |  | 2008–2022 | Monthly | Not yet loaded |
| [FRED national indicators](#fred) | Federal Reserve Bank of St. Louis | ● |  |  |  |  |  |  | 1990–2026 | Monthly | Loaded |
| [Crime Data Explorer and Return A master files](#fbi_ucr) | FBI | ● | ● | ◐ |  |  |  | ● | 1990–2025 | Weekly | Loaded |
| [National Risk Index and disaster declarations](#fema_nri) | FEMA |  |  | ● |  |  |  |  | 1953–2026 | Daily | Loaded |
| [State and local government finances](#census_govfin) | U.S. Census Bureau | ● | ● | ◐ |  |  |  |  | 1994–2026 | Monthly | Loaded |
| [Monthly Treasury Statement, federal receipts and outlays](#treasury_mts) | U.S. Treasury, Bureau of the Fiscal Service | ● |  |  |  |  |  |  | 2015–2026 | Monthly | Not loaded yet |
| [Federal award money by place of performance](#usaspending) | USAspending.gov (U.S. Treasury) |  | ● | ● |  |  |  |  | 2008–2026 | Weekly | Not loaded yet |
| [Election results and seat holders](#elections) | MIT Election Data and Science Lab; congress-legislators; Wikidata |  | ● | ● |  |  |  | ● | 1969–2024 | Monthly + daily lean groups | Loaded |
| [Chronic Disease Indicators and PLACES](#cdc) | CDC | ● | ● | ● |  |  |  |  | 2019–2023 | Weekly | Loaded |
| [AirData annual monitor concentrations](#epa_aqs) | EPA |  |  | ◐ |  |  |  |  | 2020–2024 | Monthly | Loaded |
| [U.S. Climate Normals 1991-2020](#noaa_normals) | NOAA NCEI |  |  | ◐ |  |  |  |  | 2020 | Quarterly | Loaded |
| [nClimDiv county monthly temperature and precipitation](#noaa_climdiv) | NOAA NCEI |  |  | ● |  |  |  |  | 1895–2026 | Monthly | Not loaded yet |
| [Storm Events Database](#noaa_storm_events) | NOAA NCEI | ● | ● | ● |  |  |  |  | 1950–2026 | Annual | Not loaded yet |
| [Common Core of Data, public schools](#nces_ccd) | NCES |  | ◐ | ◐ |  |  |  |  | 2023–2024 | Quarterly | Loaded |
| [National Broadband Map availability](#fcc_bdc) | FCC | ● | ● | ● |  | ● |  |  | 2024–2025 | Monthly | Loaded |
| [Rural-urban codes, county typology, food atlas](#usda_ers) | USDA Economic Research Service |  |  | ● |  |  |  |  | 2015–2025 | Monthly | Loaded |
| [Quick Stats crops](#usda_nass) | USDA NASS | ● | ● | ● |  |  |  |  | 1990–2024 | Business days | Loaded |
| [Census geography (Gazetteer, boundaries, delineations)](#census_geo) | U.S. Census Bureau and OMB | ● | ● | ● | ● | ● | ● | ● | 1990–2025 | Monthly | Partly loaded |

Other areas: Census regions and divisions and county subdivisions (PEP), CPI and average-price areas (BLS), BEA state metro/nonmetro portions, EIA PADDs and cities, reviewed law-enforcement agencies (FBI), House districts (elections, as contests only).

## People and population

<a id="census_acs"></a>
### American Community Survey (`CENSUS_ACS`)

- **Publisher:** U.S. Census Bureau
- **Code:** `src/data_ingestion_toolbox/census_acs` · DAG `acs_ingest` · Monthly check, 1st 06:00 UTC
- **Publisher releases:** 1-year each September, 5-year each December
- **Credential:** CENSUS_API_KEY
- **Years:** 1-year from 2005 (no 2020 release); 5-year from 2009; tracts 5-year only
- **Datasets:** acs1 (1-year estimates); acs5 (5-year estimates)
- **Measures:** 60 curated detailed tables (age, sex, race, origin, language, mobility, commute, education, disability, veterans, poverty, employment, income, earnings, industry, occupation, housing stock, tenure, rent, value, costs, property tax, health insurance, internet). 23 of them at tract grain. B25103 (median real estate taxes paid) was added 2026-11-05 through `AcsConfig.added_tables`, so held years request only that table. Derived: median property tax as a percent of median home value (B25103_001 ÷ B25077_001).
- **Capture and control:** `raw_census.acs_datasets`, `raw_census.acs_tables`, `raw_census.acs_variables`, `control.acs_ingestion_slices`
- **Silver:** `silver_census.observation_revision`, `silver_census.fact_demographics`
- **Gold:** `gold_census.rpt_acs_observations (partitioned by year)`, `gold_census.mv_acs_latest`, `gold_census.fact_acs_observation`, `gold_census.acs_tract_comparable`, `gold_census.dim_acs_table`, `gold_census.dim_acs_variable`
- **Status (live count 2026-10-31):** Partly loaded. 5-year: 2009–2024 at nation, state, county, place and tract. 1-year: 2005–2024 at nation, state, county and place (19 releases; the Bureau published no standard 2020 1-year). Metro: 2022–2024 so far; the metro backfill (2024 back to 2005) and the tract wide reload were running at the count.
- **Shown on:** every place page (People, Work and Money, Housing, Health chapters); the property tax load card in Work and Money's Taxes and public money group; the map, explorer and compare pages.

<a id="census_decennial"></a>
### Census 2000 and 1990 (`CENSUS_ACS_DECENNIAL`)

- **Publisher:** U.S. Census Bureau
- **Code:** `src/data_ingestion_toolbox/census_acs (decennial.py, load_1990.py)` · DAG `acs_ingest (2000); operator load (1990 discs)` · One-time history load
- **Publisher releases:** Fixed historical releases
- **Credential:** CENSUS_API_KEY (2000 only)
- **Years:** 2000 SF1 and SF3 from the Data API; 1990 STF1A and STF3A from the Bureau's archived discs
- **Datasets:** dec2000sf1 (100-percent counts); dec2000sf3 (long-form sample); dec1990stf1; dec1990stf3
- **Measures:** The ACS curated tables, crosswalked to the 2000 and 1990 tables (decennial_crosswalk.json, decennial_1990_crosswalk.json). No margins of error.
- **Capture and control:** `raw_census.acs_variables`, `control.acs_ingestion_slices`
- **Silver:** `silver_census.observation_revision`, `silver_census.fact_demographics`
- **Gold:** `gold_census.rpt_acs_observations (2000, 1990 partitions)`
- **Status (live count 2026-10-31):** Partly loaded. 2000 SF1 and SF3 loaded at nation, state, county, place and tract. 1990 is not in the ingestion ledger yet.

<a id="census_pep"></a>
### Population Estimates Program (`CENSUS_PEP`)

- **Publisher:** U.S. Census Bureau
- **Code:** `src/data_ingestion_toolbox/census_pep` · DAG `pep_ingest` · Monthly check, 1st 06:00 UTC
- **Publisher releases:** New vintage each year (states Dec, counties Mar, cities May)
- **Credential:** none
- **Years:** Postcensal vintages 2024 and 2025 (2020-2025); 2010s, 2000s intercensal, and 1990s, 1980s, 1970s county totals
- **Datasets:** pep_nst_alldata; pep_county_alldata; pep_subcounty; pep_county_alldata_2010s; pep_nst_alldata_2010s; pep_county_alldata_2000s; pep_county_intercensal_2000s; pep_county_totals_1990s / 1980s / 1970s
- **Measures:** Population estimate; components of change from 2000 (births, deaths, natural change, international and domestic migration, net migration, and their rates). Before 2000, totals only.
- **Capture and control:** `silver_pep.release_load`
- **Silver:** `silver_pep.pep_dataset`, `silver_pep.pep_release`, `silver_pep.observation_revision`, `silver_pep.fact_population_estimate`, `silver_pep.dim_measure`
- **Gold:** `gold_pep.rpt_pep_observations`, `gold_pep.population_estimate_revision`, `gold_pep.population_estimate_latest`, `gold_pep.population_change`, `gold_pep.mv_pep_latest (view)`
- **Status (live count 2026-10-31):** Loaded. County 1970–2025 (56 years), state 46 years, nation 36 years, place 2020–2025; about 1.56 million rows. No other-area rows (regions, divisions, county subdivisions) were seen.

<a id="irs_migration"></a>
### SOI county-to-county migration (`IRS_MIGRATION`)

- **Publisher:** IRS Statistics of Income
- **Code:** `src/data_ingestion_toolbox/irs_migration` · DAG `irs_migration_ingest` · Monthly, 5th 15:00 UTC
- **Publisher releases:** One new year pair a year
- **Credential:** none
- **Years:** Filing-year pairs 1990-1991 to 2022-2023; before 2018-2019 the individuals count is exemptions claimed, before 2011-2012 returns processed by late September, and before 1995-1996 no AGI
- **Datasets:** countyinflowYYYY.csv; countyoutflowYYYY.csv; countyYYYY.zip; YYYYtoYYYYcountymigration.zip
- **Measures:** Returns, individuals and adjusted gross income for each county pair, plus non-migrants and SOI's totals (same state, other state, foreign). Small flows are in SOI's Other flows rows.
- **Capture and control:** `control.irs_migration_file`
- **Silver:** `silver_irs_migration.flow_revision`, `silver_irs_migration.fact_flow`, `silver_irs_migration.flow_quarantine`
- **Gold:** `gold_irs_migration.flow_revision`, `gold_irs_migration.flow_latest`, `gold_irs_migration.total_observation_revision`, `gold_irs_migration.total_observation_latest`
- **Status (operator checks 2026-11-07):** Loaded. Every filing-year pair from 1990-1991 to 2022-2023 is served (66 inflow and outflow files published); `fact_flow` holds 6,803,025 rows after the superseded first-parser rows were cleaned. 81 rows from 2009–2011 stay quarantined.
- **Shown on:** county pages, People chapter: where people came from and went (`MigrationFlows`).

## Work, income and business

<a id="bls"></a>
### BLS labor and prices (LAUS, CPS, CES, CPI, JOLTS, average prices, Consumer Expenditure) (`BLS`)

- **Publisher:** Bureau of Labor Statistics
- **Code:** `src/data_ingestion_toolbox/bls` · DAG `bls_ingest` · Monthly, 1st 07:00 UTC
- **Publisher releases:** Monthly
- **Credential:** BLS_API_KEY
- **Years:** History band from 1990 plus a rolling two-year window
- **Datasets:** LAUS (state, county, metro, city); CPS / LN (national); CES (national payroll); CPI-U `cu` (national, plus 4 items at regions, divisions and metros); JOLTS (national); Average Price `ap` (nation, 4 regions, 9 divisions, 23 metros)
- **Measures:** Unemployment rate and level, employment, labor force, participation, employment-population ratio; 17 national CPS series; 17 CES series; 13 JOLTS series.
  - **CPI-U (`cu`), 11 items:** all items, all items less food and energy, shelter, medical care, rent of primary residence, owners' equivalent rent, day care and preschool (`CUUR0000SEEB03`, the national change shown beside the Women's Bureau's county childcare prices, never applied to them) (national only); food at home, food, energy and gasoline (nation and the 36 BLS price areas: 4 regions, 9 divisions, 23 metros). Not seasonally adjusted, monthly periodicity (`bls/price_series.py`).
  - **Average Price (`ap`), 11 items, dollars:** regular and all-types gasoline per gallon (nation and all 36 areas); eggs per dozen, whole milk per gallon, white bread, ground beef, boneless chicken breast, bananas and ground coffee per pound, electricity per kilowatt-hour and utility gas per therm (the nation and the four regions, where BLS publishes them). Chicago, Los Angeles and New York are monthly; the other metros every other month.
  - **Consumer Expenditure Survey (`cx`), 10 series, dollars per household a year:** average annual spending on food at home for the nation (`CXUFOODHOMELB1101M`), the four Census regions (`LB1102` Northeast, `LB1103` Midwest, `LB1104` South, `LB1105` West) and, at the nation, households of one, two, three, four and five or more people (`LB0502`, `LB0504` to `LB0507`). Annual (`A01`); BLS publishes 1984 onward and the loader reads 1990 onward. Their series records are BLS API v2's own catalog entries, read in one keyed `catalog=true` request; the 32 MB `cx.series` list is read only when the API cannot give every series one, and then only these ten rows are kept. Gold carries the survey with observation basis `SPENDING`: it is a mean of what households spent, not a price.
  - Which series are requested is read from BLS's own `cu.series` / `ap.series` lists, so an item an area does not carry is not asked for. A series BLS stopped is still served as history; the catalog marks it `ended` once its newest real value is older than 548 days for a monthly series (about 18 months; 120 days weekly, 730 quarterly; `GLOSSARY_ENDED_AFTER_DAYS` overrides).
- **Capture and control:** `raw_bls.bls_datasets`, `raw_bls.bls_series`, `control.bls_ingestion_slices`
- **Silver:** `silver_bls.observation_revision`, `silver_bls.fact_labor_statistics`
- **Gold:** `gold_bls.rpt_bls_observations`, `gold_bls.mv_bls_latest`, `gold_bls.fact_bls_observation`, `gold_bls.provider_annual_average`, `gold_bls.calendar_window_observation`, `gold_bls.derived_calendar_rollup`, `gold_bls.dim_bls_series / measure / survey`
- **Status (live count 2026-10-31):** Loaded. 1990–2026 at county, metro, state and nation, plus BLS's own areas (regions from 1990, divisions from 2017). No place-level rows were seen. Operator check 2026-11-06: the 9 grocery and gasoline `ap` items and 10 `cu` items are loaded from 1990 (eggs from 1990, milk from 1995, ground beef from 1998, chicken from 2006) to September 2026; the LAUS reload finished 2026-11-07. The BLS key allows 500 requests in a rolling 24 hours, so a quota-stopped batch resumes on the next window.
- **Shown on:** place pages: unemployment rate card and chart (LAUS); the Cost of living section (CPI food, gasoline and energy; everyday prices with minutes of work); the national page's work charts; explorer and map.

<a id="irs_soi"></a>
### SOI individual income tax statistics by county and state (`IRS_SOI`)

- **Publisher:** IRS Statistics of Income
- **Code:** `src/data_ingestion_toolbox/irs_soi` · DAG `irs_soi_ingest` · Monthly, 7th 15:00 UTC
- **Publisher releases:** One new tax year a year, about three years after it ends
- **Credential:** none
- **Years:** County files for tax years 1989–2023, each state's total included (1989–2009 carry six items with no bands and no tax; by size of AGI from 2010); Historic Table 2 state files, with the nation, for 2020–2023
- **Datasets:** YYYYcountyincome.zip (1989–2009); 2010countydata.zip; YYincyallnoagi.csv; YYincyallagi.csv (2012: 12cyallnoagi.csv, 12cyallagi.csv); YYin55cmcsv.csv
- **Measures:** Returns, individuals, adjusted gross income, wages, interest, dividends, business income, capital gains, pensions, Social Security, unemployment compensation, taxable income, income tax, total tax liability, earned income tax credit, additional child tax credit, and itemized state and local income and real estate taxes, for all returns and by size of adjusted gross income. Derived: income tax as a share of AGI; share of returns claiming the EITC.
- **Capture and control:** `control.irs_soi_file`
- **Silver:** `silver_irs_soi.observation_revision`, `silver_irs_soi.fact_observation`, `silver_irs_soi.observation_quarantine`
- **Gold:** `gold_irs_soi.observation_revision`, `gold_irs_soi.observation_latest`, `gold_irs_soi.measure_export`, `gold_irs_soi.metric_publisher`
- **Status (operator checks 2026-11-07):** Loaded (first run 2026-11-05, fixes in #303); DQ-IRS-002 passes. Rows SOI prints twice are kept once and the repeat set aside as `repeated_row`.
- **Shown on:** place pages for a county, a state and the nation: Work and Money, Taxes and public money, "Federal income tax as a share of adjusted gross income" (county from 2010, nation from 2020); explorer and map for every published item.

<a id="bls_qcew"></a>
### Quarterly Census of Employment and Wages (`BLS_QCEW`)

- **Publisher:** Bureau of Labor Statistics
- **Code:** `src/data_ingestion_toolbox/bls_qcew` · DAG `bls_qcew_ingest` · Monthly, 20th 12:00 UTC
- **Publisher releases:** Quarterly, about five months after quarter end
- **Credential:** none
- **Years:** Quarters and annual averages from 2014 (the open-data interface's first year)
- **Datasets:** Industry slices: total (all and private) + 21 NAICS sectors
- **Measures:** Establishments, monthly employment, total quarterly wages, average weekly wage (jobs located in an area).
- **Capture and control:** `control.bls_qcew_slice`
- **Silver:** `silver_bls_qcew.observation_revision`, `silver_bls_qcew.fact_observation`, `silver_bls_qcew.dim_industry`, `silver_bls_qcew.dim_measure`
- **Gold:** `gold_bls_qcew.observation_revision`, `gold_bls_qcew.observation_latest`
- **Status (live count 2026-10-31):** Partly loaded. Only 2025–2026 is loaded; the registry declares 2014–2025. Operator check 2026-11-07: 22,948,366 serving rows after the duplicate cleanup; the schedule loads the missing years.
- **Shown on:** place pages: jobs and average weekly wage cards; its county annual average weekly wage ÷ 40 is the hourly wage behind "minutes of work" in the Cost of living section; lean groups' jobs measure.

<a id="census_cbp"></a>
### County Business Patterns (`CENSUS_CBP`)

- **Publisher:** U.S. Census Bureau
- **Code:** `src/data_ingestion_toolbox/census_cbp` · DAG `census_cbp_ingest` · Monthly, 15th 13:00 UTC
- **Publisher releases:** One year a year (2023 came out June 2025)
- **Credential:** none
- **Years:** 2016-2023 registered
- **Datasets:** cbpYYco (counties); cbpYYst (states); cbpYYus (nation)
- **Measures:** Employer establishments, March employment, first-quarter payroll, annual payroll; total and two-digit NAICS sectors.
- **Capture and control:** `control.census_cbp_file`
- **Silver:** `silver_census_cbp.observation_revision`, `silver_census_cbp.fact_observation`
- **Gold:** `gold_census_cbp.observation_revision`, `gold_census_cbp.observation_latest`, `gold_census_cbp.sector_definition`
- **Status (live count 2026-10-31):** Loaded. 2016–2023.

<a id="bea"></a>
### Regional economic accounts (`BEA`)

- **Publisher:** Bureau of Economic Analysis
- **Code:** `src/data_ingestion_toolbox/bea` · DAG `bea_regional_ingest` · Weekly, Wednesday 14:00 UTC
- **Publisher releases:** County income each November, county GDP each December
- **Credential:** none (bulk zips)
- **Years:** Personal income from 1969; earnings by industry and GDP from 2001; price parities from 2008
- **Datasets:** CAINC1; CAINC4; CAINC35 (transfer receipts by program, county); CAINC5N; CAGDP1; CAGDP2; SAINC35 (transfer receipts, state); SAINC50 (personal current taxes, state); SAINC51 (disposable personal income, state); SARPP; MARPP; PARPP; MAIRPD
- **Measures:** Personal income, population, per capita income, earnings, transfer receipts, wages; dividends, interest and rent; earnings and GDP by industry; real and current GDP; regional price parities and deflators.
  - **Added 2026-11-05/06 (taxes and public money):** transfer receipts by program (Social Security, Medicare, Medicaid, SSI, EITC, SNAP, unemployment insurance, veterans' benefits and the rest; county from CAINC35, state from SAINC35); personal current taxes, federal, state and local (SAINC50); disposable (after-tax) income and per person (SAINC51), also deflated to 2017 dollars in `gold_bea.disposable_income_real`.
  - **Derived:** each program's share of personal income (county and state); all payments from government and others as a share of personal income; dividends, interest and rent share and transfer receipts share ("where income comes from"); personal taxes as a share of income, federal, state and local.
  - CAINC35's county file ends in 2022 while the other county tables reach 2024, so a county's program shares use the newest year both publish.
- **Capture and control:** `control.bea_table_capture`
- **Silver:** `silver_bea.observation_revision`, `silver_bea.fact_observation`, `silver_bea.dim_line`
- **Gold:** `gold_bea.observation_revision`, `gold_bea.observation_latest`
- **Status (live count 2026-10-31):** Loaded. 1969–2024 at county, metro, state and nation; BEA's state metro/nonmetro portions 2008–2024; about 5.7 million rows. Metro income and GDP are summed from counties (918 of 925 metros for 2023; Connecticut's 7 metros 2024 only). The CAINC35/SAINC tables loaded on the weekly run after 2026-11-05; the income-sources card has served since 2026-11-07.
- **Shown on:** place pages: per capita income and real GDP cards; Taxes and public money ("Payments from government and others" by program, "Where income comes from"); the denominator of the state and local tax burden; the Cost of living section's regional price parity; governor and lean-group comparisons; map and explorer.

<a id="census_saipe_sahie"></a>
### SAIPE and SAHIE (`CENSUS_SAIPE_SAHIE`)

- **Publisher:** U.S. Census Bureau
- **Code:** `src/data_ingestion_toolbox/census_saipe_sahie` · DAG `census_saipe_sahie_ingest` · Monthly, 15th 06:00 UTC
- **Publisher releases:** SAIPE each December, SAHIE each spring
- **Credential:** CENSUS_API_KEY
- **Years:** SAIPE 1989-2024; SAHIE 2006-2023
- **Datasets:** saipe (/timeseries/poverty/saipe); sahie (/timeseries/healthins/sahie)
- **Measures:** Poverty rate and count (all ages, under 18), median household income; uninsured share and count under 65. All with 90% intervals.
- **Capture and control:** `control.census_sae_slice`
- **Silver:** `silver_census_sae.observation_revision`, `silver_census_sae.fact_estimate`, `silver_census_sae.dim_measure`
- **Gold:** `gold_census_sae.estimate_revision`, `gold_census_sae.estimate_latest`
- **Status (live count 2026-10-31):** Partly loaded. Only 2022–2024 is loaded; the registry declares 1989–2024.

<a id="census_decennial"></a>
### 2020 Census counts (`CENSUS_DECENNIAL`)

- **Publisher:** U.S. Census Bureau
- **Code:** `src/data_ingestion_toolbox/census_decennial` · DAG `census_decennial_ingest` · On demand (the count is published once a decade)
- **Publisher releases:** 2020 redistricting data (P.L. 94-171), August 2021
- **Credential:** CENSUS_API_KEY
- **Years:** 2020 (Census Day, 2020-04-01)
- **Datasets:** pl (/2020/dec/pl)
- **Measures:** Total population; people in group quarters by type (correctional facilities for adults, juvenile facilities, nursing homes, other institutional; college/university student housing, military quarters, other noninstitutional); housing units, occupied and vacant. Each the Bureau's published integer, nothing estimated.
- **Capture and control:** `control.census_decennial_slice`
- **Silver:** `silver_census_decennial.observation_revision`, `silver_census_decennial.fact_count`, `silver_census_decennial.dim_variable`
- **Gold:** `gold_census_decennial.count_revision`, `gold_census_decennial.count_latest`
- **Status:** Not loaded yet (added 2026-11-12, plan `group-quarters-by-type`). Blocks are captured only for a county the registry scopes, and none is scoped yet.

<a id="census_lodes"></a>
### LEHD Origin-Destination Employment Statistics (`CENSUS_LODES`)

- **Publisher:** U.S. Census Bureau
- **Code:** `src/data_ingestion_toolbox/census_lodes` · DAG `census_lodes_ingest` · Monthly, 20th 14:00 UTC (newest year)
- **Publisher releases:** Yearly vintage
- **Credential:** none
- **Years:** LODES8, 2002-2023 registered; the DAG keeps the newest, `census_lodes.history` loads the rest
- **Datasets:** rac (residence); wac (workplace); od_main; od_aux
- **Measures:** Resident workers, jobs, live-and-work, inbound and in-state outbound commuters. Summed from census blocks.
- **Capture and control:** `control.census_lodes_slice`, `control.census_lodes_file`
- **Silver:** `silver_census_lodes.fact_area`, `silver_census_lodes.fact_flow`
- **Gold:** `gold_census_lodes.observation_fact`, `gold_census_lodes.observation_revision`, `gold_census_lodes.observation_latest`
- **Status (live count 2026-10-31):** Partly loaded. Only 2023 is loaded; the registry declares 2002–2023. The scheduled run captures only the newest year.

## Housing

<a id="census_bps"></a>
### Building Permits Survey (`CENSUS_BPS`)

- **Publisher:** U.S. Census Bureau
- **Code:** `src/data_ingestion_toolbox/census_bps` · DAG `census_building_permits_ingest` · Monthly, 25th 13:00 UTC
- **Publisher releases:** Monthly
- **Credential:** none
- **Years:** Monthly from January 2000; places annual from 2007
- **Datasets:** County monthly and annual; State monthly and annual; Place annual by region
- **Measures:** Housing units, buildings and valuation authorized, by structure type (1, 2, 3-4, 5+ units); estimate and reported value.
- **Capture and control:** `control.census_bps_slice`
- **Silver:** `silver_census_bps.observation_revision`, `silver_census_bps.fact_observation`, `silver_census_bps.dim_measure`
- **Gold:** `gold_census_bps.observation_revision`, `gold_census_bps.observation_latest`
- **Rules:** a jurisdiction the Bureau prints twice with identical figures loads once (the repeat is quarantined `repeated_row`; disagreeing copies are both set aside as `conflicting_repeated_row`). The parts of one place are summed: New York City's five boroughs under FIPS 36-51000, and a village split across two counties. A row naming a different place under another place's code is quarantined `conflicting_place_code`. A place that reported no month carries `not_reported`, never zero.
- **Status (operator checks 2026-11-07):** Loaded. Monthly and annual county and state 2000–2026 and annual places 2007–2025, after the reconciliation fix (#315); DQ-BPS-002 passes.
- **Shown on:** place pages, Housing chapter: single-family and multifamily units authorized in the year, and the newest month; map and explorer.

<a id="fhfa_hpi"></a>
### House Price Index (annual, all transactions) (`FHFA_HPI`)

- **Publisher:** Federal Housing Finance Agency
- **Code:** `src/data_ingestion_toolbox/fhfa_hpi` · DAG `fhfa_hpi_ingest` · Monthly, 25th 15:00 UTC
- **Publisher releases:** Annual file, revised in full
- **Credential:** none
- **Years:** Each county from its first recorded year in FHFA's file
- **Datasets:** hpi_at_county.xlsx
- **Measures:** Annual change (%), HPI with 2000 base.
- **Capture and control:** `control.fhfa_hpi_file`
- **Silver:** `silver_fhfa_hpi.observation_revision`, `silver_fhfa_hpi.fact_observation`
- **Gold:** `gold_fhfa_hpi.observation_revision`, `gold_fhfa_hpi.observation_latest`
- **Status (live count 2026-10-31):** Loaded. County 1975–2025.

<a id="hud_fmr_il"></a>
### Fair Market Rents and income limits (`HUD_FMR_IL`)

- **Publisher:** HUD
- **Code:** `src/data_ingestion_toolbox/hud_fmr_il` · DAG `hud_fmr_il_ingest` · Monthly, 25th 16:00 UTC
- **Publisher releases:** Each fiscal year, with reissues
- **Credential:** HUD_USER_API_TOKEN
- **Years:** FMR FY2026 (and May 2026 reissue), FY2027; income limits FY2026
- **Datasets:** HUD User API fmr/statedata; HUD User API il/data; Workbooks (manual)
- **Measures:** Fair market rent 0-4 bedrooms; median family income; 30/50/80% income limits for 4 people. Values are set per HUD area and repeated per county.
- **Capture and control:** `control.hud_fmr_il_api_capture`, `control.hud_fmr_il_file`
- **Silver:** `silver_hud_fmr_il.observation_revision`, `silver_hud_fmr_il.fact_observation`
- **Gold:** `gold_hud_fmr_il.observation_revision`, `gold_hud_fmr_il.observation_latest`
- **Status (live count 2026-10-31):** Loaded. Fiscal years 2026–2027.

## Prices and the national economy

<a id="eia"></a>
### Gasoline, diesel and home energy prices (`EIA`)

- **Publisher:** Energy Information Administration
- **Code:** `src/data_ingestion_toolbox/eia` · DAG `eia_retail_gasoline_ingest` · Weekly, Tuesday 15:00 UTC
- **Publisher releases:** Every Monday
- **Credential:** EIA_API_KEY
- **Years:** Gasoline and diesel every week from 2015-01-05; residential electricity monthly from 2001, natural gas monthly from 1973; heating oil and propane weekly October to March from 1990
- **Datasets:** petroleum/pri/gnd (EPMR, EPMM, EPMP, EPM0, EPD2D); electricity/retail-sales (RES); natural-gas/pri/sum (PRS); petroleum/pri/wfr (EPD2F, EPLLPA)
- **Measures:** Regular, midgrade, premium and all-grades gasoline and diesel, dollars per gallon; residential electricity, cents per kilowatt-hour; residential natural gas, dollars per thousand cubic feet; residential heating oil and propane, dollars per gallon.
- **Capture and control:** `control.eia_read`, `control.eia_page`
- **Silver:** `silver_eia.price_revision`, `silver_eia.fact_retail_price`, `silver_eia.energy_price_revision`, `silver_eia.fact_energy_price`
- **Gold:** `gold_eia.observation_revision`, `gold_eia.observation_latest`
- **Areas:** gasoline and diesel for the nation, 9 states and 19 cities and EIA regions (PADDs); electricity and natural gas by state; heating oil and propane by state where EIA surveys it, else its petroleum region.
- **Status (operator checks 2026-11-06):** Partly loaded. Gasoline loaded every week 2015-01-05 to 2026-11-02 for the nation, 9 states and 19 areas. The diesel and home-energy routes (#314) load on their first weekly run after deploy; check with `LIVE_CHECKS_314.sql`.
- **Shown on:** place pages, Cost of living section: gasoline and diesel per gallon, home heating (natural gas, heating oil, propane) and electricity, each from the smallest area EIA publishes that contains the place, named beside the price.

<a id="dol_childcare"></a>
### County childcare prices (`DOL_CHILDCARE`)

- **Publisher:** U.S. Department of Labor Women's Bureau (National Database of Childcare Prices)
- **Code:** `src/data_ingestion_toolbox/dol_childcare` · DAG `dol_childcare_ingest` · Monthly, 9th 16:00 UTC
- **Publisher releases:** Every year or two, as a workbook and through DOL's data portal API
- **Credential:** `DOL_API_KEY`, a free DOL data portal key (www.dol.gov refuses the warehouse's client, so the API is read)
- **Years:** study years 2008–2022 (`NDCP2022.xlsx`, which restates the 2008–2018 file)
- **Datasets:** DOL API `WB/ndcp` (`https://apiprod.dol.gov/v4/get/WB/ndcp/json`), the same database as `NDCP2022.xlsx`
- **Measures:** Median weekly full-time price, dollars per week, for center-based and family childcare, each for infants, toddlers, preschoolers and school-age children; the Women's Bureau's imputation flag and code kept beside each price.
- **Capture and control:** `control.dol_childcare_file`
- **Silver:** `silver_dol_childcare.price_revision`, `silver_dol_childcare.fact_price`, `silver_dol_childcare.observation_quarantine`
- **Gold:** `gold_dol_childcare.observation_revision`, `gold_dol_childcare.observation_latest`
- **Status:** Not yet loaded. Loads whole on the first monthly run after deploy; see `docs/user-guides/DOL_CHILDCARE_PIPELINE_OPERATIONS.md`.
- **Shown on:** county pages, Cost of living section: the eight prices for the newest study year, imputed prices marked; other pages say the prices are published for counties only.

<a id="fred"></a>
### FRED national indicators (`FRED`)

- **Publisher:** Federal Reserve Bank of St. Louis
- **Code:** `src/data_ingestion_toolbox/fred` · DAG `fred_ingest` · Monthly, 1st 08:00 UTC
- **Publisher releases:** Varies by series
- **Credential:** FRED_API_KEY
- **Years:** Full series history as FRED serves it (start varies by series)
- **Datasets:** 25 curated series
- **Measures:** Payrolls, unemployment, participation, openings, claims, industrial production, permits, starts, mortgage rate, house supply and price, CPI, PCE, fed funds, Treasury yields and spread, breakeven inflation, financial conditions, real GDP, real consumption and income, saving rate, retail sales.
- **Capture and control:** `raw_fred.fred_datasets`, `raw_fred.fred_series`, `control.fred_ingestion_slices`
- **Silver:** `silver_fred.observation_revision`, `silver_fred.fact_economic_indicators`
- **Gold:** `gold_fred.rpt_fred_observations`, `gold_fred.mv_fred_latest`, `gold_fred.fact_fred_observation`, `gold_fred.dim_fred_series`
- **Status (live count 2026-10-31):** Loaded. 1970–2026, national.

## Crime and hazards

<a id="fbi_ucr"></a>
### Crime Data Explorer and Return A master files (`FBI_UCR`)

- **Publisher:** FBI
- **Code:** `src/data_ingestion_toolbox/fbi_ucr` · DAG `fbi_ucr_ingest` · Weekly, Monday 10:00 UTC
- **Publisher releases:** UCR refresh
- **Credential:** FBI_CDE_API_KEY
- **Years:** CDE API: monthly, January 1990 to June 2023, for the registered agencies. Return A master files: every reporting agency, monthly, 2015–2025
- **Datasets:** CDE API: 10 offense products: violent, assault, burglary, larceny, motor vehicle theft, homicide, rape, robbery, arson, property. Return A: one national fixed-width file a year, `master_files/reta/reta-<year>.zip`, read with the FBI's own layout; products `return_a_violent_crime` and `return_a_property_crime`
- **Measures:** Offenses and offenses cleared by arrest, as counts and rates, and monthly reporting participation (population covered). Return A has no violent or property total, so each is the CDE's definition summed from the file's offense components and labelled as derived; overlapping agencies are checked against the CDE's own totals (DQ-FBI-011). County rate per ADR-0012: agency totals summed, refused below 50% agency coverage.
- **Capture and control:** `control.fbi_ucr_release`, `control.fbi_return_a_year`
- **Silver:** `silver_fbi.observation_revision`, `silver_fbi.return_a_source_row` (one row per agency line read, which every Return A fact names), `silver_fbi.fact_crime_observation`, `silver_fbi.dim_agency(_version)`, `silver_fbi.agency_geography_relationship`, `silver_fbi.fact_reporting_participation`
- **Gold:** `gold_fbi.crime_observation`, `gold_fbi.latest_release_observation`, `gold_fbi.county_rollup`, `gold_fbi.latest_county_rollup`, `gold_fbi.reporting_coverage`, `gold_fbi.agency_observation_area_filter`
- **Status (live count 2026-10-31):** Loaded. Agency, state and nation 1990–2023; county rollup 1990–2023 (33 years) from the six-agency Wisconsin API sample. Return A 2015–2025 for every agency was loaded 2026-10-31 by `python -m data_ingestion_toolbox.fbi_ucr.return_a.load --years 2025-2015`: 13,290,720 crime and 6,645,360 participation facts. Their source rows (#319) are rebuilt by the scheduled run's `backfill_return_a_source_rows` task (expected 276,890 rows over 11 years; DQ-FBI-015 checks every fact has one). The master-file load is not yet on a schedule; that work is in progress.
- **Shown on:** place pages, Crime chapter: violent and property crime rates as the program published them, and the county rollup with its agency coverage (`CountyCrimeRollup`); map and explorer.

<a id="fema_nri"></a>
### National Risk Index and disaster declarations (`FEMA_NRI`)

- **Publisher:** FEMA
- **Code:** `src/data_ingestion_toolbox/fema_nri` · DAG `fema_nri_ingest` · Daily, 06:00 UTC
- **Publisher releases:** NRI by version; declarations every 20 minutes
- **Credential:** none
- **Years:** NRI December 2025 (v1.20.0), one vintage; declarations counted by calendar year
- **Datasets:** NRI county layer (ArcGIS); OpenFEMA DisasterDeclarationsSummaries v2
- **Measures:** Expected annual loss (total and 18 hazards), annualized frequency (5 hazards); major disaster, emergency and fire management declaration counts; declaration counts by FEMA incident type.
- **Capture and control:** `control.fema_nri_run`, `control.fema_nri_page`
- **Silver:** `silver_fema_nri.nri_fact`, `silver_fema_nri.declaration_revision`
- **Gold:** `gold_fema_nri.nri_observation`, `gold_fema_nri.declaration_count`, `gold_fema_nri.declaration_count_by_incident`, `gold_fema_nri.observation_revision`, `gold_fema_nri.observation_latest`
- **Status (live count 2026-10-31):** Loaded. County 1959–2026.

## Elections and government

<a id="census_govfin"></a>
### State and local government finances (`CENSUS_GOVFIN`)

- **Publisher:** U.S. Census Bureau
- **Code:** `src/data_ingestion_toolbox/census_govfin` · DAG `census_govfin_ingest` · Monthly, 18th 07:00 UTC
- **Publisher releases:** Annual finances each autumn; Census of Governments unit files every five years; QTAX about three months after each quarter
- **Credential:** CENSUS_API_KEY (the unit files need none)
- **Years:** annual state and local finances, fiscal years 2017-2024; QTAX state taxes, quarters from 1994; county areas from the Census of Governments 2002, 2012, 2017 and 2022 (2007 is published only in the 1967-2012 historical database and is not loaded)
- **Datasets:** asslgf (/timeseries/govslocalfin); qtax (/timeseries/eits/qtax, category QTAXCAT3); cog_units (Individual Unit Files)
- **Measures:** Taxes in total and by type (property, general sales, selective sales, individual and corporate income, other), current charges, miscellaneous and own-source general revenue, aid from federal, state and local governments, debt outstanding; each for state and local combined, state, or local governments. Derived: each tax type's share of taxes, and taxes as a share of BEA personal income and per person.
- **Capture and control:** `control.census_govfin_slice`
- **Silver:** `silver_census_govfin.fact_observation`, `silver_census_govfin.unit_amount`, `silver_census_govfin.unit_government`, `silver_census_govfin.state_type_total`, `silver_census_govfin.api_revision`, `silver_census_govfin.dim_measure`
- **Gold:** `gold_census_govfin.observation_revision`, `gold_census_govfin.observation_latest`, `gold_census_govfin.county_area_government`, `gold_census_govfin.tax_mix`, `gold_census_govfin.tax_burden`
- **Status (operator checks 2026-11-07):** Loaded: API fiscal years 2017–2024 and the 2002, 2012, 2017 and 2022 unit files (Wisconsin's fiscal 2022 tax burden reads 9.609%). Tax burden exists only for 2017–2024, so a governor whose term starts earlier has none. County areas are sums of each county's governments, a government spanning counties counted whole in its Census home county ([operations](../user-guides/CENSUS_GOVFIN_PIPELINE_OPERATIONS.md)).
- **Shown on:** place pages, Work and Money, Taxes and public money: "State and local taxes collected, share of personal income" (states and the nation every fiscal year; a county's local governments in Census of Governments years); a state's governor table (tax burden and tax mix by type over each tenure); the lean groups' "similar tax burden" peer group.

<a id="treasury_mts"></a>
### Monthly Treasury Statement, federal receipts and outlays (`TREASURY_MTS`)

- **Publisher:** U.S. Department of the Treasury, Bureau of the Fiscal Service
- **Code:** `src/data_ingestion_toolbox/treasury_mts` · DAG `treasury_mts_ingest` · Monthly, 16th 16:00 UTC
- **Publisher releases:** Monthly, on the eighth business day of the following month; a recent month can be restated
- **Credential:** none
- **Years:** statements from March 2015; fiscal-year totals from fiscal 2015 (federal fiscal years end September 30)
- **Datasets:** Table 9, "Summary of Receipts and Outlays of the U.S. Government by Source/Function" (Fiscal Data API `v1/accounting/mts/mts_table_9`)
- **Measures:** Federal receipts by source and outlays by budget function (19 functions and their total), each month, the open fiscal year to date, and fiscal-year totals read from the September statement. Derived: each function's share of total outlays. Federal government only, not where the money was spent.
- **Capture and control:** `control.treasury_mts_page`
- **Silver:** `silver_treasury_mts.statement_line`, `silver_treasury_mts.statement_line_revision`
- **Gold:** `gold_treasury_mts.observation_revision`, `gold_treasury_mts.observation_latest`, `gold_treasury_mts.outlays_by_function`
- **Status:** Not loaded yet (WHERE_PUBLIC_MONEY_GOES_PLAN slice 3). About 4,600 rows, under 0.05 GB ([operations](../user-guides/TREASURY_MTS_PIPELINE_OPERATIONS.md)).

<a id="usaspending"></a>
### Federal award money by place of performance (`USASPENDING`)

- **Publisher:** USAspending.gov, U.S. Department of the Treasury
- **Code:** `src/data_ingestion_toolbox/usaspending` · DAG `usaspending_ingest` · Weekly, Sunday 09:00 UTC
- **Publisher releases:** Continuously; agencies report within about 30 days (defense about 90), so the current and prior fiscal years change
- **Credential:** none
- **Years:** fiscal 2008 onward (federal fiscal years end September 30)
- **Datasets:** `POST /api/v2/search/spending_by_geography/` (scope `place_of_performance`, state and county layers) and `POST /api/v2/search/spending_by_category/awarding_agency/`
- **Measures:** Federal award obligations by state and county where the work is performed: every award, and contracts, grants, direct payments, loans and other assistance. Derived: each award type group's and each awarding agency's share of the place's all-award total. Not where the money is felt (pass-through grants sit with the state agency; Medicare with the claims processor), not all federal spending, and never added to what local governments spend.
- **Capture and control:** `control.usaspending_request`
- **Silver:** `silver_usaspending.geography_amount`, `silver_usaspending.geography_amount_revision`, `silver_usaspending.agency_year`
- **Gold:** `gold_usaspending.observation_revision`, `gold_usaspending.observation_latest`, `gold_usaspending.awards_by_award_type`, `gold_usaspending.awards_by_agency`
- **Status:** Not loaded yet (WHERE_PUBLIC_MONEY_GOES_PLAN slice 4). About 3,200 requests, 0.5-1 GB of captures and up to about 2 GB of silver rows ([operations](../user-guides/USASPENDING_PIPELINE_OPERATIONS.md)).

<a id="elections"></a>
### Election results and seat holders (`ELECTIONS`)

- **Publisher:** MIT Election Data and Science Lab (results, on Harvard Dataverse); the unitedstates/congress-legislators project (U.S. House and Senate members, CC0 1.0); Wikidata (governors, CC0 1.0)
- **Code:** `src/data_ingestion_toolbox/elections` · DAG `elections_ingest` · Monthly, 3rd, 07:00 UTC; DAG `elections_lean_groups` · Daily, 13:00 UTC (rebuilds only when its inputs changed)
- **Publisher releases:** MEDSL after each certified election; congress-legislators as members change; Wikidata continuously
- **Credential:** none; MEDSL's county and House files need a Dataverse guestbook answer (`ELECTIONS_GUESTBOOK_*` in `stack.env`)
- **Years:** seat terms in office on or after 1969-01-01; presidential results by county 2000–2024 and by state 1976–2024; Senate 1976–2024; House 1976–2024 (MEDSL's own spans)
- **Datasets:** MEDSL `countypres_2000-2024`, `1976-2024-president`, `1976-2024-senate-state`, `1976-2024-house`; `legislators-current.json`, `legislators-historical.json`; one Wikidata SPARQL query for governors
- **Measures:** Presidential vote at state and county: Republican share of the two-party vote, two-party margin (Republican minus Democratic), total votes. Seat-term spans (party, caucus, dates) and contest results are served as relations, not measures (ADR-0014, proposed).
  - **Lean groups:** counties are banded by the two-party presidential margin in a window's first year: Republican by 20 points or more, Republican by less than 20, Democratic by less than 20, Democratic by 20 or more. For each band the warehouse reports how its counties' real income per person, real GDP, jobs, unemployment rate and poverty rate changed over the window, net of the nation, one measure at a time (middle county and middle half, never scored or ranked). Peer groups: all counties, same state, same rural-urban type, similar starting level, same industry dependence, similar state and local tax burden.
  - **Governor terms:** each governor's tenure (from Wikidata) with the state's change against the nation's in tax burden and in payments from government as a share of income, and the tax mix by type (property, general sales, income, other).
- **Access:** MEDSL's county president and House files sit behind a Dataverse guestbook; the loader fills it from `ELECTIONS_GUESTBOOK_*` variables in the stack's environment file (values are kept out of the repository).
- **Capture and control:** `control.elections_run`, `control.elections_file`
- **Silver:** `silver_elections.seat_term`, `silver_elections.candidate_result`, `silver_elections.quarantine`
- **Gold:** `gold_elections.seat_term`, `gold_elections.contest_result`, `gold_elections.candidate_total`, `gold_elections.observation_revision`, `gold_elections.observation_latest`; lean groups `gold_elections.lean_place_change`, `lean_group_change`, `lean_refresh`; governors `gold_elections.governor_tenure`, `governor_fiscal_series`, `governor_term_fiscal`, `governor_term_tax_mix`
- **Status (operator checks 2026-11-07):** Loaded; lean groups are rebuilt once the LAUS reload's serving refresh finishes. House districts are kept as contests until district boundaries are a shared geography; state legislatures wait on decision L-1.
- **Shown on:** state pages, Work and Money: the governor table; the presidential vote measures in the explorer and map (for example a county's two-party margin over time). The lean-group tables are built in the warehouse; no API route or page reads them yet.

## Health

<a id="cdc"></a>
### Chronic Disease Indicators and PLACES (`CDC`)

- **Publisher:** CDC
- **Code:** `src/data_ingestion_toolbox/cdc` · DAG `cdc_ingest` · Weekly, Monday 09:00 UTC
- **Publisher releases:** CDI irregular; PLACES annual
- **Credential:** CDC_SOCRATA_APP_TOKEN (optional)
- **Years:** CDI multi-year indicators (nation, state); PLACES 2025 release (nation, county)
- **Datasets:** U.S. Chronic Disease Indicators (hksd-2xuw); PLACES county 2025 (swc5-untb)
- **Measures:** Indicator-specific surveillance measures with strata (CDI); model-based adult health measures (PLACES).
- **Capture and control:** `control.cdc_dataset_release`
- **Silver:** `silver_cdc.observation_revision`, `silver_cdc.fact_health_observation`, `silver_cdc.dim_measure`, `silver_cdc.dim_stratum`, `silver_cdc.dim_dataset_release`
- **Gold:** `gold_cdc.health_observation`, `gold_cdc.latest_release_observation`
- **Status (live count 2026-10-31):** Loaded. Nation and state 2015–2023, county 2022–2023. The catalog watermark is the publisher's last-update time (2026-06-04 UTC), stored as Unix seconds.

## Environment

<a id="epa_aqs"></a>
### AirData annual monitor concentrations (`EPA_AQS`)

- **Publisher:** EPA
- **Code:** `src/data_ingestion_toolbox/epa_aqs` · DAG `epa_aqs_ingest` · Monthly, 25th 18:00 UTC
- **Publisher releases:** Regenerated June and December
- **Credential:** none
- **Years:** 2020-2024 registered
- **Datasets:** annual_conc_by_monitor_YYYY.zip
- **Measures:** PM2.5 annual mean, ozone 8-hour 4th max: the highest complete monitor in each county.
- **Capture and control:** `control.epa_aqs_file`
- **Silver:** `silver_epa_aqs.monitor_fact`
- **Gold:** `gold_epa_aqs.monitor_observation`, `gold_epa_aqs.observation_revision`, `gold_epa_aqs.observation_latest`
- **Status (live count 2026-10-31):** Loaded. 2020–2024.

<a id="noaa_normals"></a>
### U.S. Climate Normals 1991-2020 (`NOAA_NORMALS`)

- **Publisher:** NOAA NCEI
- **Code:** `src/data_ingestion_toolbox/noaa_normals` · DAG `noaa_normals_ingest` · Quarterly, 2nd 19:00 UTC
- **Publisher releases:** Once per decade
- **Credential:** none
- **Years:** 30-year normals, labelled 2020
- **Datasets:** annual/seasonal and monthly multivariate by station archives
- **Measures:** Annual mean, max and min temperature, precipitation, heating and cooling degree days; the same by calendar month plus monthly snowfall: mean of stations placed in the county.
- **Capture and control:** `control.noaa_normals_file`
- **Silver:** `silver_noaa_normals.station`, `silver_noaa_normals.station_normal`, `silver_noaa_normals.station_monthly_normal`
- **Gold:** `gold_noaa_normals.station_observation`, `gold_noaa_normals.station_monthly_observation`, `gold_noaa_normals.observation_revision`, `gold_noaa_normals.observation_latest`
- **Status (live count 2026-10-31):** Loaded. The 1991–2020 normals (period 2020).

<a id="noaa_storm_events"></a>
### Storm Events Database (`NOAA_STORM_EVENTS`)

- **Publisher:** NOAA NCEI (events); NWS (zone-county correlation files)
- **Code:** `src/data_ingestion_toolbox/noaa_storm_events` · DAG `noaa_storm_events_ingest` · Monthly, 20th 16:00 UTC
- **Publisher releases:** The current year's file is restamped each month; an earlier year is restamped when NCEI revises it
- **Credential:** none
- **Years:** 1950 onward; 1950-1954 tornado only, 1955-1995 tornado, thunderstorm wind and hail, 1996 onward all 48 event types
- **Datasets:** `StormEvents_details` yearly files; NWS `bpDDmmYY.dbx` zone-county files
- **Measures:** Events reported per year and event type, deaths, injuries, property and crop damage as published; every event's record. Zone events count on a county only when zone code and name match a published zone-county file.
- **Capture and control:** `control.noaa_storm_events_file`
- **Silver:** `silver_noaa_storm_events.event`, `event_revision`, `event_place`, `place_year`, `zone_county`
- **Gold:** `gold_noaa_storm_events.observation_revision`, `observation_latest`, `event_detail`, `state_coverage`
- **Status:** Not loaded yet (WEATHER_AND_DISASTER_HISTORY_PLAN slice 1). About 1.9 million events, 6 to 8 GB.

<a id="noaa_climdiv"></a>
### nClimDiv county monthly temperature and precipitation (`NOAA_CLIMDIV`)

- **Publisher:** NOAA NCEI
- **Code:** `src/data_ingestion_toolbox/noaa_climdiv` · DAG `noaa_climdiv_ingest` · Monthly, 12th 15:00 UTC
- **Publisher releases:** Monthly; NCEI revises the latest two years each month
- **Credential:** none
- **Years:** Monthly from January 1895; degree days for the 48 contiguous states only
- **Datasets:** climdiv county files `pcpn`, `tmpc`, `tmax`, `tmin`, `hddc`, `cddc`
- **Measures:** Monthly precipitation, average, maximum and minimum temperature, heating and cooling degree days per county, as NCEI publishes them. Never added into annual figures.
- **Capture and control:** `control.noaa_climdiv_file`
- **Silver:** `silver_noaa_climdiv.county_month`, `silver_noaa_climdiv.county_month_revision`
- **Gold:** `gold_noaa_climdiv.observation_revision`, `gold_noaa_climdiv.observation_latest`
- **Status:** Not loaded yet (WEATHER_AND_DISASTER_HISTORY_PLAN slice 2). About 30 million county months, 4 to 5 GB.

## Schools and broadband

<a id="nces_ccd"></a>
### Common Core of Data, public schools (`NCES_CCD`)

- **Publisher:** NCES
- **Code:** `src/data_ingestion_toolbox/nces_ccd` · DAG `nces_ccd_ingest` · Quarterly, 3rd 20:00 UTC
- **Publisher releases:** Per school year
- **Credential:** none
- **Years:** School years 2023-24 and 2024-25 (year = fall)
- **Datasets:** EDGE geocodes; Directory; Membership; Staff; Lunch
- **Measures:** Operating and charter schools, students, teacher FTE, free/reduced lunch eligibility, direct certification: summed over schools placed in the area.
- **Capture and control:** `control.nces_ccd_file`
- **Silver:** `silver_nces_ccd.school_directory`, `silver_nces_ccd.school_location`, `silver_nces_ccd.school_count`
- **Gold:** `gold_nces_ccd.school_placement`, `gold_nces_ccd.school_observation`, `gold_nces_ccd.observation_revision`, `gold_nces_ccd.observation_latest`
- **Status (live count 2026-10-31):** Loaded. 2023–2024 at county and state.

<a id="fcc_bdc"></a>
### National Broadband Map availability (`FCC_BDC`)

- **Publisher:** FCC
- **Code:** `src/data_ingestion_toolbox/fcc_bdc` · DAG `fcc_bdc_ingest` · Monthly, 12th 21:00 UTC
- **Publisher releases:** Twice a year (December vintages registered)
- **Credential:** FCC_BDC_USERNAME, FCC_BDC_API_TOKEN
- **Years:** As of 2024-12-31 and 2025-12-31
- **Datasets:** Summary by geography: national + census places by state
- **Measures:** Residential units; share served at 25/3, 100/20, 1000/100 Mbps (any technology), 100/20 terrestrial and wired.
- **Capture and control:** `control.fcc_bdc_read`, `control.fcc_bdc_file`
- **Silver:** `silver_fcc_bdc.availability_row`
- **Gold:** `gold_fcc_bdc.availability_observation`, `gold_fcc_bdc.observation_revision`, `gold_fcc_bdc.observation_latest`
- **Status (live count 2026-10-31):** Loaded. 2024–2025, including places (384,108 place rows).

## Rural and farm

<a id="usda_ers"></a>
### Rural-urban codes, county typology, food atlas (`USDA_ERS`)

- **Publisher:** USDA Economic Research Service
- **Code:** `src/data_ingestion_toolbox/usda_ers` · DAG `usda_ers_ingest` · Monthly, 25th 17:00 UTC
- **Publisher releases:** Editions replaced in place
- **Credential:** none
- **Years:** RUCC 2023; Typology 2025; Food Environment Atlas 2015, 2017, 2019, 2023
- **Datasets:** RUCC 2023; County Typology Codes 2025; Food Environment Atlas (July 2025)
- **Measures:** Rural-urban continuum code, 12 typology flags, industry dependence, SNAP stores and per 1,000, SNAP households with low store access.
- **Capture and control:** `control.usda_ers_file`
- **Silver:** `silver_usda_ers.observation_revision`, `silver_usda_ers.fact_observation`
- **Gold:** `gold_usda_ers.observation_revision`, `gold_usda_ers.observation_latest`
- **Status (live count 2026-10-31):** Loaded. Five vintages, 2015–2025.

<a id="usda_nass"></a>
### Quick Stats crops (`USDA_NASS`)

- **Publisher:** USDA NASS
- **Code:** `src/data_ingestion_toolbox/usda_nass` · DAG `usda_nass_crop_ingest` · Business days, 10:00 UTC (full sweep on the 1st)
- **Publisher releases:** Survey releases; Census of Agriculture every 5 years
- **Credential:** USDA_NASS_API_KEY
- **Years:** Survey 1990-2024; Census of Agriculture 2022
- **Datasets:** corn, soybeans, wheat, hay surveys; corn Census of Agriculture
- **Measures:** Acres planted and harvested, yield, production.
- **Capture and control:** `control.usda_nass_release`, `control.usda_nass_slice`
- **Silver:** `silver_nass.observation_revision`, `silver_nass.fact_crop_observation`, `silver_nass.dim_commodity / statistic / domain`
- **Gold:** `gold_nass.crop_series`, `gold_nass.crop_observation`, `gold_nass.latest_release_observation`
- **Status (live count 2026-10-31):** Loaded. 1990–2024.

## Reference geography

<a id="census_geo"></a>
### Census geography (Gazetteer, boundaries, delineations) (`CENSUS_GEO`)

- **Publisher:** U.S. Census Bureau and OMB
- **Code:** `src/data_ingestion_toolbox/silver_ref` · DAG `silver_ref` · Monthly, 1st 05:00 UTC
- **Publisher releases:** Yearly vintages
- **Credential:** none
- **Years:** Gazetteer and cartographic boundaries from 2013; county history from 1990; 2000 and 1990 place/tract vintages for history
- **Datasets:** Gazetteer (state, county, place, tract, ZCTA); Cartographic boundaries 500k; CBSA delineation (OMB 23-01); Tract 2000-2010 relationship and decade crosswalks; County adjacency; BEA and EIA provider areas
- **Measures:** Every place's identity, names, areas, coordinates, boundaries and relationships (contains, intersects, adjacent). Joined by geo_sk/geo_id everywhere.
- **Capture and control:** `raw_capture.response_capture`
- **Silver:** `silver_ref.dim_geo_entity`, `silver_ref.dim_geo_entity_version`, `silver_ref.dim_geo_geometry_version`, `silver_ref.bridge_geo_relationship_version`, `silver_ref.tract_decade_crosswalk`, `silver_ref.geography_resolution`, `silver_ref.dim_time`
- **Gold:** `gold_glossary.geo_boundary`, `gold_glossary.geo_relationship`, `gold_glossary.geo_search`, `gold.tile_boundary / tile_tract (Martin tiles)`
- **Status (live count 2026-10-31):** Partly loaded. One boundary vintage each: state, county, tract and place 2025; metro 2023 (925); ZCTA 2020. Historical place boundaries 2010–2024 are a recorded gap.

<a id="added-2026-11"></a>
## What was added in November 2026

Between 2026-11-03 and 2026-11-07 the warehouse gained three groups of measures (taxes and public money, elections,
cost of living) and two bulk histories (FBI Return A, building permits back to 2000). Every source follows the same
layers: **bronze** is the publisher's bytes in `raw_capture.response_capture` / `payload_blob` with the source's
`control.*` ledger, **silver** is the parsed revisions and facts in `silver_<source>`, and **gold** is the serving views
in `gold_<source>`, whose `metric_publisher` view feeds the catalog the API and site read. Diagrams: [erd.md](erd.md).

| Group | Source | Publisher and product | Geography | Years | Refresh | What the site shows | Bronze → silver → gold | Pages |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Taxes | [`CENSUS_GOVFIN`](#census_govfin) | Census Annual Survey of State and Local Government Finances (`asslgf` API); Census of Governments unit files | Nation, state; county areas summed from their governments | FY 2017–2024 (API); 2002, 2012, 2017, 2022 (unit files; 2007 only in the 1967–2012 history file, not loaded) | Monthly, 18th | Taxes collected by type (property, sales, income, other), charges, aid, debt; tax mix; tax burden | `control.census_govfin_slice` → `silver_census_govfin.*` → `gold_census_govfin.tax_mix`, `tax_burden` | Taxes and public money; governor table |
| Taxes | [`IRS_SOI`](#irs_soi) | IRS Statistics of Income county and state income data | Nation, state, county | Tax years 1989–2023 (tax and EITC items from 2010; nation from 2020) | Monthly, 7th | AGI, wages, income tax, EITC and more; federal income tax as a share of AGI; share of returns claiming the EITC | `control.irs_soi_file` → `silver_irs_soi.*` → `gold_irs_soi.observation_latest` | Taxes and public money; explorer, map |
| Taxes | [`IRS_MIGRATION`](#irs_migration) (extended) | IRS SOI county-to-county migration | County pairs | 1990-1991 to 2022-2023 (txt 1990–91, xls 1995–2010, CSV from 2011) | Monthly, 5th | Movers in and out, their AGI, by county pair | `control.irs_migration_file` → `silver_irs_migration.fact_flow` → `gold_irs_migration.flow_latest` | County People chapter |
| Taxes | [`BEA`](#bea) (extended) | BEA regional accounts CAINC4, CAINC35, SAINC35, SAINC50, SAINC51 | County (CAINC), state and nation (SAINC) | County programs to 2022; state to 2025 | Weekly, Wednesday | Payments by program; personal taxes; after-tax income; where income comes from | `control.bea_table_capture` → `silver_bea.fact_observation` → `gold_bea.observation_latest`, `disposable_income_real` | Taxes and public money |
| Taxes | [`CENSUS_ACS`](#census_acs) (extended) | ACS table B25103, median real estate taxes paid | Nation to tract | ACS years held | Monthly check | Median property tax; as a percent of median home value | `control.acs_ingestion_slices` → `silver_census.fact_demographics` → `gold_census.rpt_acs_observations` | Taxes and public money (property tax load) |
| Elections | [`ELECTIONS`](#elections) | MIT Election Data and Science Lab on Harvard Dataverse; congress-legislators; Wikidata | State, county; House districts as contests | President by county 2000–2024, by state 1976–2024; Senate and House 1976–2024; seat terms from 1969 | Monthly, 3rd; lean groups daily | Republican two-party share, two-party margin, total votes; seat holders; governors; lean groups; governor-term fiscal change | `control.elections_run`, `elections_file` → `silver_elections.*` → `gold_elections.*` | State governor table; explorer and map |
| Cost of living | [`BLS`](#bls) (extended) | BLS Average Price (`ap`), CPI-U (`cu`) and Consumer Expenditure Survey (`cx`) | Nation, 4 regions, 9 divisions, 23 metros | 1990–2026, monthly (spending 1990–2025, annual) | Monthly, 1st | 11 dollar prices (gasoline, eggs, milk, bread, beef, chicken, bananas, coffee, electricity, utility gas); 11 CPI items (incl. national day care and preschool); minutes of work; yearly household grocery spending (nation, 4 regions, 5 household sizes) | `control.bls_ingestion_slices` → `silver_bls.fact_labor_statistics` → `gold_bls.mv_bls_latest` | Cost of living section |
| Cost of living | [`EIA`](#eia) (extended) | EIA petroleum, electricity and natural gas prices | Nation, 9 states, 19 areas (fuel); every state (electricity, gas) | Gasoline and diesel from 2015; electricity from 2001; natural gas from 1973; heating oil and propane from 1990 (winter) | Weekly, Tuesday | Gasoline and diesel per gallon; electricity, natural gas, heating oil, propane | `control.eia_read`, `eia_page` → `silver_eia.fact_retail_price`, `fact_energy_price` → `gold_eia.observation_latest` | Cost of living section |
| Crime | [`FBI_UCR`](#fbi_ucr) (extended) | FBI Return A master files (`reta-<year>.zip`) | Every agency, placed in counties; state; nation | 2015–2025, monthly | Operator load (schedule in progress); source-row backfill weekly | Violent and property crime, offenses cleared, participation | `control.fbi_return_a_year` → `silver_fbi.return_a_source_row`, `fact_crime_observation` → `gold_fbi.county_rollup` | Crime chapter |
| Housing | [`CENSUS_BPS`](#census_bps) (history) | Census Building Permits Survey | Nation, state, county monthly and annual; place annual | 2000–2026 (places from 2007) | Monthly, 25th | Units, buildings and valuation authorized, by structure type | `control.census_bps_slice` → `silver_census_bps.fact_observation` → `gold_census_bps.observation_latest` | Housing chapter |

### Measures the warehouse derives from them

Each pairing is approved in [`docs/semantics/derived_ratios.json`](../semantics/derived_ratios.json) (ADR-0009) or
defined in the view named; nothing is added together, scored or ranked, and a missing input gives no value, never zero.

- **State and local tax burden:** Census state and local taxes collected in the fiscal year ÷ BEA personal income of residents (CAINC1 line 1) in the calendar year the fiscal year ends; 2017–2024 (`gold_census_govfin.tax_burden`). It is a different definition from the Tax Foundation's.
- **Tax mix:** each tax type's share of all state and local taxes, adding to 100 (`gold_census_govfin.tax_mix`).
- **Payments from government and others:** BEA transfer receipts ÷ personal income, in total and for Social Security, Medicare, Medicaid, SSI, EITC, SNAP, unemployment insurance and veterans' benefits (county CAINC35 ÷ CAINC1; state SAINC35 ÷ SAINC50 line 10).
- **Where income comes from:** dividends, interest and rent, and transfer receipts, each as a share of personal income (CAINC4).
- **Personal taxes:** personal current taxes, federal, state and local, as a share of personal income (SAINC50).
- **Federal income tax as a share of AGI** and **share of returns claiming the EITC** (IRS SOI).
- **Property tax to home value:** ACS median real estate taxes ÷ median home value (B25103_001 ÷ B25077_001).
- **Minutes of work:** a BLS average price ÷ (the QCEW annual average weekly wage of the page's county, state or nation ÷ 40 hours), × 60; for eggs, milk, bread, ground beef, chicken and electricity, using the region's or the nation's price. Both areas are named beside the value.
- **Lean groups:** county change net of the nation in real income per person, real GDP, jobs (QCEW), unemployment rate (LAUS) and poverty rate (SAIPE), grouped into four bands by the window-start presidential margin (`gold_elections.lean_group_change`).
- **Governor-term change:** a state's change in tax burden and payments share over each governor's tenure, minus the nation's, with the tax mix (`gold_elections.governor_term_fiscal`, `governor_term_tax_mix`).
- **Ended series:** a key whose newest real value is older than its grain allows (monthly 548 days, weekly 120, daily 90, quarterly 730) is published with catalog state `ended`; its history is still served.

## Keeping this current

- A new source: add a row here and a section above when its operations guide lands (`docs/reference/ADDING_A_DATA_SOURCE.md`).
- A new year or edition registered: update **Years**.
- What is actually loaded: run `psql "$WAREHOUSE_DSN" -f docs/warehouse/coverage.sql` (read-only; ACS is read from its ingestion ledger, every other source from its latest-serving relation) and update **Status**.
