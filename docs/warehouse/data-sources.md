# Data sources

What the warehouse loads, at which geographic levels, for which years, how often, and where each source lands.
Everything here is what the repository declares (each source's `registry.py`/`config.py`, its DAG schedule, and its
operations guide under `docs/user-guides/`). The **status** column and each source's loaded years are a read-only count of
the internal warehouse with [`coverage.sql`](coverage.sql), taken 2026-10-31 17:05 UTC. **Loaded** means the declared years
are there; **Partly loaded** means years or levels are still missing.

Interactive version: [Warehouse Source Map](https://claude.ai/artifact/R8x3zkZjRAXFcPEYNr45s7). Table structures: [erd.md](erd.md).

**Live version (admins):** the site's `/admin/warehouse` page shows this map and the schema atlas read from the
warehouse each time it opens: grains and metric counts from `gold_glossary.dim_metric_catalog`, years per grain
measured from each source's serving relations (kept until a new release lands), and every table and view from `pg_catalog`. The
descriptions it shows come from `apps/api/services/warehouse_inventory.py`; a unit test fails if a served source has
no entry there, so add one with every new source. Turn the page on by listing operator labels or sign-in emails in
`API_ADMIN_ACCOUNTS`. This file and `erd.md` are the snapshot of 2026-10-31.

Marks: ● the publisher's own figure · ◐ built by the warehouse (summed, averaged or placed from finer records) · ◌ some programs or areas only.

| Source | Publisher | Nation | State | County | Metro | Place | Tract | Other | Years | Refresh | Status |
| --- | --- | :-: | :-: | :-: | :-: | :-: | :-: | :-: | --- | --- | --- |
| [American Community Survey](#census_acs) | U.S. Census Bureau | ● | ● | ● | ● | ● | ● |  | 2005–2024 | Monthly check | Partly loaded |
| [Census 2000 and 1990](#census_decennial) | U.S. Census Bureau | ● | ● | ● |  | ● | ● |  | 1990–2000 | One-time history load | Partly loaded |
| [Population Estimates Program](#census_pep) | U.S. Census Bureau | ● | ● | ● |  | ● |  | ● | 1970–2025 | Monthly check | Loaded |
| [SOI county-to-county migration](#irs_migration) | IRS Statistics of Income |  |  | ● |  |  |  |  | 2019–2023 | Monthly | Loaded |
| [BLS labor and prices (LAUS, CPS, CES, CPI, JOLTS, average prices)](#bls) | Bureau of Labor Statistics | ● | ● | ● | ● | ◌ |  | ◌ | 1990–2026 | Monthly | Loaded |
| [Quarterly Census of Employment and Wages](#bls_qcew) | Bureau of Labor Statistics | ● | ● | ● |  |  |  |  | 2014–2025 | Monthly | Partly loaded |
| [County Business Patterns](#census_cbp) | U.S. Census Bureau | ● | ● | ● |  |  |  |  | 2016–2023 | Monthly | Loaded |
| [Regional economic accounts](#bea) | Bureau of Economic Analysis | ● | ● | ● | ◐ |  |  | ● | 1969–2024 | Weekly | Loaded |
| [SAIPE and SAHIE](#census_saipe_sahie) | U.S. Census Bureau | ● | ● | ● |  |  |  |  | 1989–2024 | Monthly | Partly loaded |
| [LEHD Origin-Destination Employment Statistics](#census_lodes) | U.S. Census Bureau |  | ◐ | ◐ |  |  |  |  | 2002–2023 | Monthly | Partly loaded |
| [Building Permits Survey](#census_bps) | U.S. Census Bureau | ● | ● | ● |  | ● |  |  | 2000–2026 | Monthly | Partly loaded |
| [House Price Index (annual, all transactions)](#fhfa_hpi) | Federal Housing Finance Agency |  |  | ● |  |  |  |  | 1975–2025 | Monthly | Loaded |
| [Fair Market Rents and income limits](#hud_fmr_il) | HUD |  |  | ● |  |  |  |  | 2026–2027 | Monthly | Loaded |
| [Weekly retail gasoline prices](#eia) | Energy Information Administration | ● | ◌ |  |  |  |  | ● | 2015–2026 | Weekly | Loaded |
| [FRED national indicators](#fred) | Federal Reserve Bank of St. Louis | ● |  |  |  |  |  |  | 1990–2026 | Monthly | Loaded |
| [Crime Data Explorer summarized offenses](#fbi_ucr) | FBI | ● | ● | ◐ |  |  |  | ● | 1990–2023 | Weekly | Loaded |
| [National Risk Index and disaster declarations](#fema_nri) | FEMA |  |  | ● |  |  |  |  | 1953–2026 | Daily | Loaded |
| [Chronic Disease Indicators and PLACES](#cdc) | CDC | ● | ● | ● |  |  |  |  | 2019–2023 | Weekly | Loaded |
| [AirData annual monitor concentrations](#epa_aqs) | EPA |  |  | ◐ |  |  |  |  | 2020–2024 | Monthly | Loaded |
| [U.S. Climate Normals 1991-2020](#noaa_normals) | NOAA NCEI |  |  | ◐ |  |  |  |  | 2020 | Quarterly | Loaded |
| [Common Core of Data, public schools](#nces_ccd) | NCES |  | ◐ | ◐ |  |  |  |  | 2023–2024 | Quarterly | Loaded |
| [National Broadband Map availability](#fcc_bdc) | FCC | ● | ● | ● |  | ● |  |  | 2024–2025 | Monthly | Loaded |
| [Rural-urban codes, county typology, food atlas](#usda_ers) | USDA Economic Research Service |  |  | ● |  |  |  |  | 2015–2025 | Monthly | Loaded |
| [Quick Stats crops](#usda_nass) | USDA NASS | ● | ● | ● |  |  |  |  | 1990–2024 | Business days | Loaded |
| [Census geography (Gazetteer, boundaries, delineations)](#census_geo) | U.S. Census Bureau and OMB | ● | ● | ● | ● | ● | ● | ● | 1990–2025 | Monthly | Partly loaded |

Other areas: Census regions and divisions and county subdivisions (PEP), CPI and average-price areas (BLS), BEA state metro/nonmetro portions, EIA PADDs and cities, reviewed law-enforcement agencies (FBI).

## People and population

<a id="census_acs"></a>
### American Community Survey (`CENSUS_ACS`)

- **Publisher:** U.S. Census Bureau
- **Code:** `src/data_ingestion_toolbox/census_acs` · DAG `acs_ingest` · Monthly check, 1st 06:00 UTC
- **Publisher releases:** 1-year each September, 5-year each December
- **Credential:** CENSUS_API_KEY
- **Years:** 1-year from 2005 (no 2020 release); 5-year from 2009; tracts 5-year only
- **Datasets:** acs1 (1-year estimates); acs5 (5-year estimates)
- **Measures:** 59 curated detailed tables (age, sex, race, origin, language, mobility, commute, education, disability, veterans, poverty, employment, income, earnings, industry, occupation, housing stock, tenure, rent, value, costs, health insurance, internet). 22 of them at tract grain.
- **Capture and control:** `raw_census.acs_datasets`, `raw_census.acs_tables`, `raw_census.acs_variables`, `control.acs_ingestion_slices`
- **Silver:** `silver_census.observation_revision`, `silver_census.fact_demographics`
- **Gold:** `gold_census.rpt_acs_observations (partitioned by year)`, `gold_census.mv_acs_latest`, `gold_census.fact_acs_observation`, `gold_census.acs_tract_comparable`, `gold_census.dim_acs_table`, `gold_census.dim_acs_variable`
- **Status (live count 2026-10-31):** Partly loaded. 5-year: 2009–2024 at nation, state, county, place and tract. 1-year: 2005–2024 at nation, state, county and place (19 releases; the Bureau published no standard 2020 1-year). Metro: 2022–2024 so far; the metro backfill (2024 back to 2005) and the tract wide reload were running at the count.

<a id="census_decennial"></a>
### Census 2000 and 1990 (`CENSUS_DECENNIAL`)

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
- **Years:** Filing-year pairs 2018-2019 to 2022-2023
- **Datasets:** countyinflowYYYY.csv; countyoutflowYYYY.csv
- **Measures:** Returns, individuals and adjusted gross income for each county pair, plus non-migrants and SOI's totals (same state, other state, foreign). Flows under 20 returns are withheld.
- **Capture and control:** `control.irs_migration_file`
- **Silver:** `silver_irs_migration.flow_revision`, `silver_irs_migration.fact_flow`, `silver_irs_migration.flow_quarantine`
- **Gold:** `gold_irs_migration.flow_revision`, `gold_irs_migration.flow_latest`, `gold_irs_migration.total_observation_revision`, `gold_irs_migration.total_observation_latest`
- **Status (live count 2026-10-31):** Loaded. County 2019–2023, 468,528 rows.

## Work, income and business

<a id="bls"></a>
### BLS labor and prices (LAUS, CPS, CES, CPI, JOLTS, average prices) (`BLS`)

- **Publisher:** Bureau of Labor Statistics
- **Code:** `src/data_ingestion_toolbox/bls` · DAG `bls_ingest` · Monthly, 1st 07:00 UTC
- **Publisher releases:** Monthly
- **Credential:** BLS_API_KEY
- **Years:** History band from 1990 plus a rolling two-year window
- **Datasets:** LAUS (state, county, metro, city); CPS / LN (national); CES (national payroll); CPI-U (national); JOLTS (national); Average prices (nation, 4 regions, 9 divisions, 23 metros)
- **Measures:** Unemployment rate and level, employment, labor force, participation, employment-population ratio; 17 national CPS series; 17 CES series; 9 CPI-U series; 13 JOLTS series; grocery and fuel average prices.
- **Capture and control:** `raw_bls.bls_datasets`, `raw_bls.bls_series`, `control.bls_ingestion_slices`
- **Silver:** `silver_bls.observation_revision`, `silver_bls.fact_labor_statistics`
- **Gold:** `gold_bls.rpt_bls_observations`, `gold_bls.mv_bls_latest`, `gold_bls.fact_bls_observation`, `gold_bls.provider_annual_average`, `gold_bls.calendar_window_observation`, `gold_bls.derived_calendar_rollup`, `gold_bls.dim_bls_series / measure / survey`
- **Status (live count 2026-10-31):** Loaded. 1990–2026 at county, metro, state and nation, plus BLS's own areas (regions from 1990, divisions from 2017). No place-level rows were seen.

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
- **Status (live count 2026-10-31):** Partly loaded. Only 2025–2026 is loaded; the registry declares 2014–2025.

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
- **Datasets:** CAINC1; CAINC4; CAINC5N; CAGDP1; CAGDP2; SARPP; MARPP; PARPP; MAIRPD
- **Measures:** Personal income, population, per capita income, earnings, transfer receipts, wages; earnings and GDP by industry; real and current GDP; regional price parities and deflators.
- **Capture and control:** `control.bea_table_capture`
- **Silver:** `silver_bea.observation_revision`, `silver_bea.fact_observation`, `silver_bea.dim_line`
- **Gold:** `gold_bea.observation_revision`, `gold_bea.observation_latest`
- **Status (live count 2026-10-31):** Loaded. 1969–2024 at county, metro, state and nation; BEA's state metro/nonmetro portions 2008–2024; about 5.7 million rows. Metro income and GDP are summed from counties (918 of 925 metros for 2023; Connecticut's 7 metros 2024 only).

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

<a id="census_lodes"></a>
### LEHD Origin-Destination Employment Statistics (`CENSUS_LODES`)

- **Publisher:** U.S. Census Bureau
- **Code:** `src/data_ingestion_toolbox/census_lodes` · DAG `census_lodes_ingest` · Monthly, 20th 14:00 UTC (newest year)
- **Publisher releases:** Yearly vintage
- **Credential:** none
- **Years:** LODES8, 2002-2023 registered; older years by raising recent_years
- **Datasets:** rac (residence); wac (workplace); od_main; od_aux
- **Measures:** Resident workers, jobs, live-and-work, inbound and in-state outbound commuters. Summed from census blocks.
- **Capture and control:** `control.census_lodes_slice`, `control.census_lodes_file`
- **Silver:** `silver_census_lodes.fact_area`, `silver_census_lodes.fact_flow`
- **Gold:** `gold_census_lodes.observation_revision`, `gold_census_lodes.observation_latest`
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
- **Status (live count 2026-10-31):** Partly loaded. Only 2026 is loaded; the registry declares 2000–2026. No place-level rows.

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
### Weekly retail gasoline prices (`EIA`)

- **Publisher:** Energy Information Administration
- **Code:** `src/data_ingestion_toolbox/eia` · DAG `eia_retail_gasoline_ingest` · Weekly, Tuesday 15:00 UTC
- **Publisher releases:** Every Monday
- **Credential:** EIA_API_KEY
- **Years:** Every week from 2015-01-05
- **Datasets:** petroleum/pri/gnd (EPMR, EPMM, EPMP, EPM0)
- **Measures:** Regular, midgrade, premium and all-grades gasoline, dollars per gallon.
- **Capture and control:** `control.eia_read`, `control.eia_page`
- **Silver:** `silver_eia.price_revision`, `silver_eia.fact_retail_price`
- **Gold:** `gold_eia.observation_revision`, `gold_eia.observation_latest`
- **Status (live count 2026-10-31):** Loaded. 2015–2026.

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
### Crime Data Explorer summarized offenses (`FBI_UCR`)

- **Publisher:** FBI
- **Code:** `src/data_ingestion_toolbox/fbi_ucr` · DAG `fbi_ucr_ingest` · Weekly, Monday 10:00 UTC
- **Publisher releases:** UCR refresh
- **Credential:** FBI_CDE_API_KEY
- **Years:** Monthly, January 1990 to June 2023
- **Datasets:** 10 offense products: violent, assault, burglary, larceny, motor vehicle theft, homicide, rape, robbery, arson, property
- **Measures:** Offenses and clearances, as counts and rates. County rollup sums agency-reported totals (ADR-0012 county rate with a 50% coverage floor is in progress).
- **Capture and control:** `control.fbi_ucr_release`
- **Silver:** `silver_fbi.observation_revision`, `silver_fbi.fact_crime_observation`, `silver_fbi.dim_agency(_version)`, `silver_fbi.agency_geography_relationship`, `silver_fbi.fact_reporting_participation`
- **Gold:** `gold_fbi.crime_observation`, `gold_fbi.latest_release_observation`, `gold_fbi.county_rollup`, `gold_fbi.latest_county_rollup`, `gold_fbi.reporting_coverage`, `gold_fbi.agency_observation_area_filter`
- **Status (live count 2026-10-31):** Loaded. Agency, state and nation 1990–2023; county rollup 1990–2023 (33 years). The agencies are the six-agency Wisconsin sample in the registry, so few counties get a rate; a nationwide bulk load is planned.

<a id="fema_nri"></a>
### National Risk Index and disaster declarations (`FEMA_NRI`)

- **Publisher:** FEMA
- **Code:** `src/data_ingestion_toolbox/fema_nri` · DAG `fema_nri_ingest` · Daily, 06:00 UTC
- **Publisher releases:** NRI by version; declarations every 20 minutes
- **Credential:** none
- **Years:** NRI December 2025 (v1.20.0), one vintage; declarations counted by calendar year
- **Datasets:** NRI county layer (ArcGIS); OpenFEMA DisasterDeclarationsSummaries v2
- **Measures:** Expected annual loss (total and 18 hazards), annualized frequency (5 hazards); major disaster, emergency and fire management declaration counts.
- **Capture and control:** `control.fema_nri_run`, `control.fema_nri_page`
- **Silver:** `silver_fema_nri.nri_fact`, `silver_fema_nri.declaration_revision`
- **Gold:** `gold_fema_nri.nri_observation`, `gold_fema_nri.declaration_count`, `gold_fema_nri.observation_revision`, `gold_fema_nri.observation_latest`
- **Status (live count 2026-10-31):** Loaded. County 1959–2026.

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
- **Datasets:** annual/seasonal multivariate by station archive
- **Measures:** Annual mean, max and min temperature, precipitation, heating and cooling degree days: mean of stations placed in the county.
- **Capture and control:** `control.noaa_normals_file`
- **Silver:** `silver_noaa_normals.station`, `silver_noaa_normals.station_normal`
- **Gold:** `gold_noaa_normals.station_observation`, `gold_noaa_normals.observation_revision`, `gold_noaa_normals.observation_latest`
- **Status (live count 2026-10-31):** Loaded. The 1991–2020 normals (period 2020).

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

## Keeping this current

- A new source: add a row here and a section above when its operations guide lands (`docs/reference/ADDING_A_DATA_SOURCE.md`).
- A new year or edition registered: update **Years**.
- What is actually loaded: run `psql "$WAREHOUSE_DSN" -f docs/warehouse/coverage.sql` (read-only; ACS is read from its ingestion ledger, every other source from its latest-serving relation) and update **Status**.
