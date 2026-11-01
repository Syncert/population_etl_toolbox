"""What each warehouse source is, as the repository declares it.

The admin warehouse page (``/admin/warehouse``) joins this declared inventory to
what the live warehouse reports: the metric catalog (grains, metric counts,
newest watermark), each source's serving relation (years per grain), and the
Postgres catalog (every table, view and key). Only the descriptive part lives
here -- names, datasets, schedules, the tables a source lands in -- because
none of it is in the warehouse to be read.

A unit test holds this list to the serving registry: every source the API
serves must be described here, so a source cannot arrive without its entry.

Grain marks: ``n`` is the publisher's own figure; ``d`` is built by the
warehouse (summed, averaged or placed from finer records); ``p`` covers some
programs or areas only. Years are inclusive observation-year ranges.
"""

from __future__ import annotations

from dataclasses import dataclass

#: (code, label) in display order. ``other`` is every other area kind a source
#: publishes: Census regions and divisions, BEA state portions, EIA PADDs and
#: cities, CPI areas, law-enforcement agencies.
GRAINS: tuple[tuple[str, str], ...] = (
    ("nation", "Nation"),
    ("state", "State"),
    ("county", "County"),
    ("metro", "Metro"),
    ("place", "Place"),
    ("tract", "Tract"),
    ("other", "Other areas"),
)

TOPICS: tuple[str, ...] = (
    "People and population",
    "Work, income and business",
    "Housing",
    "Prices and the national economy",
    "Crime and hazards",
    "Health",
    "Environment",
    "Schools and broadband",
    "Rural and farm",
    "Reference geography",
)

#: The warehouse's served grain vocabulary (``gold_glossary.geo_grain``) folded
#: onto the page's columns.
GRAIN_OF_SERVED_LEVEL: dict[str, str] = {
    "NATIONAL": "nation",
    "STATE": "state",
    "COUNTY": "county",
    "METRO": "metro",
    "PLACE": "place",
    "TRACT": "tract",
}


@dataclass(frozen=True)
class WarehouseSource:
    code: str
    name: str
    publisher: str
    topic: str
    module: str
    dag: str
    cadence: str
    release: str
    grains: dict[str, str]
    years: list[tuple[int, int]]
    years_note: str
    datasets: list[str]
    measures: str
    raw: list[str]
    control: list[str]
    silver: list[str]
    gold: list[str]
    credential: str
    #: The ``source_code`` the metric catalog publishes this source under, or
    #: ``None`` for the reference geography, which publishes no metrics.
    catalog_source_code: str | None


WAREHOUSE_SOURCES: tuple[WarehouseSource, ...] = (
    WarehouseSource(
        code="CENSUS_ACS",
        name="American Community Survey",
        publisher="U.S. Census Bureau",
        topic="People and population",
        module="census_acs",
        dag="acs_ingest",
        cadence="Monthly check, 1st 06:00 UTC",
        release="1-year each September, 5-year each December",
        grains={
            "nation": "n",
            "state": "n",
            "county": "n",
            "metro": "n",
            "place": "n",
            "tract": "n",
        },
        years=[(2005, 2024)],
        years_note="1-year from 2005 (no 2020 release); 5-year from 2009; tracts 5-year only",
        datasets=["acs1 (1-year estimates)", "acs5 (5-year estimates)"],
        measures=(
            "59 curated detailed tables (age, sex, race, origin, language, mobility, commute, education, "
            "disability, veterans, poverty, employment, income, earnings, industry, occupation, housing "
            "stock, tenure, rent, value, costs, health insurance, internet). 22 of them at tract grain."
        ),
        raw=[
            "raw_census.acs_datasets",
            "raw_census.acs_tables",
            "raw_census.acs_variables",
        ],
        control=["control.acs_ingestion_slices"],
        silver=[
            "silver_census.observation_revision",
            "silver_census.fact_demographics",
        ],
        gold=[
            "gold_census.rpt_acs_observations (partitioned by year)",
            "gold_census.mv_acs_latest",
            "gold_census.fact_acs_observation",
            "gold_census.acs_tract_comparable",
            "gold_census.tract_decade_change",
            "gold_census.dim_acs_table",
            "gold_census.dim_acs_variable",
        ],
        credential="CENSUS_API_KEY",
        catalog_source_code="CENSUS_ACS",
    ),
    WarehouseSource(
        code="CENSUS_DECENNIAL",
        name="Census 2000 and 1990",
        publisher="U.S. Census Bureau",
        topic="People and population",
        module="census_acs (decennial.py, load_1990.py)",
        dag="acs_ingest (2000); operator load (1990 discs)",
        cadence="One-time history load",
        release="Fixed historical releases",
        grains={"nation": "n", "state": "n", "county": "n", "place": "n", "tract": "n"},
        years=[(1990, 1990), (2000, 2000)],
        years_note="2000 SF1 and SF3 from the Data API; 1990 STF1A and STF3A from the Bureau's archived discs",
        datasets=[
            "dec2000sf1 (100-percent counts)",
            "dec2000sf3 (long-form sample)",
            "dec1990stf1",
            "dec1990stf3",
        ],
        measures=(
            "The ACS curated tables, crosswalked to the 2000 and 1990 tables (decennial_crosswalk.json, "
            "decennial_1990_crosswalk.json). No margins of error."
        ),
        raw=["raw_census.acs_variables"],
        control=["control.acs_ingestion_slices"],
        silver=[
            "silver_census.observation_revision",
            "silver_census.fact_demographics",
        ],
        gold=["gold_census.rpt_acs_observations (2000, 1990 partitions)"],
        credential="CENSUS_API_KEY (2000 only)",
        catalog_source_code="CENSUS_ACS",
    ),
    WarehouseSource(
        code="CENSUS_PEP",
        name="Population Estimates Program",
        publisher="U.S. Census Bureau",
        topic="People and population",
        module="census_pep",
        dag="pep_ingest",
        cadence="Monthly check, 1st 06:00 UTC",
        release="New vintage each year (states Dec, counties Mar, cities May)",
        grains={"nation": "n", "state": "n", "county": "n", "place": "n", "other": "n"},
        years=[(1970, 2025)],
        years_note=(
            "Postcensal vintages 2024 and 2025 (2020-2025); 2010s, 2000s intercensal, and 1990s, 1980s, "
            "1970s county totals"
        ),
        datasets=[
            "pep_nst_alldata",
            "pep_county_alldata",
            "pep_subcounty",
            "pep_county_alldata_2010s",
            "pep_nst_alldata_2010s",
            "pep_county_alldata_2000s",
            "pep_county_intercensal_2000s",
            "pep_county_totals_1990s / 1980s / 1970s",
        ],
        measures=(
            "Population estimate; components of change from 2000 (births, deaths, natural change, "
            "international and domestic migration, net migration, and their rates). Before 2000, totals "
            "only."
        ),
        raw=[],
        control=["silver_pep.release_load"],
        silver=[
            "silver_pep.pep_dataset",
            "silver_pep.pep_release",
            "silver_pep.observation_revision",
            "silver_pep.fact_population_estimate",
            "silver_pep.dim_measure",
        ],
        gold=[
            "gold_pep.rpt_pep_observations",
            "gold_pep.population_estimate_revision",
            "gold_pep.population_estimate_latest",
            "gold_pep.population_change",
            "gold_pep.mv_pep_latest (view)",
        ],
        credential="none",
        catalog_source_code="CENSUS_PEP",
    ),
    WarehouseSource(
        code="IRS_MIGRATION",
        name="SOI county-to-county migration",
        publisher="IRS Statistics of Income",
        topic="People and population",
        module="irs_migration",
        dag="irs_migration_ingest",
        cadence="Monthly, 5th 15:00 UTC",
        release="One new year pair a year",
        grains={"county": "n"},
        years=[(2019, 2023)],
        years_note="Filing-year pairs 2018-2019 to 2022-2023",
        datasets=["countyinflowYYYY.csv", "countyoutflowYYYY.csv"],
        measures=(
            "Returns, individuals and adjusted gross income for each county pair, plus non-migrants and "
            "SOI's totals (same state, other state, foreign). Flows under 20 returns are withheld."
        ),
        raw=[],
        control=["control.irs_migration_file"],
        silver=[
            "silver_irs_migration.flow_revision",
            "silver_irs_migration.fact_flow",
            "silver_irs_migration.flow_quarantine",
        ],
        gold=[
            "gold_irs_migration.flow_revision",
            "gold_irs_migration.flow_latest",
            "gold_irs_migration.total_observation_revision",
            "gold_irs_migration.total_observation_latest",
        ],
        credential="none",
        catalog_source_code="IRS_MIGRATION",
    ),
    WarehouseSource(
        code="BLS",
        name="BLS labor and prices (LAUS, CPS, CES, CPI, JOLTS, average prices)",
        publisher="Bureau of Labor Statistics",
        topic="Work, income and business",
        module="bls",
        dag="bls_ingest",
        cadence="Monthly, 1st 07:00 UTC",
        release="Monthly",
        grains={
            "nation": "n",
            "state": "n",
            "county": "n",
            "metro": "n",
            "place": "p",
            "other": "p",
        },
        years=[(1990, 2026)],
        years_note="History band from 1990 plus a rolling two-year window",
        datasets=[
            "LAUS (state, county, metro, city)",
            "CPS / LN (national)",
            "CES (national payroll)",
            "CPI-U (national)",
            "JOLTS (national)",
            "Average prices (nation, 4 regions, 9 divisions, 23 metros)",
        ],
        measures=(
            "Unemployment rate and level, employment, labor force, participation, employment-population "
            "ratio; 17 national CPS series; 17 CES series; 9 CPI-U series; 13 JOLTS series; grocery and "
            "fuel average prices."
        ),
        raw=["raw_bls.bls_datasets", "raw_bls.bls_series"],
        control=["control.bls_ingestion_slices"],
        silver=["silver_bls.observation_revision", "silver_bls.fact_labor_statistics"],
        gold=[
            "gold_bls.rpt_bls_observations",
            "gold_bls.mv_bls_latest",
            "gold_bls.fact_bls_observation",
            "gold_bls.provider_annual_average",
            "gold_bls.calendar_window_observation",
            "gold_bls.derived_calendar_rollup",
            "gold_bls.dim_bls_series / measure / survey",
        ],
        credential="BLS_API_KEY",
        catalog_source_code="BLS",
    ),
    WarehouseSource(
        code="BLS_QCEW",
        name="Quarterly Census of Employment and Wages",
        publisher="Bureau of Labor Statistics",
        topic="Work, income and business",
        module="bls_qcew",
        dag="bls_qcew_ingest",
        cadence="Monthly, 20th 12:00 UTC",
        release="Quarterly, about five months after quarter end",
        grains={"nation": "n", "state": "n", "county": "n"},
        years=[(2014, 2025)],
        years_note="Quarters and annual averages from 2014 (the open-data interface's first year)",
        datasets=["Industry slices: total (all and private) + 21 NAICS sectors"],
        measures=(
            "Establishments, monthly employment, total quarterly wages, average weekly wage (jobs located "
            "in an area)."
        ),
        raw=[],
        control=["control.bls_qcew_slice"],
        silver=[
            "silver_bls_qcew.observation_revision",
            "silver_bls_qcew.fact_observation",
            "silver_bls_qcew.dim_industry",
            "silver_bls_qcew.dim_measure",
        ],
        gold=["gold_bls_qcew.observation_revision", "gold_bls_qcew.observation_latest"],
        credential="none",
        catalog_source_code="BLS_QCEW",
    ),
    WarehouseSource(
        code="CENSUS_CBP",
        name="County Business Patterns",
        publisher="U.S. Census Bureau",
        topic="Work, income and business",
        module="census_cbp",
        dag="census_cbp_ingest",
        cadence="Monthly, 15th 13:00 UTC",
        release="One year a year (2023 came out June 2025)",
        grains={"nation": "n", "state": "n", "county": "n"},
        years=[(2016, 2023)],
        years_note="2016-2023 registered",
        datasets=["cbpYYco (counties)", "cbpYYst (states)", "cbpYYus (nation)"],
        measures=(
            "Employer establishments, March employment, first-quarter payroll, annual payroll; total and "
            "two-digit NAICS sectors."
        ),
        raw=[],
        control=["control.census_cbp_file"],
        silver=[
            "silver_census_cbp.observation_revision",
            "silver_census_cbp.fact_observation",
        ],
        gold=[
            "gold_census_cbp.observation_revision",
            "gold_census_cbp.observation_latest",
            "gold_census_cbp.sector_definition",
        ],
        credential="none",
        catalog_source_code="CENSUS_CBP",
    ),
    WarehouseSource(
        code="BEA",
        name="Regional economic accounts",
        publisher="Bureau of Economic Analysis",
        topic="Work, income and business",
        module="bea",
        dag="bea_regional_ingest",
        cadence="Weekly, Wednesday 14:00 UTC",
        release="County income each November, county GDP each December",
        grains={"nation": "n", "state": "n", "county": "n", "metro": "d", "other": "n"},
        years=[(1969, 2024)],
        years_note="Personal income from 1969; earnings by industry and GDP from 2001; price parities from 2008",
        datasets=[
            "CAINC1",
            "CAINC4",
            "CAINC5N",
            "CAGDP1",
            "CAGDP2",
            "SARPP",
            "MARPP",
            "PARPP",
            "MAIRPD",
        ],
        measures=(
            "Personal income, population, per capita income, earnings, transfer receipts, wages; earnings "
            "and GDP by industry; real and current GDP; regional price parities and deflators."
        ),
        raw=[],
        control=["control.bea_table_capture"],
        silver=[
            "silver_bea.observation_revision",
            "silver_bea.fact_observation",
            "silver_bea.dim_line",
        ],
        gold=["gold_bea.observation_revision", "gold_bea.observation_latest"],
        credential="none (bulk zips)",
        catalog_source_code="BEA",
    ),
    WarehouseSource(
        code="CENSUS_SAIPE_SAHIE",
        name="SAIPE and SAHIE",
        publisher="U.S. Census Bureau",
        topic="Work, income and business",
        module="census_saipe_sahie",
        dag="census_saipe_sahie_ingest",
        cadence="Monthly, 15th 06:00 UTC",
        release="SAIPE each December, SAHIE each spring",
        grains={"nation": "n", "state": "n", "county": "n"},
        years=[(1989, 2024)],
        years_note="SAIPE 1989-2024; SAHIE 2006-2023",
        datasets=[
            "saipe (/timeseries/poverty/saipe)",
            "sahie (/timeseries/healthins/sahie)",
        ],
        measures=(
            "Poverty rate and count (all ages, under 18), median household income; uninsured share and "
            "count under 65. All with 90% intervals."
        ),
        raw=[],
        control=["control.census_sae_slice"],
        silver=[
            "silver_census_sae.observation_revision",
            "silver_census_sae.fact_estimate",
            "silver_census_sae.dim_measure",
        ],
        gold=["gold_census_sae.estimate_revision", "gold_census_sae.estimate_latest"],
        credential="CENSUS_API_KEY",
        catalog_source_code="CENSUS_SAIPE_SAHIE",
    ),
    WarehouseSource(
        code="CENSUS_LODES",
        name="LEHD Origin-Destination Employment Statistics",
        publisher="U.S. Census Bureau",
        topic="Work, income and business",
        module="census_lodes",
        dag="census_lodes_ingest",
        cadence="Monthly, 20th 14:00 UTC (newest year)",
        release="Yearly vintage",
        grains={"state": "d", "county": "d"},
        years=[(2002, 2023)],
        years_note="LODES8, 2002-2023 registered; the DAG keeps the newest, census_lodes.history loads the rest",
        datasets=["rac (residence)", "wac (workplace)", "od_main", "od_aux"],
        measures=(
            "Resident workers, jobs, live-and-work, inbound and in-state outbound commuters. Summed from "
            "census blocks."
        ),
        raw=[],
        control=["control.census_lodes_slice", "control.census_lodes_file"],
        silver=["silver_census_lodes.fact_area", "silver_census_lodes.fact_flow"],
        gold=[
            "gold_census_lodes.observation_fact",
            "gold_census_lodes.observation_revision",
            "gold_census_lodes.observation_latest",
        ],
        credential="none",
        catalog_source_code="CENSUS_LODES",
    ),
    WarehouseSource(
        code="CENSUS_BPS",
        name="Building Permits Survey",
        publisher="U.S. Census Bureau",
        topic="Housing",
        module="census_bps",
        dag="census_building_permits_ingest",
        cadence="Monthly, 25th 13:00 UTC",
        release="Monthly",
        grains={"nation": "n", "state": "n", "county": "n", "place": "n"},
        years=[(2000, 2026)],
        years_note="Monthly from January 2000; places annual from 2007",
        datasets=[
            "County monthly and annual",
            "State monthly and annual",
            "Place annual by region",
        ],
        measures=(
            "Housing units, buildings and valuation authorized, by structure type (1, 2, 3-4, 5+ units); "
            "estimate and reported value."
        ),
        raw=[],
        control=["control.census_bps_slice"],
        silver=[
            "silver_census_bps.observation_revision",
            "silver_census_bps.fact_observation",
            "silver_census_bps.dim_measure",
        ],
        gold=[
            "gold_census_bps.observation_revision",
            "gold_census_bps.observation_latest",
        ],
        credential="none",
        catalog_source_code="CENSUS_BPS",
    ),
    WarehouseSource(
        code="FHFA_HPI",
        name="House Price Index (annual, all transactions)",
        publisher="Federal Housing Finance Agency",
        topic="Housing",
        module="fhfa_hpi",
        dag="fhfa_hpi_ingest",
        cadence="Monthly, 25th 15:00 UTC",
        release="Annual file, revised in full",
        grains={"county": "n"},
        years=[(1975, 2025)],
        years_note="Each county from its first recorded year in FHFA's file",
        datasets=["hpi_at_county.xlsx"],
        measures="Annual change (%), HPI with 2000 base.",
        raw=[],
        control=["control.fhfa_hpi_file"],
        silver=[
            "silver_fhfa_hpi.observation_revision",
            "silver_fhfa_hpi.fact_observation",
        ],
        gold=["gold_fhfa_hpi.observation_revision", "gold_fhfa_hpi.observation_latest"],
        credential="none",
        catalog_source_code="FHFA_HPI",
    ),
    WarehouseSource(
        code="HUD_FMR_IL",
        name="Fair Market Rents and income limits",
        publisher="HUD",
        topic="Housing",
        module="hud_fmr_il",
        dag="hud_fmr_il_ingest",
        cadence="Monthly, 25th 16:00 UTC",
        release="Each fiscal year, with reissues",
        grains={"county": "n"},
        years=[(2026, 2027)],
        years_note="FMR FY2026 (and May 2026 reissue), FY2027; income limits FY2026",
        datasets=[
            "HUD User API fmr/statedata",
            "HUD User API il/data",
            "Workbooks (manual)",
        ],
        measures=(
            "Fair market rent 0-4 bedrooms; median family income; 30/50/80% income limits for 4 people. "
            "Values are set per HUD area and repeated per county."
        ),
        raw=[],
        control=["control.hud_fmr_il_api_capture", "control.hud_fmr_il_file"],
        silver=[
            "silver_hud_fmr_il.observation_revision",
            "silver_hud_fmr_il.fact_observation",
        ],
        gold=[
            "gold_hud_fmr_il.observation_revision",
            "gold_hud_fmr_il.observation_latest",
        ],
        credential="HUD_USER_API_TOKEN",
        catalog_source_code="HUD_FMR_IL",
    ),
    WarehouseSource(
        code="EIA",
        name="Weekly retail gasoline prices",
        publisher="Energy Information Administration",
        topic="Prices and the national economy",
        module="eia",
        dag="eia_retail_gasoline_ingest",
        cadence="Weekly, Tuesday 15:00 UTC",
        release="Every Monday",
        grains={"nation": "n", "state": "p", "other": "n"},
        years=[(2015, 2026)],
        years_note="Every week from 2015-01-05",
        datasets=["petroleum/pri/gnd (EPMR, EPMM, EPMP, EPM0)"],
        measures="Regular, midgrade, premium and all-grades gasoline, dollars per gallon.",
        raw=[],
        control=["control.eia_read", "control.eia_page"],
        silver=["silver_eia.price_revision", "silver_eia.fact_retail_price"],
        gold=["gold_eia.observation_revision", "gold_eia.observation_latest"],
        credential="EIA_API_KEY",
        catalog_source_code="EIA",
    ),
    WarehouseSource(
        code="FRED",
        name="FRED national indicators",
        publisher="Federal Reserve Bank of St. Louis",
        topic="Prices and the national economy",
        module="fred",
        dag="fred_ingest",
        cadence="Monthly, 1st 08:00 UTC",
        release="Varies by series",
        grains={"nation": "n"},
        years=[(1990, 2026)],
        years_note="Full series history as FRED serves it (start varies by series)",
        datasets=["25 curated series"],
        measures=(
            "Payrolls, unemployment, participation, openings, claims, industrial production, permits, "
            "starts, mortgage rate, house supply and price, CPI, PCE, fed funds, Treasury yields and "
            "spread, breakeven inflation, financial conditions, real GDP, real consumption and income, "
            "saving rate, retail sales."
        ),
        raw=["raw_fred.fred_datasets", "raw_fred.fred_series"],
        control=["control.fred_ingestion_slices"],
        silver=[
            "silver_fred.observation_revision",
            "silver_fred.fact_economic_indicators",
        ],
        gold=[
            "gold_fred.rpt_fred_observations",
            "gold_fred.mv_fred_latest",
            "gold_fred.fact_fred_observation",
            "gold_fred.dim_fred_series",
        ],
        credential="FRED_API_KEY",
        catalog_source_code="FRED",
    ),
    WarehouseSource(
        code="FBI_UCR",
        name="Crime Data Explorer summarized offenses",
        publisher="FBI",
        topic="Crime and hazards",
        module="fbi_ucr",
        dag="fbi_ucr_ingest",
        cadence="Weekly, Monday 10:00 UTC",
        release="UCR refresh",
        grains={"nation": "n", "state": "n", "county": "d", "other": "n"},
        years=[(1990, 2023)],
        years_note="Monthly, January 1990 to June 2023",
        datasets=[
            "10 offense products: violent, assault, burglary, larceny, motor vehicle theft, homicide, rape, robbery, arson, property"
        ],
        measures=(
            "Offenses and clearances, as counts and rates. County rollup sums agency-reported totals; "
            "ADR-0012 sets the county rate rule (refused below 50% agency coverage)."
        ),
        raw=[],
        control=["control.fbi_ucr_release"],
        silver=[
            "silver_fbi.observation_revision",
            "silver_fbi.fact_crime_observation",
            "silver_fbi.dim_agency(_version)",
            "silver_fbi.agency_geography_relationship",
            "silver_fbi.fact_reporting_participation",
        ],
        gold=[
            "gold_fbi.crime_observation",
            "gold_fbi.latest_release_observation",
            "gold_fbi.county_rollup",
            "gold_fbi.latest_county_rollup",
            "gold_fbi.reporting_coverage",
            "gold_fbi.agency_observation_area_filter",
        ],
        credential="FBI_CDE_API_KEY",
        catalog_source_code="FBI_UCR",
    ),
    WarehouseSource(
        code="FEMA_NRI",
        name="National Risk Index and disaster declarations",
        publisher="FEMA",
        topic="Crime and hazards",
        module="fema_nri",
        dag="fema_nri_ingest",
        cadence="Daily, 06:00 UTC",
        release="NRI by version; declarations every 20 minutes",
        grains={"county": "n"},
        years=[(1953, 2026)],
        years_note="NRI December 2025 (v1.20.0), one vintage; declarations counted by calendar year",
        datasets=[
            "NRI county layer (ArcGIS)",
            "OpenFEMA DisasterDeclarationsSummaries v2",
        ],
        measures=(
            "Expected annual loss (total and 18 hazards), annualized frequency (5 hazards); major "
            "disaster, emergency and fire management declaration counts."
        ),
        raw=[],
        control=["control.fema_nri_run", "control.fema_nri_page"],
        silver=["silver_fema_nri.nri_fact", "silver_fema_nri.declaration_revision"],
        gold=[
            "gold_fema_nri.nri_observation",
            "gold_fema_nri.declaration_count",
            "gold_fema_nri.observation_revision",
            "gold_fema_nri.observation_latest",
        ],
        credential="none",
        catalog_source_code="FEMA_NRI",
    ),
    WarehouseSource(
        code="CDC",
        name="Chronic Disease Indicators and PLACES",
        publisher="CDC",
        topic="Health",
        module="cdc",
        dag="cdc_ingest",
        cadence="Weekly, Monday 09:00 UTC",
        release="CDI irregular; PLACES annual",
        grains={"nation": "n", "state": "n", "county": "n"},
        years=[(2019, 2023)],
        years_note="CDI multi-year indicators (nation, state); PLACES 2025 release (nation, county)",
        datasets=[
            "U.S. Chronic Disease Indicators (hksd-2xuw)",
            "PLACES county 2025 (swc5-untb)",
        ],
        measures=(
            "Indicator-specific surveillance measures with strata (CDI); model-based adult health "
            "measures (PLACES)."
        ),
        raw=[],
        control=["control.cdc_dataset_release"],
        silver=[
            "silver_cdc.observation_revision",
            "silver_cdc.fact_health_observation",
            "silver_cdc.dim_measure",
            "silver_cdc.dim_stratum",
            "silver_cdc.dim_dataset_release",
        ],
        gold=["gold_cdc.health_observation", "gold_cdc.latest_release_observation"],
        credential="CDC_SOCRATA_APP_TOKEN (optional)",
        catalog_source_code="CDC",
    ),
    WarehouseSource(
        code="EPA_AQS",
        name="AirData annual monitor concentrations",
        publisher="EPA",
        topic="Environment",
        module="epa_aqs",
        dag="epa_aqs_ingest",
        cadence="Monthly, 25th 18:00 UTC",
        release="Regenerated June and December",
        grains={"county": "d"},
        years=[(2020, 2024)],
        years_note="2020-2024 registered",
        datasets=["annual_conc_by_monitor_YYYY.zip"],
        measures="PM2.5 annual mean, ozone 8-hour 4th max: the highest complete monitor in each county.",
        raw=[],
        control=["control.epa_aqs_file"],
        silver=["silver_epa_aqs.monitor_fact"],
        gold=[
            "gold_epa_aqs.monitor_observation",
            "gold_epa_aqs.observation_revision",
            "gold_epa_aqs.observation_latest",
        ],
        credential="none",
        catalog_source_code="EPA_AQS",
    ),
    WarehouseSource(
        code="NOAA_NORMALS",
        name="U.S. Climate Normals 1991-2020",
        publisher="NOAA NCEI",
        topic="Environment",
        module="noaa_normals",
        dag="noaa_normals_ingest",
        cadence="Quarterly, 2nd 19:00 UTC",
        release="Once per decade",
        grains={"county": "d"},
        years=[(2020, 2020)],
        years_note="30-year normals, labelled 2020",
        datasets=["annual/seasonal multivariate by station archive"],
        measures=(
            "Annual mean, max and min temperature, precipitation, heating and cooling degree days: mean "
            "of stations placed in the county."
        ),
        raw=[],
        control=["control.noaa_normals_file"],
        silver=["silver_noaa_normals.station", "silver_noaa_normals.station_normal"],
        gold=[
            "gold_noaa_normals.station_observation",
            "gold_noaa_normals.observation_revision",
            "gold_noaa_normals.observation_latest",
        ],
        credential="none",
        catalog_source_code="NOAA_NORMALS",
    ),
    WarehouseSource(
        code="NCES_CCD",
        name="Common Core of Data, public schools",
        publisher="NCES",
        topic="Schools and broadband",
        module="nces_ccd",
        dag="nces_ccd_ingest",
        cadence="Quarterly, 3rd 20:00 UTC",
        release="Per school year",
        grains={"state": "d", "county": "d"},
        years=[(2023, 2024)],
        years_note="School years 2023-24 and 2024-25 (year = fall)",
        datasets=["EDGE geocodes", "Directory", "Membership", "Staff", "Lunch"],
        measures=(
            "Operating and charter schools, students, teacher FTE, free/reduced lunch eligibility, direct "
            "certification: summed over schools placed in the area."
        ),
        raw=[],
        control=["control.nces_ccd_file"],
        silver=[
            "silver_nces_ccd.school_directory",
            "silver_nces_ccd.school_location",
            "silver_nces_ccd.school_count",
        ],
        gold=[
            "gold_nces_ccd.school_placement",
            "gold_nces_ccd.school_observation",
            "gold_nces_ccd.observation_revision",
            "gold_nces_ccd.observation_latest",
        ],
        credential="none",
        catalog_source_code="NCES_CCD",
    ),
    WarehouseSource(
        code="FCC_BDC",
        name="National Broadband Map availability",
        publisher="FCC",
        topic="Schools and broadband",
        module="fcc_bdc",
        dag="fcc_bdc_ingest",
        cadence="Monthly, 12th 21:00 UTC",
        release="Twice a year (December vintages registered)",
        grains={"nation": "n", "state": "n", "county": "n", "place": "n"},
        years=[(2024, 2025)],
        years_note="As of 2024-12-31 and 2025-12-31",
        datasets=["Summary by geography: national + census places by state"],
        measures=(
            "Residential units; share served at 25/3, 100/20, 1000/100 Mbps (any technology), 100/20 "
            "terrestrial and wired."
        ),
        raw=[],
        control=["control.fcc_bdc_read", "control.fcc_bdc_file"],
        silver=["silver_fcc_bdc.availability_row"],
        gold=[
            "gold_fcc_bdc.availability_observation",
            "gold_fcc_bdc.observation_revision",
            "gold_fcc_bdc.observation_latest",
        ],
        credential="FCC_BDC_USERNAME, FCC_BDC_API_TOKEN",
        catalog_source_code="FCC_BDC",
    ),
    WarehouseSource(
        code="USDA_ERS",
        name="Rural-urban codes, county typology, food atlas",
        publisher="USDA Economic Research Service",
        topic="Rural and farm",
        module="usda_ers",
        dag="usda_ers_ingest",
        cadence="Monthly, 25th 17:00 UTC",
        release="Editions replaced in place",
        grains={"county": "n"},
        years=[(2015, 2015), (2017, 2017), (2019, 2019), (2023, 2023), (2025, 2025)],
        years_note="RUCC 2023; Typology 2025; Food Environment Atlas 2015, 2017, 2019, 2023",
        datasets=[
            "RUCC 2023",
            "County Typology Codes 2025",
            "Food Environment Atlas (July 2025)",
        ],
        measures=(
            "Rural-urban continuum code, 12 typology flags, industry dependence, SNAP stores and per "
            "1,000, SNAP households with low store access."
        ),
        raw=[],
        control=["control.usda_ers_file"],
        silver=[
            "silver_usda_ers.observation_revision",
            "silver_usda_ers.fact_observation",
        ],
        gold=["gold_usda_ers.observation_revision", "gold_usda_ers.observation_latest"],
        credential="none",
        catalog_source_code="USDA_ERS",
    ),
    WarehouseSource(
        code="USDA_NASS",
        name="Quick Stats crops",
        publisher="USDA NASS",
        topic="Rural and farm",
        module="usda_nass",
        dag="usda_nass_crop_ingest",
        cadence="Business days, 10:00 UTC (full sweep on the 1st)",
        release="Survey releases; Census of Agriculture every 5 years",
        grains={"nation": "n", "state": "n", "county": "n"},
        years=[(1990, 2024)],
        years_note="Survey 1990-2024; Census of Agriculture 2022",
        datasets=["corn, soybeans, wheat, hay surveys", "corn Census of Agriculture"],
        measures="Acres planted and harvested, yield, production.",
        raw=[],
        control=["control.usda_nass_release", "control.usda_nass_slice"],
        silver=[
            "silver_nass.observation_revision",
            "silver_nass.fact_crop_observation",
            "silver_nass.dim_commodity / statistic / domain",
        ],
        gold=[
            "gold_nass.crop_series",
            "gold_nass.crop_observation",
            "gold_nass.latest_release_observation",
        ],
        credential="USDA_NASS_API_KEY",
        catalog_source_code="USDA_NASS",
    ),
    WarehouseSource(
        code="CENSUS_GEO",
        name="Census geography (Gazetteer, boundaries, delineations)",
        publisher="U.S. Census Bureau and OMB",
        topic="Reference geography",
        module="silver_ref",
        dag="silver_ref",
        cadence="Monthly, 1st 05:00 UTC",
        release="Yearly vintages",
        grains={
            "nation": "n",
            "state": "n",
            "county": "n",
            "metro": "n",
            "place": "n",
            "tract": "n",
            "other": "n",
        },
        years=[(1990, 2025)],
        years_note=(
            "Gazetteer and cartographic boundaries from 2013; county history from 1990; 2000 and 1990 "
            "place/tract vintages for history"
        ),
        datasets=[
            "Gazetteer (state, county, place, tract, ZCTA)",
            "Cartographic boundaries 500k",
            "CBSA delineation (OMB 23-01)",
            "Tract 2000-2010 relationship and decade crosswalks",
            "County adjacency",
            "BEA and EIA provider areas",
        ],
        measures=(
            "Every place's identity, names, areas, coordinates, boundaries and relationships (contains, "
            "intersects, adjacent). Joined by geo_sk/geo_id everywhere."
        ),
        raw=[],
        control=["raw_capture.response_capture"],
        silver=[
            "silver_ref.dim_geo_entity",
            "silver_ref.dim_geo_entity_version",
            "silver_ref.dim_geo_geometry_version",
            "silver_ref.bridge_geo_relationship_version",
            "silver_ref.tract_decade_crosswalk",
            "silver_ref.geography_resolution",
            "silver_ref.dim_time",
        ],
        gold=[
            "gold_glossary.geo_boundary",
            "gold_glossary.geo_relationship",
            "gold_glossary.geo_search",
            "gold.tile_boundary / tile_tract (Martin tiles)",
        ],
        credential="none",
        catalog_source_code=None,
    ),
)

SOURCE_BY_CODE: dict[str, WarehouseSource] = {s.code: s for s in WAREHOUSE_SOURCES}
