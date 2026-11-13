"""The executable inventory of data products and their authoritative E2E owners.

E2E-PRODUCT-001 exists because broad marker selection cannot prove coverage: a
new source can land a complete publisher and API surface, and ``pytest -m e2e``
still passes while nothing exercises it. This registry names, for every
implemented data product, the reviewed fixtures it replays, the published
relations its owner asserts against, the API routes it exercises, and the one
test node that owns that evidence.

The registry is deliberately test-owned and declarative. Discovery of *what
exists* is not: ``tests/unit/shared/test_data_product_e2e_coverage.py`` derives
the implemented publisher surface from ``quality.inventory`` and the real
FastAPI application, so a source added without an owner here fails a
deterministic unit test rather than passing silently.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

#: API route prefixes that belong to no single source. A product never claims
#: these; the coverage test uses them to decide which routes still need an
#: owner, so a new *source* router cannot slip in unclaimed while the
#: cross-source platform surface keeps evolving under its own plan.
#: Routes owned by the API platform rather than by any one data product:
#: the provider-neutral resources, the deployment probes, and the API-owned
#: user storage. A data product claims only its own source-scoped routes, so
#: these need no end-to-end product owner. ``/api/v1/models`` is absent because
#: API-006 retired that probe endpoint outright.
SHARED_API_PREFIXES: tuple[str, ...] = (
    "/api/v1/health",
    "/api/v1/catalog",
    "/api/v1/observations",
    "/api/v1/distribution",
    "/api/v1/comparison",
    # Derived across either PEP or ACS baselines, owned by the API platform;
    # the scenario is not a new provider data product or warehouse publisher.
    "/api/v1/population/scenario",
    # Within-parent percentile ranks over reviewed measures from several
    # sources (API-165): a derived reading owned by the platform, not a
    # provider data product.
    "/api/v1/place/distinctive",
    # Age and sex structure (ADR-0013): sums and ratios of one ACS table's
    # lines, computed at request time; a derived reading owned by the
    # platform, not a provider data product or warehouse publisher.
    "/api/v1/age-structure",
    # One tract's history on its 2020 boundary (API-190): the published ACS
    # values beside the derived comparable series, read from gold views the
    # ACS pipeline publishes; a reading owned by the platform.
    "/api/v1/place/tract-history",
    # Taxes and public money (taxes-and-public-money T-6): shares of BEA, IRS
    # SOI and ACS figures under approved pairings, the Census tax burden view,
    # and governor tenures from the elections source joined to them; a
    # derived reading across five sources, owned by the platform.
    "/api/v1/public-money",
    "/api/v1/governor-terms",
    # Where public money goes (where-public-money-goes slice 5): each
    # government level's spending lines for one place, read from the Treasury,
    # Census government finance and USAspending gold views side by side and
    # never added; a reading across three sources, owned by the platform.
    "/api/v1/where-money-goes",
    "/api/v1/analysis-configurations",
    "/api/v1/evidence-packets",
    # Identity (ADR-0005). Owned by the API platform and by no data product:
    # a sign-in reaches no source, no gold schema, and no warehouse row.
    "/api/v1/auth",
    "/api/v1/account",
    # The admin warehouse pages describe every source at once (inventory,
    # coverage, catalog schema); owned by the platform, not by one product.
    "/api/v1/admin",
)


class ProductCoverageError(ValueError):
    """Raised when a registry entry cannot describe a real data product."""


@dataclass(frozen=True, slots=True)
class DataProductE2E:
    """One data product and the single test node that owns its E2E evidence."""

    product_id: str
    #: Owning source, as declared in ``quality.inventory.SOURCES``.
    source: str
    #: The source's publisher schema; every published relation in this schema
    #: belongs to this product.
    publisher_schema: str
    #: Provider dataset/product identities the owner replays.
    datasets: tuple[str, ...]
    #: Reviewed fixtures, repository-relative.
    fixtures: tuple[str, ...]
    #: Published relations the owner asserts against directly.
    serving_relations: tuple[str, ...]
    #: Source-specific API routes the owner exercises.
    source_api_routes: tuple[str, ...]
    #: Provider-neutral API routes the owner exercises.
    neutral_api_routes: tuple[str, ...]
    #: ``<file>::<test function>`` — the authoritative owner.
    owner: str
    #: Required when the product publishes no source-specific HTTP route.
    api_absence_reason: str = ""

    def __post_init__(self) -> None:
        if not self.datasets:
            raise ProductCoverageError(
                f"{self.product_id}: a product must name the provider datasets "
                "its owner replays."
            )
        if not self.fixtures:
            raise ProductCoverageError(
                f"{self.product_id}: a product must name its reviewed fixtures."
            )
        if not self.serving_relations:
            raise ProductCoverageError(
                f"{self.product_id}: a product must name the published relations "
                "its owner asserts against."
            )
        if not self.neutral_api_routes:
            raise ProductCoverageError(
                f"{self.product_id}: every product must be discoverable through a "
                "provider-neutral API contract."
            )
        if "::" not in self.owner:
            raise ProductCoverageError(
                f"{self.product_id}: owner must be '<file>::<test>', not "
                f"'{self.owner}'."
            )
        if not self.source_api_routes and not self.api_absence_reason:
            raise ProductCoverageError(
                f"{self.product_id}: a product without a source-specific route "
                "must record why, so an unbuilt API is visible rather than "
                "silently uncovered."
            )

    @property
    def owner_path(self) -> Path:
        return REPOSITORY_ROOT / self.owner.split("::", 1)[0]

    @property
    def owner_test(self) -> str:
        return self.owner.split("::", 1)[1]

    @property
    def api_routes(self) -> tuple[str, ...]:
        return self.source_api_routes + self.neutral_api_routes


PRODUCTS: tuple[DataProductE2E, ...] = (
    DataProductE2E(
        product_id="census_acs.survey_estimate",
        source="CENSUS_ACS",
        publisher_schema="gold_census",
        datasets=("acs5",),
        fixtures=("tests/fixtures/census/e2e_pipeline.json",),
        serving_relations=(
            "gold_census.fact_acs_observation",
            "gold_census.mv_acs_latest",
            "gold_census.rpt_acs_observations",
        ),
        source_api_routes=(
            "/api/v1/census/observations/latest",
            "/api/v1/census/observations/timeseries",
        ),
        neutral_api_routes=("/api/v1/observations/latest",),
        owner=(
            "tests/e2e/test_census_bls_pipeline.py::"
            "test_census_fixture_flows_raw_to_gold_and_replays_identically"
        ),
    ),
    DataProductE2E(
        product_id="bls.labor_series",
        source="BLS",
        publisher_schema="gold_bls",
        datasets=("laus", "ces"),
        fixtures=("tests/fixtures/bls/e2e_pipeline.json",),
        serving_relations=(
            "gold_bls.fact_bls_observation",
            "gold_bls.mv_bls_latest",
            "gold_bls.rpt_bls_observations",
        ),
        source_api_routes=(
            "/api/v1/bls/observations/latest",
            "/api/v1/bls/observations/timeseries",
        ),
        neutral_api_routes=("/api/v1/observations/latest",),
        owner=(
            "tests/e2e/test_census_bls_pipeline.py::"
            "test_bls_fixture_flows_raw_to_gold_and_replays_identically"
        ),
    ),
    DataProductE2E(
        product_id="fred.economic_series",
        source="FRED",
        publisher_schema="gold_fred",
        datasets=("fred_series",),
        fixtures=(
            "tests/fixtures/fred/e2e_pipeline.json",
            "tests/fixtures/fred/e2e_invalid.json",
            "tests/fixtures/fred/e2e_dimension_miss.json",
        ),
        serving_relations=(
            "gold_fred.fact_fred_observation",
            "gold_fred.mv_fred_latest",
            "gold_fred.rpt_fred_observations",
        ),
        source_api_routes=(
            "/api/v1/fred/observations/latest",
            "/api/v1/fred/observations/timeseries",
        ),
        neutral_api_routes=("/api/v1/observations/latest",),
        owner=(
            "tests/e2e/test_fred_pipeline.py::"
            "test_fred_fixture_replay_revision_and_missing_data_reconcile_end_to_end"
        ),
    ),
    DataProductE2E(
        product_id="cdc.health_indicator",
        source="CDC",
        publisher_schema="gold_cdc",
        datasets=("cdi", "places_county"),
        fixtures=(
            "tests/fixtures/cdc/cdi_metadata.json",
            "tests/fixtures/cdc/cdi_observations.json",
            "tests/fixtures/cdc/places_county_metadata.json",
            "tests/fixtures/cdc/places_county_observations.json",
        ),
        serving_relations=(
            "gold_cdc.health_observation",
            "gold_cdc.latest_release_observation",
        ),
        source_api_routes=("/api/v1/cdc/observations",),
        neutral_api_routes=("/api/v1/catalog/metrics",),
        owner=(
            "tests/e2e/test_cdc_pipeline.py::"
            "test_cdc_fixtures_reach_the_api_and_retain_every_published_release"
        ),
    ),
    DataProductE2E(
        product_id="census_pep.population_estimate",
        source="CENSUS_PEP",
        publisher_schema="gold_pep",
        datasets=("pep_nst_alldata", "pep_subcounty"),
        fixtures=(
            "tests/fixtures/census_pep/nst_2024.csv",
            "tests/fixtures/census_pep/nst_2025.csv",
            "tests/fixtures/census_pep/subcounty_2025.csv",
        ),
        serving_relations=(
            "gold_pep.population_estimate_latest",
            "gold_pep.population_estimate_revision",
            "gold_pep.mv_pep_latest",
            "gold_pep.rpt_pep_observations",
        ),
        source_api_routes=(
            "/api/v1/pep/observations/latest",
            "/api/v1/pep/observations/timeseries",
        ),
        neutral_api_routes=("/api/v1/observations/latest", "/api/v1/catalog/metrics"),
        owner=(
            "tests/e2e/test_pep_pipeline.py::"
            "test_pep_fixtures_reach_the_api_with_vintage_and_place_identity_intact"
        ),
    ),
    DataProductE2E(
        product_id="fbi_ucr.summarized_violent_crime",
        source="FBI_UCR",
        publisher_schema="gold_fbi",
        datasets=(
            "summarized_violent_crime",
            "summarized_assault",
            "summarized_burglary",
            "summarized_larceny",
            "summarized_motor_vehicle_theft",
            "summarized_homicide",
            "summarized_rape",
            "summarized_robbery",
            "summarized_arson",
            "summarized_property_crime",
        ),
        fixtures=(
            "tests/fixtures/fbi_ucr/summarized_national_V.json",
            "tests/fixtures/fbi_ucr/summarized_national_V_revised.json",
            "tests/fixtures/fbi_ucr/summarized_state_WI_V.json",
            "tests/fixtures/fbi_ucr/agency_directory_WI.json",
            "tests/fixtures/fbi_ucr/summarized_national_ASS.json",
            "tests/fixtures/fbi_ucr/summarized_state_WI_ASS.json",
            "tests/fixtures/fbi_ucr/summarized_national_BUR.json",
            "tests/fixtures/fbi_ucr/summarized_state_WI_BUR.json",
            "tests/fixtures/fbi_ucr/summarized_national_LAR.json",
            "tests/fixtures/fbi_ucr/summarized_state_WI_LAR.json",
            "tests/fixtures/fbi_ucr/summarized_national_MVT.json",
            "tests/fixtures/fbi_ucr/summarized_state_WI_MVT.json",
            "tests/fixtures/fbi_ucr/summarized_national_HOM.json",
            "tests/fixtures/fbi_ucr/summarized_state_WI_HOM.json",
            "tests/fixtures/fbi_ucr/summarized_national_RPE.json",
            "tests/fixtures/fbi_ucr/summarized_state_WI_RPE.json",
            "tests/fixtures/fbi_ucr/summarized_national_ROB.json",
            "tests/fixtures/fbi_ucr/summarized_state_WI_ROB.json",
            "tests/fixtures/fbi_ucr/summarized_national_ARS.json",
            "tests/fixtures/fbi_ucr/summarized_state_WI_ARS.json",
            "tests/fixtures/fbi_ucr/summarized_national_P.json",
            "tests/fixtures/fbi_ucr/summarized_state_WI_P.json",
        ),
        serving_relations=(
            "gold_fbi.crime_observation",
            "gold_fbi.latest_release_observation",
            "gold_fbi.reporting_coverage",
            "gold_fbi.agency_observation_area_filter",
            # The declared-derived county roll-up (ETL-053) and its
            # latest-release projection, served by /api/v1/crime/county-rollup.
            "gold_fbi.county_rollup",
            "gold_fbi.latest_county_rollup",
            # The county rate the FBI's way (ADR-0012), served by
            # /api/v1/observations/derived for its agency_rate pairings.
            "gold_fbi.county_agency_rate",
            "gold_fbi.latest_county_agency_rate",
        ),
        source_api_routes=("/api/v1/crime/county-rollup",),
        neutral_api_routes=("/api/v1/catalog/metrics", "/api/v1/catalog/sources"),
        owner=(
            "tests/e2e/test_fbi_ucr_pipeline.py::"
            "test_fbi_fixtures_reach_the_published_boundary_without_inventing_totals"
        ),
    ),
    DataProductE2E(
        product_id="usda_nass.crop_statistic",
        source="USDA_NASS",
        publisher_schema="gold_nass",
        datasets=(
            "corn_survey_annual",
            "corn_census_county",
            "soybeans_survey_annual",
        ),
        fixtures=(
            "tests/fixtures/usda_nass/corn_survey_annual.json",
            "tests/fixtures/usda_nass/corn_survey_annual_revised.json",
            "tests/fixtures/usda_nass/corn_census_county.json",
            "tests/fixtures/usda_nass/soybeans_survey_annual.json",
        ),
        serving_relations=(
            "gold_nass.crop_observation",
            "gold_nass.crop_series",
            "gold_nass.latest_release_observation",
        ),
        source_api_routes=(
            "/api/v1/usda-nass/observations",
            "/api/v1/usda-nass/series",
            "/api/v1/usda-nass/measures",
            "/api/v1/usda-nass/source-notes",
        ),
        neutral_api_routes=("/api/v1/catalog/metrics",),
        owner=(
            "tests/e2e/test_usda_nass_pipeline.py::"
            "test_nass_fixtures_reach_the_api_without_losing_source_classification"
        ),
    ),
    DataProductE2E(
        product_id="bea.regional_income_and_gdp",
        source="BEA",
        publisher_schema="gold_bea",
        datasets=("CAINC1", "CAGDP1", "CAGDP2"),
        fixtures=(
            "tests/fixtures/bea/CAINC1.zip",
            "tests/fixtures/bea/CAGDP1.zip",
            "tests/fixtures/bea/CAGDP2.zip",
        ),
        serving_relations=(
            "gold_bea.observation_revision",
            "gold_bea.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_bea_pipeline.py::"
            "test_income_and_gdp_reach_the_neutral_api_with_their_dollar_basis"
        ),
        api_absence_reason=(
            "BEA regional accounts publish no source-specific HTTP route: each "
            "row is the neutral observation shape with its table, line, dollar "
            "basis and provider cell code as declared dimensions, so "
            "`/api/v1/observations` serves it."
        ),
    ),
    DataProductE2E(
        product_id="bls_qcew.county_employment_and_wages",
        source="BLS_QCEW",
        publisher_schema="gold_bls_qcew",
        datasets=("10", "62"),
        fixtures=(
            "tests/fixtures/bls_qcew/2024_1_industry_10.csv",
            "tests/fixtures/bls_qcew/2024_1_industry_62.csv",
            "tests/fixtures/bls_qcew/2023_a_industry_10.csv",
        ),
        serving_relations=(
            "gold_bls_qcew.observation_revision",
            "gold_bls_qcew.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/metrics",
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_bls_qcew_pipeline.py::"
            "test_qcew_reaches_the_neutral_api_with_industry_ownership_and_basis"
        ),
        api_absence_reason=(
            "BLS QCEW publishes no source-specific HTTP route: each row is the "
            "neutral observation shape with its industry, ownership and basis "
            "as declared dimensions, so `/api/v1/observations` serves it through "
            "the dispatch registry."
        ),
    ),
    DataProductE2E(
        product_id="census_bps.housing_units_authorized",
        source="CENSUS_BPS",
        publisher_schema="gold_census_bps",
        datasets=("county", "state", "place:south"),
        fixtures=(
            "tests/fixtures/census_bps/County_co2403c.txt",
            "tests/fixtures/census_bps/County_co2412y.txt",
            "tests/fixtures/census_bps/State_st2403c.txt",
            "tests/fixtures/census_bps/Place_South_so2024a.txt",
        ),
        serving_relations=(
            "gold_census_bps.observation_revision",
            "gold_census_bps.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_census_bps_pipeline.py::"
            "test_permits_reach_the_neutral_api_as_authorizations_for_a_county_and_a_place"
        ),
        api_absence_reason=(
            "The Building Permits Survey publishes no source-specific HTTP route: "
            "each row is the neutral observation shape with its structure type, "
            "frequency, reported figure and authorization basis as declared "
            "dimensions, so `/api/v1/observations` serves it."
        ),
    ),
    DataProductE2E(
        product_id="census_saipe_sahie.estimate",
        source="CENSUS_SAIPE_SAHIE",
        publisher_schema="gold_census_sae",
        datasets=("saipe", "sahie"),
        fixtures=(
            "tests/fixtures/census_saipe_sahie/saipe_2023_us.json",
            "tests/fixtures/census_saipe_sahie/saipe_2023_state.json",
            "tests/fixtures/census_saipe_sahie/saipe_2023_county.json",
            "tests/fixtures/census_saipe_sahie/sahie_2023_us.json",
            "tests/fixtures/census_saipe_sahie/sahie_2023_state.json",
            "tests/fixtures/census_saipe_sahie/sahie_2023_county.json",
        ),
        serving_relations=(
            "gold_census_sae.estimate_revision",
            "gold_census_sae.estimate_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/metrics",
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_census_saipe_sahie_pipeline.py::"
            "test_saipe_and_sahie_reach_the_neutral_api_with_their_intervals"
        ),
        api_absence_reason=(
            "Census SAIPE and SAHIE publish no source-specific HTTP route: "
            "their rows are the neutral observation shape with an interval, "
            "so `/api/v1/observations` serves them through the dispatch "
            "registry and a source route would repeat it."
        ),
    ),
    DataProductE2E(
        product_id="census_decennial.count",
        source="CENSUS_DECENNIAL",
        publisher_schema="gold_census_decennial",
        datasets=("pl",),
        fixtures=(
            "tests/fixtures/census_decennial/pl_us_1.json",
            "tests/fixtures/census_decennial/pl_state_all.json",
            "tests/fixtures/census_decennial/pl_county_all.json",
            "tests/fixtures/census_decennial/pl_tract_55.json",
        ),
        serving_relations=(
            "gold_census_decennial.count_revision",
            "gold_census_decennial.count_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/metrics",
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_census_decennial_pipeline.py::"
            "test_group_quarters_reach_the_neutral_api_for_dane_and_its_tracts"
        ),
        api_absence_reason=(
            "The 2020 Census counts publish no source-specific HTTP route: each "
            "row is the neutral observation shape with its variable, universe "
            "and parent variable as dimensions, so `/api/v1/observations` "
            "serves them through the dispatch registry."
        ),
    ),
    DataProductE2E(
        product_id="cms_ltc.area_total",
        source="CMS_LTC",
        publisher_schema="gold_cms_ltc",
        datasets=("cost_report", "characteristics"),
        fixtures=(
            "tests/fixtures/cms_ltc/data_json_excerpt.json",
            "tests/fixtures/cms_ltc/CostReportsnf_Final_24_wi.csv",
            "tests/fixtures/cms_ltc/Form_671_Q2_2026_wi.csv",
            "tests/fixtures/cms_ltc/Form_671_Q3_2026_wi.csv",
        ),
        serving_relations=(
            "gold_cms_ltc.area_revision",
            "gold_cms_ltc.area_latest",
            "gold_cms_ltc.facility_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/metrics",
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_cms_ltc_pipeline.py::"
            "test_dane_nursing_home_residents_and_beds_reach_the_neutral_api"
        ),
        api_absence_reason=(
            "The CMS long-term care files publish no source-specific HTTP "
            "route: each area sum is the neutral observation shape with its "
            "series, basis and facility count as dimensions, so "
            "`/api/v1/observations` serves it through the dispatch registry."
        ),
    ),
    DataProductE2E(
        product_id="nces_ipeds.area_total",
        source="NCES_IPEDS",
        publisher_schema="gold_nces_ipeds",
        datasets=("HD", "IC", "EFA"),
        fixtures=(
            "tests/fixtures/nces_ipeds/HD2023_wi.zip",
            "tests/fixtures/nces_ipeds/IC2023_wi.zip",
            "tests/fixtures/nces_ipeds/EF2023A_wi.zip",
            "tests/fixtures/nces_ipeds/HD2012_uw.zip",
            "tests/fixtures/nces_ipeds/IC2012_uw.zip",
            "tests/fixtures/nces_ipeds/EF2012A_uw.zip",
        ),
        serving_relations=(
            "gold_nces_ipeds.area_revision",
            "gold_nces_ipeds.area_latest",
            "gold_nces_ipeds.institution_year",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/metrics",
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_nces_ipeds_pipeline.py::"
            "test_dane_college_housing_and_enrolment_reach_the_neutral_api"
        ),
        api_absence_reason=(
            "The IPEDS files publish no source-specific HTTP route: each area "
            "sum is the neutral observation shape with its basis and "
            "institution count as dimensions, so `/api/v1/observations` "
            "serves it through the dispatch registry."
        ),
    ),
    DataProductE2E(
        product_id="hhs_hospital_capacity.area_week",
        source="HHS_HOSPITAL_CAPACITY",
        publisher_schema="gold_hhs_hospital_capacity",
        datasets=("anag-cw7u",),
        fixtures=(
            "tests/fixtures/hhs_hospital_capacity/anag_view.json",
            "tests/fixtures/hhs_hospital_capacity/anag_dane_offset_0.json",
            "tests/fixtures/hhs_hospital_capacity/anag_dane_offset_400.json",
            "tests/fixtures/hhs_hospital_capacity/anag_dane_offset_800.json",
        ),
        serving_relations=(
            "gold_hhs_hospital_capacity.area_revision",
            "gold_hhs_hospital_capacity.area_latest",
            "gold_hhs_hospital_capacity.facility_latest",
            "gold_hhs_hospital_capacity.facility_week",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/metrics",
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_hhs_hospital_capacity_pipeline.py::"
            "test_dane_weekly_hospital_beds_reach_the_neutral_api"
        ),
        api_absence_reason=(
            "The HHS weekly file publishes no source-specific HTTP route: each "
            "area sum is the neutral observation shape with its reporting, "
            "suppressed and missing hospital counts as dimensions, so "
            "`/api/v1/observations` serves it through the dispatch registry."
        ),
    ),
    DataProductE2E(
        product_id="census_govfin.taxes_and_revenue",
        source="CENSUS_GOVFIN",
        publisher_schema="gold_census_govfin",
        datasets=("cog_units", "asslgf", "qtax"),
        fixtures=(
            "tests/fixtures/census_govfin/finestdat_2022_excerpt.txt",
            "tests/fixtures/census_govfin/fin_pid_2022_excerpt.txt",
            "tests/fixtures/census_govfin/statetypepu_2022_excerpt.txt",
            "tests/fixtures/census_govfin/asslgf_2022_us.json",
            "tests/fixtures/census_govfin/asslgf_2022_state.json",
            "tests/fixtures/census_govfin/qtax_2024_us.json",
            "tests/fixtures/census_govfin/qtax_2024_state.json",
        ),
        serving_relations=(
            "gold_census_govfin.observation_revision",
            "gold_census_govfin.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/metrics",
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_census_govfin_pipeline.py::"
            "test_county_area_and_state_taxes_reach_the_neutral_api"
        ),
        api_absence_reason=(
            "Census government finance publishes no source-specific HTTP route: "
            "each figure is the neutral observation shape with its government "
            "level, derivation and record coverage as declared dimensions, so "
            "`/api/v1/observations` serves it. The tax mix and tax burden views "
            "get routes in the taxes API step."
        ),
    ),
    DataProductE2E(
        product_id="irs_migration.county_flows",
        source="IRS_MIGRATION",
        publisher_schema="gold_irs_migration",
        datasets=("inflow:2021-2022", "inflow:2022-2023", "outflow:2022-2023"),
        fixtures=(
            "tests/fixtures/irs_migration/countyinflow2122.csv",
            "tests/fixtures/irs_migration/countyinflow2223.csv",
            "tests/fixtures/irs_migration/countyoutflow2223.csv",
        ),
        serving_relations=(
            "gold_irs_migration.flow_revision",
            "gold_irs_migration.flow_latest",
            "gold_irs_migration.total_observation_revision",
            "gold_irs_migration.total_observation_latest",
        ),
        source_api_routes=("/api/v1/migration-flows",),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_irs_migration_pipeline.py::"
            "test_top_origins_and_destinations_reach_the_api_with_withheld_categories"
        ),
    ),
    DataProductE2E(
        product_id="dol_childcare.prices",
        source="DOL_CHILDCARE",
        publisher_schema="gold_dol_childcare",
        datasets=("ndcp2022",),
        fixtures=("tests/fixtures/dol_childcare/NDCP2022.xlsx",),
        serving_relations=(
            "gold_dol_childcare.observation_revision",
            "gold_dol_childcare.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_dol_childcare_pipeline.py::"
            "test_county_childcare_prices_reach_the_neutral_api_with_imputation_flags"
        ),
        api_absence_reason=(
            "NDCP childcare prices publish no source-specific HTTP route: each "
            "price is the neutral observation shape with care-type, age-group "
            "and imputation dimensions, so `/api/v1/observations` serves them "
            "through the dispatch registry."
        ),
    ),
    DataProductE2E(
        product_id="irs_soi.income",
        source="IRS_SOI",
        publisher_schema="gold_irs_soi",
        datasets=(
            "county_total:1989",
            "county_total:2010",
            "county_agi:2010",
            "county_total:2023",
            "county_agi:2023",
            "state:2023",
        ),
        fixtures=(
            "tests/fixtures/irs_soi/1989countyincome.zip",
            "tests/fixtures/irs_soi/2010countydata.zip",
            "tests/fixtures/irs_soi/23incyallnoagi.csv",
            "tests/fixtures/irs_soi/23incyallagi.csv",
            "tests/fixtures/irs_soi/23in55cmcsv.csv",
        ),
        serving_relations=(
            "gold_irs_soi.observation_revision",
            "gold_irs_soi.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_irs_soi_pipeline.py::"
            "test_county_and_state_income_reach_the_neutral_api_by_agi_band"
        ),
        api_absence_reason=(
            "IRS SOI income publishes no source-specific HTTP route: its rows "
            "are the neutral observation shape with an AGI band dimension, so "
            "`/api/v1/observations` serves them through the dispatch registry "
            "and a source route would repeat it."
        ),
    ),
    DataProductE2E(
        product_id="eia.retail_gasoline",
        source="EIA",
        publisher_schema="gold_eia",
        datasets=("petroleum/pri/gnd",),
        fixtures=(
            "tests/fixtures/eia/weekly_2026-08-31_2026-09-07.json",
            "tests/fixtures/eia/duoarea_facet.json",
        ),
        serving_relations=(
            "gold_eia.observation_revision",
            "gold_eia.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_eia_pipeline.py::"
            "test_weekly_gasoline_reaches_the_neutral_api_at_every_area_kind"
        ),
        api_absence_reason=(
            "EIA retail gasoline publishes no source-specific HTTP route: each "
            "row is the neutral observation shape with its grade, EIA series id, "
            "area code and name as declared dimensions, so `/api/v1/observations` "
            "serves it."
        ),
    ),
    DataProductE2E(
        product_id="census_cbp.business_patterns",
        source="CENSUS_CBP",
        publisher_schema="gold_census_cbp",
        datasets=("county:2016", "county:2023", "state:2023", "nation:2023"),
        fixtures=(
            "tests/fixtures/census_cbp/cbp16co.zip",
            "tests/fixtures/census_cbp/cbp23co.zip",
            "tests/fixtures/census_cbp/cbp23st.zip",
            "tests/fixtures/census_cbp/cbp23us.zip",
        ),
        serving_relations=(
            "gold_census_cbp.observation_revision",
            "gold_census_cbp.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_census_cbp_pipeline.py::"
            "test_business_patterns_reach_the_neutral_api_with_flags_and_coverage"
        ),
        api_absence_reason=(
            "County Business Patterns publishes no source-specific HTTP route: "
            "each row is the neutral observation shape with its measure, sector, "
            "coverage statement and withheld cell's size range as declared "
            "dimensions and its noise flag as uncertainty, so "
            "`/api/v1/observations` serves it."
        ),
    ),
    DataProductE2E(
        product_id="census_lodes.commuting_counts",
        source="CENSUS_LODES",
        publisher_schema="gold_census_lodes",
        datasets=("de:2023",),
        fixtures=(
            "tests/fixtures/census_lodes/version.txt",
            "tests/fixtures/census_lodes/lodes_de.sha256sum",
            "tests/fixtures/census_lodes/de_rac_S000_JT00_2023.csv.gz",
            "tests/fixtures/census_lodes/de_wac_S000_JT00_2023.csv.gz",
            "tests/fixtures/census_lodes/de_od_main_JT00_2023.csv.gz",
            "tests/fixtures/census_lodes/de_od_aux_JT00_2023.csv.gz",
        ),
        serving_relations=(
            "gold_census_lodes.observation_revision",
            "gold_census_lodes.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_census_lodes_pipeline.py::"
            "test_commuting_counts_reach_the_neutral_api_with_their_basis"
        ),
        api_absence_reason=(
            "LEHD LODES publishes no source-specific HTTP route: each county "
            "or state count is the neutral observation shape with its measure "
            "and the statement that it is this warehouse's sum of protected "
            "block estimates as declared dimensions, so `/api/v1/observations` "
            "serves it."
        ),
    ),
    DataProductE2E(
        product_id="epa_aqs.county_air_quality",
        source="EPA_AQS",
        publisher_schema="gold_epa_aqs",
        datasets=("annual_conc_by_monitor:2024",),
        fixtures=("tests/fixtures/epa_aqs/annual_conc_by_monitor_2024.zip",),
        serving_relations=(
            "gold_epa_aqs.observation_revision",
            "gold_epa_aqs.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_epa_aqs_pipeline.py::"
            "test_county_air_quality_reaches_the_neutral_api_as_derived"
        ),
        api_absence_reason=(
            "EPA air quality publishes no source-specific HTTP route: each county "
            "row is the neutral observation shape with its highest monitor, its "
            "complete-monitor count and certification as declared dimensions, so "
            "`/api/v1/observations` serves it."
        ),
    ),
    DataProductE2E(
        product_id="noaa_normals.county_climate_normals",
        source="NOAA_NORMALS",
        publisher_schema="gold_noaa_normals",
        datasets=("normals-annualseasonal:1991-2020",),
        fixtures=("tests/fixtures/noaa_normals/annualseasonal_by_station.tar.gz",),
        serving_relations=(
            "gold_noaa_normals.observation_revision",
            "gold_noaa_normals.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_noaa_normals_pipeline.py::"
            "test_county_climate_normals_reach_the_neutral_api_as_derived"
        ),
        api_absence_reason=(
            "NOAA climate normals publish no source-specific HTTP route: each "
            "county row is the neutral observation shape with its stations, "
            "station count and boundary vintage as declared dimensions, so "
            "`/api/v1/observations` serves it."
        ),
    ),
    DataProductE2E(
        product_id="noaa_climdiv.county_monthly_series",
        source="NOAA_CLIMDIV",
        publisher_schema="gold_noaa_climdiv",
        datasets=("climdiv-county:v1.0.0",),
        fixtures=(
            "tests/fixtures/noaa_climdiv/climdiv-pcpncy-v1.0.0-20261105",
            "tests/fixtures/noaa_climdiv/climdiv-tmpccy-v1.0.0-20261105",
            "tests/fixtures/noaa_climdiv/climdiv-tmaxcy-v1.0.0-20261105",
            "tests/fixtures/noaa_climdiv/climdiv-tmincy-v1.0.0-20261105",
            "tests/fixtures/noaa_climdiv/climdiv-hddccy-v1.0.0-20261105",
            "tests/fixtures/noaa_climdiv/climdiv-cddccy-v1.0.0-20261105",
        ),
        serving_relations=(
            "gold_noaa_climdiv.observation_revision",
            "gold_noaa_climdiv.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_noaa_climdiv_pipeline.py::"
            "test_county_months_reach_the_neutral_api_as_published"
        ),
        api_absence_reason=(
            "NOAA nClimDiv publishes no source-specific HTTP route: each county "
            "month is the neutral observation shape with its calendar month and "
            "NCEI's own text as declared dimensions, so `/api/v1/observations` "
            "serves it."
        ),
    ),
    DataProductE2E(
        product_id="treasury_mts.receipts_and_outlays",
        source="TREASURY_MTS",
        publisher_schema="gold_treasury_mts",
        datasets=("mts_table_9:v1",),
        fixtures=("tests/fixtures/treasury_mts/mts_table_9_2026-08_2026-09.json",),
        serving_relations=(
            "gold_treasury_mts.observation_revision",
            "gold_treasury_mts.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_treasury_mts_pipeline.py::"
            "test_fiscal_year_outlays_reach_the_neutral_api_as_published"
        ),
        api_absence_reason=(
            "The Monthly Treasury Statement publishes no source-specific HTTP "
            "route: each fiscal-year line total is the neutral observation "
            "shape for the nation, so `/api/v1/observations` serves it."
        ),
    ),
    DataProductE2E(
        product_id="usaspending.place_of_performance_obligations",
        source="USASPENDING",
        publisher_schema="gold_usaspending",
        datasets=("usaspending_geography:v1",),
        fixtures=(
            "tests/fixtures/usaspending/state_all_fy2024.json",
            "tests/fixtures/usaspending/county_grants_fy2024_wi.json",
            "tests/fixtures/usaspending/awarding_agencies_fy2024.json",
        ),
        serving_relations=(
            "gold_usaspending.observation_revision",
            "gold_usaspending.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_usaspending_pipeline.py::"
            "test_place_obligations_reach_the_neutral_api_as_published"
        ),
        api_absence_reason=(
            "USAspending award obligations publish no source-specific HTTP "
            "route: each state or county fiscal-year amount is the neutral "
            "observation shape, so `/api/v1/observations` serves it."
        ),
    ),
    DataProductE2E(
        product_id="noaa_storm_events.event_counts",
        source="NOAA_STORM_EVENTS",
        publisher_schema="gold_noaa_storm_events",
        datasets=("storm-events-details:v1.0", "nws-zone-county"),
        fixtures=(
            "tests/fixtures/noaa_storm_events/StormEvents_details-ftp_v1.0_d1950_c20260323.csv.gz",
            "tests/fixtures/noaa_storm_events/StormEvents_details-ftp_v1.0_d1995_c20260323.csv.gz",
            "tests/fixtures/noaa_storm_events/StormEvents_details-ftp_v1.0_d2024_c20260728.csv.gz",
            "tests/fixtures/noaa_storm_events/bp16ap26.dbx",
            "tests/fixtures/noaa_storm_events/bp18mr25.dbx",
        ),
        serving_relations=(
            "gold_noaa_storm_events.observation_revision",
            "gold_noaa_storm_events.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_noaa_storm_events_pipeline.py::"
            "test_event_counts_reach_the_neutral_api_as_counted"
        ),
        api_absence_reason=(
            "The Storm Events Database publishes no source-specific HTTP route: each "
            "count and impact is the neutral observation shape with its event type and "
            "how many events were county- or zone-reported as declared dimensions, so "
            "`/api/v1/observations` serves it; the event records are "
            "`gold_noaa_storm_events.event_detail`."
        ),
    ),
    DataProductE2E(
        product_id="fcc_bdc.broadband_availability",
        source="FCC_BDC",
        publisher_schema="gold_fcc_bdc",
        datasets=("availability:2024-12-31", "availability:2025-12-31"),
        fixtures=(
            "tests/fixtures/fcc_bdc/listAvailabilityData_2025-12-31.json",
            "tests/fixtures/fcc_bdc/1820956.zip",
            "tests/fixtures/fcc_bdc/1820975.zip",
        ),
        serving_relations=(
            "gold_fcc_bdc.observation_revision",
            "gold_fcc_bdc.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_fcc_bdc_pipeline.py::"
            "test_county_and_place_availability_reach_the_neutral_api"
        ),
        api_absence_reason=(
            "FCC broadband availability publishes no source-specific HTTP route: "
            "each row is the neutral observation shape with its as-of date, the "
            "FCC's revision and the unit denominator as declared dimensions, so "
            "`/api/v1/observations` serves it."
        ),
    ),
    DataProductE2E(
        product_id="elections.presidential_vote",
        source="ELECTIONS",
        publisher_schema="gold_elections",
        datasets=(
            "medsl:countypres_2000-2024",
            "medsl:1976-2020-president",
            "medsl:1976-2024-senate-state",
            "medsl:1976-2024-house",
            "congress-legislators:gh-pages",
            "wikidata:governors",
        ),
        fixtures=(
            "tests/fixtures/elections/county_president.csv",
            "tests/fixtures/elections/state_president.csv",
            "tests/fixtures/elections/legislators-current.json",
            "tests/fixtures/elections/wikidata-governors.json",
        ),
        serving_relations=(
            "gold_elections.observation_revision",
            "gold_elections.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_elections_pipeline.py::"
            "test_presidential_margins_reach_the_neutral_api"
        ),
        api_absence_reason=(
            "The elections source publishes no source-specific HTTP route yet: "
            "each presidential row is the neutral observation shape with how the "
            "share or margin is counted and the leading party as declared "
            "dimensions, so `/api/v1/observations` serves it. Seat terms and "
            "contest results get their own routes in the API step (E-4)."
        ),
    ),
    DataProductE2E(
        product_id="fema_nri.risk_and_declarations",
        source="FEMA_NRI",
        publisher_schema="gold_fema_nri",
        datasets=("nri:December 2025", "declarations:openfema-v2"),
        fixtures=(
            "tests/fixtures/fema_nri/nri_counties.json",
            "tests/fixtures/fema_nri/declarations.json",
        ),
        serving_relations=(
            "gold_fema_nri.observation_revision",
            "gold_fema_nri.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_fema_nri_pipeline.py::"
            "test_losses_and_declarations_reach_the_neutral_api"
        ),
        api_absence_reason=(
            "FEMA publishes no source-specific HTTP route here: each county row "
            "is the neutral observation shape with FEMA's rating, missing reason "
            "and the declarations a count is made of as declared dimensions, so "
            "`/api/v1/observations` serves it."
        ),
    ),
    DataProductE2E(
        product_id="fhfa_hpi.county_annual_index",
        source="FHFA_HPI",
        publisher_schema="gold_fhfa_hpi",
        datasets=("county:2026-03-31",),
        fixtures=("tests/fixtures/fhfa_hpi/hpi_at_county.xlsx",),
        serving_relations=(
            "gold_fhfa_hpi.observation_revision",
            "gold_fhfa_hpi.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_fhfa_hpi_pipeline.py::"
            "test_house_price_index_reaches_the_neutral_api_with_its_notice"
        ),
        api_absence_reason=(
            "The FHFA House Price Index publishes no source-specific HTTP route: "
            "each county-year is the neutral observation shape with its measure, "
            "its basis and FHFA's notice, and a missing cell's reason as declared "
            "dimensions, so `/api/v1/observations` serves it."
        ),
    ),
    DataProductE2E(
        product_id="hud_fmr_il.county_reference_values",
        source="HUD_FMR_IL",
        publisher_schema="gold_hud_fmr_il",
        datasets=(
            "fmr:fy2026:original",
            "fmr:fy2026:revised",
            "fmr:fy2027:original",
            "il:fy2026:original",
        ),
        fixtures=(
            "tests/fixtures/hud_fmr_il/FY26_FMRs.xlsx",
            "tests/fixtures/hud_fmr_il/FY26_FMRs_revised.xlsx",
            "tests/fixtures/hud_fmr_il/FY27_FMRs.xlsx",
            "tests/fixtures/hud_fmr_il/Section8-FY26.xlsx",
        ),
        serving_relations=(
            "gold_hud_fmr_il.observation_revision",
            "gold_hud_fmr_il.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_hud_fmr_il_pipeline.py::"
            "test_rents_and_limits_reach_the_neutral_api_with_their_area"
        ),
        api_absence_reason=(
            "HUD Fair Market Rents and income limits publish no source-specific "
            "HTTP route: each county row is the neutral observation shape with its "
            "HUD area, edition, effective date and basis as declared dimensions, "
            "so `/api/v1/observations` serves it."
        ),
    ),
    DataProductE2E(
        product_id="nces_ccd.county_public_schools",
        source="NCES_CCD",
        publisher_schema="gold_nces_ccd",
        datasets=(
            "geocode:2024-2025",
            "directory:2024-2025",
            "membership:2024-2025",
            "staff:2024-2025",
            "lunch:2024-2025",
        ),
        fixtures=(
            "tests/fixtures/nces_ccd/EDGE_GEOCODE_PUBLICSCH_2425.zip",
            "tests/fixtures/nces_ccd/ccd_sch_029_2425_w_1a_073025.zip",
            "tests/fixtures/nces_ccd/ccd_sch_052_2425_l_1a_073025.zip",
            "tests/fixtures/nces_ccd/ccd_sch_059_2425_l_1a_073025.zip",
            "tests/fixtures/nces_ccd/ccd_sch_033_2425_l_2a_073025.zip",
        ),
        serving_relations=(
            "gold_nces_ccd.observation_revision",
            "gold_nces_ccd.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_nces_ccd_pipeline.py::"
            "test_county_school_figures_reach_the_neutral_api_as_rollups"
        ),
        api_absence_reason=(
            "NCES public schools publish no source-specific HTTP route: each county "
            "and state row is the neutral observation shape with its school counts, "
            "completeness and files as declared dimensions, so "
            "`/api/v1/observations` serves it."
        ),
    ),
    DataProductE2E(
        product_id="usda_ers.county_codes_and_food_atlas",
        source="USDA_ERS",
        publisher_schema="gold_usda_ers",
        datasets=("rucc:2023", "typology:2025", "fea:2025-07"),
        fixtures=(
            "tests/fixtures/usda_ers/2023-rural-urban-continuum-codes.csv",
            "tests/fixtures/usda_ers/ers-county-typology-codes-2025-edition.csv",
            "tests/fixtures/usda_ers/food-environment-atlas-csv-files.zip",
        ),
        serving_relations=(
            "gold_usda_ers.observation_revision",
            "gold_usda_ers.observation_latest",
        ),
        source_api_routes=(),
        neutral_api_routes=(
            "/api/v1/catalog/capabilities",
            "/api/v1/observations",
        ),
        owner=(
            "tests/e2e/test_usda_ers_pipeline.py::"
            "test_codes_flags_and_atlas_reach_the_neutral_api"
        ),
        api_absence_reason=(
            "USDA ERS publishes no source-specific HTTP route: each county row is "
            "the neutral observation shape with its code label, edition and "
            "missing reason as declared dimensions, so `/api/v1/observations` "
            "serves it."
        ),
    ),
)


def products_by_schema() -> dict[str, DataProductE2E]:
    """Index the registry by publisher schema, rejecting a duplicate claim."""
    indexed: dict[str, DataProductE2E] = {}
    for product in PRODUCTS:
        if product.publisher_schema in indexed:
            raise ProductCoverageError(
                f"{product.publisher_schema} is claimed by both "
                f"{indexed[product.publisher_schema].product_id} and "
                f"{product.product_id}; a product needs exactly one owner."
            )
        indexed[product.publisher_schema] = product
    return indexed


def owner_node_ids() -> tuple[str, ...]:
    """Return every owning node id, for explicit scheduled selection."""
    return tuple(product.owner for product in PRODUCTS)


def main() -> int:
    """Print the owner node ids, one per line, for CI selection."""
    for node_id in owner_node_ids():
        print(node_id)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
