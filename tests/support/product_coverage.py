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
    "/api/v1/analysis-configurations",
    "/api/v1/evidence-packets",
    # Identity (ADR-0005). Owned by the API platform and by no data product:
    # a sign-in reaches no source, no gold schema, and no warehouse row.
    "/api/v1/auth",
    "/api/v1/account",
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
