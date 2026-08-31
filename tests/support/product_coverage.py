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
SHARED_API_PREFIXES: tuple[str, ...] = (
    "/api/health",
    "/api/catalog",
    "/api/observations",
    "/api/distribution",
    "/api/comparison",
    "/api/models",
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
            "/api/census/observations/latest",
            "/api/census/observations/timeseries",
        ),
        neutral_api_routes=("/api/observations/latest",),
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
            "/api/bls/observations/latest",
            "/api/bls/observations/timeseries",
        ),
        neutral_api_routes=("/api/observations/latest",),
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
            "/api/fred/observations/latest",
            "/api/fred/observations/timeseries",
        ),
        neutral_api_routes=("/api/observations/latest",),
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
        source_api_routes=("/api/cdc/observations",),
        neutral_api_routes=("/api/catalog/metrics",),
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
            "/api/pep/observations/latest",
            "/api/pep/observations/timeseries",
        ),
        neutral_api_routes=("/api/observations/latest", "/api/catalog/metrics"),
        owner=(
            "tests/e2e/test_pep_pipeline.py::"
            "test_pep_fixtures_reach_the_api_with_vintage_and_place_identity_intact"
        ),
    ),
    DataProductE2E(
        product_id="fbi_ucr.summarized_violent_crime",
        source="FBI_UCR",
        publisher_schema="gold_fbi",
        datasets=("summarized_violent_crime",),
        fixtures=(
            "tests/fixtures/fbi_ucr/summarized_national_V.json",
            "tests/fixtures/fbi_ucr/summarized_national_V_revised.json",
            "tests/fixtures/fbi_ucr/summarized_state_WI_V.json",
            "tests/fixtures/fbi_ucr/agency_directory_WI.json",
        ),
        serving_relations=(
            "gold_fbi.crime_observation",
            "gold_fbi.latest_release_observation",
            "gold_fbi.reporting_coverage",
            "gold_fbi.agency_observation_area_filter",
        ),
        source_api_routes=(),
        neutral_api_routes=("/api/catalog/metrics", "/api/catalog/sources"),
        api_absence_reason=(
            "FBI UCR publishes no source-specific HTTP route: the accepted FBI "
            "plan delivered the agency-grain contract as the gold_fbi views and "
            "the glossary publisher, and a crime router is API-platform work "
            "owned by API_DEVELOPMENT_PLAN.md. The owner therefore proves the "
            "published boundary at the gold views and the provider-neutral "
            "catalog the glossary harvest feeds."
        ),
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
            "/api/usda-nass/observations",
            "/api/usda-nass/series",
            "/api/usda-nass/measures",
        ),
        neutral_api_routes=("/api/catalog/metrics",),
        owner=(
            "tests/e2e/test_usda_nass_pipeline.py::"
            "test_nass_fixtures_reach_the_api_without_losing_source_classification"
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
