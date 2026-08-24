"""Census PEP registry contracts: registration, lookup, vintage, and series."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_pep.config import CONFIG, PEPConfig, PEPDataset
from data_ingestion_toolbox.census_pep.registry import (
    PEPRegistry,
    PEPReleaseSeries,
    PEPVintage,
    get_registry,
    reset_registry,
)

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _reset_default_registry() -> None:
    """Ensure the module-level singleton is reset between tests."""
    yield
    reset_registry()


@pytest.fixture
def minimal_config() -> PEPConfig:
    """Create a config with a single test dataset."""
    ds = PEPDataset(
        code="test_ds",
        title="Test Dataset",
        api_path="/data/test/test_ds",
        geography_levels=frozenset({"state"}),
        variables=frozenset({"POPULATION"}),
        release_status="active",
        decennial_base=2020,
    )
    return PEPConfig(datasets={"test_ds": ds})


@pytest.fixture
def registry_with_config(minimal_config: PEPConfig) -> PEPRegistry:
    """Create a registry initialized with the minimal config."""
    return PEPRegistry(minimal_config)


# -----------------------------------------------------------------------
# Dataset registration and lookup
# -----------------------------------------------------------------------


def test_register_dataset_success(registry_with_config: PEPRegistry) -> None:
    """Covers: ETL-030 — Registry registers a new dataset."""
    new_ds = PEPDataset(
        code="new_ds",
        title="New Dataset",
        geography_levels=frozenset({"county"}),
        release_status="pending",
    )
    registry_with_config.register_dataset(new_ds)
    assert "new_ds" in registry_with_config.datasets


def test_register_dataset_duplicate_raises(registry_with_config: PEPRegistry) -> None:
    """Covers: ETL-030 — Registry rejects a duplicate dataset code."""
    dup_ds = PEPDataset(
        code="test_ds",
        title="Duplicate",
        geography_levels=frozenset(),
        release_status="pending",
    )
    with pytest.raises(ValueError, match="dataset already registered"):
        registry_with_config.register_dataset(dup_ds)


def test_get_dataset_returns_descriptor(registry_with_config: PEPRegistry) -> None:
    """Covers: ETL-030 — Registry returns a registered dataset."""
    ds = registry_with_config.get_dataset("test_ds")
    assert ds is not None
    assert ds.code == "test_ds"
    assert ds.title == "Test Dataset"


def test_get_dataset_missing_returns_none(registry_with_config: PEPRegistry) -> None:
    """Covers: ETL-030 — Registry returns None for an unknown dataset."""
    assert registry_with_config.get_dataset("nonexistent") is None


def test_list_datasets_no_filter(registry_with_config: PEPRegistry) -> None:
    """Covers: ETL-030 — Registry lists all datasets when unfiltered."""
    all_ds = registry_with_config.list_datasets()
    assert len(all_ds) == 1
    assert all_ds[0].code == "test_ds"


def test_list_datasets_by_geography(registry_with_config: PEPRegistry) -> None:
    """Covers: ETL-030 — Registry filters datasets by geography."""
    county_ds = PEPDataset(
        code="county_ds",
        title="County Dataset",
        geography_levels=frozenset({"county"}),
        release_status="active",
    )
    registry_with_config.register_dataset(county_ds)

    state_results = registry_with_config.list_datasets(geography_level="state")
    assert len(state_results) == 1
    assert state_results[0].code == "test_ds"

    county_results = registry_with_config.list_datasets(geography_level="county")
    assert len(county_results) == 1
    assert county_results[0].code == "county_ds"


def test_list_datasets_by_status(registry_with_config: PEPRegistry) -> None:
    """Covers: ETL-030 — Registry filters datasets by status."""
    pending_ds = PEPDataset(
        code="pending_ds",
        title="Pending Dataset",
        release_status="pending",
    )
    registry_with_config.register_dataset(pending_ds)

    active_results = registry_with_config.list_datasets(release_status="active")
    assert len(active_results) == 1
    assert active_results[0].code == "test_ds"

    pending_results = registry_with_config.list_datasets(release_status="pending")
    assert len(pending_results) == 1
    assert pending_results[0].code == "pending_ds"


def test_list_datasets_combined_filters(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: ETL-030 — Registry intersects dataset filters."""
    state_active = PEPDataset(
        code="state_active",
        title="State Active",
        geography_levels=frozenset({"state"}),
        release_status="active",
    )
    state_pending = PEPDataset(
        code="state_pending",
        title="State Pending",
        geography_levels=frozenset({"state"}),
        release_status="pending",
    )
    registry_with_config.register_dataset(state_active)
    registry_with_config.register_dataset(state_pending)

    results = registry_with_config.list_datasets(
        geography_level="state",
        release_status="pending",
    )
    assert len(results) == 1
    assert results[0].code == "state_pending"


# -----------------------------------------------------------------------
# Vintage registration and lookup
# -----------------------------------------------------------------------


def test_register_vintage_and_get(registry_with_config: PEPRegistry) -> None:
    """Covers: ETL-030 — Registry stores and retrieves a vintage."""
    vintage = PEPVintage(
        vintage_year=2020,
        decennial_base=2020,
        release_date="2021-04-26",
        is_current=True,
        datasets=frozenset({"test_ds"}),
    )
    registry_with_config.register_vintage(vintage)

    retrieved = registry_with_config.get_vintage(2020)
    assert retrieved is not None
    assert retrieved.vintage_year == 2020
    assert retrieved.is_current is True


def test_get_current_vintage_returns_most_recent(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: ETL-030 — Registry returns the latest current vintage."""
    v2019 = PEPVintage(
        vintage_year=2019,
        decennial_base=2020,
        release_date="2021-04-26",
        is_current=True,
        datasets=frozenset(),
    )
    v2020 = PEPVintage(
        vintage_year=2020,
        decennial_base=2020,
        release_date="2021-04-26",
        is_current=True,
        datasets=frozenset({"test_ds"}),
    )
    registry_with_config.register_vintage(v2019)
    registry_with_config.register_vintage(v2020)

    current = registry_with_config.get_current_vintage()
    assert current is not None
    assert current.vintage_year == 2020


def test_get_current_vintage_none_when_none_current(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: ETL-030 — Registry handles a missing current vintage."""
    v2019 = PEPVintage(
        vintage_year=2019,
        decennial_base=2020,
        release_date="2021-04-26",
        is_current=False,
        datasets=frozenset(),
    )
    registry_with_config.register_vintage(v2019)
    assert registry_with_config.get_current_vintage() is None


# -----------------------------------------------------------------------
# Release series
# -----------------------------------------------------------------------


def test_register_vintage_creates_series(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: ETL-030 — Registry creates a dataset release series."""
    vintage = PEPVintage(
        vintage_year=2020,
        decennial_base=2020,
        release_date="2021-04-26",
        is_current=True,
        datasets=frozenset({"test_ds"}),
    )
    registry_with_config.register_vintage(vintage)

    series = registry_with_config.get_release_series("test_ds")
    assert series is not None
    assert series.dataset_code == "test_ds"
    assert 2020 in series.vintages
    assert series.earliest_vintage == 2020
    assert series.latest_vintage == 2020
    assert series.status == "pending"


def test_register_vintage_updates_existing_series(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: ETL-030 — Registry extends a dataset release series."""
    v2019 = PEPVintage(
        vintage_year=2019,
        decennial_base=2020,
        release_date="2021-04-26",
        is_current=False,
        datasets=frozenset({"test_ds"}),
    )
    v2020 = PEPVintage(
        vintage_year=2020,
        decennial_base=2020,
        release_date="2021-04-26",
        is_current=True,
        datasets=frozenset({"test_ds"}),
    )
    registry_with_config.register_vintage(v2019)
    registry_with_config.register_vintage(v2020)

    series = registry_with_config.get_release_series("test_ds")
    assert series is not None
    assert 2019 in series.vintages
    assert 2020 in series.vintages
    assert series.earliest_vintage == 2019
    assert series.latest_vintage == 2020


def test_list_release_series_status_filter(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: ETL-030 — Registry filters release series by status."""
    v2020 = PEPVintage(
        vintage_year=2020,
        decennial_base=2020,
        release_date="2021-04-26",
        is_current=True,
        datasets=frozenset({"test_ds"}),
    )
    registry_with_config.register_vintage(v2020)

    pending = registry_with_config.list_release_series(status="pending")
    assert len(pending) == 1
    assert pending[0].dataset_code == "test_ds"

    active = registry_with_config.list_release_series(status="active")
    assert len(active) == 0


def test_list_release_series_vintage_filter(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: ETL-030 — Registry filters release series by vintage."""
    v2015 = PEPVintage(
        vintage_year=2015,
        decennial_base=2010,
        release_date="2021-04-26",
        is_current=False,
        datasets=frozenset({"old_ds"}),
    )
    v2020 = PEPVintage(
        vintage_year=2020,
        decennial_base=2020,
        release_date="2021-04-26",
        is_current=True,
        datasets=frozenset({"test_ds"}),
    )
    registry_with_config.register_vintage(v2015)
    registry_with_config.register_vintage(v2020)

    recent = registry_with_config.list_release_series(has_vintage_at_least=2018)
    assert len(recent) == 1
    assert recent[0].dataset_code == "test_ds"


def test_release_series_is_complete_property(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: ETL-030 — Release series reports completion state."""
    series_pending = PEPReleaseSeries(
        dataset_code="test",
        vintages=(2020,),
        earliest_vintage=2020,
        latest_vintage=2020,
        status="pending",
    )
    assert series_pending.is_complete is False

    series_completed = PEPReleaseSeries(
        dataset_code="test",
        vintages=(2020,),
        earliest_vintage=2020,
        latest_vintage=2020,
        status="completed",
    )
    assert series_completed.is_complete is True


# -----------------------------------------------------------------------
# Initialization
# -----------------------------------------------------------------------


def test_initialize_registers_decennial_vintage(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: ETL-030 — Legacy registry derives a decennial vintage."""
    registry_with_config.initialize()
    assert 2020 in registry_with_config.vintages

    vintage = registry_with_config.vintages[2020]
    assert vintage.is_current is True
    assert "test_ds" in vintage.datasets


def test_initialize_is_idempotent(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: ETL-030 — Registry initialization is idempotent."""
    registry_with_config.initialize()
    first_count = len(registry_with_config.vintages)
    registry_with_config.initialize()
    assert len(registry_with_config.vintages) == first_count


def test_reset_clears_vintages_and_series(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: ETL-030 — Registry reset clears runtime state."""
    vintage = PEPVintage(
        vintage_year=2020,
        decennial_base=2020,
        release_date="2021-04-26",
        is_current=True,
        datasets=frozenset({"test_ds"}),
    )
    registry_with_config.register_vintage(vintage)
    assert len(registry_with_config.vintages) > 0

    registry_with_config.reset()
    assert len(registry_with_config.vintages) == 0
    assert len(registry_with_config.series) == 0


# -----------------------------------------------------------------------
# Release discovery
# -----------------------------------------------------------------------


def test_discover_releases_returns_summaries(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: ETL-030 — Registry discovers legacy release summaries."""
    v2019 = PEPVintage(
        vintage_year=2019,
        decennial_base=2020,
        release_date="2021-04-26",
        is_current=False,
        datasets=frozenset(),
    )
    v2020 = PEPVintage(
        vintage_year=2020,
        decennial_base=2020,
        release_date="2021-04-26",
        is_current=True,
        datasets=frozenset({"test_ds"}),
    )
    registry_with_config.register_vintage(v2019)
    registry_with_config.register_vintage(v2020)

    releases = registry_with_config.discover_releases()
    assert len(releases) == 2
    assert releases[0]["vintage_year"] == 2019
    assert releases[1]["vintage_year"] == 2020
    assert releases[1]["is_current"] is True
    assert releases[1]["dataset_count"] == 1
    assert "test_ds" in releases[1]["datasets"]


# -----------------------------------------------------------------------
# Module-level singleton
# -----------------------------------------------------------------------


def test_get_registry_returns_singleton(
    minimal_config: PEPConfig,
) -> None:
    """Covers: ETL-030 — Default registry is a singleton."""
    reset_registry()
    reg1 = get_registry(minimal_config)
    reg2 = get_registry(minimal_config)
    assert reg1 is reg2


def test_get_registry_uses_config(
    minimal_config: PEPConfig,
) -> None:
    """Covers: ETL-030 — Default registry accepts an explicit config."""
    reset_registry()
    reg = get_registry(minimal_config)
    assert "test_ds" in reg.datasets


def test_reset_registry_clears_singleton() -> None:
    """Covers: ETL-030 — Default registry singleton can be reset."""
    reset_registry()
    reg1 = get_registry()
    reset_registry()
    reg2 = get_registry()
    assert reg1 is not reg2


# -----------------------------------------------------------------------
# Default registry (no config)
# -----------------------------------------------------------------------


def test_default_registry_uses_curated_products() -> None:
    """Covers: ETL-030 — Default registry exposes supported PEP products."""
    reset_registry()
    reg = PEPRegistry()
    assert set(reg.datasets) == {
        "pep_nst_alldata",
        "pep_county_alldata",
        "pep_subcounty",
    }


def test_registry_exposes_versioned_release_contract() -> None:
    """Covers: ETL-030 — Dataset/vintage resolves to one bulk product."""
    reg = PEPRegistry(CONFIG)

    release = reg.get_release("pep_nst_alldata", 2025)

    assert release is not None
    assert release.product_code == "NST-EST2025-ALLDATA"
    assert release.observation_start_year == 2020
    assert release.observation_end_year == 2025
    assert release.status == "published"
    assert release.data_url.endswith("/NST-EST2025-ALLDATA.csv")
    assert release.layout_url.endswith("/NST-EST2025-ALLDATA.pdf")


def test_registry_selects_current_release_per_dataset() -> None:
    """Covers: ETL-030 — Current selection uses publication status."""
    reg = PEPRegistry(CONFIG)

    releases = reg.list_releases(dataset_code="pep_county_alldata")
    current = reg.get_current_release("pep_county_alldata")

    assert [release.vintage_year for release in releases] == [2024, 2025]
    assert current is not None
    assert current.vintage_year == 2025
    assert current.status == "published"


def test_initialize_discovers_actual_release_vintages() -> None:
    """Covers: ETL-030 — Initialization derives configured release vintages."""
    reg = PEPRegistry(CONFIG)

    reg.initialize()
    discovered = reg.discover_releases()

    assert set(reg.vintages) == {2024, 2025}
    assert reg.get_current_vintage() is not None
    assert reg.get_current_vintage().vintage_year == 2025
    assert len(discovered) == 6
    assert discovered[-1] == {
        "dataset_code": "pep_subcounty",
        "vintage_year": 2025,
        "product_code": "SUB-EST2025",
        "release_date": "2026-05-14",
        "status": "published",
        "observation_start_year": 2020,
        "observation_end_year": 2025,
        "geography_basis_date": "2025-01-01",
        "schema_version": "sub-est2025",
        "data_url": "https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/cities/totals/sub-est2025.csv",
        "layout_url": "https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-2025/SUB-EST2025.pdf",
    }
