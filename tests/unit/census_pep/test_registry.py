"""Census PEP registry contracts: registration, lookup, vintage, and series."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_pep.config import PEPConfig, PEPDataset
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
    """Covers: Registry.register_dataset — new dataset registered without error."""
    new_ds = PEPDataset(
        code="new_ds",
        title="New Dataset",
        geography_levels=frozenset({"county"}),
        release_status="pending",
    )
    registry_with_config.register_dataset(new_ds)
    assert "new_ds" in registry_with_config.datasets


def test_register_dataset_duplicate_raises(registry_with_config: PEPRegistry) -> None:
    """Covers: Registry.register_dataset — duplicate code raises ValueError."""
    dup_ds = PEPDataset(
        code="test_ds",
        title="Duplicate",
        geography_levels=frozenset(),
        release_status="pending",
    )
    with pytest.raises(ValueError, match="dataset already registered"):
        registry_with_config.register_dataset(dup_ds)


def test_get_dataset_returns_descriptor(registry_with_config: PEPRegistry) -> None:
    """Covers: Registry.get_dataset — returns the dataset or None."""
    ds = registry_with_config.get_dataset("test_ds")
    assert ds is not None
    assert ds.code == "test_ds"
    assert ds.title == "Test Dataset"


def test_get_dataset_missing_returns_none(registry_with_config: PEPRegistry) -> None:
    """Covers: Registry.get_dataset — missing code returns None."""
    assert registry_with_config.get_dataset("nonexistent") is None


def test_list_datasets_no_filter(registry_with_config: PEPRegistry) -> None:
    """Covers: Registry.list_datasets — returns all when unfiltered."""
    all_ds = registry_with_config.list_datasets()
    assert len(all_ds) == 1
    assert all_ds[0].code == "test_ds"


def test_list_datasets_by_geography(registry_with_config: PEPRegistry) -> None:
    """Covers: Registry.list_datasets — geography filter works."""
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
    """Covers: Registry.list_datasets — status filter works."""
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
    """Covers: Registry.list_datasets — combined filters intersect."""
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
    """Covers: Registry.register_vintage — vintage stored and retrievable."""
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
    """Covers: Registry.get_current_vintage — returns the latest current."""
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
    """Covers: Registry.get_current_vintage — None when no current vintage."""
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
    """Covers: Registry.register_vintage — creates release series for new dataset."""
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
    """Covers: Registry.register_vintage — extends series vintages tuple."""
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
    """Covers: Registry.list_release_series — status filter works."""
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
    """Covers: Registry.list_release_series — vintage filter works."""
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

    recent = registry_with_config.list_release_series(
        has_vintage_at_least=2018
    )
    assert len(recent) == 1
    assert recent[0].dataset_code == "test_ds"


def test_release_series_is_complete_property(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: PEPReleaseSeries.is_complete — True only when completed."""
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
    """Covers: Registry.initialize — registers decennial base vintage."""
    registry_with_config.initialize()
    assert 2020 in registry_with_config.vintages

    vintage = registry_with_config.vintages[2020]
    assert vintage.is_current is True
    assert "test_ds" in vintage.datasets


def test_initialize_is_idempotent(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: Registry.initialize — calling twice does not duplicate."""
    registry_with_config.initialize()
    first_count = len(registry_with_config.vintages)
    registry_with_config.initialize()
    assert len(registry_with_config.vintages) == first_count


def test_reset_clears_vintages_and_series(
    registry_with_config: PEPRegistry,
) -> None:
    """Covers: Registry.reset — clears runtime state."""
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
    """Covers: Registry.discover_releases — returns list of release dicts."""
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
    """Covers: get_registry — returns same instance on repeated calls."""
    reset_registry()
    reg1 = get_registry(minimal_config)
    reg2 = get_registry(minimal_config)
    assert reg1 is reg2


def test_get_registry_uses_config(
    minimal_config: PEPConfig,
) -> None:
    """Covers: get_registry — registry is initialized with provided config."""
    reset_registry()
    reg = get_registry(minimal_config)
    assert "test_ds" in reg.datasets


def test_reset_registry_clears_singleton() -> None:
    """Covers: reset_registry — clears the module-level singleton."""
    reset_registry()
    reg1 = get_registry()
    reset_registry()
    reg2 = get_registry()
    assert reg1 is not reg2


# -----------------------------------------------------------------------
# Default registry (no config)
# -----------------------------------------------------------------------


def test_default_registry_empty_datasets() -> None:
    """Covers: PEPRegistry — default config has no curated datasets."""
    reset_registry()
    reg = PEPRegistry()
    assert len(reg.datasets) == 0
