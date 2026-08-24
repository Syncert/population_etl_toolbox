"""PEP dataset and vintage registry with release discovery.

This module manages:
- The curated dataset registry (code → PEPDataset mapping)
- Vintage metadata (year, decennial base, release status)
- Release series tracking across vintages
- Discovery of active datasets by geography level and release status
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from data_ingestion_toolbox.census_pep.config import (
    CONFIG,
    PEPConfig,
    PEPDataset,
    PEPRelease,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Vintage descriptor
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PEPVintage:
    """Metadata for a single PEP vintage (publication year).

    Attributes:
        vintage_year: The vintage year (e.g., 2020).
        decennial_base: The decennial census used as the base.
        release_date: Official publication date (ISO string or None).
        is_current: Whether this is the most recent vintage.
        datasets: Dataset codes available in this vintage.
    """

    vintage_year: int
    decennial_base: int
    release_date: str | None
    is_current: bool
    datasets: frozenset[str]


# ---------------------------------------------------------------------------
# Release series
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PEPReleaseSeries:
    """A release series tracks a dataset across vintages.

    Attributes:
        dataset_code: The dataset identifier (e.g., 'pepprst2020').
        vintages: Ordered list of vintage years available.
        earliest_vintage: Earliest vintage year.
        latest_vintage: Most recent vintage year.
        status: Current release status ('active', 'deprecated', 'pending').
    """

    dataset_code: str
    vintages: tuple[int, ...]
    earliest_vintage: int
    latest_vintage: int
    status: str

    @property
    def is_complete(self) -> bool:
        """Whether the series has reached its final vintage."""
        return self.status == "completed"


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class PEPRegistry:
    """Immutable dataset and vintage registry.

    The registry is populated from the curated dataset definitions in config
    and augmented with vintage/release metadata at runtime.
    """

    def __init__(self, config: PEPConfig | None = None) -> None:
        self._config = CONFIG if config is None else config
        self._datasets: dict[str, PEPDataset] = dict(self._config.datasets)
        self._releases: dict[tuple[str, int], PEPRelease] = {
            (release.dataset_code, release.vintage_year): release
            for release in self._config.releases
        }
        self._vintages: dict[int, PEPVintage] = {}
        self._series: dict[str, PEPReleaseSeries] = {}
        self._initialized = False

    @property
    def datasets(self) -> dict[str, PEPDataset]:
        """Return a copy of the dataset registry."""
        return dict(self._datasets)

    @property
    def vintages(self) -> dict[int, PEPVintage]:
        """Return a copy of the vintage registry."""
        return dict(self._vintages)

    @property
    def series(self) -> dict[str, PEPReleaseSeries]:
        """Return a copy of the release series registry."""
        return dict(self._series)

    @property
    def releases(self) -> dict[tuple[str, int], PEPRelease]:
        """Return versioned release contracts keyed by dataset and vintage."""
        return dict(self._releases)

    # ------------------------------------------------------------------
    # Dataset registration
    # ------------------------------------------------------------------

    def register_dataset(self, dataset: PEPDataset) -> None:
        """Register a single dataset in the registry.

        Args:
            dataset: The dataset descriptor to register.

        Raises:
            ValueError: If a dataset with the same code already exists.
        """
        if dataset.code in self._datasets:
            raise ValueError(f"dataset already registered: {dataset.code}")
        self._datasets[dataset.code] = dataset
        logger.info("Registered dataset: %s", dataset.code)

    def register_datasets(self, datasets: dict[str, PEPDataset]) -> None:
        """Register multiple datasets at once."""
        for code, dataset in datasets.items():
            self.register_dataset(dataset)

    # ------------------------------------------------------------------
    # Dataset lookup
    # ------------------------------------------------------------------

    def get_dataset(self, code: str) -> PEPDataset | None:
        """Look up a dataset by its code.

        Args:
            code: The dataset code (e.g., 'pepprst2020').

        Returns:
            The dataset descriptor, or None if not found.
        """
        return self._datasets.get(code)

    def list_datasets(
        self,
        *,
        geography_level: str | None = None,
        release_status: str | None = None,
    ) -> list[PEPDataset]:
        """List datasets, optionally filtered by geography or status.

        Args:
            geography_level: Filter by geography level (e.g., 'state', 'county').
            release_status: Filter by release status ('active', 'deprecated', 'pending').

        Returns:
            List of matching dataset descriptors.
        """
        results = list(self._datasets.values())

        if geography_level is not None:
            results = [ds for ds in results if geography_level in ds.geography_levels]

        if release_status is not None:
            results = [ds for ds in results if ds.release_status == release_status]

        return results

    # ------------------------------------------------------------------
    # Versioned release lookup
    # ------------------------------------------------------------------

    def get_release(
        self,
        dataset_code: str,
        vintage_year: int,
    ) -> PEPRelease | None:
        """Return the exact bulk-file contract for a dataset vintage."""
        return self._releases.get((dataset_code, vintage_year))

    def list_releases(
        self,
        *,
        dataset_code: str | None = None,
        status: str | None = None,
    ) -> list[PEPRelease]:
        """List stable release contracts in dataset/vintage order."""
        releases = self._releases.values()
        if dataset_code is not None:
            releases = (
                release for release in releases if release.dataset_code == dataset_code
            )
        if status is not None:
            releases = (release for release in releases if release.status == status)
        return sorted(
            releases,
            key=lambda release: (release.dataset_code, release.vintage_year),
        )

    def get_current_release(self, dataset_code: str) -> PEPRelease | None:
        """Return the latest published release for a dataset."""
        published = self.list_releases(
            dataset_code=dataset_code,
            status="published",
        )
        if not published:
            return None
        return max(published, key=lambda release: release.vintage_year)

    # ------------------------------------------------------------------
    # Vintage registration and discovery
    # ------------------------------------------------------------------

    def register_vintage(self, vintage: PEPVintage) -> None:
        """Register a vintage in the registry.

        Args:
            vintage: The vintage metadata to register.
        """
        self._vintages[vintage.vintage_year] = vintage
        logger.info("Registered vintage: %d", vintage.vintage_year)

        # Update release series for each dataset in this vintage
        for ds_code in vintage.datasets:
            if ds_code in self._series:
                series = self._series[ds_code]
                new_vintages = tuple(
                    sorted(set(series.vintages) | {vintage.vintage_year})
                )
                self._series[ds_code] = PEPReleaseSeries(
                    dataset_code=ds_code,
                    vintages=new_vintages,
                    earliest_vintage=min(new_vintages),
                    latest_vintage=max(new_vintages),
                    status=series.status,
                )
            else:
                self._series[ds_code] = PEPReleaseSeries(
                    dataset_code=ds_code,
                    vintages=(vintage.vintage_year,),
                    earliest_vintage=vintage.vintage_year,
                    latest_vintage=vintage.vintage_year,
                    status="pending",
                )

    def get_vintage(self, year: int) -> PEPVintage | None:
        """Look up a vintage by year.

        Args:
            year: The vintage year.

        Returns:
            The vintage metadata, or None if not found.
        """
        return self._vintages.get(year)

    def get_current_vintage(self) -> PEPVintage | None:
        """Return the most recent current vintage.

        Returns:
            The current vintage, or None if no current vintage is registered.
        """
        current_vintages = [v for v in self._vintages.values() if v.is_current]
        if not current_vintages:
            return None
        return max(current_vintages, key=lambda v: v.vintage_year)

    # ------------------------------------------------------------------
    # Release series discovery
    # ------------------------------------------------------------------

    def get_release_series(self, dataset_code: str) -> PEPReleaseSeries | None:
        """Look up the release series for a dataset.

        Args:
            dataset_code: The dataset code.

        Returns:
            The release series, or None if not found.
        """
        return self._series.get(dataset_code)

    def list_release_series(
        self,
        *,
        status: str | None = None,
        has_vintage_at_least: int | None = None,
    ) -> list[PEPReleaseSeries]:
        """List release series with optional filters.

        Args:
            status: Filter by status ('active', 'deprecated', 'pending').
            has_vintage_at_least: Minimum vintage year required.

        Returns:
            List of matching release series.
        """
        results = list(self._series.values())

        if status is not None:
            results = [s for s in results if s.status == status]

        if has_vintage_at_least is not None:
            results = [s for s in results if s.latest_vintage >= has_vintage_at_least]

        return results

    # ------------------------------------------------------------------
    # Release discovery
    # ------------------------------------------------------------------

    def discover_releases(self) -> list[dict[str, Any]]:
        """Discover available releases across all datasets and vintages.

        Returns a list of release summaries containing:
        - vintage_year: The vintage year
        - is_current: Whether this is the current vintage
        - dataset_count: Number of datasets available
        - datasets: List of dataset codes
        - series: Release series information

        Returns:
            List of release summary dictionaries.
        """
        if self._releases:
            return [
                {
                    "dataset_code": release.dataset_code,
                    "vintage_year": release.vintage_year,
                    "product_code": release.product_code,
                    "release_date": release.release_date,
                    "status": release.status,
                    "observation_start_year": release.observation_start_year,
                    "observation_end_year": release.observation_end_year,
                    "geography_basis_date": release.geography_basis_date,
                    "schema_version": release.schema_version,
                    "data_url": release.data_url,
                    "layout_url": release.layout_url,
                }
                for release in sorted(
                    self._releases.values(),
                    key=lambda item: (item.vintage_year, item.dataset_code),
                )
            ]

        releases = []
        for year, vintage in sorted(self._vintages.items()):
            series_info = {}
            for ds_code in vintage.datasets:
                series = self._series.get(ds_code)
                if series:
                    series_info[ds_code] = {
                        "vintages": series.vintages,
                        "status": series.status,
                    }
            releases.append(
                {
                    "vintage_year": year,
                    "is_current": vintage.is_current,
                    "dataset_count": len(vintage.datasets),
                    "datasets": sorted(vintage.datasets),
                    "series": series_info,
                }
            )
        return releases

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def initialize(self) -> None:
        """Initialize the registry with default vintages and series.

        This method scans the curated datasets and registers default
        vintage metadata for the decennial base year.
        """
        if self._initialized:
            return

        if self._releases:
            current_year = max(
                release.vintage_year
                for release in self._releases.values()
                if release.status == "published"
            )
            release_years = sorted(
                {release.vintage_year for release in self._releases.values()}
            )
            for year in release_years:
                year_releases = [
                    release
                    for release in self._releases.values()
                    if release.vintage_year == year
                ]
                dataset_codes = frozenset(
                    release.dataset_code for release in year_releases
                )
                bases = {
                    self._datasets[code].decennial_base
                    for code in dataset_codes
                    if self._datasets[code].decennial_base is not None
                }
                if len(bases) != 1:
                    raise ValueError(f"vintage {year} must have one decennial base")
                self.register_vintage(
                    PEPVintage(
                        vintage_year=year,
                        decennial_base=bases.pop(),
                        release_date=max(
                            release.release_date for release in year_releases
                        ),
                        is_current=year == current_year,
                        datasets=dataset_codes,
                    )
                )

            for dataset_code in self._datasets:
                dataset_releases = self.list_releases(dataset_code=dataset_code)
                if not dataset_releases:
                    continue
                vintages = tuple(release.vintage_year for release in dataset_releases)
                self._series[dataset_code] = PEPReleaseSeries(
                    dataset_code=dataset_code,
                    vintages=vintages,
                    earliest_vintage=min(vintages),
                    latest_vintage=max(vintages),
                    status=(
                        "active"
                        if any(
                            release.status == "published"
                            for release in dataset_releases
                        )
                        else "completed"
                    ),
                )

            self._initialized = True
            return

        # Compatibility path for caller-provided datasets without releases.
        decennial_bases = {
            ds.decennial_base for ds in self._datasets.values() if ds.decennial_base
        }
        for base in decennial_bases:
            # The decennial base year is the current vintage
            vintage = PEPVintage(
                vintage_year=base,
                decennial_base=base,
                release_date=None,
                is_current=True,
                datasets=frozenset(
                    ds.code
                    for ds in self._datasets.values()
                    if ds.decennial_base == base
                ),
            )
            self.register_vintage(vintage)

        self._initialized = True

    def reset(self) -> None:
        """Reset the registry to its initial curated state."""
        self._vintages.clear()
        self._series.clear()
        self._initialized = False


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_default_registry: PEPRegistry | None = None


def get_registry(config: PEPConfig | None = None) -> PEPRegistry:
    """Return the module-level default registry (singleton).

    Args:
        config: Optional config to initialize the registry with.

    Returns:
        The default PEPRegistry instance.
    """
    global _default_registry
    if _default_registry is None:
        _default_registry = PEPRegistry(config)
    return _default_registry


def reset_registry() -> None:
    """Reset the module-level default registry (for testing)."""
    global _default_registry
    _default_registry = None
