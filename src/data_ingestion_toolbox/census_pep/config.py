"""Census PEP adapter configuration.

Defines source scope, API endpoint, timeouts, concurrency, Airflow pool,
and PostgreSQL connection ID without performing I/O at import time.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal

# Environment variable name for the Census API key.
# Must be provided by the deployment environment; validated at request time.
CENSUS_API_KEY_ENV = "CENSUS_API_KEY"

# Census Data API base URL.
CENSUS_API_BASE = "https://api.census.gov/data"

# Bulk file base URL for subcounty and other large datasets.
CENSUS_BULK_BASE = "https://www2.census.gov/geo/docs/maps-data/data/popest"


@dataclass(frozen=True)
class PEPConfig:
    """Immutable configuration for the Census PEP adapter.

    No I/O is performed at import or instantiation time.
    API key presence is checked only when a request executes.
    """

    # Source code identifier for capture/lineage tracking.
    source_code: str = "CENSUS_PEP"

    # Airflow connection ID for PostgreSQL (or None for env-based lookup).
    postgres_conn_id: str | None = None

    # Census Data API key (read from env at request time, not stored).
    _api_key: str | None = None

    # Maximum HTTP request timeout in seconds.
    request_timeout: float = 30.0

    # Maximum concurrent requests per domain.
    max_concurrency: int = 4

    # Airflow pool name for PEP ingestion tasks.
    airflow_pool: str = "census_pep"

    # PEP dataset registry: maps dataset_code to a frozen dataset descriptor.
    # Each entry records the API/bulk endpoint, supported geography levels,
    # variables, layout version, and release status.
    datasets: dict[str, "PEPDataset"] = field(default_factory=dict)

    @property
    def has_api_key(self) -> bool:
        """Whether a Census API key is available."""
        if self._api_key:
            return True
        return bool(os.environ.get(CENSUS_API_KEY_ENV))

    def get_api_key(self) -> str:
        """Return the Census API key from the instance or environment.

        Raises:
            ValueError: If no API key is available.
        """
        if self._api_key:
            return self._api_key
        key = os.environ.get(CENSUS_API_KEY_ENV)
        if not key:
            raise ValueError(
                f"{CENSUS_API_KEY_ENV} environment variable is required "
                "for Census API access"
            )
        return key

    def with_api_key(self, key: str) -> "PEPConfig":
        """Return a new config with the API key set (for testing)."""
        return PEPConfig(
            postgres_conn_id=self.postgres_conn_id,
            _api_key=key,
            request_timeout=self.request_timeout,
            max_concurrency=self.max_concurrency,
            airflow_pool=self.airflow_pool,
            datasets=self.datasets,
        )


# ---------------------------------------------------------------------------
# Forward reference for the dataset descriptor
# ---------------------------------------------------------------------------

class PEPDataset:
    """Descriptor for a single PEP dataset/product.

    Attributes:
        code: Stable dataset identifier (e.g., 'pepprst2020').
        title: Human-readable dataset title.
        api_path: API endpoint path relative to CENSUS_API_BASE.
        bulk_path: Bulk file path relative to CENSUS_BULK_BASE.
        geography_levels: Supported geography levels (national, state, county, place).
        variables: PEP variable codes included in this dataset.
        layout_version: Schema/layout version string.
        release_status: One of 'active', 'deprecated', 'pending'.
        decennial_base: The decennial census used as the base for this dataset.
        release_date: Official publication date (ISO format string or None).
    """

    __slots__ = (
        "code",
        "title",
        "api_path",
        "bulk_path",
        "geography_levels",
        "variables",
        "layout_version",
        "release_status",
        "decennial_base",
        "release_date",
    )

    def __init__(
        self,
        code: str,
        title: str,
        api_path: str = "",
        bulk_path: str = "",
        geography_levels: frozenset[str] = frozenset(),
        variables: frozenset[str] = frozenset(),
        layout_version: str = "1",
        release_status: Literal["active", "deprecated", "pending"] = "pending",
        decennial_base: int | None = None,
        release_date: str | None = None,
    ) -> None:
        object.__setattr__(self, "code", code)
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "api_path", api_path)
        object.__setattr__(self, "bulk_path", bulk_path)
        object.__setattr__(self, "geography_levels", geography_levels)
        object.__setattr__(self, "variables", variables)
        object.__setattr__(self, "layout_version", layout_version)
        object.__setattr__(self, "release_status", release_status)
        object.__setattr__(self, "decennial_base", decennial_base)
        object.__setattr__(self, "release_date", release_date)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"PEPDataset is immutable: cannot set {name}")

    def __hash__(self) -> int:
        return hash(self.code)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PEPDataset):
            return False
        return self.code == other.code


# ---------------------------------------------------------------------------
# Curated dataset registry
# ---------------------------------------------------------------------------

# Initial curated datasets: national/state totals, county totals, and
# incorporated-place totals. These represent the first release scope.
_CURATED_DATASETS: dict[str, PEPDataset] = {
    "pepprst2020": PEPDataset(
        code="pepprst2020",
        title="Annual Population Estimates and Components of Change: Vintage 2020 (National and State)",
        api_path="/data/2020/pep/pepprst2020",
        bulk_path="/geo/docs/maps-data/data/popest/2020s-national-total.csv",
        geography_levels=frozenset({"national", "state"}),
        variables=frozenset(
            {
                "POPULATION",
                "BIRTHS",
                "DEATHS",
                "NATURAL_INCREASE",
                "DOMESTIC_MIGRATION",
                "INTERNATIONAL_MIGRATION",
                "TOTAL_CHANGE",
            }
        ),
        layout_version="1",
        release_status="active",
        decennial_base=2020,
    ),
    "pecp2020": PEPDataset(
        code="pecp2020",
        title="Annual Population Estimates: Components of Change (County)",
        api_path="/data/2020/pep/pecp2020",
        bulk_path="/geo/docs/maps-data/data/popest/2020s-counties-estimates.csv",
        geography_levels=frozenset({"national", "state", "county"}),
        variables=frozenset(
            {
                "POPULATION",
                "BIRTHS",
                "DEATHS",
                "NATURAL_INCREASE",
                "DOMESTIC_MIGRATION",
                "INTERNATIONAL_MIGRATION",
                "TOTAL_CHANGE",
            }
        ),
        layout_version="1",
        release_status="active",
        decennial_base=2020,
    ),
    "gvc2020": PEPDataset(
        code="gvc2020",
        title="Gazetteer File and Census Places (Incorporated Place)",
        api_path="",
        bulk_path="/geo/docs/gazetteer-files/2020/gazetteer/2020/gaz_2020_cities.csv",
        geography_levels=frozenset({"state", "place"}),
        variables=frozenset({"GEOID", "NAME", "CLASSFIP", "FUNCSTAT"}),
        layout_version="1",
        release_status="active",
        decennial_base=2020,
    ),
}

# Default config instance with curated datasets.
CONFIG = PEPConfig(datasets=_CURATED_DATASETS)
