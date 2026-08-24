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
    postgres_conn_id: str = "public_data"

    # Census Data API key (read from env at request time, not stored).
    _api_key: str | None = None

    # Maximum HTTP request timeout in seconds.
    request_timeout: float = 60.0

    # Maximum concurrent requests per domain.
    max_concurrency: int = 2

    # Airflow pool name for PEP ingestion tasks.
    airflow_pool: str = "census_api"

    # PEP dataset registry: maps dataset_code to a frozen dataset descriptor.
    # Each entry records the API/bulk endpoint, supported geography levels,
    # variables, layout version, and release status.
    datasets: dict[str, "PEPDataset"] = field(default_factory=dict)

    # Immutable release contracts for the supported current and prior vintages.
    releases: tuple["PEPRelease", ...] = ()

    def __post_init__(self) -> None:
        """Reject unusable runtime scope without reading external state."""
        if not self.postgres_conn_id.strip():
            raise ValueError("postgres_conn_id must not be empty")
        if self.request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        if not self.airflow_pool.strip():
            raise ValueError("airflow_pool must not be empty")
        if any(code != dataset.code for code, dataset in self.datasets.items()):
            raise ValueError("dataset mapping keys must match dataset codes")
        release_keys = [
            (release.dataset_code, release.vintage_year) for release in self.releases
        ]
        if len(release_keys) != len(set(release_keys)):
            raise ValueError("duplicate PEP dataset/vintage release")
        unknown_datasets = {
            release.dataset_code
            for release in self.releases
            if release.dataset_code not in self.datasets
        }
        if unknown_datasets:
            raise ValueError(
                "release references unknown datasets: "
                + ", ".join(sorted(unknown_datasets))
            )

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
            source_code=self.source_code,
            postgres_conn_id=self.postgres_conn_id,
            _api_key=key,
            request_timeout=self.request_timeout,
            max_concurrency=self.max_concurrency,
            airflow_pool=self.airflow_pool,
            datasets=self.datasets,
            releases=self.releases,
        )


@dataclass(frozen=True)
class PEPRelease:
    """Versioned Census PEP bulk-file release contract.

    Current PEP estimates are published as bulk files. The vintage identifies
    the final observation year, while each file contains a revised time series
    beginning with the 2020 estimates base.
    """

    dataset_code: str
    vintage_year: int
    product_code: str
    data_url: str
    layout_url: str
    release_date: str
    observation_start_year: int
    observation_end_year: int
    geography_basis_date: str
    schema_version: str
    status: Literal["published", "archived"]
    media_type: str = "text/csv"

    def __post_init__(self) -> None:
        if self.observation_end_year != self.vintage_year:
            raise ValueError("PEP observation end year must equal its vintage")
        if self.observation_start_year > self.observation_end_year:
            raise ValueError("PEP observation range is reversed")
        if not self.data_url.startswith("https://www2.census.gov/"):
            raise ValueError("PEP data URL must use the official Census host")
        if not self.layout_url.startswith("https://www2.census.gov/"):
            raise ValueError("PEP layout URL must use the official Census host")


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
        "transport",
        "geography_levels",
        "summary_levels",
        "variables",
        "layout_version",
        "parser_version",
        "release_page_url",
        "data_url_template",
        "layout_url_template",
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
        transport: Literal["api_json", "bulk_csv"] = "bulk_csv",
        geography_levels: frozenset[str] = frozenset(),
        summary_levels: frozenset[str] = frozenset(),
        variables: frozenset[str] = frozenset(),
        layout_version: str = "1",
        parser_version: str = "census-pep-bulk-csv-v1",
        release_page_url: str = "",
        data_url_template: str = "",
        layout_url_template: str = "",
        release_status: Literal["active", "deprecated", "pending"] = "pending",
        decennial_base: int | None = None,
        release_date: str | None = None,
    ) -> None:
        object.__setattr__(self, "code", code)
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "api_path", api_path)
        object.__setattr__(self, "bulk_path", bulk_path)
        object.__setattr__(self, "transport", transport)
        object.__setattr__(self, "geography_levels", geography_levels)
        object.__setattr__(self, "summary_levels", summary_levels)
        object.__setattr__(self, "variables", variables)
        object.__setattr__(self, "layout_version", layout_version)
        object.__setattr__(self, "parser_version", parser_version)
        object.__setattr__(self, "release_page_url", release_page_url)
        object.__setattr__(self, "data_url_template", data_url_template)
        object.__setattr__(self, "layout_url_template", layout_url_template)
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

# Official current PEP estimates are bulk products. Census states that current
# estimates are not presently supported by its Data API, so the initial scope
# deliberately excludes speculative API dataset paths.
_CURATED_DATASETS: dict[str, PEPDataset] = {
    "pep_nst_alldata": PEPDataset(
        code="pep_nst_alldata",
        title="National and State Population Estimates and Components of Change",
        transport="bulk_csv",
        geography_levels=frozenset({"national", "region", "division", "state"}),
        summary_levels=frozenset({"010", "020", "030", "040"}),
        variables=frozenset(
            {
                "ESTIMATESBASE",
                "POPESTIMATE",
                "NPOPCHG",
                "BIRTHS",
                "DEATHS",
                "NATURALCHG",
                "INTERNATIONALMIG",
                "DOMESTICMIG",
                "NETMIG",
                "RESIDUAL",
                "RBIRTH",
                "RDEATH",
                "RNATURALCHG",
                "RINTERNATIONALMIG",
                "RDOMESTICMIG",
                "RNETMIG",
            }
        ),
        layout_version="vintage-specific-official-layout",
        parser_version="census-pep-bulk-csv-v1",
        release_page_url="https://www.census.gov/data/tables/time-series/demo/popest/2020s-national-total.html",
        data_url_template="https://www2.census.gov/programs-surveys/popest/datasets/2020-{vintage}/state/totals/NST-EST{vintage}-ALLDATA.csv",
        layout_url_template="https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-{vintage}/NST-EST{vintage}-ALLDATA.pdf",
        release_status="active",
        decennial_base=2020,
    ),
    "pep_county_alldata": PEPDataset(
        code="pep_county_alldata",
        title="State and County Population Estimates and Components of Change",
        transport="bulk_csv",
        geography_levels=frozenset({"state", "county"}),
        summary_levels=frozenset({"040", "050"}),
        variables=frozenset(
            {
                "ESTIMATESBASE",
                "POPESTIMATE",
                "NPOPCHG",
                "BIRTHS",
                "DEATHS",
                "NATURALCHG",
                "INTERNATIONALMIG",
                "DOMESTICMIG",
                "NETMIG",
                "RESIDUAL",
                "RBIRTH",
                "RDEATH",
                "RNATURALCHG",
                "RINTERNATIONALMIG",
                "RDOMESTICMIG",
                "RNETMIG",
            }
        ),
        layout_version="vintage-specific-official-layout",
        parser_version="census-pep-bulk-csv-v1",
        release_page_url="https://www.census.gov/data/datasets/time-series/demo/popest/2020s-counties-total.html",
        data_url_template="https://www2.census.gov/programs-surveys/popest/datasets/2020-{vintage}/counties/totals/co-est{vintage}-alldata.csv",
        layout_url_template="https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-{vintage}/CO-EST{vintage}-ALLDATA.pdf",
        release_status="active",
        decennial_base=2020,
    ),
    "pep_subcounty": PEPDataset(
        code="pep_subcounty",
        title="Subcounty Resident Population Estimates",
        transport="bulk_csv",
        geography_levels=frozenset(
            {"state", "county", "county_subdivision", "place", "consolidated_city"}
        ),
        summary_levels=frozenset(
            {"040", "050", "061", "071", "157", "162", "170", "172"}
        ),
        variables=frozenset({"ESTIMATESBASE", "POPESTIMATE"}),
        layout_version="vintage-specific-official-layout",
        parser_version="census-pep-bulk-csv-v1",
        release_page_url="https://www.census.gov/data/tables/time-series/demo/popest/2020s-total-cities-and-towns.html",
        data_url_template="https://www2.census.gov/programs-surveys/popest/datasets/2020-{vintage}/cities/totals/sub-est{vintage}.csv",
        layout_url_template="https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-{vintage}/SUB-EST{vintage}.pdf",
        release_status="active",
        decennial_base=2020,
    ),
}


def _release(
    dataset_code: str,
    vintage_year: int,
    product_code: str,
    release_date: str,
    *,
    status: Literal["published", "archived"],
) -> PEPRelease:
    dataset = _CURATED_DATASETS[dataset_code]
    return PEPRelease(
        dataset_code=dataset_code,
        vintage_year=vintage_year,
        product_code=product_code,
        data_url=dataset.data_url_template.format(vintage=vintage_year),
        layout_url=dataset.layout_url_template.format(vintage=vintage_year),
        release_date=release_date,
        observation_start_year=2020,
        observation_end_year=vintage_year,
        geography_basis_date=f"{vintage_year}-01-01",
        schema_version=product_code.lower(),
        status=status,
    )


_CURATED_RELEASES = (
    _release(
        "pep_nst_alldata", 2024, "NST-EST2024-ALLDATA", "2024-12-19", status="archived"
    ),
    _release(
        "pep_nst_alldata", 2025, "NST-EST2025-ALLDATA", "2026-01-27", status="published"
    ),
    _release(
        "pep_county_alldata",
        2024,
        "CO-EST2024-ALLDATA",
        "2025-03-13",
        status="archived",
    ),
    _release(
        "pep_county_alldata",
        2025,
        "CO-EST2025-ALLDATA",
        "2026-03-26",
        status="published",
    ),
    _release("pep_subcounty", 2024, "SUB-EST2024", "2025-05-15", status="archived"),
    _release("pep_subcounty", 2025, "SUB-EST2025", "2026-05-14", status="published"),
)

# Default config contains only nonsecret, import-safe release contracts.
CONFIG = PEPConfig(datasets=_CURATED_DATASETS, releases=_CURATED_RELEASES)
