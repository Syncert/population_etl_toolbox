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

    PEP estimates are published as bulk files, one decade at a time. The
    Bureau publishes two kinds of series and this contract distinguishes
    them, because only one of them ends at its own vintage:

    ``postcensal``
        Published during the decade it estimates and revised each year. The
        vintage names the last observation year, so a Vintage 2025 file
        carries July 2020 through July 2025.
    ``intercensal``
        Published after the following census, once the decade can be closed
        against two enumerations. Its observation range ends years before
        the publication that carries it, so a release covering 2000 through
        2010 may be published under a later vintage.

    ``observation_start_year`` and ``observation_end_year`` are therefore
    read from the release definition rather than assumed, and the vintage
    equality holds only for the postcensal kind.
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
    series_kind: Literal["postcensal", "intercensal"] = "postcensal"
    #: Member path inside the archive when the product ships as a zip. The
    #: registered ``data_url`` is always the archive itself, so raw capture
    #: keeps the bytes the Bureau published rather than an extract of them.
    archive_member: str | None = None
    #: Partition keys for a product the Bureau splits across several files
    #: (the 2000s intercensal county product is one file per state). Empty
    #: for a single-file product. Each partition is captured separately and
    #: the release is complete only when every one of them is present.
    partitions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.series_kind == "postcensal" and (
            self.observation_end_year != self.vintage_year
        ):
            raise ValueError(
                "postcensal PEP observation end year must equal its vintage"
            )
        if self.observation_end_year > self.vintage_year:
            raise ValueError("PEP observation range ends after its own vintage")
        if self.observation_start_year > self.observation_end_year:
            raise ValueError("PEP observation range is reversed")
        if not self.data_url.startswith("https://www2.census.gov/"):
            raise ValueError("PEP data URL must use the official Census host")
        if not self.layout_url.startswith("https://www2.census.gov/"):
            raise ValueError("PEP layout URL must use the official Census host")
        if self.archive_member is not None and not self.archive_member:
            raise ValueError("PEP archive member must not be empty when declared")
        if len(self.partitions) != len(set(self.partitions)):
            raise ValueError("PEP release declares a duplicate partition key")
        if self.partitions and "{partition}" not in self.data_url:
            raise ValueError(
                "a partitioned PEP release must template its data URL on {partition}"
            )

    def source_files(self) -> tuple[tuple[str | None, str], ...]:
        """The ``(partition, url)`` pairs this release is captured from.

        A single-file product yields one pair whose partition is ``None``;
        a partitioned product yields one pair per declared partition. The
        caller captures each pair separately, so a partition that fails to
        answer is a missing file rather than a silently shorter release.
        """
        if not self.partitions:
            return ((None, self.data_url),)
        return tuple(
            (partition, self.data_url.format(partition=partition))
            for partition in self.partitions
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
        "transport",
        "geography_levels",
        "summary_levels",
        "variables",
        "layout_version",
        "parser_version",
        "text_encoding",
        "release_page_url",
        "data_url_template",
        "layout_url_template",
        "release_status",
        "decennial_base",
        "release_date",
        "series_kind",
        "era",
        "native_grain",
        "derivation",
        "archive_member",
        "partitions",
        "required_columns",
        "minimum_states",
        "minimum_principal_rows",
    )

    def __init__(
        self,
        code: str,
        title: str,
        api_path: str = "",
        bulk_path: str = "",
        transport: Literal["api_json", "bulk_csv", "bulk_zip"] = "bulk_csv",
        geography_levels: frozenset[str] = frozenset(),
        summary_levels: frozenset[str] = frozenset(),
        variables: frozenset[str] = frozenset(),
        layout_version: str = "1",
        parser_version: str = "census-pep-bulk-csv-v1",
        text_encoding: Literal["utf-8-sig", "cp1252", "latin-1"] = "utf-8-sig",
        release_page_url: str = "",
        data_url_template: str = "",
        layout_url_template: str = "",
        release_status: Literal["active", "deprecated", "pending"] = "pending",
        decennial_base: int | None = None,
        release_date: str | None = None,
        series_kind: Literal["postcensal", "intercensal"] = "postcensal",
        era: str = "",
        native_grain: str = "",
        derivation: str | None = None,
        archive_member: str | None = None,
        partitions: tuple[str, ...] = (),
        required_columns: frozenset[str] = frozenset(),
        minimum_states: int = 0,
        minimum_principal_rows: int = 0,
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
        object.__setattr__(self, "text_encoding", text_encoding)
        object.__setattr__(self, "release_page_url", release_page_url)
        object.__setattr__(self, "data_url_template", data_url_template)
        object.__setattr__(self, "layout_url_template", layout_url_template)
        object.__setattr__(self, "release_status", release_status)
        object.__setattr__(self, "decennial_base", decennial_base)
        object.__setattr__(self, "release_date", release_date)
        object.__setattr__(self, "series_kind", series_kind)
        object.__setattr__(self, "era", era)
        object.__setattr__(self, "native_grain", native_grain)
        object.__setattr__(self, "derivation", derivation)
        object.__setattr__(self, "archive_member", archive_member)
        object.__setattr__(self, "partitions", partitions)
        object.__setattr__(self, "required_columns", required_columns)
        object.__setattr__(self, "minimum_states", minimum_states)
        object.__setattr__(self, "minimum_principal_rows", minimum_principal_rows)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"PEPDataset is immutable: cannot set {name}")

    def __hash__(self) -> int:
        return hash(self.code)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PEPDataset):
            return False
        return self.code == other.code


# ---------------------------------------------------------------------------
# Variable spelling across eras
# ---------------------------------------------------------------------------

#: The Bureau renamed two component families when it opened the 2020s series:
#: the 2000s and 2010s "all data" files publish ``NATURALINC``/``RNATURALINC``
#: where the 2020s files publish ``NATURALCHG``/``RNATURALCHG``. The measure is
#: the same, so a dataset declares the spelling its own file uses and the
#: parser maps it onto the one canonical metric code. A family absent from this
#: map is already canonical.
SOURCE_VARIABLE_ALIASES: dict[str, str] = {
    "NATURALINC": "NATURALCHG",
    "RNATURALINC": "RNATURALCHG",
}

#: The decennial count column, whose year sits inside the name rather than
#: after it (``CENSUS2010POP``). It is an April enumeration, not a July
#: estimate, so it is published as its own measure and never folded into
#: ``POPESTIMATE`` -- that separation is what keeps a decade's closing count
#: from colliding with the next decade's opening estimate.
#: Only the closed-decade files carry it: the 2020s "all data" files publish
#: no ``CENSUS*POP`` column, so those products do not declare one.
CENSUS_COUNT_VARIABLE = "CENSUSPOP"

#: The identifying columns each published layout family carries. A file
#: missing one of them is not the product it claims to be, so the parser
#: refuses it rather than reading whatever columns happen to line up.
NST_LAYOUT_COLUMNS = frozenset({"SUMLEV", "REGION", "DIVISION", "STATE", "NAME"})
COUNTY_LAYOUT_COLUMNS = frozenset({"SUMLEV", "STATE", "COUNTY", "STNAME", "CTYNAME"})
SUBCOUNTY_LAYOUT_COLUMNS = frozenset(
    {
        "SUMLEV",
        "STATE",
        "COUNTY",
        "PLACE",
        "COUSUB",
        "CONCIT",
        "FUNCSTAT",
        "NAME",
        "STNAME",
    }
)

#: Component families shared by every "all data" file, in the spelling used
#: by the 2000s and 2010s releases.
_LEGACY_ALLDATA_VARIABLES = frozenset(
    {
        CENSUS_COUNT_VARIABLE,
        "ESTIMATESBASE",
        "POPESTIMATE",
        "NPOPCHG",
        "BIRTHS",
        "DEATHS",
        "NATURALINC",
        "INTERNATIONALMIG",
        "DOMESTICMIG",
        "NETMIG",
        "RESIDUAL",
        "RBIRTH",
        "RDEATH",
        "RNATURALINC",
        "RINTERNATIONALMIG",
        "RDOMESTICMIG",
        "RNETMIG",
    }
)

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
        text_encoding="utf-8-sig",
        release_page_url="https://www.census.gov/data/tables/time-series/demo/popest/2020s-national-total.html",
        data_url_template="https://www2.census.gov/programs-surveys/popest/datasets/2020-{vintage}/state/totals/NST-EST{vintage}-ALLDATA.csv",
        layout_url_template="https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-{vintage}/NST-EST{vintage}-ALLDATA.pdf",
        release_status="active",
        decennial_base=2020,
        series_kind="postcensal",
        era="2020s",
        native_grain="040",
        required_columns=NST_LAYOUT_COLUMNS,
        minimum_states=50,
        minimum_principal_rows=50,
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
        text_encoding="cp1252",
        release_page_url="https://www.census.gov/data/datasets/time-series/demo/popest/2020s-counties-total.html",
        data_url_template="https://www2.census.gov/programs-surveys/popest/datasets/2020-{vintage}/counties/totals/co-est{vintage}-alldata.csv",
        layout_url_template="https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-{vintage}/CO-EST{vintage}-ALLDATA.pdf",
        release_status="active",
        decennial_base=2020,
        series_kind="postcensal",
        era="2020s",
        native_grain="050",
        required_columns=COUNTY_LAYOUT_COLUMNS,
        minimum_states=50,
        minimum_principal_rows=3000,
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
        text_encoding="cp1252",
        release_page_url="https://www.census.gov/data/tables/time-series/demo/popest/2020s-total-cities-and-towns.html",
        data_url_template="https://www2.census.gov/programs-surveys/popest/datasets/2020-{vintage}/cities/totals/sub-est{vintage}.csv",
        layout_url_template="https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-{vintage}/SUB-EST{vintage}.pdf",
        release_status="active",
        decennial_base=2020,
        series_kind="postcensal",
        era="2020s",
        native_grain="162",
        required_columns=SUBCOUNTY_LAYOUT_COLUMNS,
        minimum_states=50,
        minimum_principal_rows=18000,
    ),
    # --- 2010s: the closed decade, Vintage 2020 -----------------------------
    "pep_county_alldata_2010s": PEPDataset(
        code="pep_county_alldata_2010s",
        title=(
            "State and County Population Estimates and Components of Change, "
            "2010-2020 (Vintage 2020)"
        ),
        transport="bulk_csv",
        geography_levels=frozenset({"state", "county"}),
        summary_levels=frozenset({"040", "050"}),
        variables=_LEGACY_ALLDATA_VARIABLES,
        layout_version="vintage-specific-official-layout",
        parser_version="census-pep-bulk-csv-v1",
        text_encoding="cp1252",
        release_page_url="https://www.census.gov/programs-surveys/popest/data/tables.html",
        data_url_template="https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv",
        layout_url_template="https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2010-2020/co-est2020-alldata.pdf",
        release_status="active",
        decennial_base=2010,
        series_kind="postcensal",
        era="2010s",
        native_grain="050",
        required_columns=COUNTY_LAYOUT_COLUMNS,
        minimum_states=50,
        minimum_principal_rows=3000,
    ),
    "pep_nst_alldata_2010s": PEPDataset(
        code="pep_nst_alldata_2010s",
        title=(
            "National and State Population Estimates and Components of Change, "
            "2010-2020 (Vintage 2020)"
        ),
        transport="bulk_csv",
        geography_levels=frozenset({"national", "region", "division", "state"}),
        summary_levels=frozenset({"010", "020", "030", "040"}),
        variables=_LEGACY_ALLDATA_VARIABLES,
        layout_version="vintage-specific-official-layout",
        parser_version="census-pep-bulk-csv-v1",
        text_encoding="utf-8-sig",
        release_page_url="https://www.census.gov/programs-surveys/popest/data/tables.html",
        data_url_template="https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/state/totals/nst-est2020-alldata.csv",
        layout_url_template="https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2010-2020/nst-est2020-alldata.pdf",
        release_status="active",
        decennial_base=2010,
        series_kind="postcensal",
        era="2010s",
        native_grain="040",
        required_columns=NST_LAYOUT_COLUMNS,
        minimum_states=50,
        minimum_principal_rows=50,
    ),
    # --- 2000s: the closed decade, Vintage 2009 -----------------------------
    "pep_county_alldata_2000s": PEPDataset(
        code="pep_county_alldata_2000s",
        title=(
            "State and County Population Estimates and Components of Change, "
            "2000-2009 (Vintage 2009)"
        ),
        transport="bulk_csv",
        geography_levels=frozenset({"state", "county"}),
        summary_levels=frozenset({"040", "050"}),
        variables=_LEGACY_ALLDATA_VARIABLES,
        layout_version="vintage-specific-official-layout",
        parser_version="census-pep-bulk-csv-v1",
        text_encoding="cp1252",
        release_page_url="https://www.census.gov/programs-surveys/popest/data/tables.html",
        data_url_template="https://www2.census.gov/programs-surveys/popest/datasets/2000-2009/counties/totals/co-est2009-alldata.csv",
        layout_url_template="https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2000-2009/co-est2009-alldata.pdf",
        release_status="active",
        decennial_base=2000,
        series_kind="postcensal",
        era="2000s",
        native_grain="050",
        required_columns=COUNTY_LAYOUT_COLUMNS,
        minimum_states=50,
        minimum_principal_rows=3000,
    ),
}


def _vintage_url(template: str, vintage_year: int) -> str:
    """Fill the vintage placeholder, leaving any partition placeholder alone.

    ``str.format`` would raise on a partitioned template, whose
    ``{partition}`` is filled per file at capture time rather than here.
    """
    return template.replace("{vintage}", str(vintage_year))


def _release(
    dataset_code: str,
    vintage_year: int,
    product_code: str,
    release_date: str,
    *,
    status: Literal["published", "archived"],
    observation_start_year: int,
    observation_end_year: int | None = None,
    geography_basis_date: str | None = None,
) -> PEPRelease:
    """One immutable release contract for a registered PEP product.

    The observation range is a property of the published file, so it is
    passed in rather than assumed: a postcensal release ends at its own
    vintage (the default), while an intercensal release closes a decade
    that ended before the vintage that published it.
    """
    dataset = _CURATED_DATASETS[dataset_code]
    return PEPRelease(
        dataset_code=dataset_code,
        vintage_year=vintage_year,
        product_code=product_code,
        data_url=_vintage_url(dataset.data_url_template, vintage_year),
        layout_url=_vintage_url(dataset.layout_url_template, vintage_year),
        release_date=release_date,
        observation_start_year=observation_start_year,
        observation_end_year=(
            vintage_year if observation_end_year is None else observation_end_year
        ),
        geography_basis_date=(
            f"{vintage_year}-01-01"
            if geography_basis_date is None
            else geography_basis_date
        ),
        schema_version=product_code.lower(),
        status=status,
        series_kind=dataset.series_kind,
        archive_member=dataset.archive_member,
        partitions=dataset.partitions,
    )


_CURATED_RELEASES = (
    # --- 2020s: the current decade, revised each vintage -------------------
    _release(
        "pep_nst_alldata",
        2024,
        "NST-EST2024-ALLDATA",
        "2024-12-19",
        status="archived",
        observation_start_year=2020,
    ),
    _release(
        "pep_nst_alldata",
        2025,
        "NST-EST2025-ALLDATA",
        "2026-01-27",
        status="published",
        observation_start_year=2020,
    ),
    _release(
        "pep_county_alldata",
        2024,
        "CO-EST2024-ALLDATA",
        "2025-03-13",
        status="archived",
        observation_start_year=2020,
    ),
    _release(
        "pep_county_alldata",
        2025,
        "CO-EST2025-ALLDATA",
        "2026-03-26",
        status="published",
        observation_start_year=2020,
    ),
    _release(
        "pep_subcounty",
        2024,
        "SUB-EST2024",
        "2025-05-15",
        status="archived",
        observation_start_year=2020,
    ),
    _release(
        "pep_subcounty",
        2025,
        "SUB-EST2025",
        "2026-05-14",
        status="published",
        observation_start_year=2020,
    ),
    # --- Closed decades ----------------------------------------------------
    # Each is the Bureau's final publication for its decade, so each product
    # carries exactly one release and that release is its own current one.
    #
    # `release_date` for an archival product is the date the file was
    # published at the URL this release registers, read from the server's
    # `Last-Modified`, unless the file states its own issue date (the 1970s
    # table does). The Bureau's original press date is not recoverable from
    # the artifact, and inventing one would put an unverifiable date on a
    # published contract. The dates still order the eras correctly, which is
    # what release precedence reads them for.
    _release(
        "pep_county_alldata_2010s",
        2020,
        "CO-EST2020-ALLDATA",
        "2021-05-04",
        status="published",
        observation_start_year=2010,
        geography_basis_date="2020-01-01",
    ),
    _release(
        "pep_nst_alldata_2010s",
        2020,
        "NST-EST2020-ALLDATA",
        "2021-05-04",
        status="published",
        observation_start_year=2010,
        geography_basis_date="2020-01-01",
    ),
    _release(
        "pep_county_alldata_2000s",
        2009,
        "CO-EST2009-ALLDATA",
        "2016-07-19",
        status="published",
        observation_start_year=2000,
        geography_basis_date="2009-01-01",
    ),
)

# Default config contains only nonsecret, import-safe release contracts.
CONFIG = PEPConfig(datasets=_CURATED_DATASETS, releases=_CURATED_RELEASES)
