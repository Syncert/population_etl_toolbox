"""Versioned USDA NASS Quick Stats product and slice contracts.

Quick Stats exposes one very large multidimensional result space. Production
ingestion is therefore allowlist driven: a request may only be generated from a
registered product, and every registered product freezes its own source
selections, geography scope, year range, expected units, suppression symbols,
request partitioning, and parser contract version.

Nothing in this module performs I/O, and nothing here holds a credential.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

#: Registered aggregate levels. Quick Stats also publishes agricultural
#: districts, regions, watersheds, and ZIP Codes; those are deliberately out of
#: scope and must never be coerced into national/state/county identity.
NATIONAL = "NATIONAL"
STATE = "STATE"
COUNTY = "COUNTY"
SUPPORTED_AGG_LEVELS: tuple[str, ...] = (NATIONAL, STATE, COUNTY)

#: Slice selection modes. ``recent`` retrieves the bounded recent window used by
#: ordinary operation; ``full`` sweeps the whole registered history for periodic
#: reconciliation and for a fresh historical bootstrap.
SliceMode = Literal["recent", "full"]

#: Exact provider record fields consumed by this adapter. Quick Stats returns
#: every key on every record, so a missing key is a contract change rather than
#: an absent value, and the row is quarantined instead of parsed.
QUICK_STATS_FIELDS: tuple[str, ...] = (
    # What
    "source_desc",
    "sector_desc",
    "group_desc",
    "commodity_desc",
    "class_desc",
    "prodn_practice_desc",
    "util_practice_desc",
    "statisticcat_desc",
    "unit_desc",
    "short_desc",
    "domain_desc",
    "domaincat_desc",
    # Where
    "agg_level_desc",
    "state_ansi",
    "state_fips_code",
    "state_alpha",
    "state_name",
    "asd_code",
    "asd_desc",
    "county_ansi",
    "county_code",
    "county_name",
    "region_desc",
    "zip_5",
    "watershed_code",
    "watershed_desc",
    "congr_district_code",
    "country_code",
    "country_name",
    "location_desc",
    # When
    "year",
    "freq_desc",
    "begin_code",
    "end_code",
    "reference_period_desc",
    "week_ending",
    "load_time",
    # Value
    "Value",
    "CV (%)",
)

#: Provider-published non-numeric markers that may appear in ``Value`` or
#: ``CV (%)``. They are retained verbatim and are never coerced to zero.
#: Definitions are recorded in ``tests/fixtures/usda_nass/SOURCE_NOTES.md``.
SUPPRESSION_SYMBOLS: tuple[str, ...] = (
    "(D)",
    "(S)",
    "(X)",
    "(Z)",
    "(NA)",
    "(H)",
    "(L)",
)


@dataclass(frozen=True)
class NassStatistic:
    """One registered statistic selection and its source-backed semantics."""

    statisticcat_desc: str
    expected_units: tuple[str, ...]
    value_kind: str
    calculation_basis: str
    additive_behavior: str

    @property
    def additive_behavior_known(self) -> bool:
        """Return True only when the source establishes additive behavior."""
        return self.additive_behavior != "not_established"


@dataclass(frozen=True)
class NassProduct:
    """Complete source contract for one registered Quick Stats crop product."""

    product_id: str
    label: str
    source_desc: str
    sector_desc: str
    group_desc: str
    commodity_desc: str
    statistics: tuple[NassStatistic, ...]
    agg_level_descs: tuple[str, ...]
    freq_descs: tuple[str, ...]
    domain_desc: str
    year_start: int
    year_end: int
    recent_year_window: int
    partition_fields: tuple[str, ...]
    parser_contract_version: str
    incremental_field: str
    release_expectation: str
    methodology_url: str
    enabled: bool = True
    media_type: str = "application/json"

    def __post_init__(self) -> None:
        if not self.statistics:
            raise ValueError(f"registered product {self.product_id!r} has no statistic")
        if self.year_end < self.year_start:
            raise ValueError(
                f"registered product {self.product_id!r} has a reversed year range"
            )
        if self.recent_year_window < 1:
            raise ValueError(
                f"registered product {self.product_id!r} needs a positive window"
            )
        unsupported = [
            level for level in self.agg_level_descs if level not in SUPPORTED_AGG_LEVELS
        ]
        if unsupported:
            raise ValueError(
                f"registered product {self.product_id!r} selects unsupported "
                f"aggregate levels: {unsupported}"
            )
        if self.source_desc not in {"SURVEY", "CENSUS"}:
            raise ValueError(
                f"registered product {self.product_id!r} has an unknown source_desc"
            )

    @property
    def statisticcat_descs(self) -> tuple[str, ...]:
        return tuple(statistic.statisticcat_desc for statistic in self.statistics)

    @property
    def expected_units(self) -> tuple[str, ...]:
        seen: list[str] = []
        for statistic in self.statistics:
            for unit in statistic.expected_units:
                if unit not in seen:
                    seen.append(unit)
        return tuple(seen)

    @property
    def suppression_symbols(self) -> tuple[str, ...]:
        return SUPPRESSION_SYMBOLS

    def statistic(self, statisticcat_desc: str) -> NassStatistic | None:
        for statistic in self.statistics:
            if statistic.statisticcat_desc == statisticcat_desc:
                return statistic
        return None

    def years(self, mode: SliceMode = "full") -> tuple[int, ...]:
        """Return the registered years for one selection mode."""
        if mode == "full":
            return tuple(range(self.year_start, self.year_end + 1))
        if mode == "recent":
            first = max(self.year_start, self.year_end - self.recent_year_window + 1)
            return tuple(range(first, self.year_end + 1))
        raise ValueError(f"unknown slice mode: {mode!r}")


@dataclass(frozen=True)
class NassSlice:
    """One deterministic, bounded request partition of a registered product."""

    product_id: str
    agg_level_desc: str
    year: int

    @property
    def slice_key(self) -> str:
        """Return the stable control-plane identity for this partition."""
        return f"{self.product_id}|{self.agg_level_desc}|{self.year}"


def _statistic(
    statisticcat_desc: str,
    *,
    units: tuple[str, ...],
    value_kind: str,
    calculation_basis: str,
    additive_behavior: str,
) -> NassStatistic:
    return NassStatistic(
        statisticcat_desc=statisticcat_desc,
        expected_units=units,
        value_kind=value_kind,
        calculation_basis=calculation_basis,
        additive_behavior=additive_behavior,
    )


# Grain measures share one statistic contract across corn, soybeans, and wheat.
_GRAIN_STATISTICS: tuple[NassStatistic, ...] = (
    _statistic(
        "AREA PLANTED",
        units=("ACRES",),
        value_kind="area",
        calculation_basis="provider_published_estimate",
        additive_behavior="not_established",
    ),
    _statistic(
        "AREA HARVESTED",
        units=("ACRES",),
        value_kind="area",
        calculation_basis="provider_published_estimate",
        additive_behavior="not_established",
    ),
    _statistic(
        "YIELD",
        units=("BU / ACRE",),
        value_kind="rate",
        calculation_basis="provider_published_ratio",
        additive_behavior="non_additive",
    ),
    _statistic(
        "PRODUCTION",
        units=("BU",),
        value_kind="quantity",
        calculation_basis="provider_published_estimate",
        additive_behavior="not_established",
    ),
)

_HAY_STATISTICS: tuple[NassStatistic, ...] = (
    _statistic(
        "AREA HARVESTED",
        units=("ACRES",),
        value_kind="area",
        calculation_basis="provider_published_estimate",
        additive_behavior="not_established",
    ),
    _statistic(
        "YIELD",
        units=("TONS / ACRE",),
        value_kind="rate",
        calculation_basis="provider_published_ratio",
        additive_behavior="non_additive",
    ),
    _statistic(
        "PRODUCTION",
        units=("TONS",),
        value_kind="quantity",
        calculation_basis="provider_published_estimate",
        additive_behavior="not_established",
    ),
)

#: Reviewed bounded history. The window is frozen so a fresh bootstrap and a
#: reconciliation sweep reproduce exactly the same slices; widening it is a
#: registry change with its own review. Extended to the warehouse-wide 1990
#: floor on 2026-08-29; Quick Stats serves county survey estimates well before
#: then, so every registered year returns provider data.
REGISTERED_YEAR_START = 1990
REGISTERED_YEAR_END = 2024

_METHODOLOGY_URL = (
    "https://www.nass.usda.gov/Surveys/Guide_to_NASS_Surveys/Crops_Stocks/index.php"
)


def _crop_product(
    product_id: str,
    *,
    label: str,
    commodity_desc: str,
    statistics: tuple[NassStatistic, ...],
) -> NassProduct:
    return NassProduct(
        product_id=product_id,
        label=label,
        source_desc="SURVEY",
        sector_desc="CROPS",
        group_desc="FIELD CROPS",
        commodity_desc=commodity_desc,
        statistics=statistics,
        agg_level_descs=SUPPORTED_AGG_LEVELS,
        freq_descs=("ANNUAL",),
        domain_desc="TOTAL",
        year_start=REGISTERED_YEAR_START,
        year_end=REGISTERED_YEAR_END,
        recent_year_window=1,
        partition_fields=("agg_level_desc", "year"),
        parser_contract_version="quickstats-crop-v1",
        incremental_field="load_time",
        release_expectation="survey_estimates_revised_until_final",
        methodology_url=_METHODOLOGY_URL,
    )


CORN_PRODUCT = _crop_product(
    "corn_survey_annual",
    label="Corn survey acreage, yield, and production",
    commodity_desc="CORN",
    statistics=_GRAIN_STATISTICS,
)

SOYBEANS_PRODUCT = _crop_product(
    "soybeans_survey_annual",
    label="Soybeans survey acreage, yield, and production",
    commodity_desc="SOYBEANS",
    statistics=_GRAIN_STATISTICS,
)

WHEAT_PRODUCT = _crop_product(
    "wheat_survey_annual",
    label="Wheat survey acreage, yield, and production",
    commodity_desc="WHEAT",
    statistics=_GRAIN_STATISTICS,
)

HAY_PRODUCT = _crop_product(
    "hay_survey_annual",
    label="Hay survey acreage, yield, and production",
    commodity_desc="HAY",
    statistics=_HAY_STATISTICS,
)

#: Census of Agriculture years in the registered window. The Census is
#: periodic, so its registered range is a single reviewed census year rather
#: than the survey window; survey and census observations never merge.
CENSUS_YEAR = 2022

_CENSUS_METHODOLOGY_URL = (
    "https://www.nass.usda.gov/Surveys/Guide_to_NASS_Surveys/Census_of_Agriculture/"
)

CORN_CENSUS_PRODUCT = NassProduct(
    product_id="corn_census_county",
    label="Corn Census of Agriculture harvested acreage and production",
    source_desc="CENSUS",
    sector_desc="CROPS",
    group_desc="FIELD CROPS",
    commodity_desc="CORN",
    statistics=(
        _statistic(
            "AREA HARVESTED",
            units=("ACRES",),
            value_kind="area",
            calculation_basis="census_of_agriculture_enumeration",
            additive_behavior="not_established",
        ),
        _statistic(
            "PRODUCTION",
            units=("BU",),
            value_kind="quantity",
            calculation_basis="census_of_agriculture_enumeration",
            additive_behavior="not_established",
        ),
    ),
    agg_level_descs=SUPPORTED_AGG_LEVELS,
    freq_descs=("ANNUAL",),
    domain_desc="TOTAL",
    year_start=CENSUS_YEAR,
    year_end=CENSUS_YEAR,
    recent_year_window=1,
    partition_fields=("agg_level_desc", "year"),
    parser_contract_version="quickstats-crop-census-v1",
    incremental_field="load_time",
    release_expectation="census_periodic_final",
    methodology_url=_CENSUS_METHODOLOGY_URL,
)

ALL_PRODUCTS: tuple[NassProduct, ...] = (
    CORN_PRODUCT,
    SOYBEANS_PRODUCT,
    WHEAT_PRODUCT,
    HAY_PRODUCT,
    CORN_CENSUS_PRODUCT,
)


def enabled_products() -> list[NassProduct]:
    """Return enabled products in deterministic registry order."""
    return [product for product in ALL_PRODUCTS if product.enabled]


def get_product(product_id: str) -> NassProduct:
    """Look up one registered product by its stable internal identity."""
    for product in ALL_PRODUCTS:
        if product.product_id == product_id:
            return product
    raise KeyError(f"unknown USDA NASS product: {product_id!r}")


def iter_slices(product: NassProduct, *, mode: SliceMode = "full") -> list[NassSlice]:
    """Return every deterministic request partition for one product.

    Slices are ordered by aggregate level and then year so a replayed run,
    a reconciliation sweep, and a fresh bootstrap all issue the same requests in
    the same order.
    """
    return [
        NassSlice(product.product_id, agg_level_desc, year)
        for agg_level_desc in product.agg_level_descs
        for year in product.years(mode)
    ]


def slice_query_parameters(product: NassProduct, item: NassSlice) -> dict[str, object]:
    """Return the exact provider selections for one registered slice.

    The returned mapping is the captured, fingerprinted request identity. The
    API key is never a member: the transport adds it immediately before the
    request and it is dropped from everything durable.
    """
    if item.product_id != product.product_id:
        raise ValueError("slice does not belong to the supplied product")
    if item.agg_level_desc not in product.agg_level_descs:
        raise ValueError(
            f"aggregate level {item.agg_level_desc!r} is not registered for "
            f"product {product.product_id!r}"
        )
    if item.year not in product.years("full"):
        raise ValueError(
            f"year {item.year} is outside the registered range for "
            f"product {product.product_id!r}"
        )
    return {
        "source_desc": product.source_desc,
        "sector_desc": product.sector_desc,
        "group_desc": product.group_desc,
        "commodity_desc": product.commodity_desc,
        "statisticcat_desc": list(product.statisticcat_descs),
        "domain_desc": product.domain_desc,
        "freq_desc": list(product.freq_descs),
        "agg_level_desc": item.agg_level_desc,
        "year": str(item.year),
        "format": "JSON",
    }
