"""Versioned CDC Open Data asset contracts.

The registry is deliberately explicit. A CDC publisher identity alone does not
make two products comparable, so each asset freezes its own source columns,
paging key, release semantics, geography basis, and methodology.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CdcColumn:
    """One consumed Socrata field and its provider-declared type."""

    name: str
    data_type: str


@dataclass(frozen=True)
class CdcAsset:
    """Complete source contract for one supported CDC dataset."""

    asset_id: str
    socrata_id: str
    label: str
    parser_contract_version: str
    expected_columns: tuple[CdcColumn, ...]
    stable_order: tuple[str, ...]
    source_key: tuple[str, ...]
    geography_levels: tuple[str, ...]
    geography_basis: str
    release_field: str
    update_cadence: str
    methodology_url: str
    estimate_method: str
    population_basis: str
    enabled: bool = True
    media_type: str = "application/json"

    @property
    def api_path(self) -> str:
        return f"/resource/{self.socrata_id}.json"

    @property
    def metadata_path(self) -> str:
        return f"/api/views/{self.socrata_id}"

    @property
    def select_columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.expected_columns)


def _columns(*definitions: tuple[str, str]) -> tuple[CdcColumn, ...]:
    return tuple(CdcColumn(name, data_type) for name, data_type in definitions)


CDI_COLUMNS = _columns(
    ("yearstart", "number"),
    ("yearend", "number"),
    ("locationabbr", "text"),
    ("locationdesc", "text"),
    ("datasource", "text"),
    ("topic", "text"),
    ("question", "text"),
    ("response", "text"),
    ("datavalueunit", "text"),
    ("datavaluetype", "text"),
    ("datavalue", "number"),
    ("datavaluealt", "number"),
    ("datavaluefootnotesymbol", "text"),
    ("datavaluefootnote", "text"),
    ("lowconfidencelimit", "number"),
    ("highconfidencelimit", "number"),
    ("stratificationcategory1", "text"),
    ("stratification1", "text"),
    ("stratificationcategory2", "text"),
    ("stratification2", "text"),
    ("stratificationcategory3", "text"),
    ("stratification3", "text"),
    ("locationid", "text"),
    ("topicid", "text"),
    ("questionid", "text"),
    ("responseid", "text"),
    ("datavaluetypeid", "text"),
    ("stratificationcategoryid1", "text"),
    ("stratificationid1", "text"),
    ("stratificationcategoryid2", "text"),
    ("stratificationid2", "text"),
    ("stratificationcategoryid3", "text"),
    ("stratificationid3", "text"),
)

PLACES_COUNTY_COLUMNS = _columns(
    ("year", "text"),
    ("stateabbr", "text"),
    ("statedesc", "text"),
    ("locationname", "text"),
    ("datasource", "text"),
    ("category", "text"),
    ("measure", "text"),
    ("data_value_unit", "text"),
    ("data_value_type", "text"),
    ("data_value", "number"),
    ("data_value_footnote_symbol", "text"),
    ("data_value_footnote", "text"),
    ("low_confidence_limit", "number"),
    ("high_confidence_limit", "number"),
    ("totalpopulation", "number"),
    ("totalpop18plus", "number"),
    ("locationid", "text"),
    ("categoryid", "text"),
    ("measureid", "text"),
    ("datavaluetypeid", "text"),
    ("short_question_text", "text"),
)


# Verified against the official CDC Socrata metadata endpoints on 2026-08-26.
CDI_ASSET = CdcAsset(
    asset_id="cdi",
    socrata_id="hksd-2xuw",
    label="U.S. Chronic Disease Indicators",
    parser_contract_version="cdi-soda-v1",
    expected_columns=CDI_COLUMNS,
    stable_order=(
        "yearstart",
        "yearend",
        "locationid",
        "questionid",
        "responseid",
        "datavaluetypeid",
        "stratificationcategoryid1",
        "stratificationid1",
        "stratificationcategoryid2",
        "stratificationid2",
        "stratificationcategoryid3",
        "stratificationid3",
        "datasource",
    ),
    source_key=(
        "yearstart",
        "yearend",
        "locationid",
        "questionid",
        "responseid",
        "datavaluetypeid",
        "stratificationcategoryid1",
        "stratificationid1",
        "stratificationcategoryid2",
        "stratificationid2",
        "stratificationcategoryid3",
        "stratificationid3",
        "datasource",
    ),
    geography_levels=("us", "state"),
    geography_basis="CDC location codes; US=59 and two-digit state/territory codes",
    release_field="rowsUpdatedAt",
    update_cadence="irregular; metadata checked weekly",
    methodology_url="https://www.cdc.gov/cdi/about/index.html",
    estimate_method="provider_published",
    population_basis="indicator-specific surveillance population",
)

PLACES_COUNTY_ASSET = CdcAsset(
    asset_id="places_county",
    socrata_id="swc5-untb",
    label="PLACES: Local Data for Better Health, County Data, 2025 release",
    parser_contract_version="places-county-2025-soda-v1",
    expected_columns=PLACES_COUNTY_COLUMNS,
    stable_order=("year", "locationid", "measureid", "datavaluetypeid"),
    source_key=("year", "locationid", "measureid", "datavaluetypeid"),
    geography_levels=("us", "county"),
    geography_basis="2020 Census counties and county equivalents",
    release_field="rowsUpdatedAt",
    update_cadence="annual; metadata checked weekly",
    methodology_url="https://www.cdc.gov/places/methodology/index.html",
    estimate_method="model_based_small_area_estimate",
    population_basis="adults age 18 years and older",
)

ALL_ASSETS: tuple[CdcAsset, ...] = (CDI_ASSET, PLACES_COUNTY_ASSET)


def enabled_assets() -> list[CdcAsset]:
    """Return enabled assets in deterministic registry order."""
    return [asset for asset in ALL_ASSETS if asset.enabled]


def get_asset(asset_id: str) -> CdcAsset:
    """Look up one asset by its stable internal identity."""
    for asset in ALL_ASSETS:
        if asset.asset_id == asset_id:
            return asset
    raise KeyError(f"unknown CDC asset: {asset_id!r}")


def asset_by_socrata_id(socrata_id: str) -> CdcAsset | None:
    """Return the asset with the exact Socrata identity, when registered."""
    for asset in ALL_ASSETS:
        if asset.socrata_id == socrata_id:
            return asset
    return None
