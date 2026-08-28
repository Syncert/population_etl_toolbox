"""Pure USDA NASS classification, geography, and period identity.

Quick Stats identity is multidimensional. A commodity is not ``commodity_desc``
alone, a statistic is not ``statisticcat_desc`` alone, and a "TOTAL" domain
category is an explicit source member rather than an absent one. These helpers
build stable surrogate identities from the exact provider fields only; they
never infer identity from a name and never interpret a value.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

from data_ingestion_toolbox.silver_ref.geography_contract import canonical_geo_id

from ..registry import COUNTY, NATIONAL, STATE, NassProduct

#: Geography classes this adapter models. Agricultural districts, regions,
#: watersheds, ZIP Codes, and congressional districts stay ``unsupported``: they
#: are retained as raw evidence and must never be coerced into a county.
GEO_TYPE_BY_AGG_LEVEL: dict[str, str] = {
    NATIONAL: "nation",
    STATE: "state",
    COUNTY: "county",
}

UNSUPPORTED_GEO_TYPE = "unsupported"

_STATE_CODE = re.compile(r"^\d{2}$")
_COUNTY_CODE = re.compile(r"^\d{3}$")
_WEEK_ENDING = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class CommodityIdentity:
    """The full Quick Stats commodity classification for one record."""

    commodity_sk: str
    sector_desc: str
    group_desc: str
    commodity_desc: str
    class_desc: str
    prodn_practice_desc: str
    util_practice_desc: str


@dataclass(frozen=True)
class StatisticIdentity:
    """One source-backed statistic identity and its declared semantics."""

    statistic_sk: str
    source_desc: str
    statisticcat_desc: str
    short_desc: str
    unit_desc: str
    freq_desc: str
    value_kind: str
    calculation_basis: str
    additive_behavior: str
    additive_behavior_known: bool


@dataclass(frozen=True)
class DomainIdentity:
    """One explicit domain/category member, including ``TOTAL``."""

    domain_sk: str
    domain_desc: str
    domaincat_desc: str


@dataclass(frozen=True)
class GeographyIdentity:
    """Resolved geography identity plus the retained source evidence."""

    geo_type: str
    geo_id: str | None
    geo_source_code: str
    agg_level_desc: str
    state_fips: str | None
    county_fips: str | None
    location_desc: str
    state_alpha: str
    state_name: str
    county_name: str
    asd_code: str
    region_desc: str
    watershed_code: str


@dataclass(frozen=True)
class PeriodIdentity:
    """Normalized reference period preserving every source period field."""

    year: int
    freq_desc: str
    begin_code: str
    end_code: str
    reference_period_desc: str
    week_ending: date | None


class NassIdentityError(ValueError):
    """A record cannot yield a source-faithful identity."""


def _text(row: Mapping[str, Any], field: str) -> str:
    value = row.get(field)
    if value is None:
        return ""
    return str(value).strip()


def _surrogate(*parts: str) -> str:
    canonical = json.dumps(list(parts), separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def commodity_identity(row: Mapping[str, Any]) -> CommodityIdentity:
    """Return the complete commodity classification, never reduced to a name."""
    parts = (
        _text(row, "sector_desc"),
        _text(row, "group_desc"),
        _text(row, "commodity_desc"),
        _text(row, "class_desc"),
        _text(row, "prodn_practice_desc"),
        _text(row, "util_practice_desc"),
    )
    if not parts[2]:
        raise NassIdentityError("record has no commodity_desc")
    return CommodityIdentity(_surrogate("commodity", *parts), *parts)


def statistic_identity(
    row: Mapping[str, Any],
    product: NassProduct,
) -> StatisticIdentity:
    """Return the statistic identity and the registry's declared semantics."""
    source_desc = _text(row, "source_desc")
    statisticcat_desc = _text(row, "statisticcat_desc")
    short_desc = _text(row, "short_desc")
    unit_desc = _text(row, "unit_desc")
    freq_desc = _text(row, "freq_desc")
    if not statisticcat_desc or not short_desc:
        raise NassIdentityError("record has no statistic classification")
    registered = product.statistic(statisticcat_desc)
    if registered is None:
        raise NassIdentityError(
            f"statistic {statisticcat_desc!r} is not registered for "
            f"product {product.product_id!r}"
        )
    if unit_desc not in registered.expected_units:
        raise NassIdentityError(
            f"unit {unit_desc!r} is not registered for statistic {statisticcat_desc!r}"
        )
    return StatisticIdentity(
        statistic_sk=_surrogate(
            "statistic",
            source_desc,
            statisticcat_desc,
            short_desc,
            unit_desc,
            freq_desc,
        ),
        source_desc=source_desc,
        statisticcat_desc=statisticcat_desc,
        short_desc=short_desc,
        unit_desc=unit_desc,
        freq_desc=freq_desc,
        value_kind=registered.value_kind,
        calculation_basis=registered.calculation_basis,
        additive_behavior=registered.additive_behavior,
        additive_behavior_known=registered.additive_behavior_known,
    )


def domain_identity(row: Mapping[str, Any]) -> DomainIdentity:
    """Return the explicit domain member, keeping ``TOTAL`` as a real value."""
    domain_desc = _text(row, "domain_desc")
    domaincat_desc = _text(row, "domaincat_desc")
    if not domain_desc:
        raise NassIdentityError("record has no domain_desc")
    return DomainIdentity(
        _surrogate("domain", domain_desc, domaincat_desc),
        domain_desc,
        domaincat_desc,
    )


def _state_code(row: Mapping[str, Any]) -> str | None:
    for field in ("state_fips_code", "state_ansi"):
        candidate = _text(row, field)
        if _STATE_CODE.fullmatch(candidate):
            return candidate
    return None


def _county_code(row: Mapping[str, Any]) -> str | None:
    for field in ("county_ansi", "county_code"):
        candidate = _text(row, field)
        if _COUNTY_CODE.fullmatch(candidate):
            return candidate
    return None


def geography_identity(row: Mapping[str, Any]) -> GeographyIdentity:
    """Resolve national/state/county identity from exact provider codes only."""
    agg_level_desc = _text(row, "agg_level_desc")
    evidence = {
        "agg_level_desc": agg_level_desc,
        "location_desc": _text(row, "location_desc"),
        "state_alpha": _text(row, "state_alpha"),
        "state_name": _text(row, "state_name"),
        "county_name": _text(row, "county_name"),
        "asd_code": _text(row, "asd_code"),
        "region_desc": _text(row, "region_desc"),
        "watershed_code": _text(row, "watershed_code"),
    }
    geo_type = GEO_TYPE_BY_AGG_LEVEL.get(agg_level_desc, UNSUPPORTED_GEO_TYPE)
    if geo_type == UNSUPPORTED_GEO_TYPE:
        return GeographyIdentity(
            UNSUPPORTED_GEO_TYPE,
            None,
            geo_source_code=evidence["location_desc"] or agg_level_desc,
            state_fips=None,
            county_fips=None,
            **evidence,
        )
    if geo_type == "nation":
        return GeographyIdentity(
            "nation",
            canonical_geo_id("nation"),
            geo_source_code="US",
            state_fips=None,
            county_fips=None,
            **evidence,
        )
    state_fips = _state_code(row)
    if state_fips is None:
        raise NassIdentityError("record has no exact state ANSI/FIPS code")
    if geo_type == "state":
        return GeographyIdentity(
            "state",
            canonical_geo_id("state", state_fips=state_fips),
            geo_source_code=state_fips,
            state_fips=state_fips,
            county_fips=None,
            **evidence,
        )
    county_fips = _county_code(row)
    if county_fips is None:
        raise NassIdentityError("record has no exact county ANSI/FIPS code")
    return GeographyIdentity(
        "county",
        canonical_geo_id("county", state_fips=state_fips, county_fips=county_fips),
        geo_source_code=f"{state_fips}{county_fips}",
        state_fips=state_fips,
        county_fips=county_fips,
        **evidence,
    )


def period_identity(row: Mapping[str, Any]) -> PeriodIdentity:
    """Normalize the reference period without discarding any source field."""
    raw_year = _text(row, "year")
    try:
        year = int(raw_year)
    except ValueError as exc:
        raise NassIdentityError(
            f"record has an unparseable year: {raw_year!r}"
        ) from exc
    if not 1800 <= year <= 2200:
        raise NassIdentityError(f"record year is outside the warehouse range: {year}")
    freq_desc = _text(row, "freq_desc")
    if not freq_desc:
        raise NassIdentityError("record has no freq_desc")
    week_ending_text = _text(row, "week_ending")
    week_ending: date | None = None
    if week_ending_text:
        if not _WEEK_ENDING.fullmatch(week_ending_text):
            raise NassIdentityError(
                f"record has an unparseable week_ending: {week_ending_text!r}"
            )
        week_ending = date.fromisoformat(week_ending_text)
    return PeriodIdentity(
        year=year,
        freq_desc=freq_desc,
        begin_code=_text(row, "begin_code"),
        end_code=_text(row, "end_code"),
        reference_period_desc=_text(row, "reference_period_desc"),
        week_ending=week_ending,
    )


def source_record_id(row: Mapping[str, Any]) -> str:
    """Return the stable identity of one observation at the full source grain."""
    return _surrogate(
        "observation",
        _text(row, "source_desc"),
        _text(row, "short_desc"),
        _text(row, "sector_desc"),
        _text(row, "group_desc"),
        _text(row, "commodity_desc"),
        _text(row, "class_desc"),
        _text(row, "prodn_practice_desc"),
        _text(row, "util_practice_desc"),
        _text(row, "statisticcat_desc"),
        _text(row, "unit_desc"),
        _text(row, "domain_desc"),
        _text(row, "domaincat_desc"),
        _text(row, "agg_level_desc"),
        _text(row, "location_desc"),
        _text(row, "state_fips_code"),
        _text(row, "county_ansi"),
        _text(row, "county_code"),
        _text(row, "asd_code"),
        _text(row, "region_desc"),
        _text(row, "watershed_code"),
        _text(row, "zip_5"),
        _text(row, "congr_district_code"),
        _text(row, "country_code"),
        _text(row, "year"),
        _text(row, "freq_desc"),
        _text(row, "begin_code"),
        _text(row, "end_code"),
        _text(row, "reference_period_desc"),
        _text(row, "week_ending"),
    )
