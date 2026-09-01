"""Read-only USDA NASS crop query service over the gold publication views.

The Quick Stats grain is multidimensional, so the service exposes the whole
classification rather than a single opaque metric: commodity, statistic, unit,
source program, domain, geography, period, release, coefficient of variation,
and suppression all travel with every row. Filters are bound parameters; no
caller value is ever interpolated into SQL.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.schemas import (
    NassMeasureListResponse,
    NassMeasureRow,
    NassObservationListResponse,
    NassObservationRow,
    NassSeriesListResponse,
    NassSeriesRow,
    NassSourceNote,
    NassSourceNotesResponse,
)
from data_ingestion_toolbox.usda_nass.registry import (
    ALL_PRODUCTS,
    SUPPORTED_AGG_LEVELS,
    SUPPRESSION_SYMBOLS,
)
from data_ingestion_toolbox.usda_nass.silver_nass.values import SYMBOL_STATUS

#: As-released history and the newest validated release, as separate relations.
AS_RELEASED_RELATION = "gold_nass.crop_observation"
LATEST_RELATION = "gold_nass.latest_release_observation"

_OBSERVATION_COLUMNS = """
    product_id, product_label, release_watermark, source_desc, sector_desc,
    group_desc, commodity_desc, class_desc, prodn_practice_desc,
    util_practice_desc, statisticcat_desc, short_desc, unit_desc, freq_desc,
    value_kind, calculation_basis, additive_behavior, additive_behavior_known,
    domain_desc, domaincat_desc, geo_id, geo_type, geography_status,
    agg_level_desc, location_desc, state_fips, county_fips, year,
    reference_period_desc, week_ending, value_source, value, value_status,
    suppression_code, cv_source, cv_value, cv_status, cv_symbol, load_time,
    methodology_url, release_expectation, source_record_id
"""

#: Caller filter to the exact gold column it binds against. The mapping is
#: closed, so an unknown filter can never reach the query.
_EQUALITY_FILTERS: dict[str, str] = {
    "product_id": "product_id",
    "source_desc": "source_desc",
    "commodity_desc": "commodity_desc",
    "class_desc": "class_desc",
    "statisticcat_desc": "statisticcat_desc",
    "short_desc": "short_desc",
    "unit_desc": "unit_desc",
    "freq_desc": "freq_desc",
    "domain_desc": "domain_desc",
    "domaincat_desc": "domaincat_desc",
    "agg_level_desc": "agg_level_desc",
    "geo_id": "geo_id",
    "state_fips": "state_fips",
    "release_watermark": "release_watermark",
    "value_status": "value_status",
}


class NassQueryError(ValueError):
    """A caller filter cannot produce a well-defined USDA NASS query."""


@dataclass
class NassObservationFilters:
    """Bound, validated filter set for one USDA NASS observation query."""

    product_id: Optional[str] = None
    source_desc: Optional[str] = None
    commodity_desc: Optional[str] = None
    class_desc: Optional[str] = None
    statisticcat_desc: Optional[str] = None
    short_desc: Optional[str] = None
    unit_desc: Optional[str] = None
    freq_desc: Optional[str] = None
    domain_desc: Optional[str] = None
    domaincat_desc: Optional[str] = None
    agg_level_desc: Optional[str] = None
    geo_id: Optional[str] = None
    state_fips: Optional[str] = None
    release_watermark: Optional[str] = None
    value_status: Optional[str] = None
    year_start: Optional[int] = None
    year_end: Optional[int] = None
    latest_release_only: bool = False
    limit: int = 100
    offset: int = 0

    def __post_init__(self) -> None:
        if self.year_start is not None and self.year_end is not None:
            if self.year_start > self.year_end:
                raise NassQueryError(
                    "year_start must be less than or equal to year_end"
                )
        if self.agg_level_desc is not None and (
            self.agg_level_desc not in SUPPORTED_AGG_LEVELS
        ):
            raise NassQueryError(
                "agg_level_desc must be one of " + ", ".join(SUPPORTED_AGG_LEVELS)
            )
        if self.source_desc is not None and self.source_desc not in {
            "SURVEY",
            "CENSUS",
        }:
            raise NassQueryError("source_desc must be SURVEY or CENSUS")
        if self.release_watermark is not None and self.latest_release_only:
            raise NassQueryError("release_watermark and latest cannot be combined")

    def clauses(self) -> tuple[list[str], dict[str, Any]]:
        """Return the WHERE fragments and their bound parameters."""
        conditions: list[str] = []
        parameters: dict[str, Any] = {}
        for name, column in _EQUALITY_FILTERS.items():
            value = getattr(self, name)
            if value is not None:
                conditions.append(f"{column} = :{name}")
                parameters[name] = value
        if self.year_start is not None:
            conditions.append("year >= :year_start")
            parameters["year_start"] = self.year_start
        if self.year_end is not None:
            conditions.append("year <= :year_end")
            parameters["year_end"] = self.year_end
        return conditions, parameters

    @property
    def relation(self) -> str:
        return LATEST_RELATION if self.latest_release_only else AS_RELEASED_RELATION


@dataclass
class NassSeriesFilters:
    """Bound, validated filter set for one USDA NASS series query."""

    product_id: Optional[str] = None
    source_desc: Optional[str] = None
    commodity_desc: Optional[str] = None
    statisticcat_desc: Optional[str] = None
    unit_desc: Optional[str] = None
    domain_desc: Optional[str] = None
    agg_level_desc: Optional[str] = None
    geo_id: Optional[str] = None
    freq_desc: Optional[str] = None
    limit: int = 100
    offset: int = 0
    _columns: dict[str, str] = field(
        default_factory=lambda: {
            "product_id": "product_id",
            "source_desc": "source_desc",
            "commodity_desc": "commodity_desc",
            "statisticcat_desc": "statisticcat_desc",
            "unit_desc": "unit_desc",
            "domain_desc": "domain_desc",
            "agg_level_desc": "agg_level_desc",
            "geo_id": "geo_id",
            "freq_desc": "freq_desc",
        },
        repr=False,
    )

    def __post_init__(self) -> None:
        if self.agg_level_desc is not None and (
            self.agg_level_desc not in SUPPORTED_AGG_LEVELS
        ):
            raise NassQueryError(
                "agg_level_desc must be one of " + ", ".join(SUPPORTED_AGG_LEVELS)
            )

    def clauses(self) -> tuple[list[str], dict[str, Any]]:
        conditions: list[str] = []
        parameters: dict[str, Any] = {}
        for name, column in self._columns.items():
            value = getattr(self, name)
            if value is not None:
                conditions.append(f"{column} = :{name}")
                parameters[name] = value
        return conditions, parameters


def _where(conditions: list[str]) -> str:
    return f"WHERE {' AND '.join(conditions)}" if conditions else ""


def list_observations(
    session: Session,
    filters: NassObservationFilters,
) -> NassObservationListResponse:
    """Return source-transparent crop observations for one filter set."""
    conditions, parameters = filters.clauses()
    where = _where(conditions)
    total = (
        session.execute(
            text(f"SELECT COUNT(*) FROM {filters.relation} {where}"), parameters
        ).scalar()
        or 0
    )
    rows = (
        session.execute(
            text(
                f"SELECT {_OBSERVATION_COLUMNS} FROM {filters.relation} {where} "
                "ORDER BY product_id, release_watermark, short_desc, geo_id, year "
                "LIMIT :limit OFFSET :offset"
            ),
            {**parameters, "limit": filters.limit, "offset": filters.offset},
        )
        .mappings()
        .all()
    )
    return NassObservationListResponse(
        total=int(total),
        limit=filters.limit,
        offset=filters.offset,
        release_scope="latest" if filters.latest_release_only else "as_released",
        items=[NassObservationRow(**dict(row)) for row in rows],
    )


def list_series(
    session: Session,
    filters: NassSeriesFilters,
) -> NassSeriesListResponse:
    """Return stable series identities for one filter set."""
    conditions, parameters = filters.clauses()
    where = _where(conditions)
    total = (
        session.execute(
            text(f"SELECT COUNT(*) FROM gold_nass.crop_series {where}"), parameters
        ).scalar()
        or 0
    )
    rows = (
        session.execute(
            text(
                """
                SELECT series_id, product_id, source_desc, sector_desc,
                       group_desc, commodity_desc, class_desc,
                       prodn_practice_desc, util_practice_desc,
                       statisticcat_desc, short_desc, unit_desc, value_kind,
                       additive_behavior, additive_behavior_known, domain_desc,
                       domaincat_desc, geo_id, geo_type, agg_level_desc,
                       freq_desc, first_year, last_year, observation_count,
                       numeric_observation_count, non_numeric_observation_count,
                       latest_release_watermark
                FROM gold_nass.crop_series
                """
                + where
                + " ORDER BY product_id, short_desc, geo_id "
                "LIMIT :limit OFFSET :offset"
            ),
            {**parameters, "limit": filters.limit, "offset": filters.offset},
        )
        .mappings()
        .all()
    )
    return NassSeriesListResponse(
        total=int(total),
        limit=filters.limit,
        offset=filters.offset,
        items=[NassSeriesRow(**dict(row)) for row in rows],
    )


def list_measures(session: Session) -> NassMeasureListResponse:
    """Return the source-backed measure export with its exact units."""
    rows = (
        session.execute(
            text(
                """
                SELECT source_dataset, source_measure_code, display_name,
                       statisticcat_desc, unit, freq_desc, value_kind,
                       calculation_basis, additive_behavior,
                       additive_behavior_known, source_program, source_watermark,
                       methodology_url, schema_version
                FROM gold_nass.measure_export
                ORDER BY source_dataset, display_name
                """
            )
        )
        .mappings()
        .all()
    )
    return NassMeasureListResponse(
        total=len(rows), items=[NassMeasureRow(**dict(row)) for row in rows]
    )


def source_notes() -> NassSourceNotesResponse:
    """Return the source-backed reading notes every consumer needs.

    The notes are derived from the registry and the parser's symbol table, not
    written by hand, so they cannot drift away from the ingested contract.
    """
    notes = [
        NassSourceNote(
            topic="units",
            summary=(
                "Every observation carries its exact provider unit_desc. Acres, "
                "bushels, tons, dollars, percentages, and per-acre rates are "
                "distinct measures and must never be summed or compared without "
                "matching unit_desc."
            ),
            detail=", ".join(
                sorted(
                    {
                        unit
                        for product in ALL_PRODUCTS
                        for unit in product.expected_units
                    }
                )
            ),
        ),
        NassSourceNote(
            topic="suppression",
            summary=(
                "A non-numeric provider symbol is preserved verbatim in "
                "value_source with a typed value_status and a NULL value. A "
                "suppressed or unavailable value is never zero, and (Z) means "
                "the value rounds below the displayed unit rather than zero."
            ),
            detail="; ".join(
                f"{symbol} = {SYMBOL_STATUS[symbol]}" for symbol in SUPPRESSION_SYMBOLS
            ),
        ),
        NassSourceNote(
            topic="release_status",
            summary=(
                "Survey estimates are revised until final and the Census of "
                "Agriculture is periodic and enumerated. Releases are retained "
                "as published: request latest=true for the newest validated "
                "release or leave it unset for as-released history."
            ),
            detail="; ".join(
                f"{product.product_id} = {product.release_expectation}"
                for product in ALL_PRODUCTS
            ),
        ),
        NassSourceNote(
            topic="source_program",
            summary=(
                "source_desc separates SURVEY from CENSUS. The two programs "
                "have different methodologies and never merge, even where the "
                "data item label and period agree."
            ),
            detail="; ".join(
                f"{product.product_id} = {product.source_desc}"
                for product in ALL_PRODUCTS
            ),
        ),
        NassSourceNote(
            topic="county_coverage",
            summary=(
                "Only NATIONAL, STATE, and COUNTY aggregate levels are modeled. "
                "County coverage is incomplete by design: county rows are "
                "frequently withheld, and agricultural districts, regions, and "
                "watersheds are never treated as counties. geography_status "
                "reports whether a row resolved to canonical geography."
            ),
            detail=", ".join(SUPPORTED_AGG_LEVELS),
        ),
        NassSourceNote(
            topic="aggregation",
            summary=(
                "additive_behavior states only what the source establishes. "
                "Rate measures are explicitly non-additive; acreage and "
                "production carry not_established because suppression, "
                "coverage, and provider reconciliation can invalidate local "
                "sums. Nothing here may be summed across geographies without a "
                "separately reviewed derivation."
            ),
            detail="non_additive; not_established",
        ),
    ]
    return NassSourceNotesResponse(total=len(notes), items=notes)
