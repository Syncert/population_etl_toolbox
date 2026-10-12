"""Query service for IRS SOI county-to-county migration flows.

Reads only ``gold_irs_migration.flow_latest`` (ADR-0008). A county's rows
leave this service as published: the flows ranked by the chosen measure,
the file's own totals, and SOI's categories with a deleted one visibly
withheld. Nothing here computes a net figure or redistributes a category.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.schemas.migration import MigrationFlowsResponse, MigrationRow
from apps.api.services.neutral_observations_service import NeutralQueryError
from data_ingestion_toolbox.irs_migration.registry import (
    CATEGORY_LABELS,
    MEASURES,
    TOTAL_CATEGORIES,
)

COUNTY_GEO_ID_PATTERN = re.compile(r"^state:\d{2}\|county:\d{3}$")
YEAR_PAIR_PATTERN = re.compile(r"^\d{4}-\d{4}$")
MEASURE_UNITS = {measure: unit for measure, _column, unit in MEASURES}

#: What the migration plan requires stated with the data.
FLOW_CAVEATS: tuple[str, ...] = (
    "Returns are tax filers whose address changed between the two filing "
    "years; individuals approximate people; AGI is the movers' adjusted "
    "gross income on the year-2 return, in thousands of dollars.",
    "Flows of fewer than 20 returns are aggregated by SOI into its Other "
    "flows categories; a category SOI deleted to protect taxpayers is "
    "withheld, not zero.",
    "These are tax filers, not the Census Bureau's population estimates, so "
    "they do not equal PEP net migration and no net figure is computed here.",
)


class MigrationNotPublishedError(LookupError):
    """No published SOI file covers the requested county, direction and years."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


_LATEST_YEAR_PAIR = text(
    """
    SELECT MAX(year_pair) FROM gold_irs_migration.flow_latest
    WHERE subject_geo_id = :geo_id AND direction = :direction
    """
)

_ROWS = text(
    """
    SELECT flow.category, flow.counterpart_label, flow.counterpart_code,
           flow.origin_geo_id, flow.destination_geo_id,
           CASE WHEN flow.direction = 'inflow' THEN flow.origin_geo_id ELSE flow.destination_geo_id END
               AS counterpart_geo_id,
           flow.returns, flow.individuals, flow.agi::TEXT AS agi,
           flow.value_status, flow.value_source, flow.period_start::TEXT AS period_start,
           flow.period_end::TEXT AS period_end, flow.release_key
    FROM gold_irs_migration.flow_latest AS flow
    WHERE flow.subject_geo_id = :geo_id AND flow.direction = :direction AND flow.year_pair = :year_pair
    """
)


def _row(record: Any) -> MigrationRow:
    category = record["category"]
    counterpart = (
        record["counterpart_geo_id"] if category in {"county", "non_migrants"} else None
    )
    return MigrationRow(
        category=category,
        category_label=CATEGORY_LABELS.get(category, category),
        counterpart_geo_id=counterpart,
        counterpart_name=record["counterpart_label"] if category == "county" else None,
        origin_geo_id=record["origin_geo_id"],
        destination_geo_id=record["destination_geo_id"],
        returns=record["returns"],
        individuals=record["individuals"],
        agi=record["agi"],
        value_status=record["value_status"],
        value_source=record["value_source"],
    )


def list_migration_flows(
    db: Session,
    *,
    geo_id: str,
    direction: str,
    year_pair: Optional[str] = None,
    measure: str = "returns",
    limit: int = 25,
) -> MigrationFlowsResponse:
    """One county's published flows, totals and categories for one pair of years."""
    geo_id = geo_id.strip()
    if COUNTY_GEO_ID_PATTERN.fullmatch(geo_id) is None:
        raise NeutralQueryError(
            "geo_id must be a county, of the form state:SS|county:CCC"
        )
    if year_pair is not None and YEAR_PAIR_PATTERN.fullmatch(year_pair) is None:
        raise NeutralQueryError(
            "year_pair must be two filing years, of the form 2022-2023"
        )
    chosen = (
        year_pair
        or db.execute(
            _LATEST_YEAR_PAIR, {"geo_id": geo_id, "direction": direction}
        ).scalar()
    )
    if not chosen:
        raise MigrationNotPublishedError(
            f"No published SOI {direction} file covers {geo_id}."
        )
    records = (
        db.execute(
            _ROWS, {"geo_id": geo_id, "direction": direction, "year_pair": chosen}
        )
        .mappings()
        .all()
    )
    if not records:
        raise MigrationNotPublishedError(
            f"No published SOI {direction} file for {chosen} covers {geo_id}."
        )
    order = {category: index for index, category in enumerate(TOTAL_CATEGORIES)}
    totals = sorted(
        (r for r in records if r["category"] in order),
        key=lambda r: order[r["category"]],
    )
    categories = sorted(
        (
            r
            for r in records
            if r["category"] not in order and r["category"] != "county"
        ),
        key=lambda r: r["counterpart_code"],
    )
    flows = [r for r in records if r["category"] == "county"]

    def rank(record: Any) -> tuple:
        value = record[measure]
        number = float(value) if value is not None else float("-inf")
        return (-number, record["counterpart_geo_id"])

    flows.sort(key=rank)
    first = records[0]
    return MigrationFlowsResponse(
        geo_id=geo_id,
        direction=direction,
        year_pair=chosen,
        period_start=first["period_start"],
        period_end=first["period_end"],
        measure=measure,
        unit=MEASURE_UNITS[measure],
        release=first["release_key"],
        caveats=list(FLOW_CAVEATS),
        totals=[_row(r) for r in totals],
        categories=[_row(r) for r in categories],
        total=len(flows),
        limit=limit,
        items=[_row(r) for r in flows[:limit]],
    )
