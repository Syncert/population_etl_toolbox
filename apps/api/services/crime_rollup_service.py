"""Query service for the derived FBI county crime roll-up.

The service reads only the declared-derived gold roll-up relations
(ETL-053). It applies no aggregation of its own: summation, multi-county
handling, coverage, and the no-zero rule are all fixed in the warehouse
view, and a row leaves this service exactly as published.

A county nothing maps to is refused explicitly rather than answered with
an empty page a consumer could not tell from "mapped but not reporting".
"""

from __future__ import annotations

import re
from typing import Optional

from sqlalchemy.orm import Session

from apps.api.schemas.crime_rollup import (
    CountyRollupListResponse,
    CountyRollupRow,
)
from apps.api.services.neutral_observations_service import NeutralQueryError
from data_ingestion_toolbox.sql.fbi_queries import (
    build_county_mapping_evidence_query,
    build_county_rollup_queries,
)

#: The one geography shape this resource serves.
COUNTY_GEO_ID_PATTERN = re.compile(r"^state:(\d{2})\|county:(\d{3})$")

#: Consequences the county-crime roll-up plan requires stated with the data.
ROLLUP_CAVEATS: tuple[str, ...] = (
    "A derived roll-up of agency-reported totals, not a provider-published "
    "county figure.",
    "An agency serving more than one county contributes its whole published "
    "count to each of its counties, so county values are not additive to "
    "state totals.",
    "Only reported agency months are summed; non-reporting mapped agencies "
    "stay visible through the coverage counts and are never counted as zero.",
    "No population-normalized rate is derived; the state program rate "
    "remains the only published crime rate.",
)


class CountyNotMappedError(LookupError):
    """No resolved agency-to-county mapping covers the requested county."""

    def __init__(self, geo_id: str, unresolved_count: int) -> None:
        self.geo_id = geo_id
        self.unresolved_count = unresolved_count
        detail = (
            f"No law-enforcement agency is mapped to {geo_id}; the roll-up "
            "publishes no value for an unmapped county."
        )
        if unresolved_count:
            detail += (
                f" {unresolved_count} provider county label(s) in this state "
                "remain unresolved or ambiguous and are never guessed into "
                "a county."
            )
        super().__init__(detail)
        self.detail = detail


def _validated_county_geo_id(geo_id: str) -> str:
    match = COUNTY_GEO_ID_PATTERN.fullmatch(geo_id.strip())
    if match is None:
        raise NeutralQueryError(
            "The county roll-up serves county geographies only; geo_id must "
            "be of the form state:SS|county:CCC."
        )
    return geo_id.strip()


def _require_mapped_county(db: Session, geo_id: str) -> None:
    state_geo_id = f"state:{COUNTY_GEO_ID_PATTERN.fullmatch(geo_id).group(1)}"
    row = (
        db.execute(
            build_county_mapping_evidence_query(),
            {"geo_id": geo_id, "state_geo_id": state_geo_id},
        )
        .mappings()
        .one()
    )
    if int(row["resolved_count"]) == 0:
        raise CountyNotMappedError(geo_id, int(row["unresolved_count"]))


def list_county_rollup(
    db: Session,
    *,
    product_id: Optional[str] = None,
    measure_id: Optional[str] = None,
    geo_id: Optional[str] = None,
    state_fips: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    release: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> CountyRollupListResponse:
    """Return one page of the derived county roll-up with an exact total."""
    if geo_id:
        geo_id = _validated_county_geo_id(geo_id)
        _require_mapped_county(db, geo_id)

    list_query, count_query, params = build_county_rollup_queries(
        product_id=product_id,
        measure_id=measure_id,
        geo_id=geo_id,
        state_fips=state_fips,
        year_from=year_from,
        year_to=year_to,
        release=release,
        limit=limit,
        offset=offset,
    )
    count_params = {
        name: value for name, value in params.items() if name not in {"limit", "offset"}
    }
    total = db.execute(count_query, count_params).scalar() or 0
    rows = db.execute(list_query, params).mappings().all()
    return CountyRollupListResponse(
        release_selection="single_release" if release else "latest_release",
        caveats=list(ROLLUP_CAVEATS),
        total=int(total),
        limit=limit,
        offset=offset,
        items=[
            CountyRollupRow.model_validate(
                {**dict(row), "contributing_oris": list(row["contributing_oris"])}
            )
            for row in rows
        ],
    )
