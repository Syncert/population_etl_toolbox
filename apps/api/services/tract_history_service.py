"""One Census tract's history on its 2020 boundaries (tract page, A-5/W-5).

Tracts are redrawn every ten years, so a tract code before 2020 can name a
different area than the same code after it. This service answers, for one
2020 tract, in one request:

* whether its code named the same area before 2020, from the Bureau's
  relationship files (``gold_census.tract_decade_change``): unchanged, split
  from a larger tract, merged from several, or redrawn from parts;
* the published ACS 5-year values on its own 2020 boundary, and before 2020
  either the published values (an unchanged tract) or the DERIVED comparable
  series of ``gold_census.acs_tract_comparable`` (ETL-103): earlier tracts'
  counts apportioned to this tract by the relationship files, each point
  marked as derived with its weight basis and source tracts.

Reads (the prevention list): gold relations only (``api_reader`` has no
grant on silver); every statement is scoped to the one tract by plain
equality (``geo_id = :geo_id`` or ``= ANY``), so it reaches the indexes on
``geo_id`` (the dimension under both views) and ``(metric_code, geo_id,
observation_date)`` on ``rpt_acs_observations``; the published read bounds
``observation_date`` from ``MAKE_DATE(first_vintage, 1, 1)`` so earlier
year partitions are pruned. Nothing reads a whole grain.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Mapping, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.schemas.tract_history import (
    TractBoundary,
    TractHistoryPoint,
    TractHistoryResponse,
    TractHistorySeries,
    TractPart,
)
from apps.api.services.contracts import require_relation
from data_ingestion_toolbox.sql.catalog_queries import (
    GEOGRAPHY_IDENTITY_QUERY,
    GEOGRAPHY_RELATION,
)

CHANGE_RELATION = "gold_census.tract_decade_change"
COMPARABLE_RELATION = "gold_census.acs_tract_comparable"
PUBLISHED_RELATION = "gold_census.rpt_acs_observations"

#: The tract vintage every value here is expressed on.
TARGET_VINTAGE = 2020
#: The decade the 2020 tracts were drawn from.
EARLIER_VINTAGE = 2010
#: The most measures one request reads.
MOST_METRIC_CODES = 24
#: A part covering less of this tract's land than this is a boundary nudge
#: (a street moved to the other side of a line), not a change of area.
SLIVER_SHARE = 0.01
#: A share at least this close to whole counts as the whole tract.
WHOLE_SHARE = 0.99

_ACS5_CODE = re.compile(r"^CENSUS_ACS:acs5:[A-Z0-9]+_[0-9]{3}[A-Z]?$")
_TRACT_CODE = re.compile(r"tract:(\d{6})$")

METHOD = (
    "Values from 2020 on are the Census Bureau's ACS 5-year estimates for this "
    "tract's 2020 boundary. Before 2020 the ACS published on 2010 tracts (2000 "
    "tracts for the 2009 release). Where the tract kept its boundary those "
    "published values are shown as they are. Where it was redrawn, a count is "
    "DERIVED: each earlier tract's published count is split by the Bureau's "
    "relationship files (share of land area from 2010; share of 2010 population "
    "from 2000) and the parts landing on this tract are summed, then rounded to "
    "a whole count. A median, rate or margin of error cannot be split that way, "
    "so none is derived. The series starts with the 2009 release (2005-2009); "
    "Census 2000 tract counts are not carried onto 2020 tracts."
)

REDRAWN_NOTE = (
    "Before 2020 this measure was published on different tract boundaries and "
    "it is not a count that can be apportioned, so its history here starts in 2020."
)

CHANGE_QUERY = text(
    f"""
    SELECT earlier_geo_id, later_geo_id, earlier_land_share, later_land_share
    FROM {CHANGE_RELATION}
    WHERE earlier_vintage = {EARLIER_VINTAGE}
      AND later_vintage = {TARGET_VINTAGE}
      AND later_geo_id = :geo_id
    """
)

ALSO_BECAME_QUERY = text(
    f"""
    SELECT earlier_geo_id, later_geo_id, earlier_land_share, later_land_share
    FROM {CHANGE_RELATION}
    WHERE earlier_vintage = {EARLIER_VINTAGE}
      AND earlier_geo_id = ANY(:earlier_geo_ids)
    """
)

COMPARABLE_QUERY = text(
    f"""
    SELECT metric_code, vintage_year, observation_date, comparable_value,
           comparable_status, weight_basis, source_geo_ids
    FROM {COMPARABLE_RELATION}
    WHERE geo_id = :geo_id
      AND metric_code = ANY(:metric_codes)
    """
)

PUBLISHED_QUERY = text(
    f"""
    SELECT metric_code, vintage_year, observation_date, estimate_value,
           margin_of_error, value_status
    FROM {PUBLISHED_RELATION}
    WHERE metric_code = ANY(:metric_codes)
      AND geo_id = :geo_id
      AND vintage_year >= :first_vintage
      AND observation_date >= :first_date
    """
)


class UnknownTract(LookupError):
    """The catalog serves no geography under this id."""


class TractHistoryRequestError(ValueError):
    """The request names something this route does not answer."""


def tract_name(geo_id: str) -> str:
    """``Census Tract 18.05`` from a tract id (the code is the number x 100)."""
    match = _TRACT_CODE.search(str(geo_id or ""))
    if not match:
        return str(geo_id or "")
    code = match.group(1)
    whole = int(code[:4])
    fraction = code[4:]
    return f"Census Tract {whole if fraction == '00' else f'{whole}.{fraction}'}"


def _number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _whole(value: Any) -> Optional[float]:
    """An apportioned count, to the whole unit it counts.

    The view sums estimates times land shares at full ``numeric`` precision
    (3332.3555077...); a count of people or homes is shown whole, as the
    Bureau publishes its own, and the decimals were never information.
    """
    number = _number(value)
    return None if number is None else float(round(number))


def _percent(share: Optional[float]) -> str:
    if share is None:
        return "part"
    rounded = round(share * 100)
    if rounded >= 100 and share < 1:
        return "nearly all"
    if rounded <= 0:
        return "under 1%"
    return f"{rounded}%"


def _names(parts: Sequence[TractPart]) -> str:
    names = [part.name for part in parts]
    if len(names) <= 1:
        return "".join(names)
    return ", ".join(names[:-1]) + " and " + names[-1]


def classify_boundary(
    geo_id: str,
    parts: Sequence[Mapping[str, Any]],
    also: Sequence[Mapping[str, Any]],
) -> TractBoundary:
    """How a 2020 tract relates to the 2010 tracts, in one plain sentence.

    ``parts`` are the relationship rows whose later tract is this one;
    ``also`` the rows whose earlier tract is one of those (for a split, the
    other 2020 tracts the same land became).
    """
    own = tract_name(geo_id)
    earlier = sorted(
        (
            TractPart(
                geo_id=str(row["earlier_geo_id"]),
                name=tract_name(str(row["earlier_geo_id"])),
                vintage=EARLIER_VINTAGE,
                share_of_that_tract=_number(row.get("earlier_land_share")),
                share_of_this_tract=_number(row.get("later_land_share")),
            )
            for row in parts
        ),
        key=lambda part: (-(part.share_of_this_tract or 0), part.geo_id),
    )
    significant = [
        part for part in earlier if (part.share_of_this_tract or 0) >= SLIVER_SHARE
    ]
    slivers = len(earlier) - len(significant)
    if not significant:
        return TractBoundary(
            status="not_recorded",
            sentence=(
                f"The Bureau's relationship file records no 2010 tract for {own}, "
                "so no earlier figures are shown for it."
            ),
            earlier=[],
            also_became=[],
            slivers_ignored=slivers,
        )
    significant_ids = {part.geo_id for part in significant}
    others: dict[str, TractPart] = {}
    for row in also:
        later = str(row["later_geo_id"])
        share = _number(row.get("earlier_land_share")) or 0
        if (
            later == geo_id
            or str(row["earlier_geo_id"]) not in significant_ids
            or share < SLIVER_SHARE
        ):
            continue
        part = others.get(later)
        if part is None or (part.share_of_that_tract or 0) < share:
            others[later] = TractPart(
                geo_id=later,
                name=tract_name(later),
                vintage=TARGET_VINTAGE,
                share_of_that_tract=share,
                share_of_this_tract=_number(row.get("later_land_share")),
            )
    also_became = sorted(
        others.values(), key=lambda part: (-(part.share_of_that_tract or 0), part.geo_id)
    )
    nudged = (
        " Boundary adjustments under 1% of its land are left aside."
        if slivers
        else ""
    )

    if len(significant) == 1:
        only = significant[0]
        whole_of_that = (only.share_of_that_tract or 0) >= WHOLE_SHARE
        whole_of_this = (only.share_of_this_tract or 0) >= WHOLE_SHARE
        if whole_of_that and whole_of_this and only.geo_id == geo_id:
            return TractBoundary(
                status="unchanged",
                sentence=(
                    f"{own} kept its boundary in 2020, so its figures from earlier "
                    f"releases describe the same area.{nudged}"
                ),
                earlier=significant,
                also_became=[],
                slivers_ignored=slivers,
            )
        if whole_of_this and not whole_of_that:
            became = also_became or []
            tail = (
                f" The rest became {_names(became)}." if became else ""
            )
            if only.geo_id == geo_id:
                sentence = (
                    f"In 2020 {own} kept its code but was split: it is now "
                    f"{_percent(only.share_of_that_tract)} of its earlier land.{tail} "
                    f"Earlier figures for the code describe the larger area, so the "
                    f"ones shown here are derived from them.{nudged}"
                )
            else:
                sentence = (
                    f"In 2020 {only.name} was split, and {own} is "
                    f"{_percent(only.share_of_that_tract)} of its land.{tail} Earlier "
                    f"figures for {own} are therefore derived from {only.name}'s."
                    f"{nudged}"
                )
            return TractBoundary(
                status="split",
                sentence=sentence,
                earlier=significant,
                also_became=also_became,
                slivers_ignored=slivers,
            )
    if all((part.share_of_that_tract or 0) >= WHOLE_SHARE for part in significant):
        return TractBoundary(
            status="merged",
            sentence=(
                f"In 2020 {own} was formed by joining {_names(significant)}. Earlier "
                f"figures are their counts added together.{nudged}"
            ),
            earlier=significant,
            also_became=[],
            slivers_ignored=slivers,
        )
    return TractBoundary(
        status="redrawn",
        sentence=(
            f"In 2020 {own} was drawn from parts of {_names(significant)}. Earlier "
            f"figures are derived from theirs by the share of land each gave it."
            f"{nudged}"
        ),
        earlier=significant,
        also_became=also_became,
        slivers_ignored=slivers,
    )


def parse_metric_codes(raw: str) -> list[str]:
    codes: list[str] = []
    for code in (raw or "").split(","):
        code = code.strip()
        if code and code not in codes:
            codes.append(code)
    if not codes:
        raise TractHistoryRequestError("metric_codes must name at least one measure")
    if len(codes) > MOST_METRIC_CODES:
        raise TractHistoryRequestError(
            f"metric_codes names {len(codes)} measures; at most {MOST_METRIC_CODES}"
        )
    refused = [code for code in codes if not _ACS5_CODE.match(code)]
    if refused:
        raise TractHistoryRequestError(
            "tract history is published for ACS 5-year measures only; refused: "
            + ", ".join(refused)
        )
    return codes


def build_series(
    metric_codes: Sequence[str],
    boundary: TractBoundary,
    published: Sequence[Mapping[str, Any]],
    comparable: Sequence[Mapping[str, Any]],
) -> list[TractHistorySeries]:
    """Join the published and derived points per measure, oldest first.

    A published value always wins over a comparable one for the same year:
    for an unchanged tract both describe the same area and only the
    published one carries its margin of error.
    """
    unchanged = boundary.status == "unchanged"
    by_code: dict[str, dict[int, TractHistoryPoint]] = {code: {} for code in metric_codes}
    for row in published:
        code = str(row["metric_code"])
        if code not in by_code:
            continue
        vintage = int(row["vintage_year"])
        status = "published" if vintage >= TARGET_VINTAGE else "unchanged"
        if status == "unchanged" and not unchanged:
            continue
        by_code[code][vintage] = TractHistoryPoint(
            vintage_year=vintage,
            observation_date=row["observation_date"],
            value=_number(row.get("estimate_value")),
            margin_of_error=_number(row.get("margin_of_error")),
            status=status,
            derived=False,
        )
    for row in comparable:
        code = str(row["metric_code"])
        vintage = int(row["vintage_year"])
        if code not in by_code or vintage >= TARGET_VINTAGE or vintage in by_code[code]:
            continue
        raw_status = str(row.get("comparable_status") or "incomplete")
        status = raw_status if raw_status in {"unchanged", "apportioned"} else "incomplete"
        by_code[code][vintage] = TractHistoryPoint(
            vintage_year=vintage,
            observation_date=row["observation_date"],
            value=None if status == "incomplete" else _whole(row.get("comparable_value")),
            margin_of_error=None,
            status=status,
            derived=status != "unchanged",
            weight_basis=row.get("weight_basis"),
            source_geo_ids=[str(item) for item in (row.get("source_geo_ids") or [])],
        )
    series: list[TractHistorySeries] = []
    for code in metric_codes:
        points = [by_code[code][year] for year in sorted(by_code[code])]
        earlier = any(point.vintage_year < TARGET_VINTAGE for point in points)
        note = None
        if not earlier and boundary.status not in {"unchanged", "not_recorded"}:
            note = REDRAWN_NOTE
        series.append(TractHistorySeries(metric_code=code, note=note, items=points))
    return series


def tract_history(db: Session, geo_id: str, metric_codes_raw: str) -> TractHistoryResponse:
    metric_codes = parse_metric_codes(metric_codes_raw)
    require_relation(db, GEOGRAPHY_RELATION)
    identity = (
        db.execute(GEOGRAPHY_IDENTITY_QUERY, {"geo_ids": [geo_id]}).mappings().first()
    )
    if identity is None:
        raise UnknownTract(geo_id)
    if str(identity.get("geo_level") or "").upper() != "TRACT":
        raise TractHistoryRequestError("geo_id is not a Census tract")

    require_relation(db, CHANGE_RELATION)
    parts = db.execute(CHANGE_QUERY, {"geo_id": geo_id}).mappings().all()
    earlier_ids = sorted({str(row["earlier_geo_id"]) for row in parts})
    also = (
        db.execute(ALSO_BECAME_QUERY, {"earlier_geo_ids": earlier_ids}).mappings().all()
        if earlier_ids
        else []
    )
    boundary = classify_boundary(geo_id, parts, also)

    first_vintage = EARLIER_VINTAGE if boundary.status == "unchanged" else TARGET_VINTAGE
    require_relation(db, PUBLISHED_RELATION)
    published = (
        db.execute(
            PUBLISHED_QUERY,
            {
                "metric_codes": metric_codes,
                "geo_id": geo_id,
                "first_vintage": first_vintage,
                "first_date": date(first_vintage, 1, 1),
            },
        )
        .mappings()
        .all()
    )
    comparable: Sequence[Mapping[str, Any]] = []
    if boundary.status not in {"unchanged", "not_recorded"}:
        require_relation(db, COMPARABLE_RELATION)
        comparable = (
            db.execute(
                COMPARABLE_QUERY, {"geo_id": geo_id, "metric_codes": metric_codes}
            )
            .mappings()
            .all()
        )
    return TractHistoryResponse(
        geo_id=geo_id,
        geo_name=identity.get("geo_name"),
        method=METHOD,
        boundary=boundary,
        series=build_series(metric_codes, boundary, published, comparable),
    )
