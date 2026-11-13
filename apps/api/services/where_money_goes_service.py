"""Where public money goes, for one place (where-public-money-goes slice 5).

One read, scoped to one place, over three publication surfaces:

* the nation: federal outlays by budget function from the Monthly Treasury
  Statement's September statement (``gold_treasury_mts.outlays_by_function``);
* the nation and a state: state and local governments' direct general
  spending by Census function line (``gold_census_govfin.spending_mix``,
  ``asslgf``, every fiscal year); a county: the local governments in the
  county area (``cog_units``, Census of Governments years only);
* a state and a county: federal award obligations recorded there by award
  type group and by awarding agency (``gold_usaspending.awards_by_award_type``
  and ``awards_by_agency``), for the newest federal fiscal year that has
  ended.

Every amount and share is served as stored; the shares were computed in gold
against the layer's own total. The layers are separate figures and are never
added: federal awards recorded in a county are not what its local
governments spend.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.schemas.where_money_goes import (
    SpendingLayer,
    SpendingLine,
    WhereMoneyGoesResponse,
)
from apps.api.services.contracts import relation_is_absent
from apps.api.services.neutral_observations_service import NeutralQueryError

NATION = "us:1"
STATE_GEO_ID = re.compile(r"^state:\d{2}$")
COUNTY_GEO_ID = re.compile(r"^state:\d{2}\|county:\d{3}$")

NOT_LOADED = "not loaded into this site yet"
NOT_PUBLISHED = "not published for this place"

SEPARATE_FIGURES = (
    "These are separate figures from separate sources. Federal awards recorded in a "
    "place are not what its own governments spend, and the two are never added."
)
FISCAL_YEAR_NOTE = (
    "A fiscal year is shown as the calendar year it ends in. The federal fiscal year "
    "runs October 1 to September 30; 46 states end theirs on June 30."
)
PLACE_OF_PERFORMANCE_NOTE = (
    "Recorded where the awarded work is performed, which is not always where the money "
    "is felt: pass-through grants sit with the state agency that receives them, and some "
    "payments (Medicare) where the claims processor is."
)
AWARDS_ARE_NOT_ALL_SPENDING = (
    "Awards are obligations by transaction date; federal salaries and most interest "
    "are not awards."
)
UNATTRIBUTED_AGENCY_NOTE = (
    "USAspending lists some obligations with no awarding agency; they count in the "
    "place's total but in no agency, so the agency shares can add to less than 100%."
)
#: Census counts every local government only in Census of Governments years.
COUNTY_AREA_NOTE = (
    "Local governments in the county area: the county, its cities, towns, school and "
    "special districts, each counted whole in its Census home county. Census counts "
    "them only in years ending in 2 and 7."
)
STATE_LOCAL_NOTE = (
    "Direct general spending by the state government and all its local governments, "
    "by Census function line; utilities, liquor stores and insurance trust payments "
    "(pensions) are outside general spending."
)
TREASURY_NOTE = (
    "Outlays by budget function from the September statement's fiscal-year-to-date "
    "column, as Treasury publishes it. Some functions can be negative (offsetting "
    "receipts)."
)


def current_fiscal_year(today: date) -> int:
    """The federal fiscal year ``today`` falls in (October starts the next)."""
    return today.year + (1 if today.month >= 10 else 0)


def _order(lines: list[SpendingLine]) -> list[SpendingLine]:
    def amount(line: SpendingLine) -> Decimal:
        try:
            return (
                Decimal(line.amount)
                if line.amount is not None
                else Decimal("-Infinity")
            )
        except InvalidOperation:
            return Decimal("-Infinity")

    return sorted(lines, key=lambda line: (-amount(line), line.label))


def _absent(db: Session, *relations: str) -> bool:
    return any(relation_is_absent(db, relation) for relation in relations)


def _empty(layer: SpendingLayer, reason: str) -> SpendingLayer:
    return layer.model_copy(
        update={"lines": [], "total": None, "unavailable_reason": reason}
    )


def _federal_outlays(db: Session) -> SpendingLayer:
    layer = SpendingLayer(
        layer="federal_outlays",
        title="What the federal government spends",
        spender="federal government",
        source="U.S. Treasury, Monthly Treasury Statement, Table 9",
        geo_id=NATION,
        unit="US dollars",
        total_label="Total outlays",
        lines=[],
        notes=[TREASURY_NOTE, FISCAL_YEAR_NOTE],
    )
    if _absent(db, "gold_treasury_mts.outlays_by_function"):
        return _empty(layer, NOT_LOADED)
    rows = (
        db.execute(
            text(
                """
            WITH newest AS (
                SELECT MAX(fiscal_year) AS fiscal_year
                FROM gold_treasury_mts.outlays_by_function
                WHERE period_kind = 'fiscal_year'
            )
            SELECT part.fiscal_year, part.statement_date::TEXT AS statement_date,
                   part.metric_key, part.budget_function,
                   part.outlays_amount::TEXT AS amount,
                   part.total_outlays_amount::TEXT AS total,
                   part.share_of_total_outlays_pct::TEXT AS share_pct,
                   part.unavailable_reason
            FROM gold_treasury_mts.outlays_by_function AS part
            JOIN newest ON newest.fiscal_year = part.fiscal_year
            WHERE part.period_kind = 'fiscal_year'
            """
            )
        )
        .mappings()
        .all()
    )
    if not rows:
        return _empty(layer, NOT_LOADED)
    first = rows[0]
    return layer.model_copy(
        update={
            "fiscal_year": int(first["fiscal_year"]),
            "period_label": f"fiscal {first['fiscal_year']}, statement of {first['statement_date']}",
            "total": first["total"],
            "lines": _order(
                [
                    SpendingLine(
                        key=row["metric_key"],
                        label=row["budget_function"],
                        amount=row["amount"],
                        share_pct=row["share_pct"],
                        unavailable_reason=row["unavailable_reason"],
                    )
                    for row in rows
                ]
            ),
        }
    )


def _state_local(db: Session, geo_id: str) -> SpendingLayer:
    county = bool(COUNTY_GEO_ID.match(geo_id))
    dataset, level = ("cog_units", "local") if county else ("asslgf", "state_and_local")
    layer = SpendingLayer(
        layer="state_local_spending",
        title=(
            "What local governments in the county area spend"
            if county
            else "What state and local governments spend"
        ),
        spender="local governments in the county area"
        if county
        else "state and local governments",
        source=(
            "Census of Governments, finance Individual Unit Files"
            if county
            else "Census Annual Survey of State and Local Government Finances"
        ),
        geo_id=geo_id,
        unit="thousands of US dollars",
        total_label="Direct general spending",
        lines=[],
        notes=[COUNTY_AREA_NOTE if county else STATE_LOCAL_NOTE, FISCAL_YEAR_NOTE],
    )
    if _absent(
        db, "gold_census_govfin.spending_mix", "gold_census_govfin.observation_latest"
    ):
        return _empty(layer, NOT_LOADED)
    params = {"geo_id": geo_id, "dataset": dataset, "level": level}
    year = db.execute(
        text(
            """
        SELECT MAX(fiscal_year) FROM gold_census_govfin.observation_latest
        WHERE geo_id = :geo_id AND dataset_id = :dataset AND government_level = :level
          AND measure_id = 'spend_general_total' AND quarter = 0
          AND value_status = 'valid'
        """
        ),
        params,
    ).scalar()
    if year is None:
        return _empty(layer, NOT_PUBLISHED)
    rows = (
        db.execute(
            text(
                """
            SELECT measure_id, measure_label, spending_amount::TEXT AS amount,
                   general_spending_total::TEXT AS total,
                   share_of_general_spending_pct::TEXT AS share_pct,
                   unavailable_reason, government_count
            FROM gold_census_govfin.spending_mix
            WHERE geo_id = :geo_id AND dataset_id = :dataset AND government_level = :level
              AND fiscal_year = :year
            """
            ),
            {**params, "year": int(year)},
        )
        .mappings()
        .all()
    )
    if not rows:
        return _empty(layer, NOT_PUBLISHED)
    notes = list(layer.notes)
    counts = {
        row["government_count"] for row in rows if row["government_count"] is not None
    }
    if county and len(counts) == 1:
        notes.insert(
            1, f"{counts.pop():,} local governments reported in fiscal {year}."
        )
    return layer.model_copy(
        update={
            "fiscal_year": int(year),
            "period_label": f"fiscal year ending in {year}",
            "total": rows[0]["total"],
            "notes": notes,
            "lines": _order(
                [
                    SpendingLine(
                        key=row["measure_id"],
                        label=row["measure_label"],
                        amount=row["amount"],
                        share_pct=row["share_pct"],
                        unavailable_reason=row["unavailable_reason"],
                    )
                    for row in rows
                ]
            ),
        }
    )


def _award_years(
    db: Session, geo_id: str, today: date
) -> tuple[int | None, int | None]:
    """The fiscal year to serve for the place, and a newer ended one still being reported.

    A fiscal year is served once it ended more than a year ago: agencies keep
    reporting a year for months after September (Defense publishes 90 days
    behind), and shares read from half-reported lines mislead. The newest
    ended year, when it is newer than the served one, is only named.
    """
    current = current_fiscal_year(today)
    newest = db.execute(
        text(
            """
        SELECT MAX(year) FROM gold_usaspending.observation_latest
        WHERE geo_id = :geo_id AND year < :current
        """
        ),
        {"geo_id": geo_id, "current": current},
    ).scalar()
    served = db.execute(
        text(
            """
        SELECT MAX(year) FROM gold_usaspending.observation_latest
        WHERE geo_id = :geo_id AND year < :reported
        """
        ),
        {"geo_id": geo_id, "reported": current - 1},
    ).scalar()
    served_year = None if served is None else int(served)
    newest_year = None if newest is None else int(newest)
    newer = (
        newest_year
        if newest_year is not None
        and (served_year is None or newest_year > served_year)
        else None
    )
    return served_year, newer


def _awards(db: Session, geo_id: str, today: date) -> list[SpendingLayer]:
    common = {
        "spender": "federal agencies",
        "source": "USAspending.gov, spending by geography, place of performance",
        "geo_id": geo_id,
        "unit": "US dollars",
        "total_label": "All federal award obligations recorded here",
        "lines": [],
    }
    by_type = SpendingLayer(
        layer="federal_awards_by_type",
        title="Federal awards recorded here, by type",
        notes=[
            PLACE_OF_PERFORMANCE_NOTE,
            AWARDS_ARE_NOT_ALL_SPENDING,
            FISCAL_YEAR_NOTE,
        ],
        **common,
    )
    by_agency = SpendingLayer(
        layer="federal_awards_by_agency",
        title="Federal awards recorded here, by agency",
        notes=[PLACE_OF_PERFORMANCE_NOTE, UNATTRIBUTED_AGENCY_NOTE, FISCAL_YEAR_NOTE],
        **common,
    )
    if _absent(
        db,
        "gold_usaspending.observation_latest",
        "gold_usaspending.awards_by_award_type",
        "gold_usaspending.awards_by_agency",
    ):
        return [_empty(by_type, NOT_LOADED), _empty(by_agency, NOT_LOADED)]
    year, newer = _award_years(db, geo_id, today)
    if year is None:
        return [_empty(by_type, NOT_LOADED), _empty(by_agency, NOT_LOADED)]
    params = {"geo_id": geo_id, "year": year}
    # The place's all-award total, served whether or not a breakdown is.
    totals = (
        db.execute(
            text(
                """
            SELECT value::TEXT AS total FROM gold_usaspending.observation_latest
            WHERE geo_id = :geo_id AND metric_key = 'federal_award_obligations'
              AND year = :year
            """
            ),
            params,
        )
        .mappings()
        .all()
    )
    place_total = totals[0]["total"] if totals else None
    still = (
        [
            (
                f"Fiscal {newer} has ended, but agencies are still reporting it; "
                f"its figures are not shown until a year after it ends."
            )
        ]
        if newer is not None
        else []
    )
    type_rows = (
        db.execute(
            text(
                """
            SELECT award_group, measure_label, obligated_amount::TEXT AS amount,
                   place_total_obligated_amount::TEXT AS total,
                   share_of_place_total_pct::TEXT AS share_pct, unavailable_reason
            FROM gold_usaspending.awards_by_award_type
            WHERE geo_id = :geo_id AND fiscal_year = :year
            """
            ),
            params,
        )
        .mappings()
        .all()
    )
    agency_rows = (
        db.execute(
            text(
                """
            SELECT agency_code, agency_name, obligated_amount::TEXT AS amount,
                   place_total_obligated_amount::TEXT AS total,
                   share_of_place_total_pct::TEXT AS share_pct, unavailable_reason
            FROM gold_usaspending.awards_by_agency
            WHERE geo_id = :geo_id AND fiscal_year = :year
            """
            ),
            params,
        )
        .mappings()
        .all()
    )
    period = f"fiscal {year} (October 1, {year - 1} to September 30, {year})"

    def filled(layer: SpendingLayer, rows, key: str, label) -> SpendingLayer:
        if not rows:
            return _empty(layer, NOT_PUBLISHED).model_copy(
                update={
                    "fiscal_year": year,
                    "period_label": period,
                    "total": place_total,
                }
            )
        return layer.model_copy(
            update={
                "fiscal_year": year,
                "period_label": period,
                "total": place_total,
                "notes": still + list(layer.notes),
                "lines": _order(
                    [
                        SpendingLine(
                            key=row[key],
                            label=label(row),
                            amount=row["amount"],
                            share_pct=row["share_pct"],
                            unavailable_reason=row["unavailable_reason"],
                        )
                        for row in rows
                    ]
                ),
            }
        )

    return [
        filled(
            by_type,
            type_rows,
            "award_group",
            lambda row: _group_label(row["measure_label"]),
        ),
        filled(
            by_agency,
            agency_rows,
            "agency_code",
            lambda row: row["agency_name"] or row["agency_code"],
        ),
    ]


_GROUP_LABEL = re.compile(
    r"^Federal award obligations by place of performance, (.+) \(USAspending\)$"
)


def _group_label(measure_label: str) -> str:
    """ "contracts" from the measure's label, capitalised."""
    match = _GROUP_LABEL.match(measure_label or "")
    name = match.group(1) if match else measure_label
    return name[:1].upper() + name[1:]


def where_money_goes(
    db: Session, geo_id: str, *, today: Optional[date] = None
) -> WhereMoneyGoesResponse:
    if geo_id == NATION:
        level = "nation"
    elif STATE_GEO_ID.match(geo_id):
        level = "state"
    elif COUNTY_GEO_ID.match(geo_id):
        level = "county"
    else:
        raise NeutralQueryError(
            "where public money goes is served for the nation (us:1), a state (state:NN) "
            "or a county (state:NN|county:NNN)"
        )
    # Small index lookups over several views: keep Postgres from JIT-compiling
    # them (as the governor read). SET LOCAL ends with this read's transaction.
    db.execute(text("SET LOCAL jit = off"))
    as_of = today or datetime.now(UTC).date()
    layers: list[SpendingLayer] = []
    if level == "nation":
        layers.append(_federal_outlays(db))
    layers.append(_state_local(db, geo_id))
    if level != "nation":
        layers.extend(_awards(db, geo_id, as_of))
    return WhereMoneyGoesResponse(
        geo_id=geo_id,
        geo_level=level,
        layers=layers,
        caveats=[
            SEPARATE_FIGURES,
            "Each share is of its own layer's total, for the same year and place.",
        ],
    )
