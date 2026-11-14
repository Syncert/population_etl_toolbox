"""Where public money goes, for one place (where-public-money-goes slice 5).

Each layer is one government level's spending (or federal awards recorded in
the place) for one year, as its source publishes it, with every line's share
of that layer's own total. The layers are separate figures: they are never
added together. Every value is text so provider precision survives JSON, and
null is missing, never zero: ``unavailable_reason`` says why.
"""

from typing import Literal, Optional

from pydantic import BaseModel

LayerKey = Literal[
    "federal_outlays",
    "state_local_spending",
    "federal_awards_by_type",
    "federal_awards_by_agency",
]


class SpendingLine(BaseModel):
    key: str
    label: str
    #: As stored, in the layer's unit.
    amount: Optional[str] = None
    #: Percent of the layer's total, computed in gold for the same year and place.
    share_pct: Optional[str] = None
    unavailable_reason: Optional[str] = None


class SpendingLayer(BaseModel):
    layer: LayerKey
    title: str
    #: Who spends or awards: "federal government", "state and local
    #: governments", "local governments in the county area", "federal agencies".
    spender: str
    source: str
    geo_id: str
    #: The fiscal year, dated by the calendar year it ends in.
    fiscal_year: Optional[int] = None
    period_label: Optional[str] = None
    unit: str
    total: Optional[str] = None
    total_label: str
    #: Lines ordered by amount, largest first; a line with no amount last.
    lines: list[SpendingLine]
    notes: list[str]
    unavailable_reason: Optional[str] = None


class WhereMoneyGoesResponse(BaseModel):
    geo_id: str
    geo_level: Literal["nation", "state", "county"]
    layers: list[SpendingLayer]
    caveats: list[str]
