"""What stands out about a place: one measure at a time (API-165)."""

from typing import Literal, Optional

from pydantic import BaseModel


class DistinctiveMeasure(BaseModel):
    """One measure's within-parent percentile rank for one geography.

    Every field describes this one measure. Nothing here combines measures,
    and the response has no field that could: no score, no average, no
    overall rank.
    """

    metric_code: str
    metric_display_name: Optional[str] = None
    source_code: Optional[str] = None
    units: Optional[str] = None
    period_start: Optional[str] = None
    value: float
    siblings_with_value: int
    siblings_withheld: int
    siblings_missing: int
    siblings_below: int
    siblings_tied: int
    percentile_rank: float
    caveats: list[str]
    #: The observation request this rank was read from, reproducible.
    request: str


class NotRankedMeasure(BaseModel):
    metric_code: str
    reason: str


class DistinctiveResponse(BaseModel):
    derived: Literal[True] = True
    geo_id: str
    geo_level: Optional[str] = None
    #: The siblings' scope: ``state:<fips>`` for a county, ``us`` for a state.
    parent_scope: Optional[str] = None
    minimum_siblings: int
    method: str
    ranked: list[DistinctiveMeasure]
    not_ranked: list[NotRankedMeasure]
