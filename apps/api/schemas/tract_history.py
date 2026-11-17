"""One Census tract's history on its 2020 boundaries (tract page, W-5/A-5)."""

from __future__ import annotations

from datetime import date
from typing import Literal, Optional

from pydantic import BaseModel, Field

BoundaryStatus = Literal["unchanged", "split", "merged", "redrawn", "not_recorded"]
PointStatus = Literal["published", "unchanged", "apportioned", "incomplete"]


class TractPart(BaseModel):
    """One part of the relationship between this tract and another decade's."""

    geo_id: str
    #: "Census Tract 18.05", read from the tract code in ``geo_id``.
    name: str
    vintage: int
    #: How much of that tract's land lies in this one (0 to 1).
    share_of_that_tract: Optional[float] = None
    #: How much of this tract's land that tract covered (0 to 1).
    share_of_this_tract: Optional[float] = None


class TractBoundary(BaseModel):
    """Whether the tract's code named the same area before the 2020 redraw."""

    status: BoundaryStatus
    #: One plain sentence a reader can take as it is.
    sentence: str
    #: The 2010 tracts this tract's land was in, largest share first.
    earlier: list[TractPart]
    #: The other 2020 tracts those 2010 tracts became, for a split.
    also_became: list[TractPart]
    #: Parts under 1 percent of this tract's land, a boundary nudge rather
    #: than a change of area, left out of ``earlier``.
    slivers_ignored: int = 0


class TractHistoryPoint(BaseModel):
    vintage_year: int
    observation_date: date
    value: Optional[float] = None
    margin_of_error: Optional[float] = None
    #: ``published``: the Bureau's estimate on this tract's 2020 boundary.
    #: ``unchanged``: published on the same area under the same code before
    #: 2020. ``apportioned``: DERIVED from earlier tracts by the relationship
    #: files. ``incomplete``: a contributing estimate was not published, so
    #: no value is given.
    status: PointStatus
    derived: bool
    weight_basis: Optional[str] = None
    source_geo_ids: list[str] = Field(default_factory=list)


class TractHistorySeries(BaseModel):
    metric_code: str
    #: Why the series stops at 2020, when it does (a median or rate cannot
    #: be apportioned to a redrawn tract).
    note: Optional[str] = None
    items: list[TractHistoryPoint]


class TractHistoryResponse(BaseModel):
    geo_id: str
    geo_name: Optional[str] = None
    target_tract_vintage: int = 2020
    derivation: Literal["decade_crosswalk"] = "decade_crosswalk"
    method: str
    boundary: TractBoundary
    series: list[TractHistorySeries]
