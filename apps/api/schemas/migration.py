"""County-to-county migration flows, as SOI publishes them (irs-county-migration)."""

from typing import Literal, Optional

from pydantic import BaseModel


class MigrationRow(BaseModel):
    """One row of a county's SOI file: a flow, a total, or an SOI category."""

    category: str
    category_label: str
    #: The other county, for a flow between two counties; the county itself
    #: for non-migrants; absent for SOI's own categories.
    counterpart_geo_id: Optional[str] = None
    counterpart_name: Optional[str] = None
    origin_geo_id: Optional[str] = None
    destination_geo_id: Optional[str] = None
    returns: Optional[int] = None
    individuals: Optional[int] = None
    #: Thousands of dollars, rendered as text so provider precision survives JSON.
    agi: Optional[str] = None
    #: `withheld` when SOI deleted the category to protect taxpayers: every
    #: measure is then null, never zero.
    value_status: str
    value_source: str


class MigrationFlowsResponse(BaseModel):
    source_code: Literal["IRS_MIGRATION"] = "IRS_MIGRATION"
    derived: Literal[False] = False
    geo_id: str
    direction: str
    year_pair: str
    period_start: str
    period_end: str
    measure: str
    unit: str
    release: str
    caveats: list[str]
    #: The file's own totals for this county, and its non-migrants.
    totals: list[MigrationRow]
    #: SOI's "Other flows" and foreign categories: every flow under 20
    #: returns, aggregated by SOI and never redistributed into counties.
    categories: list[MigrationRow]
    #: How many county-to-county flows the file publishes for this county.
    total: int
    limit: int
    #: County-to-county flows, largest first by `measure`.
    items: list[MigrationRow]
