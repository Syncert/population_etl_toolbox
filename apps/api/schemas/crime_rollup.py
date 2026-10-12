"""Derived county crime roll-up, separate from provider-published totals."""

from typing import Literal, Optional

from pydantic import BaseModel


class CountyRollupRow(BaseModel):
    product_id: str
    release: str
    refresh_date: str
    ucr_program: str
    offense_code: str
    offense_label: str
    measure_id: str
    measure_form: str
    counted_entity_basis: str
    unit: str
    geo_id: str
    county_name: Optional[str] = None
    state_fips: Optional[str] = None
    county_fips: Optional[str] = None
    period: str
    period_start: str
    period_end: str
    #: Rendered as text so the summed provider precision survives JSON.
    value: str
    contributing_oris: list[str]
    reporting_agency_count: int
    mapped_agency_count: int
    includes_multi_county_agency: bool
    derived: Literal[True]
    derivation_method: str
    result_label: str
    methodology_note: str
    counted_entity_note: Optional[str] = None
    methodology_url: Optional[str] = None
    documentation_url: Optional[str] = None


class CountyRollupListResponse(BaseModel):
    derived: Literal[True] = True
    release_selection: str
    caveats: list[str]
    total: int
    limit: int
    offset: int
    items: list[CountyRollupRow]
