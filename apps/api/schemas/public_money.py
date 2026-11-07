"""Taxes and public money for one place, and governor tenures with fiscal
change (taxes-and-public-money T-6, ADR-0014 fiscal section).

Every value is text so provider precision survives JSON, and null is
missing, never zero: ``unavailable_reason`` says why.
"""

from typing import Literal, Optional

from pydantic import BaseModel


class PublicMoneyLevel(BaseModel):
    """One geography the answer compares: the place, its state, the nation."""

    geo_id: str
    role: Literal["this place", "state", "nation"]


class ProgramShare(BaseModel):
    """One program's payments as a share of residents' personal income."""

    program: str
    label: str
    #: Percent of personal income. `other` is the total less the named
    #: programs, the rest of BEA's own total, never an estimate.
    share_pct: Optional[str] = None


class PaymentsByProgram(BaseModel):
    geo_id: str
    year: int
    #: BEA's release of the program figures and of the income they divide.
    #: A county's differ: BEA publishes its program file (CAINC35) years
    #: after the income file (CAINC1), and the year is matched across them.
    program_release: Optional[str] = None
    income_release: Optional[str] = None
    total_pct: Optional[str] = None
    programs: list[ProgramShare]
    unavailable_reason: Optional[str] = None


class IncomeSource(BaseModel):
    source: Literal["earnings", "dividends_interest_rent", "payments"]
    label: str
    share_pct: Optional[str] = None


class IncomeSources(BaseModel):
    geo_id: str
    year: int
    parts: list[IncomeSource]
    unavailable_reason: Optional[str] = None


class SeriesPoint(BaseModel):
    year: int
    value: Optional[str] = None


class LevelSeries(BaseModel):
    geo_id: str
    points: list[SeriesPoint]


class MeasureSeries(BaseModel):
    """One measure over time for each level that publishes it."""

    measure: str
    label: str
    unit: str
    source: str
    #: The approved pairing (ADR-0009) the values come from, or the gold
    #: relation for the tax burden.
    derived_from: str
    caveats: list[str]
    series: list[LevelSeries]


class PointValue(BaseModel):
    geo_id: str
    value: Optional[str] = None
    margin_of_error: Optional[str] = None
    unavailable_reason: Optional[str] = None


class PeriodValues(BaseModel):
    measure: str
    label: str
    unit: str
    source: str
    period: Optional[str] = None
    caveats: list[str]
    values: list[PointValue]


class PublicMoneyResponse(BaseModel):
    derived: Literal[True] = True
    geo_id: str
    levels: list[PublicMoneyLevel]
    #: The newest year BEA publishes program detail for this place; its
    #: state and the nation are read for the same year.
    payments: list[PaymentsByProgram]
    payments_source: str
    income_sources: list[IncomeSources]
    federal_tax_rate: MeasureSeries
    tax_burden: MeasureSeries
    property_tax: PeriodValues
    caveats: list[str]


class TenureFiscalChange(BaseModel):
    measure: str
    label: str
    window_start_year: int
    window_end_year: Optional[int] = None
    place_change: Optional[str] = None
    nation_change: Optional[str] = None
    #: The state's change minus the nation's, in percentage points.
    net_change: Optional[str] = None
    unavailable_reason: Optional[str] = None


class TaxMixShare(BaseModel):
    tax_group: Literal["property", "general_sales", "income", "other"]
    label: str
    share_pct: Optional[str] = None


class GovernorTenure(BaseModel):
    tenure_key: str
    holder_name: str
    party_code: str
    party_text: Optional[str] = None
    start_date: str
    end_date: Optional[str] = None
    so_far: bool
    term_count: int
    fiscal: list[TenureFiscalChange]
    #: Shares of state and local taxes in the window's last fiscal year.
    tax_mix_year: Optional[int] = None
    tax_mix: list[TaxMixShare]


class FiscalMeasureDefinition(BaseModel):
    measure: str
    label: str
    change_unit: str
    source: str
    basis: str


class GovernorTermsResponse(BaseModel):
    derived: Literal[True] = True
    geo_id: str
    state_code: Optional[str] = None
    measures: list[FiscalMeasureDefinition]
    caveats: list[str]
    items: list[GovernorTenure]
