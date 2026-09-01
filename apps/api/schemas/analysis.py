"""API-derived analysis contracts: comparisons and distribution summaries.

Every value in these responses is computed by the API from
provider-published inputs, which is why the inputs' identities travel with
the result."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict


class ComparisonRow(BaseModel):
    """One geography's paired inputs and their API-derived combinations.

    ``value_a``/``value_b`` are the provider-published inputs (each side's
    newest value for the geography); ``period_a``/``period_b`` carry the
    period each input describes, so differing as-of context is visible rather
    than implied away. ``difference`` and ``ratio`` are API-derived.
    """

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    geo_id: Optional[str] = None
    geo_level: Optional[str] = None
    state_fips: Optional[str] = None
    county_fips: Optional[str] = None
    state_name: Optional[str] = None
    county_name: Optional[str] = None
    metric_code_a: Optional[str] = None
    metric_code_b: Optional[str] = None
    period_a: Optional[str] = None
    period_b: Optional[str] = None
    value_a: Optional[float] = None
    value_b: Optional[float] = None
    difference: Optional[float] = None
    ratio: Optional[float] = None


class ComparisonResponse(BaseModel):
    """An aligned comparison of two compatible metrics, latest per geography.

    Served only for pairs the declared compatibility policy accepts;
    ``caveats`` lists everything the publication left unverifiable. Every
    derived field is named in ``derivations``.
    """

    metric_code_a: str
    metric_code_b: str
    source_code_a: Optional[str] = None
    source_code_b: Optional[str] = None
    units_a: Optional[str] = None
    units_b: Optional[str] = None
    derivations: list[str] = []
    caveats: list[str] = []
    total: int
    limit: int
    offset: int
    items: list[ComparisonRow]


class CompatibilityFinding(BaseModel):
    """One declared rule's verdict: ``pass``, ``fail``, or ``unknown``."""

    rule: str
    status: str
    reason: str


class ComparisonPreflightResponse(BaseModel):
    """Why two metrics can or cannot be combined, before any data moves.

    ``comparable`` is false only when a rule positively fails; an unverifiable
    rule is a caveat, not a rejection. The comparison route enforces exactly
    this decision, so a client can trust the preflight verdict.
    """

    metric_code_a: str
    metric_code_b: str
    source_code_a: Optional[str] = None
    source_code_b: Optional[str] = None
    comparable: bool
    derivations: list[str] = []
    rules: list[CompatibilityFinding]
    caveats: list[str] = []


class DistributionBin(BaseModel):
    bin_index: int
    lower_bound: float
    upper_bound: float
    count: int


class DistributionBinsResponse(BaseModel):
    """API-derived equal-width bins over one metric's latest values.

    ``derived`` marks the binning itself as an API computation; counts are
    exact row counts of provider-published numeric values, and null,
    suppressed, or missing values are excluded from the bins rather than
    coerced.
    """

    metric_code: str
    source_code: Optional[str] = None
    units: Optional[str] = None
    derived: bool = True
    geo_level: Optional[str] = None
    total: int
    bin_count: int
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    items: list[DistributionBin]
