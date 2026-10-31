"""Per-capita and share values: the declared derived product of ADR-0009.

A derived row always says what was divided by what: both inputs with their
own release, period, value and margin of error, the pairing's identity and
version, the universe the result describes, and how its margin of error was
computed. A provider's own rate is served as the provider's (``derived:
false``), with the provider's row as its one input.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel

from apps.api.schemas.labels import PlainLabelled

RatioKind = Literal["share", "per_capita", "provider_rate"]
PairingStatus = Literal["approved", "draft", "needs_review", "deprecated"]
#: How a derived margin of error was reached (ADR-0009 rule 4).
#:
#: ``proportion`` and ``ratio`` are the Census Bureau's formulas for a derived
#: proportion and ratio; ``ratio_fallback`` is the ratio formula used where
#: the proportion formula's radicand is negative, as the Bureau instructs;
#: ``unavailable`` means an input's margin is not published; ``provider``
#: means the provider's own published margin is served; ``none`` means the
#: inputs carry no sampling error the API knows of.
MarginMethod = Literal[
    "proportion", "ratio", "ratio_fallback", "unavailable", "provider", "none"
]
#: Why a row has no value (ADR-0009 rule 2). Never zero.
UnavailableReason = Literal[
    "different_period", "denominator_missing", "denominator_zero", "suppressed_input"
]


class DerivedRatioPairing(BaseModel):
    """One reviewed pairing from ``docs/semantics/derived_ratios.json``."""

    ratio_id: str
    kind: RatioKind
    derived: bool
    label: str
    unit: str
    scale: int
    numerator: str
    denominator: Optional[str] = None
    provider_rate: Optional[str] = None
    dataset: Optional[str] = None
    denominator_label: str
    universe: str
    numerator_is_subset: bool
    status: PairingStatus
    version: int
    reviewer: str
    served: bool
    refusal_reason: Optional[str] = None
    limitations: list[str]
    citations: list[str]


class DerivedRatioCatalogResponse(BaseModel):
    total: int
    items: list[DerivedRatioPairing]


class DerivedRatioInput(PlainLabelled):
    """One served provider row a derived value was computed from."""

    metric_code: str
    metric_display_name: Optional[str] = None
    release: Optional[str] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    value: Optional[str] = None
    value_status: Optional[str] = None
    margin_of_error: Optional[str] = None
    #: True when a null published margin was taken as zero because the
    #: Bureau controls this estimate at this geography level.
    margin_of_error_controlled: bool = False


class DerivedRatioObservation(BaseModel):
    ratio_id: str
    derived: bool
    pairing_status: PairingStatus
    pairing_version: int
    geo_id: Optional[str] = None
    geo_level: Optional[str] = None
    geo_name: Optional[str] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    release: Optional[str] = None
    value: Optional[str] = None
    unit: str
    scale: int
    margin_of_error: Optional[str] = None
    margin_of_error_method: MarginMethod
    unavailable_reason: Optional[UnavailableReason] = None
    numerator: Optional[DerivedRatioInput] = None
    denominator: Optional[DerivedRatioInput] = None
    provider_rate: Optional[DerivedRatioInput] = None


class DerivedRatioListResponse(BaseModel):
    """A page of one pairing's values, in the numerator's own served order."""

    ratio: DerivedRatioPairing
    scope: str
    release: Optional[str] = None
    total: int
    limit: int
    offset: int
    items: list[DerivedRatioObservation]
