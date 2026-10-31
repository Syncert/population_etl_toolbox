"""Age and sex structure responses (ADR-0013).

Every number is a string in the neutral contract's decimal form, as
`/observations/derived` serves it, and ``null`` -- never ``0`` -- when the
provider published nothing usable. Shares are percentages of the place's
total population (both sexes); ratios are per 100 people.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class AgeScheme(BaseModel):
    scheme_id: str
    version: int
    status: str = Field(description="`draft` until the band scheme is reviewed.")


class AgeCount(BaseModel):
    count: Optional[str] = None
    count_moe: Optional[str] = Field(
        None, description="90 percent margin of error; null when none is published."
    )


class AgeShare(AgeCount):
    share: Optional[str] = Field(None, description="Percent of the place's total.")
    share_moe: Optional[str] = None
    unavailable_reason: Optional[
        Literal["suppressed_input", "line_missing", "total_missing"]
    ] = None


class AgeBandSide(AgeShare):
    cv_percent: Optional[str] = Field(
        None, description="Coefficient of variation: (MOE / 1.645) / count, percent."
    )
    reliable: bool = Field(
        False, description="True when the coefficient of variation is 30 % or less."
    )


class AgeBandLines(BaseModel):
    men: list[str]
    women: list[str]


class AgeBandResult(BaseModel):
    band_id: str
    age_start: int
    age_end: Optional[int] = None
    label: str
    lines: AgeBandLines
    men: AgeBandSide
    women: AgeBandSide


class MedianAgeValue(BaseModel):
    value: Optional[str] = None
    moe: Optional[str] = None


class MedianAge(BaseModel):
    """The provider's own median age (B01002); never estimated from bands."""

    derived: bool = False
    all: MedianAgeValue
    men: MedianAgeValue
    women: MedianAgeValue


class AgeRatio(BaseModel):
    value: Optional[str] = None
    moe: Optional[str] = None
    moe_method: Literal["ratio", "unavailable", "none"] = "none"
    unit: str
    unavailable_reason: Optional[
        Literal["suppressed_input", "line_missing", "denominator_zero"]
    ] = None


class AgeIndicators(BaseModel):
    under_18: AgeShare
    age_18_64: AgeShare
    age_65_plus: AgeShare
    age_85_plus: AgeShare
    child_dependency_ratio: AgeRatio
    old_age_dependency_ratio: AgeRatio
    total_dependency_ratio: AgeRatio
    sex_ratio: AgeRatio


class AgeStructurePlace(BaseModel):
    geo_id: str
    geo_level: Optional[str] = None
    geo_name: Optional[str] = None
    role: Literal["place", "reference", "compare"]
    reference_level: Optional[Literal["COUNTY", "STATE", "NATIONAL"]] = None
    overlap_weight: Optional[float] = Field(
        None,
        description=(
            "For a city's county reference: the share of the city's area inside "
            "that county, as the geography reference records it."
        ),
    )
    release: Optional[str] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    unavailable_reason: Optional[Literal["not_published"]] = None
    derived: bool = True
    total: AgeCount = Field(default_factory=AgeCount)
    men: AgeCount = Field(default_factory=AgeCount)
    women: AgeCount = Field(default_factory=AgeCount)
    bands: list[AgeBandResult] = Field(default_factory=list)
    median_age: Optional[MedianAge] = None
    indicators: Optional[AgeIndicators] = None


class AgeStructureResponse(BaseModel):
    geo_id: str
    geo_level: Optional[str] = None
    geo_name: Optional[str] = None
    dataset: str
    scheme: AgeScheme
    default_reference_geo_id: Optional[str] = Field(
        None, description="The next level up, which the comparison toggle starts on."
    )
    places: list[AgeStructurePlace] = Field(
        description=(
            "The place first, then each enclosing level its comparison toggle "
            "offers (next level up first), then any compare_geo_ids."
        )
    )


class AgeStructureHistoryResponse(BaseModel):
    geo_id: str
    geo_level: Optional[str] = None
    geo_name: Optional[str] = None
    dataset: str
    scheme: AgeScheme
    periods: Literal["non_overlapping", "all"]
    items: list[AgeStructurePlace] = Field(description="Newest release first.")


class CohortPeriod(BaseModel):
    release: str
    period_start: Optional[str] = None
    period_end: Optional[str] = None


class CohortSide(BaseModel):
    from_count: Optional[str] = None
    from_count_moe: Optional[str] = None
    to_count: Optional[str] = None
    to_count_moe: Optional[str] = None
    change: Optional[str] = None
    change_moe: Optional[str] = None
    intervals_overlap: Optional[bool] = None


class CohortItem(BaseModel):
    from_band: str
    to_band: str
    men: CohortSide
    women: CohortSide


class CohortRefusal(BaseModel):
    reason: Literal[
        "overlapping_periods",
        "period_gap_not_five_years",
        "boundary_changed",
        "not_published",
    ]
    detail: str


class AgeCohortsResponse(BaseModel):
    geo_id: str
    geo_level: Optional[str] = None
    geo_name: Optional[str] = None
    dataset: str
    scheme: AgeScheme
    from_: Optional[CohortPeriod] = Field(None, alias="from")
    to: Optional[CohortPeriod] = None
    years_apart: Optional[int] = None
    comparable: bool
    refusal: Optional[CohortRefusal] = None
    caveat: str
    items: list[CohortItem] = Field(default_factory=list)

    model_config = {"populate_by_name": True}
