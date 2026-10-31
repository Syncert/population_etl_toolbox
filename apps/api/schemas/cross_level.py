"""One measure, two places, at any geographic level (ADR-0010).

The answer carries values, periods, intervals and reasons. It never ranks,
scores or names a "winner": a refusal says why and points at what would
answer the question, and two values the reader may set side by side come
with what limits reading one against the other -- different periods, one
place inside the other, overlapping margins of error.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel

from apps.api.schemas.labels import LabelStatus

#: What a value measures (ADR-0010 rule 2), from the metric's definition.
MeasureKind = Literal["count", "median", "mean", "rate", "share", "index"]
#: The definition's status, or ``undeclared`` when no kind is declared.
KindStatus = Literal["approved", "draft", "needs_review", "deprecated", "undeclared"]

#: Why a comparison is refused. Each is stated with what would answer instead.
RefusalReason = Literal[
    "counts_differ_in_size",
    "programs_differ",
    "universes_differ",
    "units_differ",
    "different_measures",
    "not_published",
]

#: What a served comparison cannot carry, one note per limit.
NoteReason = Literal[
    "periods_differ",
    "not_published",
    "contained",
    "overlapping_places",
    "intervals_overlap",
    "no_margin_published",
    "counts_differ_in_size",
]

#: How each side's period was chosen.
PeriodBasis = Literal["requested", "newest_common", "each_newest"]

Relationship = Literal["contains", "part_of", "intersects", "adjacent", "none_recorded"]


class ComparisonAlternative(BaseModel):
    """A served view that would answer a refused question."""

    ratio_id: str
    label: str
    unit: str
    href: str


class PlacesRefusal(BaseModel):
    reason: RefusalReason
    detail: str
    alternatives: list[ComparisonAlternative] = []


class PlacesNote(BaseModel):
    reason: NoteReason
    detail: str


class PlacesSide(BaseModel):
    """One place's value, or why it has none. A missing value is never zero."""

    geo_id: str
    geo_level: Optional[str] = None
    geo_name: Optional[str] = None
    metric_code: str
    metric_label: Optional[str] = None
    metric_label_status: Optional[LabelStatus] = None
    #: ``null`` when the comparison was refused before any read.
    published: Optional[bool] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    release: Optional[str] = None
    value: Optional[str] = None
    value_status: Optional[str] = None
    #: The published (or, for a derived value, propagated) 90 percent margin.
    margin_of_error: Optional[str] = None
    interval_low: Optional[str] = None
    interval_high: Optional[str] = None


class PlacesContainment(BaseModel):
    """How the two places relate, from side a's point of view (ADR-0010 rule 5).

    ``contains``: b is inside a. ``part_of``: a is inside b. ``intersects``:
    the two overlap without nesting (a city and a county, a city and a
    metro). ``share_of_smaller_inside_larger`` is 1 for nesting and the
    reference's area overlap share for ``intersects``.
    """

    relationship: Relationship
    smaller_side: Optional[Literal["a", "b"]] = None
    share_of_smaller_inside_larger: Optional[float] = None
    geography_vintage: Optional[int] = None
    evidence_source: Optional[str] = None


class PlacesComparisonResponse(BaseModel):
    metric_code: Optional[str] = None
    metric_code_b: Optional[str] = None
    ratio_id: Optional[str] = None
    measure_kind: MeasureKind
    measure_kind_status: KindStatus
    cross_level: bool
    comparable: bool
    refusal: Optional[PlacesRefusal] = None
    period_basis: Optional[PeriodBasis] = None
    side_a: PlacesSide
    side_b: PlacesSide
    containment: PlacesContainment
    #: ``a - b``, API-derived; only for two independent places, the same
    #: period and two published values.
    difference: Optional[str] = None
    #: Whether the two 90 percent intervals overlap; ``null`` without margins.
    intervals_overlap: Optional[bool] = None
    derived: bool
    derivations: list[str]
    notes: list[PlacesNote]
    caveats: list[str]


class PlacesBatchItem(BaseModel):
    """One measure of a batch: its answer, or the status its own request had.

    ``instead`` is the first view a refused total offered (its per-capita or
    share pairing), answered in the same request so the reader does not wait
    for a second round trip; ``null`` when there was nothing to follow.
    """

    metric_code: Optional[str] = None
    ratio_id: Optional[str] = None
    status: int = 200
    detail: Optional[str] = None
    answer: Optional[PlacesComparisonResponse] = None
    instead: Optional[PlacesComparisonResponse] = None


class PlacesComparisonBatchResponse(BaseModel):
    """Several measures for the same two places, in the order asked."""

    geo_id_a: str
    geo_id_b: str
    items: list[PlacesBatchItem]
