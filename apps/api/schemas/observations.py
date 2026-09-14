"""The provider-neutral observation contract shared by the cross-source and
per-source observation routes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict


@dataclass(frozen=True)
class FilterBound:
    """What the observation route accepts for one filter value."""

    #: Longest accepted text, for the filters the route declares as strings.
    max_length: Optional[int] = None
    #: Inclusive range, for the filters the route declares as integers.
    minimum: Optional[int] = None
    maximum: Optional[int] = None

    def rejection(self, value: Any) -> Optional[str]:
        """Why the route would refuse ``value``, or ``None``.

        The shape is stated before the length is measured (API-105). API-091
        added the bound and checked how long a value is, not what it is, and
        ``AnalysisDocument.filters`` is ``dict[str, Any]``: an array, an
        object, a null or a fractional number stored clean and reported
        valid. Measuring first produced answers of its own -- a null was
        refused for ``state_fips`` (bound 2) and stored for ``geo_id`` (bound
        200), the same question answered two ways by the width of the bound,
        and ``year_from: true`` was refused as a year below 1700 because
        ``int(True)`` is 1.
        """
        if value is None:
            return "must be a value; the route has no null to send"
        if isinstance(value, (list, tuple, set, frozenset, dict)):
            return "must be a single value, not a list or an object"
        if self.minimum is not None or self.maximum is not None:
            number = _whole_number(value)
            if number is None:
                return "must be a whole number"
            if self.minimum is not None and number < self.minimum:
                return f"must be at least {self.minimum}"
            if self.maximum is not None and number > self.maximum:
                return f"must be at most {self.maximum}"
            return None
        if self.max_length is not None and len(str(value)) > self.max_length:
            return f"must be at most {self.max_length} characters"
        return None


def _whole_number(value: Any) -> Optional[int]:
    """``value`` as the integer the route would parse, or ``None``.

    A query parameter arrives as text and the route declares an ``int``, so
    the route is the authority on what parses. Read off `/api/v1/observations`
    against a real warehouse: `?year_from=2020` and `?year_from=2020.0` pass
    validation, `?year_from=2020.7`, `?year_from=true` and `?year_from=`
    answer 422.

    Coercing with ``int()`` instead accepted `2020.7` -- truncating it to a
    year the caller never wrote, then measuring the bound against that -- and
    accepted `True` as the year 1.
    """
    if isinstance(value, bool):
        # The route can only ever receive text, and `str(True)` is not a
        # number. `int(True)` is 1, which is a year nobody asked for.
        return None
    if isinstance(value, int):
        return value
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    if not number.is_integer():
        return None
    return int(number)


#: The bound the observation route declares for each filter it accepts.
#:
#: Declared here rather than written into the route signature alone, because
#: two places read it: the route, which enforces it on a live request, and
#: `saved_analysis_service`, which must refuse to store a configuration
#: carrying a value that route would refuse. `AnalysisDocument` promises a
#: stored configuration can never encode a request the API would refuse, and
#: the names were checked while the values were not -- a 5,000-character
#: `geo_id` stored clean and failed only when its owner reopened it
#: (API-091).
OBSERVATION_FILTER_BOUNDS: dict[str, FilterBound] = {
    "geo_id": FilterBound(max_length=200),
    "geo_level": FilterBound(max_length=50),
    "state_fips": FilterBound(max_length=2),
    "county_fips": FilterBound(max_length=3),
    "stratum_id": FilterBound(max_length=200),
    "adjustment_status": FilterBound(max_length=50),
    "domain_desc": FilterBound(max_length=200),
    "domaincat_desc": FilterBound(max_length=200),
    "subject_type": FilterBound(max_length=50),
    "subject_code": FilterBound(max_length=50),
    "year_from": FilterBound(minimum=1700, maximum=2200),
    "year_to": FilterBound(minimum=1700, maximum=2200),
}


class ObservationDashboard(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    source_code: Optional[str] = None
    source: Optional[str] = None
    observation_date: Optional[Any] = None
    period: Optional[str] = None
    duration_start: Optional[Any] = None
    duration_end: Optional[Any] = None
    time_sk: Optional[int] = None
    as_of_date: Optional[Any] = None
    release_date: Optional[Any] = None
    updated_at: Optional[datetime] = None
    geo_id: Optional[str] = None
    geo_level: Optional[str] = None
    geo_name: Optional[str] = None
    state_fips: Optional[str] = None
    county_fips: Optional[str] = None
    state_name: Optional[str] = None
    county_name: Optional[str] = None
    geo_latitude: Optional[float] = None
    geo_longitude: Optional[float] = None
    metric_code: Optional[str] = None
    metric_display_name: Optional[str] = None
    value: Optional[str] = None
    value_type: Optional[str] = None
    units: Optional[str] = None
    unit: Optional[str] = None
    seasonal_adjustment_status: Optional[str] = None
    dataset_code: Optional[str] = None
    dataset: Optional[str] = None
    vintage_year: Optional[int] = None
    vintage: Optional[str] = None
    margin_of_error: Optional[str] = None
    margin_of_error_pct: Optional[str] = None


class ObservationListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[ObservationDashboard]


class ObservationUncertainty(BaseModel):
    """Source-published uncertainty, exactly as published.

    Numeric fields are text to preserve provider precision. A field the source
    does not publish is ``null``; the whole object is ``null`` when the source
    publishes no uncertainty at all -- distinguishable from "published and
    empty".
    """

    margin_of_error: Optional[str] = None
    margin_of_error_pct: Optional[str] = None
    confidence_lower: Optional[str] = None
    confidence_upper: Optional[str] = None
    cv_value: Optional[str] = None
    cv_status: Optional[str] = None
    cv_symbol: Optional[str] = None


class ObservationCoverage(BaseModel):
    """Source-published reporting coverage (FBI UCR participation).

    A not-reported subject keeps ``null`` values; coverage context explains the
    gap instead of the API inventing a zero.
    """

    population: Optional[str] = None
    participated_population: Optional[str] = None
    coverage_percent: Optional[str] = None
    coverage_basis: Optional[str] = None
    participation_status: Optional[str] = None
    population_denominator: Optional[str] = None


class NeutralObservation(BaseModel):
    """One observation from any completed source, semantics preserved.

    The core fields every source can fill honestly are typed; the source's
    **declared** fields beyond them ride in ``dimensions`` under the source's
    own published names. That set is a review, not the serving relation's
    column list -- the source-scoped routes serve that -- and
    ``/catalog/capabilities`` answers it per source as
    ``observation_dimensions``, from the same declaration these rows are
    built from (API-109). ``value`` is text to preserve provider precision and
    is ``null`` whenever the source did not publish a usable number --
    ``value_status`` (the source's own vocabulary, ``null`` when the source
    publishes none) says why. ``release`` is the source's release identity
    (a CDC/NASS release watermark, an FBI release key, a Census vintage, a
    BLS/FRED as-of date) and ``as_of`` a date-typed as-of when the source
    publishes one.
    """

    source_code: str
    metric_code: str
    metric_display_name: Optional[str] = None
    geo_id: Optional[str] = None
    geo_level: Optional[str] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    release: Optional[str] = None
    as_of: Optional[str] = None
    value: Optional[str] = None
    value_status: Optional[str] = None
    unit: Optional[str] = None
    dimensions: dict[str, Any] = {}
    uncertainty: Optional[ObservationUncertainty] = None
    coverage: Optional[ObservationCoverage] = None
    source_record_id: Optional[str] = None
    capture_id: Optional[str] = None


class NeutralObservationListResponse(BaseModel):
    """The registry-dispatched neutral observation page.

    ``scope`` is ``latest`` (the source's own latest-release or latest-value
    semantics) or ``as_released`` (every published release, each row carrying
    its release identity). ``release`` echoes a pinned release identity.
    Ordering is deterministic per source and documented as part of the
    contract.
    """

    metric_code: str
    source_code: str
    scope: str
    release: Optional[str] = None
    total: int
    limit: int
    offset: int
    items: list[NeutralObservation]


class MetricRelease(BaseModel):
    """One published release of a metric's observations."""

    release: str
    as_of: Optional[str] = None
    observation_count: int


class MetricReleaseListResponse(BaseModel):
    """Release identities for one metric, newest first."""

    metric_code: str
    source_code: str
    total: int
    limit: int
    offset: int
    items: list[MetricRelease]
