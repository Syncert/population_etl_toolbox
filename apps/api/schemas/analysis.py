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
    #: Rows this request can page: the geographies both sides published.
    total: int
    #: How many geographies each side published under this request's own
    #: filters, before the join. The join is an inner one, so a geography one
    #: side publishes and the other does not is absent from the answer
    #: entirely; without these, `total` reads as the universe rather than as
    #: the intersection it is (API-087).
    geographies_a: int = 0
    geographies_b: int = 0
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


class ComparisonCorrelationResponse(BaseModel):
    """API-derived Pearson and Spearman coefficients over one comparable pair.

    The pairs are exactly the rows ``/comparison`` would page under the same
    parameters: the same per-side newest-value-per-geography reduction, the
    same inner join on geography identity, unpaged. Nothing here is a
    provider fact, which is why ``derived`` is true, every coefficient is
    named in ``derivations``, and both inputs' identities, units and periods
    travel with the answer.

    A coefficient is ``null`` rather than ``0`` whenever the pairs cannot
    support one -- fewer than three of them, or a side publishing a single
    distinct value -- and ``caveats`` says which. Zero is a real coefficient
    meaning "no linear association", and answering it for "there was nothing
    to measure" would be the same class of defect as coercing a suppressed
    value to zero.
    """

    metric_code_a: str
    metric_code_b: str
    source_code_a: Optional[str] = None
    source_code_b: Optional[str] = None
    units_a: Optional[str] = None
    units_b: Optional[str] = None
    #: Always true. The coefficients are computed by the API from
    #: provider-published inputs; no source publishes them.
    derived: bool = True
    #: The grain the pairs were read at, in the vocabulary -- not the word the
    #: caller typed (API-094).
    geo_level: Optional[str] = None
    state_fips: Optional[str] = None
    #: The year each side was reduced within, when the caller pinned one.
    #: ``None`` means each side reduced to its newest published period, which
    #: is what ``/comparison`` does.
    year: Optional[int] = None
    #: Pairs where both sides published a number. A pair with a null,
    #: suppressed or non-numeric side is excluded from it, never counted as
    #: zero.
    n: int
    #: How many geographies each side published under this request's filters,
    #: before the join -- the same intersection reporting ``/comparison``
    #: carries (API-087), so ``n`` reads as an intersection rather than as a
    #: universe.
    geographies_a: int = 0
    geographies_b: int = 0
    #: Pairs whose two sides describe the same period. The reduction takes
    #: each side's own newest value, so a pair can combine two years; this
    #: counts how often it did not.
    contemporaneous_pairs: int = 0
    pearson_r: Optional[float] = None
    spearman_rho: Optional[float] = None
    #: The single period every pair's side came from, or ``None`` when they
    #: differ or nothing was paired -- the ``/distribution/bins`` convention
    #: (API-097), one field per side because a correlation has two.
    period_a: Optional[str] = None
    period_b: Optional[str] = None
    #: True when at least one pair combined two different periods, which is
    #: exactly ``contemporaneous_pairs < n``.
    periods_differ: bool = False
    derivations: list[str] = []
    #: What this analysis could not carry, led by the association-not-causation
    #: sentence, which is present in every answer.
    caveats: list[str] = []


class CorrelationStatistic(BaseModel):
    """One cell's API-derived coefficients, and what they were measured over.

    The same statistic ``/comparison/correlation`` serves for a pair, carried
    per cell so a matrix cell can be read on its own terms: a coefficient over
    3,000 counties and one over 40 are different claims, and a matrix that
    showed only the numbers would present them as the same.
    """

    #: Pairs where both measures published a number for the geography.
    n: int
    #: Pairs whose two measures describe the same period.
    contemporaneous_pairs: int = 0
    pearson_r: Optional[float] = None
    spearman_rho: Optional[float] = None
    #: True when at least one pair combined two different periods.
    periods_differ: bool = False
    derivations: list[str] = []


class MatrixMetricSummary(BaseModel):
    """One requested measure, as the matrix read it.

    ``geographies`` is what this measure published under the request's own
    filters, before any join -- the counterpart of ``/comparison``'s
    ``geographies_a``/``geographies_b``, one per measure because a matrix has
    more than two sides.
    """

    metric_code: str
    source_code: Optional[str] = None
    units: Optional[str] = None
    valid_time_grains: list[str] = []
    valid_geo_grains: list[str] = []
    geographies: int = 0
    #: The single period this measure's published values came from, or
    #: ``None`` when they differ -- the ``/distribution/bins`` convention.
    period: Optional[str] = None
    periods_differ: bool = False


class MatrixPair(BaseModel):
    """One unordered pair of the requested measures: served, or declined.

    A declined cell is an answer, not an omission: ``comparable`` is false,
    ``rules`` carries the verdict that decided it, and ``statistic`` is
    ``None``. The request as a whole still answers ``200`` as long as one pair
    is comparable, because a six-measure matrix with two declined cells is
    still six measures' worth of answer.
    """

    metric_code_a: str
    metric_code_b: str
    comparable: bool
    rules: list[CompatibilityFinding] = []
    caveats: list[str] = []
    statistic: Optional[CorrelationStatistic] = None


class MatrixCell(BaseModel):
    """One measure's published value for one geography, or its absence.

    A measure the geography has no published value for carries ``null`` in
    all three fields rather than being left out of the row, so the shape says
    "asked, and not published" instead of leaving a client to infer it from a
    missing key.
    """

    metric_code: str
    value: Optional[float] = None
    period: Optional[str] = None
    release: Optional[str] = None


class MatrixRow(BaseModel):
    """One geography, with one cell per requested measure."""

    geo_id: Optional[str] = None
    geo_level: Optional[str] = None
    state_fips: Optional[str] = None
    county_fips: Optional[str] = None
    state_name: Optional[str] = None
    county_name: Optional[str] = None
    values: list[MatrixCell] = []


class ComparisonMatrixResponse(BaseModel):
    """Two to eight measures aligned on geography, with pairwise statistics.

    ``/comparison`` is a pair by contract -- in its schema, its preflight and
    its saved kind -- so this is a separate resource rather than a widening of
    it. What it adds is the two things a pair cannot express: a wide row
    holding every measure for one geography, and a verdict per pair rather
    than one verdict for the request.

    The rows are the **union** of the geographies the measures published, not
    an intersection: the question a matrix answers is which measures a given
    geography is published for, and an inner join would delete exactly the
    geographies that answer it interestingly. The statistics are measured over
    the whole join, never over the page ``items`` returns.
    """

    #: Always true. Every coefficient here is computed by the API.
    derived: bool = True
    geo_level: Optional[str] = None
    state_fips: Optional[str] = None
    year: Optional[int] = None
    metrics: list[MatrixMetricSummary]
    pairs: list[MatrixPair]
    #: What applies to the whole answer, led by the association-not-causation
    #: sentence. Per-pair caveats ride each pair, so the long sentence is said
    #: once rather than twenty-eight times.
    caveats: list[str] = []
    #: Geographies in the union, which is what ``items`` pages over.
    total: int
    limit: int
    offset: int
    items: list[MatrixRow]


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
    #: The period every binned row came from, or ``None`` when they differ or
    #: nothing was published. The reduction ranks each geography's own newest
    #: period, so two geographies in one answer can describe two different
    #: years -- which the comparison route publishes per row as
    #: ``period_a``/``period_b`` and this one said nothing about (API-097).
    period: Optional[str] = None
    #: True when the binned rows came from more than one period. A histogram
    #: mixing them is a legitimate map of each geography's newest value; a
    #: histogram mixing them silently is not.
    periods_differ: bool = False
    #: What this analysis could not carry, in the caller's terms. Today that
    #: is the source's published uncertainty: an equal-width binning of
    #: estimates each carrying a margin of error draws boundaries the margins
    #: can straddle, and the comparison route has named the same thing since
    #: API-096 (API-098).
    caveats: list[str] = []
    items: list[DistributionBin]
