"""Saved analysis configuration contracts (ADR-0003, API-007).

A configuration is the user's own analysis intent — which resource, which
metric(s), which filters, and an opaque visualization block the API stores
verbatim and never interprets. It is deliberately not a copy of observation
data: the configuration is replayed against live warehouse publications, so a
saved analysis follows the warehouse instead of freezing a snapshot of it.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

#: The resources a saved configuration may describe. Each maps to a live
#: route whose capability and compatibility rules validate the document.
#:
#: ``workbench`` is the composition kind (ADR-0003's WB-6 amendment): a list
#: of series, each of which *is* an observations request, plus the
#: presentation and alignment that say how they were read together. It maps
#: to no single route, which is exactly why its series are validated
#: individually — each one against the observations contract it will replay
#: through — rather than against a composite contract that would have to be
#: kept in step with three routes at once.
ConfigurationKind = Literal["observations", "comparison", "distribution", "workbench"]

#: The presentations a stored workbench may name. A closed vocabulary,
#: because a document naming a presentation this application cannot draw
#: reopens to a control with no option for it — the WEB-074 defect in a
#: stored document rather than in a link.
WorkbenchPresentationType = Literal[
    "line", "bar", "scatter", "ranking", "correlation", "heatmap"
]


class SeriesDocument(BaseModel):
    """One series of a stored workbench: an observations request, exactly.

    Every field an ``observations`` document carries, and no others, so the
    same validation runs over it. That is the point of the shape: a stored
    series cannot encode a request the observations route refuses, because it
    is checked by the code path that checks an observations document
    (API-082's line, per series).

    The geography is in ``filters`` rather than beside them, for the same
    reason: ``geo_id`` and ``geo_level`` are declared filters the capability
    contract governs, and lifting them out would put them beyond the check
    that governs them.
    """

    model_config = ConfigDict(extra="forbid")

    metric_code: str = Field(..., min_length=1, max_length=200)
    scope: Literal["latest", "as_released"] = "latest"
    release: Optional[str] = Field(default=None, max_length=100)
    newest_per_geography: bool = False
    newest_release_per_period: bool = False
    filters: dict[str, Any] = {}


class PresentationDocument(BaseModel):
    """How a stored workbench was drawn.

    ``type`` is validated against a closed vocabulary; ``options`` is opaque
    user content, stored and returned verbatim and never inspected — the same
    treatment ``visualization`` gets, and for the same reason: what a reader
    chose about the look of their own chart is not something the API has a
    contract for.
    """

    model_config = ConfigDict(extra="forbid")

    type: WorkbenchPresentationType
    options: dict[str, Any] = {}


class AlignmentDocument(BaseModel):
    """The shared grain a cross-sectional presentation was read at.

    ``None`` on a longitudinal composition, which has no shared grain: each
    series is one geography at its own grain, and inventing one would be the
    roll-up the whole surface refuses.
    """

    model_config = ConfigDict(extra="forbid")

    geo_level: str = Field(..., min_length=1, max_length=50)
    state_fips: Optional[str] = Field(default=None, max_length=2)
    #: The same-year pin, where the reader asked for one.
    year: Optional[int] = Field(default=None, ge=1000, le=9999)


class AnalysisDocument(BaseModel):
    """One saved analysis intent.

    Validated at write time against the same contracts the live routes
    enforce, so a stored configuration can never encode a request the API
    would refuse. ``visualization`` is opaque user content: stored and
    returned verbatim, never inspected.
    """

    model_config = ConfigDict(extra="forbid")

    kind: ConfigurationKind
    metric_code: Optional[str] = Field(default=None, max_length=200)
    metric_code_a: Optional[str] = Field(default=None, max_length=200)
    metric_code_b: Optional[str] = Field(default=None, max_length=200)
    scope: Literal["latest", "as_released"] = "latest"
    release: Optional[str] = Field(default=None, max_length=100)
    #: The reductions the observations resource serves, recorded so a saved
    #: view replays as the view (API-082). A map asks for one value per
    #: geography; a settled history asks for the newest release of each
    #: period. Both default to false, so a document stored before they
    #: existed replays exactly as it did.
    newest_per_geography: bool = False
    newest_release_per_period: bool = False
    filters: dict[str, Any] = {}
    bin_count: Optional[int] = Field(default=None, ge=1, le=20)
    #: A workbench's series, each an observations request in its own right.
    #: Bounded at eight, the ceiling the composing screen and
    #: ``/comparison/matrix`` both hold, so a stored document cannot describe
    #: a composition neither can reopen.
    series: Optional[list[SeriesDocument]] = Field(default=None, max_length=8)
    presentation: Optional[PresentationDocument] = None
    alignment: Optional[AlignmentDocument] = None
    visualization: dict[str, Any] = {}


class ConfigurationValidation(BaseModel):
    """Whether a stored document still matches live capabilities.

    Reported on read rather than repaired: a configuration that has gone
    stale — a retired metric, a source whose analysis reach changed — is the
    user's content, and rewriting it silently would substitute the API's
    guess for their intent.
    """

    valid: bool
    reason: Optional[str] = None


class SavedAnalysisSummary(BaseModel):
    """List-view row: identity and lifecycle, without the document."""

    model_config = ConfigDict(from_attributes=True)

    configuration_id: int
    name: str
    kind: Optional[str] = None
    version: int
    created_at: datetime
    updated_at: datetime


class SavedAnalysisListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[SavedAnalysisSummary]


class SavedAnalysisConfiguration(BaseModel):
    """Detail view: the stored document plus its live validation state."""

    configuration_id: int
    name: str
    version: int
    document: AnalysisDocument
    validation: ConfigurationValidation
    created_at: datetime
    updated_at: datetime


class SavedAnalysisCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=200)
    document: AnalysisDocument


class SavedAnalysisUpdateRequest(BaseModel):
    """An update states the version it read; a mismatch is refused."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=200)
    document: AnalysisDocument
    expected_version: int = Field(..., ge=1)
