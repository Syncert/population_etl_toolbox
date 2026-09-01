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
ConfigurationKind = Literal["observations", "comparison", "distribution"]


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
    filters: dict[str, Any] = {}
    bin_count: Optional[int] = Field(default=None, ge=1, le=20)
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
