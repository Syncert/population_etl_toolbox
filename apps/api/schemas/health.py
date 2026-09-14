"""Liveness and readiness contracts for the API process."""

from __future__ import annotations


from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str


class ReadinessResponse(BaseModel):
    """Whether the process can actually serve, not merely that it is up.

    ``database`` is required for readiness; ``cache`` is reported but never
    gates it, because Redis is an optimization the API must survive without.
    """

    status: str
    database: str
    cache: str


class SourceContent(BaseModel):
    """What one source currently publishes, as the glossary catalog holds it.

    ``status`` is ``serving`` only when ``metrics_current`` is above zero: a
    catalog of retired measures answers no observation request, so the count
    that decides the word is the count of measures a client can still ask for.
    """

    source_code: str
    status: str
    registered: bool
    metrics_total: int
    metrics_current: int
    metrics_stale: int
    metrics_retired: int
    counts_are_complete: bool
    last_publication_time: str | None = None


class ContentHealthResponse(BaseModel):
    """Whether this deployment has anything to serve, per source.

    Answered ``200`` in every state, including ``empty``. This resource
    reports on warehouse content rather than on the process, and a caller
    cannot read a report that refuses to answer; ``/health/ready`` remains the
    signal orchestration routes on.
    """

    status: str
    sources: list[SourceContent]
    silent_sources: list[str]
