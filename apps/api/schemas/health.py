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
