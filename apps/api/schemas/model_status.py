"""Availability of the modelling surfaces, which are planned rather than built."""

from __future__ import annotations


from pydantic import BaseModel


class ModelSurfaceStatusResponse(BaseModel):
    status: str
    models_enabled: bool
    details: str
