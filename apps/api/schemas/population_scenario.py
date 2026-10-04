"""Derived planning scenarios, separate from provider-published estimates."""

from typing import Literal

from pydantic import BaseModel

from apps.api.schemas.observations import NeutralObservation


class PopulationScenarioPoint(BaseModel):
    year: int
    value: float


class PopulationScenarioResponse(BaseModel):
    derived: Literal[True] = True
    model: Literal["compound-growth-v1"] = "compound-growth-v1"
    base: NeutralObservation
    annual_change_percent: float
    horizon_years: int
    formula: str = (
        "base_population * (1 + annual_change_percent / 100) ** years_after_base"
    )
    caveats: list[str]
    items: list[PopulationScenarioPoint]
