from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

from .geography import GeoLevel
from .sources import Source


@dataclass(frozen=True)
class Metric:
    metric_id: str
    display_name: str
    source: Source
    dataset: str
    series_id_or_variable_name: str
    unit: str
    frequency: str
    description: str
    default_geo_level: GeoLevel
    default_transform: str = "none"
    higher_is_good: bool = True
    is_modeled: bool = False
    is_public: bool = True


@dataclass(frozen=True)
class Observation:
    metric_id: str
    geo_id: str
    geo_level: GeoLevel
    period: str
    value: float
    unit: str
    source: Source
    dataset: str
    vintage: Optional[str] = None
    release_date: Optional[date] = None
    margin_of_error: Optional[float] = None
    margin_of_error_pct: Optional[float] = None
