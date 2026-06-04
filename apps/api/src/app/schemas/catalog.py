from pydantic import BaseModel


class SourceResponse(BaseModel):
    source: str
    display_name: str
    description: str


class MetricResponse(BaseModel):
    metric_id: str
    display_name: str
    source: str
    dataset: str
    unit: str
    frequency: str
    description: str
    default_geo_level: str
    supports_moe: bool = False
    is_modeled: bool = False


class GeographyResponse(BaseModel):
    geo_id: str
    geo_level: str
    geo_name: str
    state_fips: str | None = None
    county_fips: str | None = None
    state_name: str | None = None
