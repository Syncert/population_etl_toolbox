from datetime import date

from pydantic import BaseModel


class ObservationResponse(BaseModel):
    metric_id: str
    geo_id: str
    geo_level: str
    period: str
    value: float
    unit: str
    source: str
    dataset: str
    vintage: str | None = None
    release_date: date | None = None
    margin_of_error: float | None = None
    margin_of_error_pct: float | None = None


class LatestObservationCollectionResponse(BaseModel):
    metric_id: str
    geo_level: str
    period: str = "latest"
    count: int
    observations: list[ObservationResponse]
