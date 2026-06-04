from pydantic import BaseModel


class ComparisonRecord(BaseModel):
    geo_id: str
    geo_level: str
    period: str
    value_a: float | None = None
    value_b: float | None = None


class ComparisonResponse(BaseModel):
    metric_a: str
    metric_b: str
    geo_level: str
    period: str = "latest"
    records: list[ComparisonRecord]
