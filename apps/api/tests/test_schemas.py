from app.schemas.catalog import MetricResponse
from app.schemas.observation import ObservationResponse


def test_metric_schema_validates() -> None:
    metric = MetricResponse(
        metric_id="population",
        display_name="Total Population",
        source="ACS",
        dataset="acs5",
        unit="count",
        frequency="annual",
        description="Total population estimate",
        default_geo_level="county",
        supports_moe=True,
        is_modeled=False,
    )
    assert metric.metric_id == "population"


def test_observation_schema_validates() -> None:
    obs = ObservationResponse(
        metric_id="population",
        geo_id="55025",
        geo_level="county",
        period="2023",
        value=575000,
        unit="count",
        source="ACS",
        dataset="acs5",
    )
    assert obs.geo_id == "55025"
