"""Covers: API-031, API-162, WEB-123 — scenarios preserve their published base."""

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from apps.api.services.population_scenario_service import population_scenario
from apps.api.services.neutral_observations_service import NeutralQueryError

pytestmark = [pytest.mark.unit, pytest.mark.api]


def publication(value="100000", **changes):
    row = dict(
        metric_code="CENSUS_PEP:POPESTIMATE",
        source_code="CENSUS_PEP",
        geo_id="state:55",
        period_start="2024",
        period_end="2024",
        value=value,
        value_status=None,
        unit="people",
        release="2025",
        as_of="2025-07-01",
        uncertainty=None,
        coverage=None,
        dimensions={},
    )
    row.update(changes)
    return SimpleNamespace(total=1, items=[SimpleNamespace(**row)])


def test_compound_scenario_reads_its_exact_published_base(monkeypatch):
    """Covers: API-162 — exact published base, explicit assumptions and future years."""
    calls = []

    def read(db, **kwargs):
        calls.append(kwargs)
        return publication()

    monkeypatch.setattr(
        "apps.api.services.population_scenario_service.list_neutral_observations", read
    )
    result = population_scenario(object(), "CENSUS_PEP:POPESTIMATE", "state:55", 2, 1.0)
    assert result.derived is True
    assert result.base.value == "100000"
    assert [(item.year, item.value) for item in result.items] == [
        (2025, 101000.0),
        (2026, 102010.0),
    ]
    assert result.annual_change_percent == 1.0
    assert calls[0]["filters"] == {"geo_id": "state:55"}
    assert calls[0]["newest_per_geography"] is True
    assert "official forecast" in " ".join(result.caveats)


@pytest.mark.parametrize(
    "value,changes",
    [
        (None, {}),
        ("NaN", {}),
        ("-1", {}),
        ("100", {"value_status": "suppressed"}),
        ("100", {"geo_id": "state:27"}),
        ("100", {"period_start": None}),
    ],
)
def test_unusable_or_wrong_base_never_becomes_a_projection(monkeypatch, value, changes):
    """Covers: API-162 — withholding and scope mismatches refuse a scenario."""
    monkeypatch.setattr(
        "apps.api.services.population_scenario_service.list_neutral_observations",
        lambda *a, **k: publication(value, **changes),
    )
    with pytest.raises(NeutralQueryError):
        population_scenario(object(), "CENSUS_PEP:POPESTIMATE", "state:55", 10, 1.0)


def test_nonpopulation_metric_and_invalid_assumptions_are_refused():
    """Covers: API-162 — only total population and bounded assumptions are accepted."""
    for metric, years, rate in [
        ("CENSUS_ACS:acs5:B25064_001", 10, 1),
        ("CENSUS_PEP:POPESTIMATE", 31, 1),
        ("CENSUS_PEP:POPESTIMATE", 10, float("nan")),
    ]:
        with pytest.raises(NeutralQueryError):
            population_scenario(object(), metric, "state:55", years, rate)


def test_decline_and_zero_growth_are_explicit_scenarios(monkeypatch):
    """Covers: API-162 — zero and negative assumptions retain modeled semantics."""
    monkeypatch.setattr(
        "apps.api.services.population_scenario_service.list_neutral_observations",
        lambda *a, **k: publication(),
    )
    assert (
        population_scenario(object(), "CENSUS_PEP:POPESTIMATE", "state:55", 1, -1)
        .items[0]
        .value
        == 99000
    )
    assert (
        population_scenario(object(), "CENSUS_PEP:POPESTIMATE", "state:55", 1, 0)
        .items[0]
        .value
        == 100000
    )


@pytest.fixture
def client():
    from apps.api.dependencies import get_db_session_dep
    from apps.api.main import app

    previous = app.dependency_overrides.copy()
    app.dependency_overrides[get_db_session_dep] = lambda: object()
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
    app.dependency_overrides.update(previous)


def test_route_returns_derived_points_and_complete_baseline(client, monkeypatch):
    """Covers: API-162 — the public route retains derivation and source context."""
    monkeypatch.setattr(
        "apps.api.services.population_scenario_service.list_neutral_observations",
        lambda *a, **k: publication(),
    )
    response = client.get(
        "/api/v1/population/scenario",
        params={
            "metric_code": "CENSUS_PEP:POPESTIMATE",
            "geo_id": "state:55",
            "annual_change_percent": 1,
            "horizon_years": 2,
        },
    )
    assert response.status_code == 200
    assert response.json()["derived"] is True
    assert response.json()["base"]["release"] == "2025"
    assert response.json()["items"][-1] == {"year": 2026, "value": 102010.0}


@pytest.mark.parametrize("rate,horizon", [(11, 10), (1, 31), ("NaN", 10)])
def test_route_rejects_invalid_assumptions(client, rate, horizon):
    """Covers: API-162 — the public route refuses invalid scenario parameters."""
    response = client.get(
        "/api/v1/population/scenario",
        params={
            "metric_code": "CENSUS_PEP:POPESTIMATE",
            "geo_id": "state:55",
            "annual_change_percent": rate,
            "horizon_years": horizon,
        },
    )
    assert response.status_code == 422


def test_unknown_baseline_and_unavailable_database_remain_distinct(client, monkeypatch):
    """Covers: API-162 — missing metrics and unavailable storage have distinct errors."""
    monkeypatch.setattr(
        "apps.api.routers.population.population_scenario", lambda *a: None
    )
    params = {
        "metric_code": "CENSUS_PEP:POPESTIMATE",
        "geo_id": "state:55",
        "annual_change_percent": 1,
    }
    assert client.get("/api/v1/population/scenario", params=params).status_code == 404

    def broken(*args):
        raise SQLAlchemyError("test unavailable database")

    monkeypatch.setattr("apps.api.routers.population.population_scenario", broken)
    assert client.get("/api/v1/population/scenario", params=params).status_code == 503
