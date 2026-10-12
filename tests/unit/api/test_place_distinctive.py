"""Within-parent percentile ranks, one measure at a time (API-165)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.schemas.distinctive import DistinctiveMeasure, DistinctiveResponse
from apps.api.services import distinctive_service
from apps.api.services.distinctive_service import (
    DISTINCTIVE_MEASURES,
    MINIMUM_SIBLINGS,
    SiblingValue,
    rank_measure,
)

pytestmark = pytest.mark.unit

PLACE = "state:55|county:025"
SIBLINGS = [PLACE] + [f"state:55|county:{index:03d}" for index in range(1, 15)]


def _rows(values, period="2020-01-01", own=70.0):
    rows = [SiblingValue(PLACE, own, period)]
    rows += [
        SiblingValue(geo, value, period) for geo, value in zip(SIBLINGS[1:], values)
    ]
    return rows


def test_a_rank_counts_withheld_and_missing_siblings_apart_from_values() -> None:
    """Covers: API-165 — withheld is counted, never ranked as zero."""
    values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, None]  # 12 rows, 2 missing
    result, reason = rank_measure(
        geo_id=PLACE, rows=_rows(values), sibling_ids=SIBLINGS
    )
    assert reason is None
    assert result == {
        "period_start": "2020-01-01",
        "value": 70.0,
        "siblings_with_value": 11,
        "siblings_withheld": 1,
        "siblings_missing": 2,
        "siblings_below": 6,
        "siblings_tied": 1,
        "percentile_rank": 6 / 11,
    }


def test_a_mixed_period_sibling_set_is_refused_not_ranked() -> None:
    """Covers: API-165 — a rank across periods is not published."""
    rows = _rows([10] * 14)
    rows[3] = SiblingValue(rows[3].geo_id, 10, "2019-01-01")
    result, reason = rank_measure(geo_id=PLACE, rows=rows, sibling_ids=SIBLINGS)
    assert result is None
    assert "different periods (2019-01-01, 2020-01-01)" in reason


def test_too_few_siblings_or_no_own_value_is_not_ranked() -> None:
    """Covers: API-165 — the minimum is declared and stated."""
    sparse = [10, 20] + [None] * 12
    result, reason = rank_measure(
        geo_id=PLACE, rows=_rows(sparse), sibling_ids=SIBLINGS
    )
    assert result is None
    assert (
        reason
        == f"2 siblings have a published value; at least {MINIMUM_SIBLINGS} are needed to rank"
    )
    result, reason = rank_measure(
        geo_id=PLACE, rows=_rows([1] * 14, own=None), sibling_ids=SIBLINGS
    )
    assert (result, reason) == (None, "no published value for this geography")


def test_no_field_combines_two_measures() -> None:
    """Covers: API-165 — the schema has nothing that could carry a score."""
    fields = set(DistinctiveResponse.model_fields) | set(
        DistinctiveMeasure.model_fields
    )
    for forbidden in (
        "score",
        "index",
        "overall",
        "average",
        "mean",
        "total_rank",
        "composite",
    ):
        assert not any(forbidden in field for field in fields), forbidden
    assert DistinctiveResponse.model_fields["derived"].default is True
    # Counts would rank a county by its size: the reviewed list holds none.
    assert not any(
        code.endswith(("B01003_001", "POPESTIMATE")) for code in DISTINCTIVE_MEASURES
    )


class _Session:
    pass


def _get(path, monkeypatch, response=None, error=None):
    def _service(db, geo_id):
        if error:
            raise error
        return response

    monkeypatch.setattr("apps.api.routers.place.distinctive_measures", _service)

    def _override():
        yield _Session()

    app.dependency_overrides[get_db_session_dep] = _override
    try:
        return TestClient(app).get(path)
    finally:
        app.dependency_overrides.clear()


def test_the_route_answers_the_derived_reading_and_refuses_an_unknown_place(
    monkeypatch,
) -> None:
    """Covers: API-165 — labelled derived; unknown geography is the stable 404."""
    response = DistinctiveResponse(
        geo_id=PLACE,
        geo_level="COUNTY",
        parent_scope="state:55",
        minimum_siblings=10,
        method=distinctive_service.METHOD,
        ranked=[],
        not_ranked=[],
    )
    ok = _get(
        f"/api/v1/place/distinctive?geo_id={PLACE}", monkeypatch, response=response
    )
    assert ok.status_code == 200
    assert ok.json()["derived"] is True
    missing = _get(
        "/api/v1/place/distinctive?geo_id=state%3A99",
        monkeypatch,
        error=distinctive_service.UnknownGeography("state:99"),
    )
    assert missing.status_code == 404
    assert missing.json() == {"detail": "geo_id not found"}
