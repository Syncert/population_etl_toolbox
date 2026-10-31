"""API unit tests: many measures' newest values for a few places, one request.

Covers: API-180 (`/observations/newest/batch` answers each metric and place
        as the single newest read does, in the order asked; the metrics a
        relation holds are one `UNION ALL` statement whose branches are each
        the single read's reduction; an unknown metric is `404` per item, a
        metric that cannot be reduced the single read's `422`, an absent
        relation `503`; empty or oversized batches are `422`).
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.registry import OBSERVATION_DISPATCH
from apps.api.routers import observations as observations_router
from apps.api.schemas import NeutralObservationListResponse
from apps.api.services import neutral_observations_service as neutral
from apps.api.services.contracts import ServingContractUnavailable

pytestmark = [pytest.mark.unit, pytest.mark.api]

COUNTY = "state:55|county:025"
STATE = "state:55"
INCOME = "CENSUS_ACS:acs5:B19013_001"
AGE = "CENSUS_ACS:acs5:B01002_001"
EARNINGS = "BEA:CAINC5N:100"
CRIME = "FBI_UCR:summarized_violent_crime:V:offense:rate"


def _metric(code: str, source: str) -> dict[str, Any]:
    dispatch = OBSERVATION_DISPATCH[source]
    lineage: dict[str, Any] = {
        "schema": dispatch.lineage_schema,
        "relation": dispatch.lineage_relation,
    }
    if dispatch.lineage_key_column is not None:
        lineage["key"] = code.split(":", 1)[1]
    for field in dispatch.identity_columns:
        lineage[field] = f"{field}-value"
    return {
        "metric_code": code,
        "metric_display_name": code,
        "source_code": source,
        "physical_lineage": lineage,
    }


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def __iter__(self):
        return iter(self._rows)


class _Session:
    """Answers each statement from ``rows``, keyed by its first table."""

    def __init__(self, rows: dict[str, list[dict]]) -> None:
        self.info: dict[str, Any] = {}
        self.rows = rows
        self.statements: list[tuple[str, dict]] = []

    def execute(self, statement, params=None):
        sql = str(getattr(statement, "text", statement))
        self.statements.append((sql, params or {}))
        for relation, rows in self.rows.items():
            if relation in sql:
                return _Result(rows)
        return _Result([])


def _row(code: str, geo_id: str, value: str) -> dict[str, Any]:
    return {
        "grouped_metric_code": code,
        "geo_id": geo_id,
        "geo_level": "COUNTY" if "county" in geo_id else "STATE",
        "period_start": "2024-01-01",
        "period_end": "2024-12-31",
        "release": "acs5-2024",
        "as_of": None,
        "value": value,
        "value_status": None,
        "unit": "dollars",
    }


@pytest.fixture
def catalog(monkeypatch: pytest.MonkeyPatch) -> dict[str, dict]:
    metrics = {
        INCOME: _metric(INCOME, "CENSUS_ACS"),
        AGE: _metric(AGE, "CENSUS_ACS"),
        EARNINGS: _metric(EARNINGS, "BEA"),
        CRIME: _metric(CRIME, "FBI_UCR"),
    }
    monkeypatch.setattr(
        neutral,
        "resolve_metrics",
        lambda db, wanted: {code: metrics[code] for code in wanted if code in metrics},
    )
    monkeypatch.setattr(neutral, "resolve_metric", lambda db, code: metrics.get(code))
    monkeypatch.setattr(neutral, "require_relation", lambda db, relation: None)
    monkeypatch.setattr(neutral, "relation_is_absent", lambda db, relation: True)
    return metrics


def test_a_relations_metrics_are_one_statement_of_single_reads(catalog) -> None:
    """Covers: API-180 — one UNION ALL per relation, each branch the reduction."""
    acs = OBSERVATION_DISPATCH["CENSUS_ACS"].latest_relation
    db = _Session({acs: [_row(INCOME, COUNTY, "97169"), _row(AGE, STATE, "40.1")]})
    answers = neutral.newest_values_batch(
        db, [INCOME, AGE, EARNINGS, "NOT:THERE"], [COUNTY, STATE]
    )
    assert [(code, geo) for code, geo, *_ in answers] == [
        (code, geo)
        for code in (INCOME, AGE, EARNINGS, "NOT:THERE")
        for geo in (COUNTY, STATE)
    ]
    acs_reads = [sql for sql, _ in db.statements if acs in sql]
    assert len(acs_reads) == 1
    [sql] = acs_reads
    assert sql.count("UNION ALL") == 1
    assert sql.count("newest_period_rank = 1") == 2
    assert sql.count("geo_id = ANY(:geo_ids)") == 2
    bea = OBSERVATION_DISPATCH["BEA"].latest_relation
    assert len([sql for sql, _ in db.statements if bea in sql]) == 1

    by_pair = {(code, geo): rest for code, geo, *rest in answers}
    status, detail, income = by_pair[(INCOME, COUNTY)]
    assert (status, detail) == (200, None)
    assert isinstance(income, NeutralObservationListResponse)
    assert (income.total, income.limit, income.offset) == (1, 1, 0)
    assert income.items[0].value == "97169"
    assert income.items[0].metric_code == INCOME
    # A pair with no row is an empty page, as the single read answers it.
    status, _, empty = by_pair[(INCOME, STATE)]
    assert status == 200 and empty.total == 0 and empty.items == []
    assert by_pair[(AGE, STATE)][2].items[0].value == "40.1"
    assert by_pair[("NOT:THERE", COUNTY)] == [404, "metric_code not found", None]


def test_a_metric_the_single_read_refuses_is_refused_per_item(catalog) -> None:
    """Covers: API-180 — the reduction's own refusal, and an absent relation."""
    dispatch = OBSERVATION_DISPATCH["FBI_UCR"]
    refusal = neutral.reduction_refusal(dispatch, "newest_per_geography")
    answers = neutral.newest_values_batch(_Session({}), [CRIME], [STATE])
    if refusal is None:
        assert answers[0][2] == 200
    else:
        assert answers[0][2:] == (422, refusal, None)


def test_an_absent_relation_is_503_for_its_metrics_only(
    catalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-180 — one missing relation does not fail the batch."""
    bea = OBSERVATION_DISPATCH["BEA"].latest_relation

    def require(db, relation):
        if relation == bea:
            raise ServingContractUnavailable(
                f"required serving relation is not present: {relation}"
            )

    monkeypatch.setattr(neutral, "require_relation", require)
    answers = {
        (c, g): s
        for c, g, s, *_ in neutral.newest_values_batch(
            _Session({}), [INCOME, EARNINGS], [COUNTY]
        )
    }
    assert answers == {(INCOME, COUNTY): 200, (EARNINGS, COUNTY): 503}


@pytest.fixture
def client():
    def _override():
        yield object()

    app.dependency_overrides[get_db_session_dep] = _override
    try:
        yield TestClient(app, raise_server_exceptions=False)
    finally:
        app.dependency_overrides.clear()


@pytest.mark.parametrize(
    "params",
    [
        {"metric_codes": INCOME, "geo_ids": ""},
        {"metric_codes": ",", "geo_ids": COUNTY},
        {"metric_codes": ",".join(f"M:{n}" for n in range(41)), "geo_ids": COUNTY},
        {
            "metric_codes": INCOME,
            "geo_ids": ",".join(f"state:{n:02d}" for n in range(9)),
        },
    ],
)
def test_an_empty_or_oversized_batch_is_explained(client, params: dict) -> None:
    """Covers: API-180 — the caller splits a batch the route will not read."""
    assert (
        client.get("/api/v1/observations/newest/batch", params=params).status_code
        == 422
    )


def test_the_route_answers_in_the_order_asked(
    client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-180 — items are the service's answers, deduplicated input."""
    seen: dict[str, Any] = {}

    def batch(db, codes, places):
        seen.update(codes=codes, places=places)
        return [
            (code, geo, 404, "metric_code not found", None)
            for code in codes
            for geo in places
        ]

    monkeypatch.setattr(observations_router, "newest_values_batch", batch)
    response = client.get(
        "/api/v1/observations/newest/batch",
        params={
            "metric_codes": f"{AGE},{INCOME},{AGE}",
            "geo_ids": f"{COUNTY},{STATE}",
        },
    )
    assert response.status_code == 200
    assert seen == {"codes": [AGE, INCOME], "places": [COUNTY, STATE]}
    body = response.json()
    assert body["metric_codes"] == [AGE, INCOME]
    assert [
        (item["metric_code"], item["geo_id"], item["status"]) for item in body["items"]
    ] == [
        (AGE, COUNTY, 404),
        (AGE, STATE, 404),
        (INCOME, COUNTY, 404),
        (INCOME, STATE, 404),
    ]
