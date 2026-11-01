"""API unit tests: many measures' settled histories for a few places, one request.

Covers: API-187 (`/observations/history/batch` answers each metric and place
        as the single settled-history read does -- `scope=as_released&
        newest_release_per_period=true&geo_id=` -- in the order asked, oldest
        period first; the metrics a released relation holds are one `UNION
        ALL` statement whose branches are each the single read's reduction,
        bounded per pair; an unknown metric is `404` per item, a metric that
        cannot be reduced the single read's `422`, an absent relation `503`;
        empty or oversized batches are `422`).
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

CITY = "state:48|place:05000"
COUNTY = "state:48|county:453"
INCOME = "CENSUS_ACS:acs5:B19013_001"
RENT = "CENSUS_ACS:acs5:B25064_001"
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


def _row(code: str, geo_id: str, year: int, value: str, row: int, total: int) -> dict:
    return {
        "grouped_metric_code": code,
        "geo_id": geo_id,
        "geo_level": "PLACE" if "place" in geo_id else "COUNTY",
        "period_start": f"{year - 4}-01-01",
        "period_end": f"{year}-12-31",
        "release": str(year),
        "as_of": None,
        "value": value,
        "value_status": "valid",
        "unit": "dollars",
        "u_margin_of_error": "1200",
        "u_margin_of_error_pct": None,
        "pair_row": row,
        "pair_total": total,
    }


@pytest.fixture
def catalog(monkeypatch: pytest.MonkeyPatch) -> dict[str, dict]:
    metrics = {
        INCOME: _metric(INCOME, "CENSUS_ACS"),
        RENT: _metric(RENT, "CENSUS_ACS"),
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
    """Covers: API-187 — one UNION ALL per released relation, each branch the
    newest-release-per-period reduction, rows oldest first per pair."""
    acs = OBSERVATION_DISPATCH["CENSUS_ACS"].released_relation
    db = _Session(
        {
            acs: [
                # Out of order on purpose: the pair's own row number orders it.
                _row(INCOME, CITY, 2024, "91461", 2, 2),
                _row(INCOME, CITY, 2009, "50132", 1, 2),
                _row(RENT, COUNTY, 2024, "1689", 1, 1),
            ]
        }
    )
    answers = neutral.history_batch(
        db, [INCOME, RENT, EARNINGS, "NOT:THERE"], [CITY, COUNTY]
    )
    assert [(code, geo) for code, geo, *_ in answers] == [
        (code, geo)
        for code in (INCOME, RENT, EARNINGS, "NOT:THERE")
        for geo in (CITY, COUNTY)
    ]
    acs_reads = [(sql, params) for sql, params in db.statements if acs in sql]
    assert len(acs_reads) == 1
    [(sql, params)] = acs_reads
    assert sql.count("UNION ALL") == 1
    assert sql.count("newest_release_rank = 1") == 2
    assert sql.count("geo_id = ANY(:geo_ids)") == 2
    assert sql.count("pair_row <= :most_rows") == 2
    assert params["most_rows"] == neutral.HISTORY_BATCH_MOST_ROWS
    assert params["geo_ids"] == [CITY, COUNTY]
    # The latest relation is not read: a history is every release's.
    assert OBSERVATION_DISPATCH["CENSUS_ACS"].latest_relation not in sql
    bea = OBSERVATION_DISPATCH["BEA"].released_relation
    assert len([s for s, _ in db.statements if bea in s]) == 1

    by_pair = {(code, geo): rest for code, geo, *rest in answers}
    status, detail, income = by_pair[(INCOME, CITY)]
    assert (status, detail) == (200, None)
    assert isinstance(income, NeutralObservationListResponse)
    assert income.scope == "as_released"
    assert (income.total, income.limit, income.offset) == (
        2,
        neutral.HISTORY_BATCH_MOST_ROWS,
        0,
    )
    assert [item.release for item in income.items] == ["2009", "2024"]
    assert income.items[1].uncertainty.margin_of_error == "1200"
    # A pair with no row is an empty page, as the single read answers it.
    status, _, empty = by_pair[(INCOME, COUNTY)]
    assert status == 200 and empty.total == 0 and empty.items == []
    assert by_pair[(RENT, COUNTY)][2].items[0].value == "1689"
    assert by_pair[("NOT:THERE", CITY)] == [404, "metric_code not found", None]


def test_a_bounded_pair_states_its_full_total(catalog) -> None:
    """Covers: API-187 — a pair past the bound says how many periods it has."""
    acs = OBSERVATION_DISPATCH["CENSUS_ACS"].released_relation
    db = _Session({acs: [_row(INCOME, CITY, 2024, "91461", 1, 1500)]})
    [(_, _, status, _, page)] = neutral.history_batch(db, [INCOME], [CITY])
    assert status == 200
    assert page.total == 1500 and len(page.items) == 1


def test_a_metric_the_single_read_refuses_is_refused_per_item(catalog) -> None:
    """Covers: API-187 — the reduction's own refusal is each item's."""
    dispatch = OBSERVATION_DISPATCH["FBI_UCR"]
    refusal = neutral.reduction_refusal(dispatch, "newest_release_per_period")
    answers = neutral.history_batch(_Session({}), [CRIME], [CITY])
    if refusal is None:
        assert answers[0][2] == 200
    else:
        assert answers[0][2:] == (422, refusal, None)


def test_an_absent_relation_is_503_for_its_metrics_only(
    catalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-187 — one missing relation does not fail the batch."""
    bea = OBSERVATION_DISPATCH["BEA"].released_relation

    def require(db, relation):
        if relation == bea:
            raise ServingContractUnavailable(
                f"required serving relation is not present: {relation}"
            )

    monkeypatch.setattr(neutral, "require_relation", require)
    answers = {
        (c, g): s
        for c, g, s, *_ in neutral.history_batch(
            _Session({}), [INCOME, EARNINGS], [CITY]
        )
    }
    assert answers == {(INCOME, CITY): 200, (EARNINGS, CITY): 503}


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
        {"metric_codes": ",", "geo_ids": CITY},
        {"metric_codes": ",".join(f"M:{n}" for n in range(13)), "geo_ids": CITY},
        {
            "metric_codes": INCOME,
            "geo_ids": ",".join(f"state:{n:02d}" for n in range(9)),
        },
    ],
)
def test_an_empty_or_oversized_batch_is_explained(client, params: dict) -> None:
    """Covers: API-187 — the caller splits a batch the route will not read."""
    assert (
        client.get("/api/v1/observations/history/batch", params=params).status_code
        == 422
    )


def test_the_route_answers_in_the_order_asked(
    client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-187 — items are the service's answers, deduplicated input."""
    seen: dict[str, Any] = {}

    def batch(db, codes, places):
        seen.update(codes=codes, places=places)
        return [
            (code, geo, 404, "metric_code not found", None)
            for code in codes
            for geo in places
        ]

    monkeypatch.setattr(observations_router, "history_batch", batch)
    response = client.get(
        "/api/v1/observations/history/batch",
        params={
            "metric_codes": f"{RENT},{INCOME},{RENT}",
            "geo_ids": f"{CITY},{COUNTY}",
        },
    )
    assert response.status_code == 200
    assert seen == {"codes": [RENT, INCOME], "places": [CITY, COUNTY]}
    assert [
        (item["metric_code"], item["geo_id"], item["status"])
        for item in response.json()["items"]
    ] == [
        (RENT, CITY, 404),
        (RENT, COUNTY, 404),
        (INCOME, CITY, 404),
        (INCOME, COUNTY, 404),
    ]
