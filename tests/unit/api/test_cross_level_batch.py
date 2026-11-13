"""API unit tests: many measures for the same two places, in one request.

Covers: API-178 (`/comparison/places/batch` answers each measure exactly as
        `/comparison/places` answers it alone, follows a refused total's
        offered view on the server, reads the pair's geographies and
        containment once per request, reads containment only for the two
        places compared through the related read, which starts each step from
        known places, and reads a source's metrics in one statement;
        `/catalog/metrics?metric_codes=` reads named rows in one request).
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.services import cross_level_service, neutral_observations_service
from data_ingestion_toolbox.semantics.derived_ratios import SERVE_DRAFTS_ENV
from data_ingestion_toolbox.sql.catalog_queries import (
    GEOGRAPHY_PAIR_RELATIONSHIP_QUERY,
    GEOGRAPHY_RELATED_QUERY_SQL,
    build_metrics_queries,
)
from tests.unit.api import test_cross_level_compare as single
from tests.unit.api.test_cross_level_compare import (
    AUSTIN,
    INCOME,
    POVERTY,
    TEXAS,
    TRAVIS,
    _row,
)

pytestmark = [pytest.mark.unit, pytest.mark.api]

# The single route's fixtures: the same stubbed reads answer both routes.
client = single.client
served = single.served


def _single(client, **params):
    return client.get("/api/v1/comparison/places", params=params).json()


def _batch(client, **params):
    return client.get("/api/v1/comparison/places/batch", params=params)


def test_each_item_is_the_single_answer_and_a_refused_total_carries_its_view(
    client, served, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-178 — same answers, in order, metric codes first."""
    monkeypatch.setenv(SERVE_DRAFTS_ENV, "1")
    rows, _, _ = served
    rows[INCOME] = [
        _row(INCOME, TRAVIS, "97169", "900"),
        _row(INCOME, TEXAS, "76292", "200"),
    ]
    share = "poverty_share:acs5"
    pair = {"geo_id_a": TRAVIS["geo_id"], "geo_id_b": TEXAS["geo_id"]}
    response = _batch(
        client,
        **pair,
        metric_codes=f"{INCOME},{POVERTY},NOT:THERE",
        ratio_ids=share,
    )
    assert response.status_code == 200
    body = response.json()
    assert [(item["metric_code"], item["ratio_id"]) for item in body["items"]] == [
        (INCOME, None),
        (POVERTY, None),
        ("NOT:THERE", None),
        (None, share),
    ]
    income, poverty, unknown, ratio = body["items"]
    assert income["answer"] == _single(client, **pair, metric_code=INCOME)
    assert income["instead"] is None
    assert poverty["answer"] == _single(client, **pair, metric_code=POVERTY)
    assert poverty["answer"]["refusal"]["reason"] == "counts_differ_in_size"
    # The refused total's first offered view, answered in the same request.
    assert poverty["instead"] == _single(client, **pair, ratio_id=share)
    assert ratio["answer"] == poverty["instead"]
    assert (unknown["status"], unknown["detail"], unknown["answer"]) == (
        404,
        "metric_code not found",
        None,
    )


def test_follow_alternatives_can_be_turned_off(
    client, served, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-178 — `instead` only when asked for."""
    monkeypatch.setenv(SERVE_DRAFTS_ENV, "1")
    body = _batch(
        client,
        geo_id_a=TRAVIS["geo_id"],
        geo_id_b=TEXAS["geo_id"],
        metric_codes=POVERTY,
        follow_alternatives="false",
    ).json()
    assert body["items"][0]["instead"] is None


@pytest.mark.parametrize(
    ("params", "status"),
    [
        ({"geo_id_a": "state:99", "geo_id_b": "state:48", "metric_codes": INCOME}, 404),
        ({"geo_id_a": "state:48", "geo_id_b": "us:1"}, 422),
        ({"geo_id_a": "state:48", "geo_id_b": "state:48", "metric_codes": INCOME}, 422),
        (
            {
                "geo_id_a": "state:48",
                "geo_id_b": "us:1",
                "metric_codes": ",".join(f"M:{n}" for n in range(61)),
            },
            422,
        ),
    ],
)
def test_the_batch_refuses_what_it_cannot_answer(
    client, served, params: dict, status: int
) -> None:
    """Covers: API-178 — an unknown place fails the batch; an empty, doubled or
    oversized one is explained."""
    assert _batch(client, **params).status_code == status


class _Session:
    """A session with a request memo, and nothing else."""

    def __init__(self) -> None:
        self.info: dict[str, Any] = {}
        self.statements: list[tuple[Any, dict]] = []

    def execute(self, statement, params=None):
        self.statements.append((statement, params or {}))
        return _Result([])


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def __iter__(self):
        return iter(self._rows)


def test_the_pair_is_read_once_per_request(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: API-178 — geographies and containment once, not once per measure."""
    calls = {"geographies": 0, "containment": 0}

    def geographies(db, a, b):
        calls["geographies"] += 1
        return AUSTIN, TRAVIS

    def containment(db, a, b):
        calls["containment"] += 1
        return cross_level_service.containment_between(a, b, [], "a")

    monkeypatch.setattr(cross_level_service, "_read_geographies", geographies)
    monkeypatch.setattr(cross_level_service, "_read_containment", containment)
    monkeypatch.setattr(
        cross_level_service,
        "_resolved",
        lambda db, code, parameter: {"metric_code": code, "metric_display_name": code},
    )
    monkeypatch.setattr(
        cross_level_service,
        "observations_for_geographies",
        lambda db, code, scope, release, geo_ids: [],
    )
    monkeypatch.setattr(
        cross_level_service, "prefetch_observations_for_geographies", lambda *a: None
    )
    codes = [INCOME, "CENSUS_ACS:acs5:B19301_001", "CENSUS_ACS:acs5:B01002_001"]
    body = cross_level_service.compare_places_batch(
        _Session(), AUSTIN["geo_id"], TRAVIS["geo_id"], metric_codes=codes
    )
    assert len(body.items) == 3
    assert calls == {"geographies": 1, "containment": 1}


def test_containment_reads_only_the_two_places() -> None:
    """Covers: API-178 — containment reads the pair query for the two places."""
    db = _Session()
    cross_level_service._read_containment(db, AUSTIN, TRAVIS)
    pair = [(s, p) for s, p in db.statements if s is GEOGRAPHY_PAIR_RELATIONSHIP_QUERY]
    assert pair, "containment did not read the pair relationship query"
    assert pair[0][1] == {"geo_id": AUSTIN["geo_id"], "other_geo_id": TRAVIS["geo_id"]}
    # The related read, filtered to the other place, so the two cannot disagree.
    assert GEOGRAPHY_PAIR_RELATIONSHIP_QUERY.text == (
        f"SELECT * FROM ({GEOGRAPHY_RELATED_QUERY_SQL}) AS pair "
        "WHERE pair.geo_id = :other_geo_id"
    )


def test_the_related_read_starts_every_step_from_known_places() -> None:
    """Covers: API-178 — each step after the first reads by an array of known
    identities, and the names are read by key, never by a guessed join."""
    sql = GEOGRAPHY_RELATED_QUERY_SQL
    assert sql.count("= ANY(ARRAY(") == 3
    assert "ancestor.related_geo_id = ANY(ARRAY(" in sql
    assert "metro_link.related_geo_id = ANY(ARRAY(" in sql
    assert "geography.geo_id = ANY(ARRAY(SELECT other_geo_id FROM related))" in sql
    # Every step is read once, before the next starts from it.
    assert sql.count("AS MATERIALIZED (") == 6


def test_a_sources_metrics_are_read_in_one_statement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-178 — one read per source relation, each code's rows kept."""
    codes = [INCOME, "CENSUS_ACS:acs5:B19301_001"]
    metrics = {
        code: {"metric_code": code, "source_code": "CENSUS_ACS"} for code in codes
    }
    monkeypatch.setattr(
        neutral_observations_service, "resolve_metrics", lambda db, wanted: metrics
    )
    monkeypatch.setattr(
        neutral_observations_service,
        "resolve_metric",
        lambda db, code: metrics.get(code),
    )
    monkeypatch.setattr(
        neutral_observations_service, "require_relation", lambda *a: None
    )
    monkeypatch.setattr(
        neutral_observations_service,
        "_observation_from",
        lambda row, dispatch, code, name: (code, row["geo_id"]),
    )

    class Session(_Session):
        def execute(self, statement, params=None):
            self.statements.append((statement, params or {}))
            return _Result(
                [
                    {"grouped_metric_code": INCOME, "geo_id": TRAVIS["geo_id"]},
                    {"grouped_metric_code": INCOME, "geo_id": TEXAS["geo_id"]},
                ]
            )

    db = Session()
    geo_ids = [TRAVIS["geo_id"], TEXAS["geo_id"]]
    neutral_observations_service.prefetch_observations_for_geographies(
        db, codes, "latest", geo_ids
    )
    assert len(db.statements) == 1
    statement, params = db.statements[0]
    assert "= ANY(:metric_codes)" in statement.text
    assert sorted(params["metric_codes"]) == sorted(codes)
    read = neutral_observations_service.observations_for_geographies
    # Answered from the request memo: no further statement.
    assert read(db, INCOME, "latest", None, list(reversed(geo_ids))) == [
        (INCOME, TRAVIS["geo_id"]),
        (INCOME, TEXAS["geo_id"]),
    ]
    assert read(db, codes[1], "latest", None, geo_ids) == []
    assert len(db.statements) == 1


def test_named_metrics_are_one_catalog_read() -> None:
    """Covers: API-178 — `metric_codes` filters the catalog list by code."""
    list_query, count_query, params = build_metrics_queries(
        None, None, None, 1000, 0, metric_codes=[INCOME, POVERTY]
    )
    assert "metric_code = ANY(:metric_codes)" in list_query.text
    assert "metric_code = ANY(:metric_codes)" in count_query.text
    assert params["metric_codes"] == [INCOME, POVERTY]
    test_client = TestClient(app, raise_server_exceptions=False)

    def _override():
        yield object()

    app.dependency_overrides[get_db_session_dep] = _override
    try:
        too_many = ",".join(f"M:{n}" for n in range(201))
        response = test_client.get(
            "/api/v1/catalog/metrics", params={"metric_codes": too_many}
        )
        assert response.status_code == 422
    finally:
        app.dependency_overrides.clear()
