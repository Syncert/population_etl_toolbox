"""Covers: API-166 — county migration flows are served as SOI publishes them.

The route serves `gold_irs_migration.flow_latest` (ADR-0008). These tests
prove the served contract: flows ranked by the chosen measure, the file's
totals and SOI's categories kept apart with a deleted category withheld,
the newest pair of filing years as the default, no net figure, and
refusals for a non-county geography and an unpublished county.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app

pytestmark = [pytest.mark.unit, pytest.mark.api]

KENT = "state:10|county:001"


class _FakeResult:
    def __init__(
        self, rows: Optional[list[dict]] = None, scalar_value: Any = None
    ) -> None:
        self._rows = rows or []
        self._scalar_value = scalar_value

    def mappings(self) -> "_FakeResult":
        return self

    def all(self) -> list[dict]:
        return self._rows

    def scalar(self) -> Any:
        return self._scalar_value


class _Session:
    def __init__(self, rows: list[dict], newest: Optional[str] = "2021-2022") -> None:
        self.rows = rows
        self.newest = newest
        self.calls: list[tuple[str, dict]] = []

    def execute(self, query, params=None):  # noqa: ANN001, ANN201
        sql = str(query)
        self.calls.append((sql, dict(params or {})))
        if "MAX(year_pair)" in sql:
            return _FakeResult(scalar_value=self.newest)
        return _FakeResult(
            rows=[row for row in self.rows if row["year_pair"] == params["year_pair"]]
        )


class _FailingSession:
    def execute(self, query, params=None):  # noqa: ANN001, ANN201
        raise OperationalError(
            "SELECT 1",
            {"password": "irs-warehouse-secret"},
            Exception("connection refused"),
        )


def _row(
    category: str,
    code: str,
    returns: Optional[int],
    *,
    origin: Optional[str] = None,
    label: str = "",
    status: str = "valid",
) -> dict:
    return {
        "year_pair": "2021-2022",
        "category": category,
        "counterpart_label": label,
        "counterpart_code": code,
        "origin_geo_id": origin,
        "destination_geo_id": KENT if origin else None,
        "counterpart_geo_id": origin,
        "returns": returns,
        "individuals": None if returns is None else returns * 2,
        "agi": None if returns is None else str(returns * 30),
        "value_status": status,
        "value_source": "-1,-1,-1"
        if status == "withheld"
        else f"{returns},{returns * 2},{returns * 30}",
        "period_start": "2021-01-01",
        "period_end": "2022-12-31",
        "release_key": "2026-10-06T00:00:00Z",
    }


ROWS = [
    _row("total_us_and_foreign", "96:000", 5473),
    _row("non_migrants", "10:001", 68336, origin=KENT),
    _row(
        "county",
        "42:101",
        257,
        origin="state:42|county:101",
        label="Philadelphia County",
    ),
    _row(
        "county",
        "10:003",
        1096,
        origin="state:10|county:003",
        label="New Castle County",
    ),
    _row("county", "10:005", 770, origin="state:10|county:005", label="Sussex County"),
    _row("other_flows_northeast", "59:001", 486),
    _row("foreign_other_flows", "57:009", None, status="withheld"),
]


@pytest.fixture
def client():
    yield TestClient(app)
    app.dependency_overrides.clear()


def _use(session) -> None:  # noqa: ANN001
    app.dependency_overrides[get_db_session_dep] = lambda: session


def test_top_origins_are_ranked_and_categories_stay_apart(client) -> None:
    """Covers: API-166 — flows largest first; totals and categories are their own lists."""
    session = _Session(ROWS)
    _use(session)
    response = client.get(
        "/api/v1/migration-flows",
        params={"geo_id": KENT, "direction": "inflow", "limit": 2},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["year_pair"] == "2021-2022" and body["derived"] is False
    assert [item["counterpart_geo_id"] for item in body["items"]] == [
        "state:10|county:003",
        "state:10|county:005",
    ]
    assert body["total"] == 3 and body["unit"] == "returns"
    assert [item["category"] for item in body["totals"]] == [
        "total_us_and_foreign",
        "non_migrants",
    ]
    withheld = next(
        item for item in body["categories"] if item["category"] == "foreign_other_flows"
    )
    assert (withheld["value_status"], withheld["returns"], withheld["agi"]) == (
        "withheld",
        None,
        None,
    )
    assert (
        withheld["category_label"] == "Foreign, other flows"
        and withheld["counterpart_geo_id"] is None
    )
    assert any("not zero" in caveat for caveat in body["caveats"])
    assert any("no net figure" in caveat for caveat in body["caveats"])
    assert "net" not in {key for item in body["items"] for key in item}


def test_a_named_year_pair_and_measure_are_honoured(client) -> None:
    """Covers: API-166 — `year_pair` pins the file; `measure` changes the ranking unit."""
    session = _Session(ROWS)
    _use(session)
    response = client.get(
        "/api/v1/migration-flows",
        params={
            "geo_id": KENT,
            "direction": "inflow",
            "year_pair": "2021-2022",
            "measure": "agi",
        },
    )
    assert response.status_code == 200
    assert response.json()["unit"] == "thousands of dollars"
    assert not any("MAX(year_pair)" in sql for sql, _ in session.calls)


def test_a_non_county_or_unpublished_county_is_refused(client) -> None:
    """Covers: API-166 — 422 for a state geography or bad years; 404 when no file covers the county."""
    _use(_Session(ROWS))
    assert (
        client.get(
            "/api/v1/migration-flows",
            params={"geo_id": "state:10", "direction": "inflow"},
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/api/v1/migration-flows", params={"geo_id": KENT, "direction": "sideways"}
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/api/v1/migration-flows",
            params={"geo_id": KENT, "direction": "inflow", "year_pair": "2022"},
        ).status_code
        == 422
    )
    _use(_Session(ROWS, newest=None))
    missing = client.get(
        "/api/v1/migration-flows", params={"geo_id": KENT, "direction": "outflow"}
    )
    assert (
        missing.status_code == 404
        and "No published SOI outflow file" in missing.json()["detail"]
    )


def test_an_unavailable_warehouse_is_a_sanitized_503(client) -> None:
    """Covers: API-166 — the database error does not leak."""
    _use(_FailingSession())
    response = client.get(
        "/api/v1/migration-flows", params={"geo_id": KENT, "direction": "inflow"}
    )
    assert response.status_code == 503
    assert "secret" not in response.text and "refused" not in response.text
