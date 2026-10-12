"""Covers: API-163 — the derived county crime roll-up keeps its derivation.

The route serves `gold_fbi.county_rollup` (ETL-053). These tests prove the
served contract: derived labeling and caveats survive to the response, the
latest-release projection is the default, an unmapped county is refused
explicitly rather than answered with an indistinguishable empty page, and
only county-shaped geographies are accepted.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app

pytestmark = [pytest.mark.unit, pytest.mark.api]


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

    def one(self) -> dict:
        assert len(self._rows) == 1
        return self._rows[0]

    def scalar(self) -> Any:
        return self._scalar_value


class _RecordingSession:
    """Record every executed statement so filters and relations are provable."""

    def __init__(
        self,
        rows: list[dict],
        total: int,
        resolved_count: int = 1,
        unresolved_count: int = 0,
    ) -> None:
        self._rows = rows
        self._total = total
        self._resolved_count = resolved_count
        self._unresolved_count = unresolved_count
        self.calls: list[tuple[str, dict]] = []

    def execute(self, query, params=None):  # noqa: ANN001, ANN201
        sql = str(query)
        self.calls.append((sql, dict(params or {})))
        if "resolved_count" in sql:
            return _FakeResult(
                rows=[
                    {
                        "resolved_count": self._resolved_count,
                        "unresolved_count": self._unresolved_count,
                    }
                ]
            )
        if sql.lstrip().upper().startswith("SELECT COUNT(*)"):
            return _FakeResult(scalar_value=self._total)
        return _FakeResult(rows=self._rows)

    @property
    def list_sql(self) -> str:
        return next(sql for sql, _ in self.calls if "ORDER BY" in sql)

    @property
    def list_params(self) -> dict:
        return next(params for sql, params in self.calls if "ORDER BY" in sql)


class _FailingSession:
    def execute(self, query, params=None):  # noqa: ANN001, ANN201
        raise OperationalError(
            "SELECT 1",
            {"password": "fbi-warehouse-secret"},
            Exception("connection refused for user population"),
        )


def _rollup_row(**overrides: Any) -> dict:
    row = {
        "product_id": "summarized_burglary",
        "release": "2026-09-01",
        "refresh_date": "2026-09-01",
        "ucr_program": "SRS",
        "offense_code": "BU",
        "offense_label": "Burglary",
        "measure_id": "BU:offense:absolute_total",
        "measure_form": "absolute_total",
        "counted_entity_basis": "offense",
        "unit": "offenses",
        "geo_id": "state:55|county:025",
        "county_name": "Dane County",
        "state_fips": "55",
        "county_fips": "025",
        "period": "01-2025",
        "period_start": "2025-01-01",
        "period_end": "2025-01-31",
        "value": "61",
        "contributing_oris": ["WI0130100", "WI0130200"],
        "reporting_agency_count": 2,
        "mapped_agency_count": 3,
        "includes_multi_county_agency": True,
        "derived": True,
        "derivation_method": "sum_of_agency_reported_totals",
        "result_label": "derived county roll-up of agency-reported totals",
        "methodology_note": (
            "Each mapped agency's whole published count is summed; an agency "
            "serving more than one county is counted in full in each of its "
            "counties, so county values are not additive to state totals. "
            "Agency months the provider did not publish are excluded, never "
            "zero."
        ),
        "counted_entity_note": "Counted offenses reported by the agency.",
        "methodology_url": "https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi",
        "documentation_url": "https://cde.ucr.cjis.gov/",
    }
    row.update(overrides)
    return row


@pytest.fixture
def session() -> _RecordingSession:
    return _RecordingSession(rows=[_rollup_row()], total=1)


@pytest.fixture
def client(session):
    previous = app.dependency_overrides.copy()
    app.dependency_overrides[get_db_session_dep] = lambda: session
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
    app.dependency_overrides.update(previous)


def _client_for(fake_session):
    previous = app.dependency_overrides.copy()
    app.dependency_overrides[get_db_session_dep] = lambda: fake_session
    test_client = TestClient(app)

    def restore() -> None:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(previous)

    return test_client, restore


def test_rollup_rows_keep_their_derivation_and_coverage(client, session):
    """Covers: API-163 — derived labeling, caveats, and coverage survive."""
    response = client.get("/api/v1/crime/county-rollup")

    assert response.status_code == 200
    body = response.json()
    assert body["derived"] is True
    assert body["release_selection"] == "latest_release"
    assert "not additive to state totals" in " ".join(body["caveats"])
    assert "never counted as zero" in " ".join(body["caveats"])
    row = body["items"][0]
    assert row["derived"] is True
    assert row["contributing_oris"] == ["WI0130100", "WI0130200"]
    assert row["reporting_agency_count"] == 2
    assert row["mapped_agency_count"] == 3
    assert row["includes_multi_county_agency"] is True
    assert row["value"] == "61"
    assert "gold_fbi.latest_county_rollup" in session.list_sql


def test_a_named_release_reads_the_history_relation(client, session):
    """Covers: API-163 — a release pin reads history, not the latest view."""
    response = client.get(
        "/api/v1/crime/county-rollup", params={"release": "2026-09-01"}
    )

    assert response.status_code == 200
    assert response.json()["release_selection"] == "single_release"
    assert "gold_fbi.county_rollup" in session.list_sql
    assert session.list_params["release"] == "2026-09-01"


def test_filters_are_bound_not_interpolated(client, session):
    """Covers: API-163 — every filter travels as a bound parameter."""
    response = client.get(
        "/api/v1/crime/county-rollup",
        params={
            "product_id": "summarized_burglary",
            "geo_id": "state:55|county:025",
            "year_from": 2024,
            "year_to": 2025,
        },
    )

    assert response.status_code == 200
    assert session.list_params["product_id"] == "summarized_burglary"
    assert session.list_params["geo_id"] == "state:55|county:025"
    assert session.list_params["year_from"] == 2024
    assert session.list_params["year_to"] == 2025
    assert "state:55|county:025" not in session.list_sql


def test_a_mapped_county_is_checked_before_it_is_served(client, session):
    """Covers: API-163 — the mapping evidence runs for the requested county."""
    response = client.get(
        "/api/v1/crime/county-rollup", params={"geo_id": "state:55|county:025"}
    )

    assert response.status_code == 200
    evidence_calls = [
        params for sql, params in session.calls if "resolved_count" in sql
    ]
    assert evidence_calls == [
        {"geo_id": "state:55|county:025", "state_geo_id": "state:55"}
    ]


def test_an_unmapped_county_is_refused_explicitly():
    """Covers: API-163 — no mapping means an explicit refusal, not an empty page."""
    fake = _RecordingSession(rows=[], total=0, resolved_count=0, unresolved_count=2)
    client, restore = _client_for(fake)
    try:
        response = client.get(
            "/api/v1/crime/county-rollup", params={"geo_id": "state:55|county:078"}
        )
    finally:
        restore()

    assert response.status_code == 404
    detail = response.json()["detail"]
    assert "state:55|county:078" in detail
    assert "unresolved or ambiguous" in detail
    assert "2" in detail
    # The refusal happened before any roll-up page was read.
    assert all("ORDER BY" not in sql for sql, _ in fake.calls)


@pytest.mark.parametrize(
    "geo_id",
    ["state:55", "us:1", "agency:WI0130100", "state:55|place:48000", "county:025"],
)
def test_a_noncounty_geography_is_refused(client, geo_id):
    """Covers: API-163 — only county geographies are served here."""
    response = client.get("/api/v1/crime/county-rollup", params={"geo_id": geo_id})

    assert response.status_code == 422
    assert "county" in response.json()["detail"]


def test_unknown_product_and_reversed_years_are_refused(client):
    """Covers: API-163 — closed product set and ordered year bounds."""
    assert (
        client.get(
            "/api/v1/crime/county-rollup", params={"product_id": "not_a_product"}
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/api/v1/crime/county-rollup",
            params={"year_from": 2025, "year_to": 2024},
        ).status_code
        == 422
    )


def test_empty_result_and_unavailable_database_remain_distinct():
    """Covers: API-163 — an empty page is 200; an outage is a sanitized 503."""
    empty = _RecordingSession(rows=[], total=0)
    client, restore = _client_for(empty)
    try:
        response = client.get("/api/v1/crime/county-rollup")
    finally:
        restore()
    assert response.status_code == 200
    assert response.json() == {
        **response.json(),
        "items": [],
        "total": 0,
    }

    failing = _FailingSession()
    client, restore = _client_for(failing)
    try:
        response = client.get("/api/v1/crime/county-rollup")
    finally:
        restore()
    assert response.status_code == 503
    assert "fbi-warehouse-secret" not in response.text
