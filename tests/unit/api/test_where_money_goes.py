"""Covers: API-196 — where public money goes, one layer per government level, never added.

`/where-money-goes` reads, for the nation, a state or a county, the Treasury
outlays by budget function (nation), state and local spending by Census line
(a county: the local governments in the county area), and federal awards
recorded in the place by award type and agency (states and counties). These
tests prove the served contract: each layer comes from its own view for one
year, lines are ordered by amount with a missing amount last and never zero,
the award year is the newest federal fiscal year that has ended, a layer with
nothing to serve says why, the separate-figures caveat rides with every
answer, and anything but a nation, state or county is refused before any
read.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.services.where_money_goes_service import (
    SEPARATE_FIGURES,
    current_fiscal_year,
    where_money_goes,
)

pytestmark = [pytest.mark.unit, pytest.mark.api]

DANE = "state:55|county:025"
WISCONSIN = "state:55"
NATION = "us:1"


class _Result:
    def __init__(self, rows: list[dict], scalar: Any = None) -> None:
        self._rows = rows
        self._scalar = scalar

    def mappings(self) -> "_Result":
        return self

    def all(self) -> list[dict]:
        return self._rows

    def scalar(self) -> Any:
        return self._scalar


class _Session:
    """Answers each gold view's rows, and each MAX read from ``newest``."""

    def __init__(self, tables: dict[str, list[dict]], newest: dict[str, Any]) -> None:
        self.tables = tables
        self.newest = newest
        self.calls: list[tuple[str, dict]] = []

    def execute(self, query, params=None):  # noqa: ANN001, ANN201
        sql = str(query)
        self.calls.append((sql, dict(params or {})))
        if sql.lstrip().startswith("SELECT MAX("):
            for relation, value in self.newest.items():
                if f"FROM {relation}" in sql:
                    if isinstance(value, list):
                        # The years held; the read's bound picks the newest below it.
                        bound = next(
                            v for k, v in (params or {}).items() if k != "geo_id"
                        )
                        below = [year for year in value if year < bound]
                        return _Result([], max(below) if below else None)
                    return _Result([], value)
            return _Result([], None)
        for name, rows in self.tables.items():
            if f"FROM {name}" in sql:
                return _Result(rows)
        return _Result([])


def _client(session: Any) -> TestClient:
    app.dependency_overrides[get_db_session_dep] = lambda: session
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():  # noqa: ANN202
    yield
    app.dependency_overrides.clear()


def _mix(measure: str, label: str, amount: str | None, share: str | None) -> dict:
    return {
        "measure_id": measure,
        "measure_label": label,
        "amount": amount,
        "total": "2000",
        "share_pct": share,
        "unavailable_reason": None if amount is not None else "suppressed_input",
        "government_count": 178,
    }


def _award(group: str, amount: str, share: str) -> dict:
    return {
        "award_group": group,
        "measure_label": f"Federal award obligations by place of performance, {group.replace('_', ' ')} (USAspending)",
        "amount": amount,
        "total": "100",
        "share_pct": share,
        "unavailable_reason": None,
    }


def _county_session() -> _Session:
    return _Session(
        {
            "gold_census_govfin.spending_mix": [
                _mix("spend_police", "Police protection", "200", "10.00"),
                _mix("spend_fire", "Fire protection", None, None),
                _mix(
                    "spend_elementary_secondary_education",
                    "Elementary and secondary education",
                    "1500",
                    "75.00",
                ),
                _mix("spend_highways", "Highways", "300", "15.00"),
            ],
            "gold_usaspending.awards_by_award_type": [
                _award("contracts", "20", "20.00"),
                _award("grants", "70", "70.00"),
                _award("loans", "-5", "-5.00"),
            ],
            "gold_usaspending.observation_latest": [{"total": "100"}],
            "gold_usaspending.awards_by_agency": [
                {
                    "agency_code": "HHS",
                    "agency_name": "Department of Health and Human Services",
                    "amount": "60",
                    "total": "100",
                    "share_pct": "60.00",
                    "unavailable_reason": None,
                },
                {
                    "agency_code": "DOD",
                    "agency_name": "Department of Defense",
                    "amount": "30",
                    "total": "100",
                    "share_pct": "30.00",
                    "unavailable_reason": None,
                },
            ],
        },
        {
            "gold_census_govfin.observation_latest": 2022,
            "gold_usaspending.observation_latest": [2024, 2025],
        },
    )


def test_a_county_gets_its_local_governments_and_federal_awards_as_separate_layers() -> (
    None
):
    """Covers: API-196 — a county area's local spending and the awards recorded there, each on its own total."""
    session = _county_session()
    answer = where_money_goes(session, DANE, today=date(2026, 3, 1))
    assert answer.geo_level == "county"
    assert [layer.layer for layer in answer.layers] == [
        "state_local_spending",
        "federal_awards_by_type",
        "federal_awards_by_agency",
    ]
    local, by_type, by_agency = answer.layers
    assert local.spender == "local governments in the county area"
    assert (local.fiscal_year, local.total, local.unit) == (
        2022,
        "2000",
        "thousands of US dollars",
    )
    # Largest first; a line with no amount last, null and never zero.
    assert [line.key for line in local.lines] == [
        "spend_elementary_secondary_education",
        "spend_highways",
        "spend_police",
        "spend_fire",
    ]
    assert (
        local.lines[-1].amount,
        local.lines[-1].share_pct,
        local.lines[-1].unavailable_reason,
    ) == (
        None,
        None,
        "suppressed_input",
    )
    assert "178 local governments reported in fiscal 2022." in local.notes
    # Fiscal 2025 ended September 30, 2025, less than a year ago: it is
    # named, and fiscal 2024 is served.
    assert (by_type.fiscal_year, by_type.total) == (2024, "100")
    assert [(line.label, line.amount) for line in by_type.lines] == [
        ("Grants", "70"),
        ("Contracts", "20"),
        ("Loans", "-5"),
    ]
    assert by_type.notes[0].startswith(
        "Fiscal 2025 has ended, but agencies are still reporting it"
    )
    assert by_agency.notes[0].startswith("Fiscal 2025 has ended")
    assert [line.key for line in by_agency.lines] == ["HHS", "DOD"]
    assert any("no awarding agency" in note for note in by_agency.notes)
    assert SEPARATE_FIGURES in answer.caveats
    _sql, local_params = next(
        call
        for call in session.calls
        if "FROM gold_census_govfin.spending_mix" in call[0]
    )
    assert local_params == {
        "geo_id": DANE,
        "dataset": "cog_units",
        "level": "local",
        "year": 2022,
    }


def test_the_award_year_is_the_newest_fiscal_year_ended_more_than_a_year_ago() -> None:
    """Covers: API-196 — October starts the next fiscal year; a year still being reported is named, not served."""
    assert current_fiscal_year(date(2026, 9, 30)) == 2026
    assert current_fiscal_year(date(2026, 10, 1)) == 2027
    session = _county_session()
    answer = where_money_goes(session, DANE, today=date(2026, 11, 13))
    reads = [
        (sql, params)
        for sql, params in session.calls
        if sql.lstrip().startswith("SELECT MAX(")
        and "gold_usaspending.observation_latest" in sql
    ]
    assert [params for _sql, params in reads] == [
        {"geo_id": DANE, "current": 2027},
        {"geo_id": DANE, "reported": 2026},
    ]
    by_type = answer.layers[1]
    # Fiscal 2025 ended more than a year before; nothing newer is held.
    assert by_type.fiscal_year == 2025
    assert not any("still reporting" in note for note in by_type.notes)


def test_the_nation_reads_treasury_outlays_and_state_and_local_spending_but_no_awards() -> (
    None
):
    """Covers: API-196 — the nation's federal layer is Treasury's; USAspending serves states and counties."""
    session = _Session(
        {
            "gold_treasury_mts.outlays_by_function": [
                {
                    "fiscal_year": 2025,
                    "statement_date": "2025-09-30",
                    "metric_key": key,
                    "budget_function": label,
                    "amount": amount,
                    "total": "7000",
                    "share_pct": share,
                    "unavailable_reason": None,
                }
                for key, label, amount, share in (
                    ("outlays_net_interest", "Net interest", "1000", "14.29"),
                    ("outlays_social_security", "Social Security", "1500", "21.43"),
                    (
                        "outlays_offsetting",
                        "Undistributed offsetting receipts",
                        "-100",
                        "-1.43",
                    ),
                )
            ],
            "gold_census_govfin.spending_mix": [
                _mix("spend_police", "Police protection", "200", "10.00")
            ],
        },
        {"gold_census_govfin.observation_latest": 2023},
    )
    answer = where_money_goes(session, NATION, today=date(2026, 3, 1))
    assert [layer.layer for layer in answer.layers] == [
        "federal_outlays",
        "state_local_spending",
    ]
    federal, state_local = answer.layers
    assert (federal.fiscal_year, federal.total, federal.unit) == (
        2025,
        "7000",
        "US dollars",
    )
    assert [line.label for line in federal.lines] == [
        "Social Security",
        "Net interest",
        "Undistributed offsetting receipts",
    ]
    assert state_local.spender == "state and local governments"
    _sql, params = next(
        call
        for call in session.calls
        if "FROM gold_census_govfin.spending_mix" in call[0]
    )
    assert (params["dataset"], params["level"]) == ("asslgf", "state_and_local")
    assert not any("gold_usaspending" in call[0] for call in session.calls)


def test_a_layer_with_nothing_published_says_why_and_shows_no_number() -> None:
    """Covers: API-196 — no year published is an empty layer with a reason, never zeros."""
    answer = where_money_goes(_Session({}, {}), WISCONSIN, today=date(2026, 3, 1))
    assert [layer.layer for layer in answer.layers] == [
        "state_local_spending",
        "federal_awards_by_type",
        "federal_awards_by_agency",
    ]
    for layer in answer.layers:
        assert (layer.lines, layer.total) == ([], None)
        assert layer.unavailable_reason


def test_the_route_answers_and_jit_is_off_before_any_read() -> None:
    """Covers: API-196 — the route serves the layers; JIT is off inside the read's transaction."""
    session = _county_session()
    response = _client(session).get("/api/v1/where-money-goes", params={"geo_id": DANE})
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["geo_id"] == DANE
    assert len(body["layers"]) == 3
    statements = [call[0] for call in session.calls]
    assert statements[0] == "SET LOCAL jit = off"


@pytest.mark.parametrize(
    "geo_id", ["state:55|place:48000", "state:5", "cbsa:31540", ""]
)
def test_anything_but_the_nation_a_state_or_a_county_is_refused_before_any_read(
    geo_id: str,
) -> None:
    """Covers: API-196 — a 422 that reads nothing."""
    session = _Session({}, {})
    response = _client(session).get(
        "/api/v1/where-money-goes", params={"geo_id": geo_id}
    )
    assert response.status_code == 422
    assert session.calls == []


class _FailingSession:
    def execute(self, query, params=None):  # noqa: ANN001, ANN201
        raise OperationalError(
            "SELECT 1", {"password": "spend-secret"}, Exception("refused")
        )


def test_a_database_failure_is_a_503_that_names_nothing() -> None:
    """Covers: API-196 — no warehouse detail leaks."""
    response = _client(_FailingSession()).get(
        "/api/v1/where-money-goes", params={"geo_id": DANE}
    )
    assert response.status_code == 503
    assert "spend-secret" not in response.text
