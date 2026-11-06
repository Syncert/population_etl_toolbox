"""Covers: API-194 — taxes and public money for one place, and governors' fiscal change.

`/public-money` composes approved pairings (ADR-0009) read for exactly the
place, its state and the nation, plus the Census tax burden view;
`/governor-terms` serves the gold governor fiscal views for one state. These
tests prove the served contract: every share comes from the pairing's own
matching, the year is the place's newest and the same for every level,
a missing input is null with a reason (never zero), the remainder of BEA's
total is null rather than negative, the wording of ADR-0014's fiscal
section rides with each answer, and anything but a nation, state or county
is refused.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.schemas.observations import NeutralObservation
from apps.api.services import public_money_service

pytestmark = [pytest.mark.unit, pytest.mark.api]

DANE = "state:55|county:025"
WISCONSIN = "state:55"
NATION = "us:1"


def _obs(metric: str, geo: str, year: int, value: Optional[str]) -> NeutralObservation:
    return NeutralObservation(
        source_code=metric.split(":")[0],
        metric_code=metric,
        geo_id=geo,
        period_start=f"{year}-01-01",
        period_end=f"{year}-12-31",
        release="r1",
        value=value,
        value_status="valid" if value is not None else "missing",
    )


#: Personal income 1,000 everywhere; program receipts chosen so the shares
#: are the numbers below (Dane 2022: Social Security 4.6 percent, and so on).
#: A county's programs are CAINC35 over CAINC1; a state's and the nation's are
#: SAINC35 over SAINC50, BEA's state table.
INCOME = "BEA:CAINC1:1"
STATE_INCOME = "BEA:SAINC50:10"
_RECEIPTS = {
    "1000": {DANE: "131", WISCONSIN: "184", NATION: "179"},
    "2110": {DANE: "46", WISCONSIN: "64", NATION: "57"},
    "2210": {DANE: "34", WISCONSIN: "49", NATION: "44"},
    "2220": {DANE: "24", WISCONSIN: "34", NATION: "40"},
    "2310": {DANE: "3", WISCONSIN: "4", NATION: "5"},
    "2320": {DANE: "2", WISCONSIN: "3", NATION: "4"},
    "2330": {DANE: "4", WISCONSIN: "6", NATION: "9"},
    # The nation's unemployment receipts were not published for 2022.
    "2400": {DANE: "2", WISCONSIN: "3"},
}
PROGRAMS = {
    **{
        f"BEA:CAINC35:{line}": {DANE: values[DANE]}
        for line, values in _RECEIPTS.items()
    },
    **{
        f"BEA:SAINC35:{line}": {
            geo: value for geo, value in values.items() if geo != DANE
        }
        for line, values in _RECEIPTS.items()
    },
    "BEA:CAINC4:46": {DANE: "200", WISCONSIN: "180", NATION: "190"},
    "BEA:CAINC4:47": {DANE: "130", WISCONSIN: "185", NATION: "180"},
}


class _Reads:
    """Stands in for `observations_for_geographies`, recording each read."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def __call__(self, db, metric_code, scope, release, geo_ids):  # noqa: ANN001, ANN204
        self.calls.append((metric_code, tuple(geo_ids)))
        rows: list[NeutralObservation] = []
        if metric_code in (INCOME, STATE_INCOME):
            for geo in geo_ids:
                rows += [_obs(INCOME, geo, year, "1000") for year in (2021, 2022, 2023)]
        elif metric_code in PROGRAMS:
            for geo in geo_ids:
                value = PROGRAMS[metric_code].get(geo)
                if value is not None:
                    rows.append(_obs(metric_code, geo, 2022, value))
                # An older year, so the newest is chosen, not the first.
                rows.append(_obs(metric_code, geo, 2021, "1"))
        elif metric_code == "IRS_SOI:income_tax_amount":
            rows += [
                _obs(metric_code, DANE, 2022, "134"),
                _obs(metric_code, NATION, 2022, "114"),
            ]
        elif metric_code == "IRS_SOI:agi":
            rows += [
                _obs(metric_code, DANE, 2022, "1000"),
                _obs(metric_code, NATION, 2022, "1000"),
            ]
        return [row for row in rows if row.geo_id in geo_ids]


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self) -> "_Result":
        return self

    def all(self) -> list[dict]:
        return self._rows


class _Session:
    def __init__(self, tables: dict[str, list[dict]]) -> None:
        self.tables = tables
        self.calls: list[tuple[str, dict]] = []

    def execute(self, query, params=None):  # noqa: ANN001, ANN201
        sql = str(query)
        self.calls.append((sql, dict(params or {})))
        for name, rows in self.tables.items():
            if f"FROM {name}" in sql:
                return _Result(rows)
        return _Result([])


@pytest.fixture
def reads(monkeypatch: pytest.MonkeyPatch) -> _Reads:
    fake = _Reads()
    monkeypatch.setattr(public_money_service, "observations_for_geographies", fake)
    return fake


def _client(session: Any) -> TestClient:
    app.dependency_overrides[get_db_session_dep] = lambda: session
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():  # noqa: ANN202
    yield
    app.dependency_overrides.clear()


BURDEN = [
    {
        "geo_id": WISCONSIN,
        "fiscal_year": 2022,
        "dataset_id": "asslgf",
        "value": "10.900",
    },
    {"geo_id": NATION, "fiscal_year": 2022, "dataset_id": "asslgf", "value": "10.200"},
    {"geo_id": DANE, "fiscal_year": 2022, "dataset_id": "cog_units", "value": "4.100"},
]


def test_a_county_reads_itself_its_state_and_the_nation_and_nothing_else(
    reads: _Reads,
) -> None:
    """Covers: API-194 — reads are scoped to three geographies; payments by program for one year."""
    session = _Session({"gold_census_govfin.tax_burden": BURDEN})
    response = _client(session).get("/api/v1/public-money", params={"geo_id": DANE})
    assert response.status_code == 200, response.text
    body = response.json()
    assert [level["role"] for level in body["levels"]] == [
        "this place",
        "state",
        "nation",
    ]
    assert {geo for _metric, geos in reads.calls for geo in geos} == {
        DANE,
        WISCONSIN,
        NATION,
    }
    assert body["derived"] is True

    dane, wisconsin, nation = body["payments"]
    assert (dane["geo_id"], dane["year"], dane["total_pct"]) == (DANE, 2022, "13.10")
    shares = {program["program"]: program["share_pct"] for program in dane["programs"]}
    assert shares == {
        "social_security": "4.60",
        "medicare": "3.40",
        "medicaid": "2.40",
        "income_support": "0.90",
        "unemployment": "0.20",
        "other": "1.60",
    }
    assert wisconsin["year"] == nation["year"] == 2022
    # A program the nation did not publish is null, and so is the rest of the
    # total it would be subtracted from: never zero.
    nation_shares = {
        program["program"]: program["share_pct"] for program in nation["programs"]
    }
    assert nation_shares["unemployment"] is None
    assert nation_shares["other"] is None
    assert nation["total_pct"] == "17.90"

    sources = {
        part["source"]: part["share_pct"] for part in body["income_sources"][0]["parts"]
    }
    assert sources == {
        "earnings": "67.00",
        "dividends_interest_rent": "20.00",
        "payments": "13.00",
    }

    rate = body["federal_tax_rate"]
    assert rate["derived_from"] == "income_tax_share_of_agi"
    assert [(s["geo_id"], s["points"]) for s in rate["series"]] == [
        (DANE, [{"year": 2022, "value": "13.40"}]),
        (NATION, [{"year": 2022, "value": "11.40"}]),
    ]
    burden = body["tax_burden"]
    assert [s["geo_id"] for s in burden["series"]] == [DANE, WISCONSIN, NATION]
    burden_sql, burden_params = next(
        call for call in session.calls if "tax_burden" in call[0]
    )
    assert burden_params == {"geo_ids": [DANE, WISCONSIN, NATION]}
    assert "cog_units" in burden_sql and "asslgf" in burden_sql

    # Nothing in the answer reads as a score, and the fiscal wording rides with it.
    assert not {"score", "rank", "index"} & set(body)
    assert any("collected is not who bears it" in caveat for caveat in body["caveats"])


def test_the_remainder_of_bea_total_is_never_negative_or_zero_filled(
    reads: _Reads, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-194 — named programs above the total leave the rest unknown."""
    monkeypatch.setitem(
        PROGRAMS, "BEA:CAINC35:2110", {DANE: "130", WISCONSIN: "64", NATION: "57"}
    )
    body = (
        _client(_Session({}))
        .get("/api/v1/public-money", params={"geo_id": DANE})
        .json()
    )
    shares = {
        program["program"]: program["share_pct"]
        for program in body["payments"][0]["programs"]
    }
    assert shares["other"] is None


def test_a_state_and_the_nation_answer_for_fewer_levels(reads: _Reads) -> None:
    """Covers: API-194 — a state reads itself and the nation; the nation itself alone."""
    client = _client(_Session({}))
    state = client.get("/api/v1/public-money", params={"geo_id": WISCONSIN}).json()
    assert [level["geo_id"] for level in state["levels"]] == [WISCONSIN, NATION]
    nation = client.get("/api/v1/public-money", params={"geo_id": NATION}).json()
    assert [level["geo_id"] for level in nation["levels"]] == [NATION]


@pytest.mark.parametrize(
    "geo_id", ["state:55|county:025|tract:000100", "metro:31540", "state:5"]
)
def test_other_geographies_are_refused(reads: _Reads, geo_id: str) -> None:
    """Covers: API-194 — only the nation, a state or a county is served (422)."""
    response = _client(_Session({})).get(
        "/api/v1/public-money", params={"geo_id": geo_id}
    )
    assert response.status_code == 422
    assert reads.calls == []


def _fiscal_row(key: str, holder: str, measure: str, **values: Any) -> dict:
    return {
        "tenure_key": key,
        "state_code": "WI",
        "holder_name": holder,
        "party_code": "DEM" if holder == "Tony Evers" else "REP",
        "party_text": None,
        "start_date": "2019-01-07" if holder == "Tony Evers" else "2011-01-03",
        "end_date": None if holder == "Tony Evers" else "2019-01-07",
        "so_far": holder == "Tony Evers",
        "term_count": 1 if holder == "Tony Evers" else 2,
        "measure": measure,
        "measure_label": measure.replace("_", " "),
        "window_start_year": 2018 if holder == "Tony Evers" else 2010,
        "window_end_year": values.get("end"),
        "place_change": values.get("place"),
        "nation_change": values.get("nation"),
        "net_change": values.get("net"),
        "unavailable_reason": values.get("reason"),
    }


def test_governor_terms_serve_each_tenure_with_its_measures_and_tax_mix() -> None:
    """Covers: API-194 — one item per tenure, net change per measure, the mix of its last year."""
    session = _Session(
        {
            "gold_elections.governor_fiscal_measure_definition": [
                {
                    "measure": "tax_burden",
                    "measure_label": "State and local taxes",
                    "change_unit": "percentage points",
                    "source_label": "Census",
                    "observation_basis": "basis",
                },
                {
                    "measure": "payments_share",
                    "measure_label": "Payments",
                    "change_unit": "percentage points",
                    "source_label": "BEA",
                    "observation_basis": "basis",
                },
            ],
            "gold_elections.governor_term_fiscal": [
                _fiscal_row(
                    "evers",
                    "Tony Evers",
                    "tax_burden",
                    end=2023,
                    place="-0.200",
                    nation="0.200",
                    net="-0.400",
                ),
                _fiscal_row(
                    "evers",
                    "Tony Evers",
                    "payments_share",
                    reason="no_year_published_since_start",
                ),
                _fiscal_row(
                    "walker",
                    "Scott Walker",
                    "tax_burden",
                    end=2018,
                    place="-1.000",
                    nation="-0.500",
                    net="-0.500",
                ),
                _fiscal_row(
                    "walker",
                    "Scott Walker",
                    "payments_share",
                    end=2018,
                    place="2.000",
                    nation="1.000",
                    net="1.000",
                ),
            ],
            "gold_elections.governor_term_tax_mix": [
                {
                    "tenure_key": "walker",
                    "fiscal_year": 2018,
                    "tax_group": group,
                    "share": share,
                }
                for group, share in (
                    ("other", "10.00"),
                    ("property", "35.00"),
                    ("income", "33.00"),
                    ("general_sales", "22.00"),
                )
            ],
        }
    )
    response = _client(session).get(
        "/api/v1/governor-terms", params={"geo_id": WISCONSIN}
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert [measure["measure"] for measure in body["measures"]] == [
        "tax_burden",
        "payments_share",
    ]
    evers, walker = body["items"]
    assert (evers["holder_name"], evers["so_far"], evers["tax_mix"]) == (
        "Tony Evers",
        True,
        [],
    )
    assert evers["fiscal"][1]["net_change"] is None
    assert evers["fiscal"][1]["unavailable_reason"] == "no_year_published_since_start"
    assert [f["net_change"] for f in walker["fiscal"]] == ["-0.500", "1.000"]
    assert walker["tax_mix_year"] == 2018
    assert [share["tax_group"] for share in walker["tax_mix"]] == [
        "property",
        "general_sales",
        "income",
        "other",
    ]
    fiscal_sql, params = next(
        call for call in session.calls if "governor_term_fiscal" in call[0]
    )
    assert params == {"fips": "55"}
    statements = [call[0] for call in session.calls]
    jit_off = statements.index("SET LOCAL jit = off")
    assert jit_off < next(
        i for i, sql in enumerate(statements) if "governor_term_fiscal" in sql
    ), "JIT is off before the governor reads, inside the request's transaction"
    assert any("comparisons, not causes" in caveat for caveat in body["caveats"])


@pytest.mark.parametrize("geo_id", [DANE, NATION, "state:5"])
def test_governor_terms_are_a_states_alone(geo_id: str) -> None:
    """Covers: API-194 — governor terms answer a state only (422 otherwise)."""
    session = _Session({})
    response = _client(session).get("/api/v1/governor-terms", params={"geo_id": geo_id})
    assert response.status_code == 422
    assert session.calls == []


class _FailingSession:
    def execute(self, query, params=None):  # noqa: ANN001, ANN201
        raise OperationalError(
            "SELECT 1", {"password": "tax-secret"}, Exception("refused")
        )


def test_a_database_failure_is_a_503_that_names_nothing(reads: _Reads) -> None:
    """Covers: API-194 — no warehouse detail leaks from either route."""
    client = _client(_FailingSession())
    for path, geo in (
        ("/api/v1/public-money", DANE),
        ("/api/v1/governor-terms", WISCONSIN),
    ):
        response = client.get(path, params={"geo_id": geo})
        assert response.status_code == 503
        assert "tax-secret" not in response.text


def test_each_level_reads_its_own_bea_program_table(reads: _Reads) -> None:
    """Covers: API-194 — a county reads CAINC35/CAINC1, a state and the nation SAINC35/SAINC50."""
    _client(_Session({})).get("/api/v1/public-money", params={"geo_id": DANE})
    read = {}
    for metric, geos in reads.calls:
        read.setdefault(metric, set()).update(geos)
    assert read["BEA:CAINC35:1000"] == {DANE}
    assert read["BEA:CAINC1:1"] >= {DANE}
    assert read["BEA:SAINC35:1000"] == {WISCONSIN, NATION}
    assert read["BEA:SAINC50:10"] == {WISCONSIN, NATION}


def test_a_county_without_program_detail_still_shows_its_state_and_nation(
    reads: _Reads, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-194 — no county figure is null with a reason; the state and nation still answer."""

    def without_county_detail(db, metric_code, scope, release, geo_ids):  # noqa: ANN001, ANN202
        rows = reads(db, metric_code, scope, release, geo_ids)
        return [] if metric_code.startswith("BEA:CAINC35:") else rows

    monkeypatch.setattr(
        public_money_service, "observations_for_geographies", without_county_detail
    )
    body = (
        _client(_Session({}))
        .get("/api/v1/public-money", params={"geo_id": DANE})
        .json()
    )
    dane, wisconsin, nation = body["payments"]
    assert (dane["total_pct"], dane["unavailable_reason"]) == (
        None,
        "not_published_that_year",
    )
    assert all(program["share_pct"] is None for program in dane["programs"])
    assert (wisconsin["year"], wisconsin["total_pct"]) == (2022, "18.40")
    assert nation["total_pct"] == "17.90"


def test_a_county_whose_bea_files_differ_in_release_is_null_beside_its_state(
    reads: _Reads, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-194 — CAINC35 (Nov 2023) and CAINC1 (Feb 2026) are not paired; the state still answers."""
    released = {"BEA:CAINC35": "2023-11-16", "BEA:CAINC1": "2026-02-05"}

    def bea_files(db, metric_code, scope, release, geo_ids):  # noqa: ANN001, ANN202
        rows = reads(db, metric_code, scope, release, geo_ids)
        file = released.get(":".join(metric_code.split(":")[:2]))
        return [
            row.model_copy(update={"release": file}) if file else row for row in rows
        ]

    monkeypatch.setattr(public_money_service, "observations_for_geographies", bea_files)
    body = (
        _client(_Session({}))
        .get("/api/v1/public-money", params={"geo_id": DANE})
        .json()
    )
    dane, wisconsin, _nation = body["payments"]
    assert (dane["total_pct"], dane["unavailable_reason"]) == (
        None,
        "not_published_that_year",
    )
    assert (wisconsin["year"], wisconsin["total_pct"]) == (2022, "18.40")
    assert "CAINC35" in body["payments_source"]
    assert "SAINC35" in body["payments_source"]
