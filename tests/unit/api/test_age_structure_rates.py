"""API unit tests: rates by age from the reviewed by-age rate schemes (ADR-0013).

Covers: API-192 (`docs/semantics/age_bands.json` declares, for B17001,
        B27010, C18108, B07001 and B21001, the lines summed into each age
        group's numerator and into its own universe, per sex where the table
        has one; the loader refuses a line from another table, a gap or
        overlap between age groups, a line used twice and a group divided by
        itself), API-193 (`/age-structure/rates` answers each group's rate as
        a percent of that group's universe with the Bureau's proportion
        margin, for everyone and each sex, for the place and every level it
        sits in at the place's release; a withheld line leaves its group null
        with the reason, a universe of nobody has no rate, and a level without
        the release is `not_published`).
"""

from __future__ import annotations

import copy
import json
from datetime import date
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.services.age_structure_service import Line, PlaceRelease
from data_ingestion_toolbox.semantics.age_bands import (
    REGISTRY_PATH,
    AgeBandRegistryError,
    label_age_range,
    load_rate_registry,
    parse_rate_registry,
    rate_label_problems,
)
from data_ingestion_toolbox.semantics.age_structure import (
    age_rate,
    proportion_margin,
)
from tests.unit.api.test_age_structure import (
    DANE,
    NATION,
    WISCONSIN,
    _Session,
)

pytestmark = [pytest.mark.unit, pytest.mark.api]

RATES = load_rate_registry()
DOCUMENT = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def _lines(table: str, first: int, last: int) -> set[str]:
    return {f"{table}_{n:03d}" for n in range(first, last + 1)}


# --- The scheme records -------------------------------------------------------


def test_the_five_by_age_tables_each_have_a_draft_scheme() -> None:
    """Covers: API-192"""
    assert {(s.rate_id, s.table_id) for s in RATES.values()} == {
        ("poverty_by_age", "B17001"),
        ("uninsured_by_age", "B27010"),
        ("disability_by_age", "C18108"),
        ("moved_by_age", "B07001"),
        ("veterans_by_age", "B21001"),
    }
    assert {s.status for s in RATES.values()} == {"draft"}


def test_poverty_reads_every_age_line_once_and_its_universe_holds_it() -> None:
    """Covers: API-192 -- the universe of an age group is below plus at or
    above the poverty level; under 18 is joined to S1701's groups."""
    scheme = RATES["poverty_by_age"]
    men_below = [c for g in scheme.groups for c in g.sides["men"].numerator]
    men_universe = [c for g in scheme.groups for c in g.sides["men"].denominator]
    women_below = [c for g in scheme.groups for c in g.sides["women"].numerator]
    assert set(men_below) == _lines("B17001", 4, 16)
    assert set(women_below) == _lines("B17001", 18, 30)
    assert set(men_universe) == _lines("B17001", 4, 16) | _lines("B17001", 33, 45)
    for group in scheme.groups:
        for side in ("all", "men", "women"):
            lines = group.sides[side]
            assert set(lines.numerator) < set(lines.denominator)
    assert [g.group_id for g in scheme.groups][:2] == ["under_5", "5_17"]
    assert scheme.groups[-1].age_end is None


def test_each_tables_numerator_is_the_line_its_label_names() -> None:
    """Covers: API-192"""
    uninsured = RATES["uninsured_by_age"]
    assert [g.sides["all"].numerator for g in uninsured.groups] == [
        ("B27010_017",),
        ("B27010_033",),
        ("B27010_050",),
        ("B27010_066",),
    ]
    assert not uninsured.by_sex and set(uninsured.groups[0].sides) == {"all"}
    disability = RATES["disability_by_age"]
    assert [g.sides["all"].numerator for g in disability.groups] == [
        ("C18108_003", "C18108_004"),
        ("C18108_007", "C18108_008"),
        ("C18108_011", "C18108_012"),
    ]
    moved = RATES["moved_by_age"]
    numerators = [c for g in moved.groups for c in g.sides["all"].numerator]
    assert set(numerators) == (
        _lines("B07001", 34, 48)
        | _lines("B07001", 50, 64)
        | _lines("B07001", 66, 80)
        | _lines("B07001", 82, 96)
    ), "the four moved lines of every age, never the total less the same house"
    assert set(c for g in moved.groups for c in g.sides["all"].denominator) == (
        _lines("B07001", 2, 16)
    )
    veterans = RATES["veterans_by_age"]
    assert [g.sides["men"].numerator for g in veterans.groups] == [
        (f"B21001_{n:03d}",) for n in (6, 9, 12, 15, 18)
    ]
    assert [g.sides["women"].denominator for g in veterans.groups] == [
        (f"B21001_{n:03d}",) for n in (21, 24, 27, 30, 33)
    ]


def _broken(rate_id: str, change) -> dict:
    document = copy.deepcopy(DOCUMENT)
    entry = next(e for e in document["rate_schemes"] if e["rate_id"] == rate_id)
    change(entry)
    return {"rate_schemes": [entry]}


@pytest.mark.parametrize(
    "rate_id, change, words",
    [
        (
            "uninsured_by_age",
            lambda e: e["groups"][0]["numerator"].update(all=["B01001_003"]),
            "not a B27010 line",
        ),
        (
            "uninsured_by_age",
            lambda e: e["groups"][1].update(age_start=21),
            "does not follow",
        ),
        (
            "disability_by_age",
            lambda e: e["groups"][1]["numerator"].update(all=["C18108_003"]),
            "used twice",
        ),
        (
            "uninsured_by_age",
            lambda e: e["groups"][0]["numerator"].update(all=["B27010_002"]),
            "divides a count by itself",
        ),
        (
            "veterans_by_age",
            lambda e: e["groups"][0]["numerator"].pop("women"),
            "must name exactly men, women",
        ),
        (
            "moved_by_age",
            lambda e: e.update(unit="per 1,000"),
            "served in percent",
        ),
    ],
)
def test_a_scheme_breaking_its_contract_is_refused(rate_id, change, words) -> None:
    """Covers: API-192"""
    with pytest.raises(AgeBandRegistryError, match=words):
        parse_rate_registry(_broken(rate_id, change))


# --- The arithmetic -----------------------------------------------------------


def _release(lines: dict[str, tuple[int | None, int | None]]) -> PlaceRelease:
    return PlaceRelease(
        geo_id=DANE["geo_id"],
        geo_level="COUNTY",
        vintage_year=2023,
        period_start="2019-01-01",
        period_end="2023-12-31",
        lines={
            code: Line(
                None if value is None else Decimal(value),
                None if moe is None else Decimal(moe),
                suppressed=value is None,
            )
            for code, (value, moe) in lines.items()
        },
    )


def test_a_rate_is_a_share_of_its_groups_universe_with_the_proportion_margin() -> None:
    """Covers: API-193"""
    release = _release({"X_001": (300, 40), "X_002": (700, 50), "X_003": (100, 30)})
    value = age_rate(release, ["X_001"], ["X_001", "X_002"])
    assert value.numerator.count == 300
    assert value.denominator.count == 1000
    assert value.rate == 30
    universe_moe = (Decimal(40) ** 2 + Decimal(50) ** 2).sqrt()
    expected = proportion_margin(Decimal(300), Decimal(40), Decimal(1000), universe_moe)
    assert value.rate_moe == expected * 100


def test_a_withheld_line_or_an_empty_universe_has_no_rate() -> None:
    """Covers: API-193"""
    release = _release({"X_001": (None, None), "X_002": (0, 10), "X_003": (0, 5)})
    assert age_rate(release, ["X_001"], ["X_002"]).reason == "suppressed_input"
    assert age_rate(release, ["X_009"], ["X_002"]).reason == "line_missing"
    empty = age_rate(release, ["X_003"], ["X_002"])
    assert empty.rate is None and empty.reason == "denominator_zero"


# --- The route ----------------------------------------------------------------


@pytest.fixture
def client_for():
    def make(session: _Session) -> TestClient:
        app.dependency_overrides[get_db_session_dep] = lambda: session
        return TestClient(app)

    yield make
    app.dependency_overrides.clear()


def _poverty_rows(geography: dict, vintage: int, below: int, above: int) -> list[dict]:
    values: dict[str, tuple[int | None, int]] = {}
    for offset in range(13):
        values[f"B17001_{4 + offset:03d}"] = (below, 20)
        values[f"B17001_{18 + offset:03d}"] = (below, 20)
        values[f"B17001_{33 + offset:03d}"] = (above, 40)
        values[f"B17001_{47 + offset:03d}"] = (above, 40)
    values["B17001_002"] = (below * 26, 90)
    values["B17001_001"] = ((below + above) * 26, 120)
    return [
        {
            "geo_id": geography["geo_id"],
            "geo_level": geography["geo_level"],
            "metric_code": f"CENSUS_ACS:acs5:{code}",
            "vintage_year": vintage,
            "observation_date": date(vintage, 1, 1),
            "duration_start": date(vintage - 4, 1, 1),
            "duration_end": date(vintage, 12, 31),
            "estimate_value": value,
            "margin_of_error": moe,
            "value_status": "valid",
            "latest": True,
        }
        for code, (value, moe) in values.items()
    ]


def test_a_county_answers_each_age_groups_rate_beside_its_state_and_nation(
    client_for,
) -> None:
    """Covers: API-193"""
    rows = [
        *_poverty_rows(DANE, 2023, below=100, above=900),
        *_poverty_rows(WISCONSIN, 2023, below=200, above=800),
        *_poverty_rows(NATION, 2022, below=150, above=850),
    ]
    session = _Session(rows)
    response = client_for(session).get(
        "/api/v1/age-structure/rates",
        params={"geo_id": DANE["geo_id"], "rate": "poverty_by_age"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["rate"]["table_id"] == "B17001"
    assert body["rate"]["status"] == "draft"
    assert body["default_reference_geo_id"] == "state:55"
    county, state, nation = body["places"]
    assert county["release"] == "2023" and county["derived"] is True
    under_5, five_17 = county["groups"][:2]
    assert under_5["men"]["rate"] == "10.00"
    assert under_5["men"]["denominator"] == "1000"
    assert under_5["all"]["denominator"] == "2000"
    # Five published ages (5, 6-11, 12-14, 15, 16-17) per sex.
    assert five_17["all"]["numerator"] == "1000"
    assert five_17["all"]["rate"] == "10.00"
    assert county["total"]["rate"] == "10.00"
    assert state["groups"][0]["women"]["rate"] == "20.00"
    assert nation["unavailable_reason"] == "not_published", "never 2022 for 2023"
    reads = [s for s, _ in session.statements if "metric_code" in s]
    assert len(reads) == 2, "one latest read, one as-released read for the nation"


def test_a_table_with_no_sex_answers_everyone_only(client_for) -> None:
    """Covers: API-193"""
    lines = {
        "B27010_001": (1000, 30),
        "B27010_002": (250, 20),
        "B27010_017": (10, 5),
        "B27010_018": (250, 20),
        "B27010_033": (40, 9),
        "B27010_034": (300, 20),
        "B27010_050": (30, 8),
        "B27010_051": (200, 10),
        "B27010_066": (None, None),
    }
    rows = [
        {
            "geo_id": DANE["geo_id"],
            "geo_level": "COUNTY",
            "metric_code": f"CENSUS_ACS:acs5:{code}",
            "vintage_year": 2023,
            "observation_date": date(2023, 1, 1),
            "duration_start": date(2019, 1, 1),
            "duration_end": date(2023, 12, 31),
            "estimate_value": value,
            "margin_of_error": moe,
            "value_status": "valid" if value is not None else "suppressed",
            "latest": True,
        }
        for code, (value, moe) in lines.items()
    ]
    response = client_for(_Session(rows)).get(
        "/api/v1/age-structure/rates",
        params={"geo_id": DANE["geo_id"], "rate": "uninsured_by_age"},
    )
    county = response.json()["places"][0]
    under_19, age_19_34, _, over_65 = county["groups"]
    assert under_19["men"] is None and under_19["women"] is None
    assert under_19["all"]["rate"] == "4.00"
    assert age_19_34["all"]["rate"] == "16.00"
    assert over_65["all"]["rate"] is None
    assert over_65["all"]["unavailable_reason"] == "suppressed_input"
    assert county["total"]["unavailable_reason"] == "suppressed_input"


@pytest.mark.parametrize(
    "params",
    [
        {"geo_id": "state:55|county:025", "rate": "B17001"},
        {"geo_id": "state:55|county:025", "rate": "poverty_by_age", "dataset": "dec"},
        {"geo_id": "state:55|county:025", "rate": "poverty_by_age", "release": "x"},
    ],
)
def test_requests_the_rates_route_refuses(client_for, params) -> None:
    """Covers: API-193"""
    response = client_for(_Session([])).get(
        "/api/v1/age-structure/rates", params=params
    )
    assert response.status_code == 422


def test_an_unknown_place_is_not_found(client_for) -> None:
    """Covers: API-193"""
    response = client_for(_Session([])).get(
        "/api/v1/age-structure/rates",
        params={"geo_id": "state:99", "rate": "poverty_by_age"},
    )
    assert response.status_code == 404


# --- The label check run against the warehouse before approval ----------------


def _poverty_labels() -> dict[str, str]:
    ages = [
        "Under 5 years",
        "5 years",
        "6 to 11 years",
        "12 to 14 years",
        "15 years",
        "16 and 17 years",
        "18 to 24 years",
        "25 to 34 years",
        "35 to 44 years",
        "45 to 54 years",
        "55 to 64 years",
        "65 to 74 years",
        "75 years and over",
    ]
    below = "Estimate!!Total:!!Income in the past 12 months below poverty level:"
    above = "Estimate!!Total:!!Income in the past 12 months at or above poverty level:"
    labels = {}
    for offset, age in enumerate(ages):
        labels[f"B17001_{4 + offset:03d}"] = f"{below}!!Male:!!{age}"
        labels[f"B17001_{18 + offset:03d}"] = f"{below}!!Female:!!{age}"
        labels[f"B17001_{33 + offset:03d}"] = f"{above}!!Male:!!{age}"
        labels[f"B17001_{47 + offset:03d}"] = f"{above}!!Female:!!{age}"
    return labels


def test_the_bureaus_labels_confirm_the_poverty_scheme() -> None:
    """Covers: API-192"""
    assert label_age_range("Estimate!!Total:!!Under 19 years:") == (0, 18)
    assert label_age_range("Estimate!!Total:!!18 and 19 years") == (18, 19)
    assert label_age_range("Estimate!!Total:!!Male:!!75 years and over") == (75, None)
    assert rate_label_problems(RATES["poverty_by_age"], _poverty_labels()) == []


def test_a_line_read_into_the_wrong_group_or_sex_is_named() -> None:
    """Covers: API-192"""
    labels = _poverty_labels()
    labels["B17001_004"], labels["B17001_010"] = (
        labels["B17001_010"],
        labels["B17001_004"],
    )
    labels["B17001_018"] = labels["B17001_018"].replace("Female", "Male")
    labels["B17001_005"] = labels["B17001_005"].replace("below", "at or above")
    problems = rate_label_problems(RATES["poverty_by_age"], labels)
    assert any("B17001_004" in p and "outside under_5" in p for p in problems)
    assert any("B17001_018" in p and "not a women's line" in p for p in problems)
    assert any("B17001_005" in p and "holds 'at or above'" in p for p in problems)
    assert len(problems) == 5, problems  # _005 both lacks and holds
