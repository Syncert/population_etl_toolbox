"""API unit tests: a place's age and sex structure (ADR-0013).

Covers: API-184 (the band scheme record sums each declared B01001 line once
        per sex into eighteen five-year bands; band, group and share margins
        follow the Bureau's formulas; a suppressed or missing line leaves its
        band unavailable and the rest served; median ages are the provider's;
        dependency and sex ratios are per 100), API-185 (the place and every
        enclosing level the comparison toggle offers come in one answer, next
        level up first, each at the place's release or `not_published`),
        API-186 (history keeps non-overlapping releases; cohorts move one band
        across periods ending five years apart and refuse overlapping periods,
        other gaps, and redrawn tracts).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.services import age_structure_service as service
from apps.api.services.age_structure_service import (
    PlaceRelease,
    Line,
    cohort_refusal,
    non_overlapping,
    place_structure,
    references_for,
    root_sum_squares,
)
from data_ingestion_toolbox.semantics.age_bands import (
    AgeBandRegistryError,
    REGISTRY_PATH,
    default_scheme,
    parse_registry,
)

import json

pytestmark = [pytest.mark.unit, pytest.mark.api]

SCHEME = default_scheme()

MADISON = {
    "geo_id": "state:55|place:48000",
    "geo_level": "PLACE",
    "geo_name": "Madison city, Wisconsin",
    "state_fips": "55",
}
DANE = {
    "geo_id": "state:55|county:025",
    "geo_level": "COUNTY",
    "geo_name": "Dane County, Wisconsin",
    "state_fips": "55",
}
WISCONSIN = {
    "geo_id": "state:55",
    "geo_level": "STATE",
    "geo_name": "Wisconsin",
    "state_fips": "55",
}
NATION = {
    "geo_id": "us:1",
    "geo_level": "NATIONAL",
    # The catalog's nation row carries its code as its name.
    "geo_name": "us:1",
    "state_fips": None,
}
TRACT = {
    "geo_id": "state:55|county:025|tract:001100",
    "geo_level": "TRACT",
    "geo_name": "Census Tract 11",
    "state_fips": "55",
}
GEOGRAPHIES = {g["geo_id"]: g for g in (MADISON, DANE, WISCONSIN, NATION, TRACT)}


def _lines(
    scale: int = 1000, moe: int = 50, bump: dict[str, int] | None = None
) -> dict[str, tuple[int, int | None]]:
    """B01001 lines: each age line ``scale`` (plus any bump), totals summed."""
    bump = bump or {}
    values: dict[str, tuple[int, int | None]] = {}
    men = women = 0
    for n in range(3, 26):
        code = f"B01001_{n:03d}"
        value = scale + bump.get(code, 0)
        values[code] = (value, moe)
        men += value
    for n in range(27, 50):
        code = f"B01001_{n:03d}"
        value = scale + bump.get(code, 0)
        values[code] = (value, moe)
        women += value
    values["B01001_002"] = (men, 30)
    values["B01001_026"] = (women, 30)
    values["B01001_001"] = (men + women, None)
    values["B01002_001"] = (35, 1)
    values["B01002_002"] = (34, 1)
    values["B01002_003"] = (36, 1)
    return values


def _release(
    geography: dict, lines: dict, vintage: int = 2023, span: int = 5
) -> PlaceRelease:
    return PlaceRelease(
        geo_id=geography["geo_id"],
        geo_level=geography["geo_level"],
        vintage_year=vintage,
        period_start=f"{vintage - span + 1}-01-01",
        period_end=f"{vintage}-12-31",
        lines={
            code: Line(Decimal(v), None if m is None else Decimal(m))
            for code, (v, m) in lines.items()
        },
    )


def _block(release: PlaceRelease, geography: dict = DANE):
    return place_structure(
        SCHEME,
        release,
        geo_id=geography["geo_id"],
        geo_level=geography["geo_level"],
        geo_name=geography["geo_name"],
        role="place",
    )


# --- The scheme record --------------------------------------------------------


def test_every_age_line_is_used_exactly_once_per_sex() -> None:
    """Covers: API-184 — eighteen equal bands from 23 unequal lines."""
    assert len(SCHEME.bands) == 18
    men = [line for band in SCHEME.bands for line in band.men]
    women = [line for band in SCHEME.bands for line in band.women]
    assert sorted(men) == [f"B01001_{n:03d}" for n in range(3, 26)]
    assert sorted(women) == [f"B01001_{n:03d}" for n in range(27, 50)]
    assert [band.age_start for band in SCHEME.bands] == list(range(0, 90, 5))
    assert SCHEME.bands[-1].age_end is None
    assert SCHEME.bands[3].men == ("B01001_006", "B01001_007")  # 15-17 + 18-19
    assert SCHEME.bands[4].men == ("B01001_008", "B01001_009", "B01001_010")


def test_a_scheme_using_a_line_twice_is_refused() -> None:
    """Covers: API-184 — nothing is grouped twice."""
    document = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    document["schemes"][0]["bands"][1]["men"] = ["B01001_003"]
    with pytest.raises(AgeBandRegistryError):
        parse_registry(document)


# --- One place's pyramid ------------------------------------------------------


def test_bands_sum_to_the_sex_totals_with_root_sum_square_margins() -> None:
    """Covers: API-184"""
    block = _block(_release(DANE, _lines()))
    men = sum(Decimal(band.men.count) for band in block.bands)
    assert men == Decimal(block.men.count) == 23000
    twenty = next(band for band in block.bands if band.band_id == "20-24")
    assert twenty.men.count == "3000"
    assert Decimal(twenty.men.count_moe) == root_sum_squares(
        [Decimal(50)] * 3
    ).quantize(Decimal("0.01"))
    # Shares are of the place's total, both sexes.
    assert twenty.men.share == "6.52"  # 3000 / 46000
    assert twenty.men.reliable is True
    assert block.derived is True


def test_groups_add_up_and_ratios_are_per_100_aged_18_to_64() -> None:
    """Covers: API-184 — the Bureau's under-18 / 18-64 / 65+ split."""
    block = _block(_release(DANE, _lines()))
    ind = block.indicators
    total = sum(
        Decimal(getattr(ind, g).count) for g in ("under_18", "age_18_64", "age_65_plus")
    )
    assert total == Decimal(block.total.count)
    # Under 18 = 4 lines per sex, 18-64 = 13, 65+ = 6.
    assert ind.under_18.count == "8000"
    assert ind.age_18_64.count == "26000"
    assert ind.child_dependency_ratio.value == "30.8"  # 8000 / 26000
    assert ind.old_age_dependency_ratio.value == "46.2"  # 12000 / 26000
    assert ind.total_dependency_ratio.value == "76.9"
    assert ind.sex_ratio.value == "100.0"
    assert ind.child_dependency_ratio.moe_method == "ratio"
    assert ind.child_dependency_ratio.unit == "per 100 people aged 18 to 64"


def test_a_suppressed_line_leaves_its_band_absent_and_the_rest_served() -> None:
    """Covers: API-184 — missing is never zero."""
    release = _release(DANE, _lines())
    release.lines["B01001_009"] = Line(None, None, suppressed=True)
    del release.lines["B01001_027"]
    block = _block(release)
    twenty = next(band for band in block.bands if band.band_id == "20-24")
    assert (
        twenty.men.count is None and twenty.men.unavailable_reason == "suppressed_input"
    )
    baby = block.bands[0]
    assert baby.women.count is None and baby.women.unavailable_reason == "line_missing"
    assert baby.men.count == "1000"
    assert block.indicators.age_18_64.count is None
    assert block.indicators.old_age_dependency_ratio.value is None


def test_median_age_is_the_providers_own() -> None:
    """Covers: API-184"""
    block = _block(_release(DANE, _lines()))
    assert block.median_age.derived is False
    assert (block.median_age.all.value, block.median_age.men.value) == ("35.0", "34.0")


def test_a_controlled_state_total_counts_as_zero_margin() -> None:
    """Covers: API-184 — the Bureau's instruction for controlled totals."""
    block = _block(_release(WISCONSIN, _lines()), WISCONSIN)
    assert block.total.count_moe == "0.00"
    assert block.bands[0].men.share_moe is not None


def test_an_unreliable_band_is_marked() -> None:
    """Covers: API-184 — CV over 30 %."""
    lines = _lines(scale=100, moe=80)
    block = _block(_release(TRACT, lines), TRACT)
    assert block.bands[0].men.reliable is False
    assert Decimal(block.bands[0].men.cv_percent) > 30


# --- Enclosing levels ---------------------------------------------------------


def test_each_level_offers_the_levels_it_sits_in_next_level_up_first() -> None:
    """Covers: API-185 — the toggle Nick asked for."""
    related = [
        {
            "relationship": "intersects",
            "geo_id": "state:55|county:025",
            "geo_level": "COUNTY",
            "overlap_weight": 0.97,
        },
        {
            "relationship": "intersects",
            "geo_id": "state:55|county:999",
            "geo_level": "COUNTY",
            "overlap_weight": 0.03,
        },
    ]
    city = references_for(MADISON["geo_id"], "PLACE", related)
    assert [r.geo_id for r in city] == ["state:55|county:025", "state:55", "us:1"]
    assert city[0].overlap_weight == 0.97
    assert [r.geo_id for r in references_for(TRACT["geo_id"], "TRACT")] == [
        "state:55|county:025",
        "state:55",
        "us:1",
    ]
    assert [r.geo_id for r in references_for(DANE["geo_id"], "COUNTY")] == [
        "state:55",
        "us:1",
    ]
    assert [r.geo_id for r in references_for("state:55", "STATE")] == ["us:1"]
    assert references_for("us:1", "NATIONAL") == []
    metro = references_for(
        "cbsa:31540",
        "METRO",
        [{"relationship": "part_of", "geo_id": "state:55", "geo_level": "STATE"}],
    )
    assert [r.geo_id for r in metro] == ["us:1", "state:55"]


class _Session:
    """Answers the service's statements from fixtures, and records them."""

    def __init__(self, rows: list[dict], related: list[dict] | None = None) -> None:
        self.rows = rows
        self.related = related or []
        self.statements: list[tuple[str, dict]] = []

    def execute(self, query, params=None):
        params = params or {}
        sql = str(query)
        self.statements.append((sql, params))
        if query is service.GEOGRAPHY_IDENTITY_QUERY:
            found = [GEOGRAPHIES[g] for g in params["geo_ids"] if g in GEOGRAPHIES]
        elif query is service.GEOGRAPHY_RELATED_QUERY:
            found = self.related
        elif query is service.LATEST_QUERY:
            found = [
                r
                for r in self.rows
                if r["metric_code"] in params["metric_codes"]
                and r["geo_id"] in params["geo_ids"]
                and r["latest"]
            ]
        elif query is service.PINNED_QUERY:
            found = [
                r
                for r in self.rows
                if r["metric_code"] in params["metric_codes"]
                and r["geo_id"] in params["geo_ids"]
                and r["vintage_year"] in params["vintages"]
            ]
        elif query is service.HISTORY_QUERY:
            found = [
                r
                for r in self.rows
                if r["metric_code"] in params["metric_codes"]
                and r["geo_id"] == params["geo_id"]
            ]
        else:  # pragma: no cover - a statement the test did not expect
            raise AssertionError(sql)
        return _Result(found)


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


def _rows(
    geography: dict,
    vintage: int,
    latest: bool = True,
    span: int = 5,
    dataset: str = "acs5",
    **kwargs: Any,
) -> list[dict]:
    return [
        {
            "geo_id": geography["geo_id"],
            "geo_level": geography["geo_level"],
            "metric_code": f"CENSUS_ACS:{dataset}:{code}",
            "vintage_year": vintage,
            "observation_date": date(vintage, 1, 1),
            "duration_start": date(vintage - span + 1, 1, 1),
            "duration_end": date(vintage, 12, 31),
            "estimate_value": value,
            "margin_of_error": moe,
            "value_status": "valid",
            "latest": latest,
        }
        for code, (value, moe) in _lines(**kwargs).items()
    ]


@pytest.fixture
def client_for():
    def make(session: _Session) -> TestClient:
        app.dependency_overrides[get_db_session_dep] = lambda: session
        return TestClient(app)

    yield make
    app.dependency_overrides.clear()


def test_a_city_answers_with_its_county_state_and_nation_in_one_request(
    client_for,
) -> None:
    """Covers: API-185"""
    rows = [
        *_rows(MADISON, 2023, bump={"B01001_010": 4000}),
        *_rows(DANE, 2023),
        *_rows(WISCONSIN, 2023),
        *_rows(NATION, 2023),
    ]
    related = [
        {
            "relationship": "intersects",
            "geo_id": DANE["geo_id"],
            "geo_level": "COUNTY",
            "overlap_weight": 0.99,
        }
    ]
    session = _Session(rows, related)
    response = client_for(session).get(
        "/api/v1/age-structure", params={"geo_id": MADISON["geo_id"]}
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["default_reference_geo_id"] == DANE["geo_id"]
    assert [(p["geo_id"], p["role"], p["reference_level"]) for p in body["places"]] == [
        (MADISON["geo_id"], "place", None),
        (DANE["geo_id"], "reference", "COUNTY"),
        ("state:55", "reference", "STATE"),
        ("us:1", "reference", "NATIONAL"),
    ]
    assert body["places"][1]["overlap_weight"] == 0.99
    assert body["places"][3]["geo_name"] == "United States", "never the code"
    assert all(p["release"] == "2023" for p in body["places"])
    assert body["places"][0]["bands"][4]["men"]["count"] == "7000"
    observation_reads = [
        s for s, _ in session.statements if "acs" in s.lower() and "metric_code" in s
    ]
    assert len(observation_reads) == 1, "one statement reads the place and every level"


def test_a_level_without_the_places_release_is_not_published(client_for) -> None:
    """Covers: API-185 — never shown at another release."""
    rows = [*_rows(DANE, 2023), *_rows(WISCONSIN, 2024), *_rows(NATION, 2023)]
    response = client_for(_Session(rows)).get(
        "/api/v1/age-structure", params={"geo_id": DANE["geo_id"]}
    )
    places = response.json()["places"]
    assert places[1]["geo_id"] == "state:55"
    assert places[1]["unavailable_reason"] == "not_published"
    assert places[1]["bands"] == []
    assert places[2]["release"] == "2023"


def test_requests_the_routes_refuse(client_for) -> None:
    """Covers: API-185"""
    client = client_for(_Session([]))
    assert (
        client.get("/api/v1/age-structure", params={"geo_id": "state:99"}).status_code
        == 404
    )
    assert (
        client.get(
            "/api/v1/age-structure",
            params={"geo_id": DANE["geo_id"], "dataset": "acs3"},
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/api/v1/age-structure",
            params={"geo_id": DANE["geo_id"], "release": "soon"},
        ).status_code
        == 422
    )
    too_many = ",".join(["state:55", "us:1", MADISON["geo_id"], TRACT["geo_id"]])
    assert (
        client.get(
            "/api/v1/age-structure",
            params={"geo_id": DANE["geo_id"], "compare_geo_ids": too_many},
        ).status_code
        == 422
    )


# --- History and cohorts ------------------------------------------------------


def test_history_keeps_releases_that_share_no_survey_year(client_for) -> None:
    """Covers: API-186"""
    releases = [_release(DANE, _lines(), vintage=year) for year in range(2009, 2024)]
    assert [r.vintage_year for r in non_overlapping(releases)] == [2023, 2018, 2013]
    rows = [
        row
        for year in range(2009, 2024)
        for row in _rows(DANE, year, latest=year == 2023)
    ]
    client = client_for(_Session(rows))
    body = client.get(
        "/api/v1/age-structure/history", params={"geo_id": DANE["geo_id"]}
    ).json()
    assert [item["release"] for item in body["items"]] == ["2023", "2018", "2013"]
    assert body["items"][0]["bands"] == []
    every = client.get(
        "/api/v1/age-structure/history",
        params={"geo_id": DANE["geo_id"], "periods": "all", "include_bands": True},
    ).json()
    assert len(every["items"]) == 15 and len(every["items"][0]["bands"]) == 18


def test_cohorts_move_one_band_across_periods_five_years_apart(client_for) -> None:
    """Covers: API-186"""
    rows = [
        *_rows(DANE, 2018, latest=False),
        *_rows(DANE, 2023, bump={"B01001_012": 500}),
    ]
    body = (
        client_for(_Session(rows))
        .get(
            "/api/v1/age-structure/cohorts",
            params={
                "geo_id": DANE["geo_id"],
                "from_release": "2018",
                "to_release": "2023",
            },
        )
        .json()
    )
    assert body["comparable"] is True and body["years_apart"] == 5
    assert len(body["items"]) == 16
    item = next(i for i in body["items"] if i["from_band"] == "25-29")
    assert item["to_band"] == "30-34"
    assert item["men"]["change"] == "500"
    assert body["caveat"].startswith("A change between two surveys")


def test_cohorts_are_refused_across_overlapping_periods_other_gaps_and_redrawn_tracts() -> (
    None
):
    """Covers: API-186"""

    def rel(geo, year):
        return _release(geo, _lines(), vintage=year)

    assert (
        cohort_refusal("COUNTY", rel(DANE, 2019), rel(DANE, 2023)).reason
        == "overlapping_periods"
    )
    assert (
        cohort_refusal("COUNTY", rel(DANE, 2013), rel(DANE, 2023)).reason
        == "period_gap_not_five_years"
    )
    assert (
        cohort_refusal("TRACT", rel(TRACT, 2018), rel(TRACT, 2023)).reason
        == "boundary_changed"
    )
    assert cohort_refusal("COUNTY", rel(DANE, 2018), rel(DANE, 2023)) is None
