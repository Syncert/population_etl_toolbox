"""API unit tests: one measure in two places, at any geographic level.

Covers: API-175 (mixed grains are accepted; a median, mean, rate, share or
        index compares across levels while a count is refused with
        `counts_differ_in_size` and the served per-capita views, and a
        different code on side b is refused by program, universe, unit or
        measure; every measure's kind is declared in the definitions
        registry), API-176 (each side's period is the newest both publish or
        each its own newest named `periods_differ`; a missing value is not
        published, never zero; containment is disclosed and no difference is
        computed between a place and its container; overlapping 90 percent
        intervals are said, with no verdict).
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.schemas.observations import NeutralObservation, ObservationUncertainty
from apps.api.services import cross_level_service
from apps.api.services.comparison_service import UnknownAnalysisMetric
from apps.api.services.cross_level_service import (
    choose_periods,
    by_period,
    code_refusal,
    containment_between,
    intervals_overlap,
    interval,
)
from data_ingestion_toolbox.semantics.definitions import (
    KINDS,
    REVIEWED_ONLY_ENV,
    DefinitionError,
    measure_kind,
    parse_registry,
    registry,
)
from data_ingestion_toolbox.semantics import derived_ratios
from data_ingestion_toolbox.semantics.derived_ratios import SERVE_DRAFTS_ENV

pytestmark = [pytest.mark.unit, pytest.mark.api]

INCOME = "CENSUS_ACS:acs5:B19013_001"
INCOME_1YR = "CENSUS_ACS:acs1:B19013_001"
POVERTY = "CENSUS_ACS:acs5:B17001_002"
POVERTY_UNIVERSE = "CENSUS_ACS:acs5:B17001_001"
POPULATION = "CENSUS_PEP:POPESTIMATE"

AUSTIN = {"geo_id": "state:48|place:05000", "geo_level": "PLACE", "geo_name": "Austin"}
TRAVIS = {"geo_id": "state:48|county:453", "geo_level": "COUNTY", "geo_name": "Travis"}
DALLAS = {"geo_id": "state:48|county:113", "geo_level": "COUNTY", "geo_name": "Dallas"}
TEXAS = {"geo_id": "state:48", "geo_level": "STATE", "geo_name": "Texas"}
NATION = {"geo_id": "us:1", "geo_level": "NATIONAL", "geo_name": "United States"}
GEOGRAPHIES = {g["geo_id"]: g for g in (AUSTIN, TRAVIS, DALLAS, TEXAS, NATION)}


def _row(
    metric_code: str,
    geography: dict,
    value: str | None,
    moe: str | None = None,
    period: str = "2019-01-01",
    end: str = "2023-12-31",
    status: str | None = "valid",
) -> NeutralObservation:
    return NeutralObservation(
        source_code=metric_code.split(":")[0],
        metric_code=metric_code,
        geo_id=geography["geo_id"],
        geo_level=geography["geo_level"],
        period_start=period,
        period_end=end,
        release="2023",
        value=value,
        value_status=status,
        uncertainty=ObservationUncertainty(margin_of_error=moe),
    )


# ---------------------------------------------------------------------------
# API-175 — kinds and refusals, pure
# ---------------------------------------------------------------------------


def test_every_definition_declares_a_kind() -> None:
    """Covers: API-175 — ADR-0010 rule 2 reads the kind from the definition."""
    undeclared = sorted(
        {d.metric_id for d in registry().values() if d.kind not in KINDS}
    )
    assert undeclared == []
    assert measure_kind(INCOME) == ("median", "draft")
    assert measure_kind(POVERTY)[0] == "count"


def test_a_metric_without_a_kind_is_a_count(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: API-175 — the conservative default, and the reviewed-only switch."""
    assert measure_kind("NOT:A:METRIC") == ("count", "undeclared")
    monkeypatch.setenv(REVIEWED_ONLY_ENV, "1")
    assert measure_kind(INCOME) == ("count", "undeclared")


def test_the_registry_refuses_an_unknown_kind() -> None:
    """Covers: API-175 — a kind outside the vocabulary is a registry error."""
    entry = {
        "metric_id": "X:Y",
        "metric_codes": ["X:Y"],
        "name": "Y",
        "definition": "Y.",
        "intended_use": "Y.",
        "limitations": [],
        "status": "draft",
        "owner": "o",
        "reviewer": "r",
        "version": 1,
        "effective_date": "2026-10-31",
        "last_reviewed_date": "2026-10-31",
        "citations": ["https://example.org"],
        "kind": "total",
    }
    with pytest.raises(DefinitionError, match="kind"):
        parse_registry({"entries": [entry]})


@pytest.mark.parametrize(
    ("code_b", "reason"),
    [
        (INCOME, None),
        (INCOME_1YR, "programs_differ"),
        ("CENSUS_ACS:acs5:B01002_001", "universes_differ"),
        ("CENSUS_ACS:acs5:B25077_001", "universes_differ"),
        ("CENSUS_SAIPE_SAHIE:saipe:SAEMHI", "programs_differ"),
        ("CENSUS_ACS:acs5:B19301_001", "universes_differ"),
    ],
)
def test_a_different_code_on_side_b_says_why(code_b: str, reason: str | None) -> None:
    """Covers: API-175 — one measure from one program, or a named refusal."""
    refusal = code_refusal(INCOME, code_b)
    assert (refusal.reason if refusal else None) == reason


def test_two_unrelated_measures_are_different_measures() -> None:
    """Covers: API-175 — the route compares one measure; /comparison compares two."""
    refusal = code_refusal("CENSUS_PEP:BIRTHS", "CENSUS_PEP:DEATHS")
    assert refusal is not None and refusal.reason == "different_measures"
    assert "/comparison" in refusal.detail


# ---------------------------------------------------------------------------
# API-176 — periods, intervals and containment, pure
# ---------------------------------------------------------------------------


def _candidate(period: str, end: str, value: str = "1") -> dict[str, Any]:
    return {"period_start": period, "period_end": end, "value": value}


def test_the_newest_common_period_is_chosen() -> None:
    """Covers: API-176 — both sides on the newest period they share."""
    a = by_period(
        [_candidate("2020-01-01", "2024-12-31"), _candidate("2019-01-01", "2023-12-31")]
    )
    b = by_period([_candidate("2019-01-01", "2023-12-31")])
    chosen_a, chosen_b, basis = choose_periods(a, b, None)
    assert basis == "newest_common"
    assert chosen_a["period_end"] == chosen_b["period_end"] == "2023-12-31"


def test_without_a_common_period_each_side_keeps_its_own() -> None:
    """Covers: API-176 — never aligned silently."""
    a = by_period([_candidate("2024-01-01", "2024-12-31")])
    b = by_period([_candidate("2023-01-01", "2023-12-31")])
    chosen_a, chosen_b, basis = choose_periods(a, b, None)
    assert basis == "each_newest"
    assert chosen_a["period_start"] == "2024-01-01"
    assert chosen_b["period_start"] == "2023-01-01"


def test_a_requested_year_names_the_periods_starting_in_it() -> None:
    """Covers: API-176 — `period_start=2019` matches 2019-01-01 on both sides."""
    a = by_period([_candidate("2019-01-01", "2023-12-31")])
    b = by_period([_candidate("2018-01-01", "2022-12-31")])
    chosen_a, chosen_b, basis = choose_periods(a, b, "2019")
    assert basis == "requested"
    assert chosen_a is not None and chosen_b is None


def test_intervals_are_the_value_plus_and_minus_its_margin() -> None:
    """Covers: API-176 — the 90 percent interval, and overlap read from it."""
    a = interval({"value": "100", "margin_of_error": "10", "value_status": "valid"})
    b = interval({"value": "115", "margin_of_error": "4", "value_status": None})
    c = interval({"value": "105", "margin_of_error": "-6", "value_status": "valid"})
    assert a is not None and str(a[0]) == "90" and str(a[1]) == "110"
    assert intervals_overlap(a, b) is False
    assert intervals_overlap(a, c) is True
    assert interval({"value": "1", "margin_of_error": None}) is None
    assert interval({"value": None, "margin_of_error": "1"}) is None


def test_containment_reads_the_finer_sides_relationships() -> None:
    """Covers: API-176 — part_of from the finer side, flipped for side b."""
    county_row = {
        "relationship": "part_of",
        "geo_id": TEXAS["geo_id"],
        "geography_vintage": 2024,
        "evidence_source": "census_codes",
    }
    contained = containment_between(TEXAS, TRAVIS, [county_row], rows_from="b")
    assert contained.relationship == "contains"
    assert contained.smaller_side == "b"
    assert contained.share_of_smaller_inside_larger == 1.0

    overlap_row = {
        "relationship": "intersects",
        "geo_id": TRAVIS["geo_id"],
        "overlap_weight": 0.93,
        "geography_vintage": 2024,
        "evidence_source": "tiger_overlay",
    }
    overlap = containment_between(AUSTIN, TRAVIS, [overlap_row], rows_from="a")
    assert overlap.relationship == "intersects"
    assert overlap.smaller_side == "a"
    assert overlap.share_of_smaller_inside_larger == 0.93

    assert containment_between(TRAVIS, DALLAS, [], "a").relationship == (
        "none_recorded"
    )
    nation = containment_between(TRAVIS, NATION, [], "a")
    assert (nation.relationship, nation.smaller_side) == ("part_of", "a")


# ---------------------------------------------------------------------------
# The route
# ---------------------------------------------------------------------------


@pytest.fixture
def client():
    def _override():
        yield object()

    app.dependency_overrides[get_db_session_dep] = _override
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.clear()


@pytest.fixture
def served(monkeypatch: pytest.MonkeyPatch):
    """Stub every read: geographies, relationships, metrics and rows."""
    rows: dict[str, list[NeutralObservation]] = {}
    relationships: dict[tuple[str, str], dict] = {}
    reads: list[tuple[str, tuple[str, ...]]] = []

    def geographies(db, geo_id_a, geo_id_b):
        for parameter, geo_id in (("geo_id_a", geo_id_a), ("geo_id_b", geo_id_b)):
            if geo_id not in GEOGRAPHIES:
                raise cross_level_service.UnknownGeography(parameter)
        return GEOGRAPHIES[geo_id_a], GEOGRAPHIES[geo_id_b]

    def containment(db, a, b):
        match = relationships.get((a["geo_id"], b["geo_id"]))
        return containment_between(a, b, [match] if match else [], "a")

    def resolved(db, code, parameter):
        if code.startswith("NOT:"):
            raise UnknownAnalysisMetric(parameter)
        return {"metric_code": code, "metric_display_name": code}

    def observations(db, code, scope, release, geo_ids):
        reads.append((code, tuple(geo_ids)))
        return [row for row in rows.get(code, []) if row.geo_id in geo_ids]

    monkeypatch.setattr(cross_level_service, "_geographies", geographies)
    monkeypatch.setattr(cross_level_service, "_containment", containment)
    monkeypatch.setattr(cross_level_service, "_resolved", resolved)
    monkeypatch.setattr(
        cross_level_service, "observations_for_geographies", observations
    )
    return rows, relationships, reads


def _as_drafts(monkeypatch: pytest.MonkeyPatch) -> None:
    """The shipped pairings, each set back to draft (Nick approved them all)."""
    drafts = {
        ratio_id: dataclasses.replace(pairing, status="draft")
        for ratio_id, pairing in derived_ratios.load_registry().items()
    }
    monkeypatch.setattr(derived_ratios, "load_registry", lambda *args: drafts)


def _get(client, **params):
    return client.get("/api/v1/comparison/places", params=params)


def test_a_median_compares_a_city_with_its_county(client, served) -> None:
    """Covers: API-175, API-176 — mixed grains accepted; containment, no difference."""
    rows, relationships, reads = served
    rows[INCOME] = [
        _row(INCOME, AUSTIN, "91461", "1200"),
        _row(INCOME, TRAVIS, "97169", "900"),
    ]
    relationships[(AUSTIN["geo_id"], TRAVIS["geo_id"])] = {
        "relationship": "intersects",
        "geo_id": TRAVIS["geo_id"],
        "overlap_weight": 0.9,
        "geography_vintage": 2024,
        "evidence_source": "tiger_overlay",
    }
    body = _get(
        client, metric_code=INCOME, geo_id_a=AUSTIN["geo_id"], geo_id_b=TRAVIS["geo_id"]
    ).json()
    assert body["comparable"] is True
    assert body["cross_level"] is True
    assert body["measure_kind"] == "median"
    assert body["side_a"]["value"] == "91461"
    assert body["side_a"]["interval_low"] == "90261"
    assert body["side_b"]["geo_level"] == "COUNTY"
    assert body["side_a"]["metric_label"]
    assert body["period_basis"] == "newest_common"
    assert body["containment"]["relationship"] == "intersects"
    assert body["difference"] is None
    assert body["intervals_overlap"] is False
    reasons = {note["reason"] for note in body["notes"]}
    assert "overlapping_places" in reasons
    assert "intervals_overlap" not in reasons
    assert reads == [(INCOME, (AUSTIN["geo_id"], TRAVIS["geo_id"]))]
    assert "winner" not in str(body).lower() and "higher" not in str(body).lower()


def test_a_count_across_levels_is_refused_with_its_per_capita_view(
    client, served, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-175 — counts_differ_in_size steers to ADR-0009, nothing read."""
    monkeypatch.setenv(SERVE_DRAFTS_ENV, "1")
    _, _, reads = served
    response = _get(
        client,
        metric_code=POVERTY,
        geo_id_a=TRAVIS["geo_id"],
        geo_id_b=TEXAS["geo_id"],
    )
    assert response.status_code == 200
    body = response.json()
    assert body["comparable"] is False
    assert body["refusal"]["reason"] == "counts_differ_in_size"
    alternatives = body["refusal"]["alternatives"]
    assert [a["ratio_id"] for a in alternatives] == ["poverty_share:acs5"]
    assert alternatives[0]["href"].startswith("/api/v1/comparison/places?ratio_id=")
    assert body["side_a"]["published"] is None and body["side_a"]["value"] is None
    assert body["difference"] is None
    assert reads == []


def test_a_refused_count_with_no_served_view_says_so(
    client, served, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-175 — a draft pairing is not offered unless drafts are served."""
    _as_drafts(monkeypatch)
    monkeypatch.delenv(SERVE_DRAFTS_ENV, raising=False)
    body = _get(
        client, metric_code=POVERTY, geo_id_a=TRAVIS["geo_id"], geo_id_b=TEXAS["geo_id"]
    ).json()
    assert body["refusal"]["alternatives"] == []
    assert "derived_ratios.json" in body["refusal"]["detail"]


def test_a_count_between_two_counties_is_served_with_a_note(client, served) -> None:
    """Covers: API-175 — same level is not cross-level; two independent places differ."""
    rows, _, _ = served
    rows[POPULATION] = [
        _row(POPULATION, TRAVIS, "1334961", period="2024-07-01", end="2024-07-01"),
        _row(POPULATION, DALLAS, "2653727", period="2024-07-01", end="2024-07-01"),
    ]
    body = _get(
        client,
        metric_code=POPULATION,
        geo_id_a=TRAVIS["geo_id"],
        geo_id_b=DALLAS["geo_id"],
    ).json()
    assert body["comparable"] is True and body["cross_level"] is False
    assert body["difference"] == "-1318766"
    assert body["derivations"][0] == "difference"
    assert body["intervals_overlap"] is None
    reasons = {note["reason"] for note in body["notes"]}
    assert {"counts_differ_in_size", "no_margin_published"} <= reasons


def test_a_ratio_compares_a_county_with_the_nation(
    client, served, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-175 — an ADR-0009 share compares across levels, derived."""
    monkeypatch.setenv(SERVE_DRAFTS_ENV, "1")
    rows, _, _ = served
    rows[POVERTY] = [
        _row(POVERTY, TRAVIS, "150000", "5000"),
        _row(POVERTY, NATION, "40000000", "200000"),
    ]
    rows[POVERTY_UNIVERSE] = [
        _row(POVERTY_UNIVERSE, TRAVIS, "1300000", "1000"),
        _row(POVERTY_UNIVERSE, NATION, "320000000", "100000"),
    ]
    body = _get(
        client,
        ratio_id="poverty_share:acs5",
        geo_id_a=TRAVIS["geo_id"],
        geo_id_b=NATION["geo_id"],
    ).json()
    assert body["comparable"] is True and body["derived"] is True
    assert body["measure_kind"] == "share"
    assert body["side_a"]["value"] == "11.54"
    assert body["side_b"]["value"] == "12.50"
    assert body["containment"]["relationship"] == "part_of"
    assert body["difference"] is None
    assert any(note["reason"] == "contained" for note in body["notes"])


def test_periods_that_differ_are_named_and_not_differenced(client, served) -> None:
    """Covers: API-176 — each side's own newest, with periods_differ."""
    rows, _, _ = served
    rows[INCOME] = [
        _row(INCOME, TRAVIS, "97169", "900", period="2020-01-01", end="2024-12-31"),
        _row(INCOME, DALLAS, "70000", "800"),
    ]
    body = _get(
        client, metric_code=INCOME, geo_id_a=TRAVIS["geo_id"], geo_id_b=DALLAS["geo_id"]
    ).json()
    assert body["period_basis"] == "each_newest"
    assert body["side_a"]["period_end"] == "2024-12-31"
    assert body["side_b"]["period_end"] == "2023-12-31"
    assert body["difference"] is None and body["intervals_overlap"] is None
    assert any(note["reason"] == "periods_differ" for note in body["notes"])


def test_a_missing_or_withheld_side_is_not_published_never_zero(client, served) -> None:
    """Covers: API-176 — no value is not zero, and no difference is made of it."""
    rows, _, _ = served
    rows[INCOME] = [_row(INCOME, TRAVIS, None, None, status="suppressed")]
    body = _get(
        client, metric_code=INCOME, geo_id_a=TRAVIS["geo_id"], geo_id_b=DALLAS["geo_id"]
    ).json()
    assert body["side_a"]["published"] is True
    assert body["side_a"]["value"] is None
    assert body["side_b"]["published"] is False
    assert body["difference"] is None
    assert body["comparable"] is False
    assert body["refusal"]["reason"] == "not_published"
    assert "withheld" in body["refusal"]["detail"]
    assert "no published value for Dallas" in body["refusal"]["detail"]
    notes = [note for note in body["notes"] if note["reason"] == "not_published"]
    assert len(notes) == 2 and all("not zero" in note["detail"] for note in notes)


def test_a_series_the_nation_does_not_publish_is_not_compared(
    client, served, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-176 — LAUS has no national row: refused, and named "United States".

    The catalog's nation row carries its code as its name; the answer must
    neither print "us:1" nor call one value beside nothing comparable.
    """
    rows, _, _ = served
    nation = {**NATION, "geo_name": "us:1"}
    monkeypatch.setitem(GEOGRAPHIES, NATION["geo_id"], nation)
    rate = "BLS:LAU:UNEMP_RATE"
    rows[rate] = [
        _row(rate, TRAVIS, "3.7", None, period="2026-09-01", end="2026-09-30")
    ]
    body = _get(
        client, metric_code=rate, geo_id_a=TRAVIS["geo_id"], geo_id_b=NATION["geo_id"]
    ).json()
    assert body["comparable"] is False
    assert body["refusal"]["reason"] == "not_published"
    assert body["refusal"]["detail"].startswith(
        "there is no published value for United States in this series"
    )
    assert body["side_b"]["geo_name"] == "United States"
    assert body["side_b"]["published"] is False
    assert body["side_a"]["value"] == "3.7"
    assert body["difference"] is None and body["intervals_overlap"] is None


def test_overlapping_intervals_are_not_distinguishable(client, served) -> None:
    """Covers: API-176 — overlap reported, difference shown without a verdict."""
    rows, _, _ = served
    rows[INCOME] = [
        _row(INCOME, TRAVIS, "70500", "900"),
        _row(INCOME, DALLAS, "70000", "800"),
    ]
    body = _get(
        client, metric_code=INCOME, geo_id_a=TRAVIS["geo_id"], geo_id_b=DALLAS["geo_id"]
    ).json()
    assert body["intervals_overlap"] is True
    assert body["difference"] == "500"
    assert any(
        "not distinguishable at 90 percent confidence" in note["detail"]
        for note in body["notes"]
    )


def test_a_different_program_on_side_b_is_refused(client, served) -> None:
    """Covers: API-175 — the ACS 1-year beside the 5-year is programs_differ."""
    body = _get(
        client,
        metric_code=INCOME,
        metric_code_b=INCOME_1YR,
        geo_id_a=AUSTIN["geo_id"],
        geo_id_b=TRAVIS["geo_id"],
    ).json()
    assert body["comparable"] is False
    assert body["refusal"]["reason"] == "programs_differ"


@pytest.mark.parametrize(
    ("params", "status", "detail"),
    [
        ({"geo_id_a": "state:99|county:999"}, 404, "geo_id_a not found"),
        ({"metric_code": "NOT:A:METRIC"}, 404, "metric_code not found"),
        ({"metric_code": None, "ratio_id": "nope"}, 404, "ratio_id not found"),
        ({"ratio_id": "poverty_share:acs5"}, 422, "exactly one"),
        ({"metric_code": None}, 422, "exactly one"),
        ({"geo_id_b": TRAVIS["geo_id"]}, 422, "same place"),
        ({"period_start": "20x9"}, 422, "period_start"),
    ],
)
def test_the_route_refuses_what_it_cannot_answer(
    client, served, params, status, detail
) -> None:
    """Covers: API-175 — unknown names are 404, malformed requests 422."""
    request = {
        "metric_code": INCOME,
        "geo_id_a": TRAVIS["geo_id"],
        "geo_id_b": DALLAS["geo_id"],
        **params,
    }
    response = _get(client, **{k: v for k, v in request.items() if v is not None})
    assert response.status_code == status
    assert detail in response.json()["detail"]


def test_a_draft_pairing_is_refused_unless_drafts_are_served(
    client, served, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: API-175 — the ADR-0009 serving gate applies here too."""
    _as_drafts(monkeypatch)
    monkeypatch.delenv(SERVE_DRAFTS_ENV, raising=False)
    response = _get(
        client,
        ratio_id="poverty_share:acs5",
        geo_id_a=TRAVIS["geo_id"],
        geo_id_b=DALLAS["geo_id"],
    )
    assert response.status_code == 422
    assert "not approved" in response.json()["detail"]
