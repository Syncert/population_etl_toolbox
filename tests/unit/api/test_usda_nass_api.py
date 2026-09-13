"""API unit tests: the USDA NASS crop explorer contract."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.services import usda_nass_service
from apps.api.services.usda_nass_service import (
    AS_RELEASED_RELATION,
    LATEST_RELATION,
    NassObservationFilters,
    NassQueryError,
    NassSeriesFilters,
)
from data_ingestion_toolbox.usda_nass.registry import SUPPRESSION_SYMBOLS

pytestmark = [pytest.mark.unit, pytest.mark.api]


class _FakeResult:
    def __init__(self, rows: list[dict[str, Any]] | None = None, scalar: Any = None):
        self._rows = rows or []
        self._scalar = scalar

    def mappings(self) -> "_FakeResult":
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows

    def scalar(self) -> Any:
        return self._scalar


class _RecordingSession:
    """Session double that records every statement and bound parameter set."""

    def __init__(self, rows: list[dict[str, Any]] | None = None, total: int = 0):
        self.rows = rows or []
        self.total = total
        self.statements: list[str] = []
        self.parameters: list[dict[str, Any]] = []

    def execute(self, statement: Any, parameters: dict[str, Any] | None = None):
        rendered = str(statement)
        self.statements.append(rendered)
        self.parameters.append(dict(parameters or {}))
        if "COUNT(*)" in rendered:
            return _FakeResult(scalar=self.total)
        return _FakeResult(rows=self.rows)


def _observation_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "product_id": "corn_survey_annual",
        "product_label": "Corn survey acreage, yield, and production",
        "release_watermark": "2025-01-10 15:20:33.123000",
        "source_desc": "SURVEY",
        "sector_desc": "CROPS",
        "group_desc": "FIELD CROPS",
        "commodity_desc": "CORN",
        "class_desc": "GRAIN",
        "prodn_practice_desc": "ALL PRODUCTION PRACTICES",
        "util_practice_desc": "ALL UTILIZATION PRACTICES",
        "statisticcat_desc": "PRODUCTION",
        "short_desc": "CORN, GRAIN - PRODUCTION, MEASURED IN BU",
        "unit_desc": "BU",
        "freq_desc": "ANNUAL",
        "value_kind": "quantity",
        "calculation_basis": "provider_published_estimate",
        "additive_behavior": "not_established",
        "additive_behavior_known": False,
        "domain_desc": "TOTAL",
        "domaincat_desc": "NOT SPECIFIED",
        "geo_id": "state:01|county:001",
        "geo_type": "county",
        "geography_status": "resolved",
        "agg_level_desc": "COUNTY",
        "location_desc": "ALABAMA, AUTAUGA",
        "state_fips": "01",
        "county_fips": "001",
        "year": 2024,
        "reference_period_desc": "YEAR",
        "week_ending": None,
        "value_source": "2,659,000",
        "value": "2659000",
        "value_status": "valid",
        "suppression_code": None,
        "cv_source": "11.3",
        "cv_value": "11.3",
        "cv_status": "valid",
        "cv_symbol": None,
        "load_time": "2025-01-10T15:20:33.123000",
        "methodology_url": "https://www.nass.usda.gov/",
        "release_expectation": "survey_estimates_revised_until_final",
        "source_record_id": "a" * 64,
    }
    row.update(overrides)
    return row


def _client(session: _RecordingSession) -> TestClient:
    app.dependency_overrides[get_db_session_dep] = lambda: session
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


def test_observations_expose_the_complete_source_classification() -> None:
    """Covers: API-013 — a crop observation carries its whole classification."""
    session = _RecordingSession(rows=[_observation_row()], total=1)
    response = _client(session).get("/api/v1/usda-nass/observations")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["release_scope"] == "as_released"
    item = body["items"][0]
    for field in (
        "commodity_desc",
        "class_desc",
        "statisticcat_desc",
        "short_desc",
        "unit_desc",
        "source_desc",
        "domain_desc",
        "domaincat_desc",
        "geo_id",
        "agg_level_desc",
        "year",
        "freq_desc",
        "release_watermark",
        "load_time",
        "cv_source",
        "value_status",
        "additive_behavior",
    ):
        assert field in item, field
    assert item["unit_desc"] == "BU"
    assert item["value_source"] == "2,659,000"


def test_a_suppressed_value_can_never_be_read_as_zero() -> None:
    """Covers: API-013 — suppression is explicit and never a numeric zero."""
    session = _RecordingSession(
        rows=[
            _observation_row(
                value_source="(D)",
                value=None,
                value_status="withheld",
                suppression_code="(D)",
                cv_source="(D)",
                cv_value=None,
                cv_status="withheld",
                cv_symbol="(D)",
            )
        ],
        total=1,
    )
    item = _client(session).get("/api/v1/usda-nass/observations").json()["items"][0]

    assert item["value"] is None
    assert item["value_status"] == "withheld"
    assert item["suppression_code"] == "(D)"
    assert item["value_source"] == "(D)"
    assert item["cv_value"] is None


def test_multidimensional_filters_are_bound_not_interpolated() -> None:
    """Covers: API-014 — every caller filter is a bound query parameter."""
    session = _RecordingSession(rows=[], total=0)
    response = _client(session).get(
        "/api/v1/usda-nass/observations",
        params={
            "commodity_desc": "CORN",
            "statisticcat_desc": "YIELD",
            "unit_desc": "BU / ACRE",
            "source_desc": "SURVEY",
            "domain_desc": "TOTAL",
            "agg_level_desc": "COUNTY",
            "geo_id": "state:01|county:001",
            "year_start": 2022,
            "year_end": 2024,
            "release_watermark": "2025-01-10 15:20:33.123000",
        },
    )

    assert response.status_code == 200
    bound = session.parameters[0]
    assert bound["commodity_desc"] == "CORN"
    assert bound["statisticcat_desc"] == "YIELD"
    assert bound["unit_desc"] == "BU / ACRE"
    assert bound["agg_level_desc"] == "COUNTY"
    assert bound["geo_id"] == "state:01|county:001"
    assert bound["year_start"] == 2022 and bound["year_end"] == 2024
    for statement in session.statements:
        assert "CORN" not in statement
        assert "state:01|county:001" not in statement
        assert ":commodity_desc" in statement or "COUNT(*)" in statement


def test_a_sql_injection_attempt_stays_a_bound_literal() -> None:
    """Covers: API-014 — a hostile filter value never becomes SQL."""
    session = _RecordingSession(rows=[], total=0)
    hostile = "CORN'; DROP TABLE silver_nass.fact_crop_observation; --"
    response = _client(session).get(
        "/api/v1/usda-nass/observations", params={"commodity_desc": hostile}
    )

    assert response.status_code == 200
    assert session.parameters[0]["commodity_desc"] == hostile
    for statement in session.statements:
        assert "DROP TABLE" not in statement


def test_latest_and_as_released_read_from_different_relations() -> None:
    """Covers: API-013 — latest and as-released are distinct contracts."""
    as_released = NassObservationFilters()
    latest = NassObservationFilters(latest_release_only=True)

    assert as_released.relation == AS_RELEASED_RELATION
    assert latest.relation == LATEST_RELATION

    session = _RecordingSession(rows=[_observation_row()], total=1)
    body = (
        _client(session)
        .get("/api/v1/usda-nass/observations", params={"latest": "true"})
        .json()
    )
    assert body["release_scope"] == "latest"
    assert all(LATEST_RELATION in statement for statement in session.statements)


@pytest.mark.parametrize(
    ("params", "message"),
    [
        ({"year_start": 2024, "year_end": 2022}, "year_start must be"),
        ({"agg_level_desc": "AGRICULTURAL DISTRICT"}, "agg_level_desc must be"),
        ({"source_desc": "ADMIN"}, "source_desc must be"),
        (
            {"latest": "true", "release_watermark": "2025-01-10 15:20:33.123000"},
            "cannot be combined",
        ),
    ],
)
def test_contradictory_or_unmodeled_filters_are_rejected(
    params: dict[str, Any], message: str
) -> None:
    """Covers: API-007 — contradictory or unmodeled filters fail explicitly."""
    session = _RecordingSession()
    response = _client(session).get("/api/v1/usda-nass/observations", params=params)

    assert response.status_code == 422
    assert message in str(response.json())
    assert session.statements == []


def test_filter_validation_is_pure_and_reusable() -> None:
    """Covers: API-007 — filter validation needs no database session."""
    with pytest.raises(NassQueryError, match="year_start must be"):
        NassObservationFilters(year_start=2025, year_end=2024)
    with pytest.raises(NassQueryError, match="agg_level_desc must be"):
        NassSeriesFilters(agg_level_desc="WATERSHED")


def test_series_expose_stable_identity_and_value_completeness() -> None:
    """Covers: API-013 — a series reports its own numeric completeness."""
    row = {
        "series_id": "f" * 32,
        "product_id": "corn_survey_annual",
        "source_desc": "SURVEY",
        "sector_desc": "CROPS",
        "group_desc": "FIELD CROPS",
        "commodity_desc": "CORN",
        "class_desc": "GRAIN",
        "prodn_practice_desc": "ALL PRODUCTION PRACTICES",
        "util_practice_desc": "ALL UTILIZATION PRACTICES",
        "statisticcat_desc": "YIELD",
        "short_desc": "CORN, GRAIN - YIELD, MEASURED IN BU / ACRE",
        "unit_desc": "BU / ACRE",
        "value_kind": "rate",
        "additive_behavior": "non_additive",
        "additive_behavior_known": True,
        "domain_desc": "TOTAL",
        "domaincat_desc": "NOT SPECIFIED",
        "geo_id": "state:01",
        "geo_type": "state",
        "agg_level_desc": "STATE",
        "freq_desc": "ANNUAL",
        "first_year": 2022,
        "last_year": 2024,
        "observation_count": 3,
        "numeric_observation_count": 2,
        "non_numeric_observation_count": 1,
        "latest_release_watermark": "2025-01-10 15:20:33.123000",
    }
    session = _RecordingSession(rows=[row], total=1)
    body = _client(session).get("/api/v1/usda-nass/series").json()

    item = body["items"][0]
    assert item["series_id"] == row["series_id"]
    assert item["additive_behavior"] == "non_additive"
    assert item["additive_behavior_known"] is True
    assert (
        item["numeric_observation_count"] + item["non_numeric_observation_count"]
        == item["observation_count"]
    )


def test_measures_expose_exact_units_and_declared_additivity() -> None:
    """Covers: API-013 — the measure export never hides a unit."""
    row = {
        "source_dataset": "hay_survey_annual",
        "source_measure_code": "b" * 64,
        "display_name": "HAY - YIELD, MEASURED IN TONS / ACRE",
        "statisticcat_desc": "YIELD",
        "unit": "TONS / ACRE",
        "freq_desc": "ANNUAL",
        "value_kind": "rate",
        "calculation_basis": "provider_published_ratio",
        "additive_behavior": "non_additive",
        "additive_behavior_known": True,
        "source_program": "SURVEY",
        "source_watermark": "2025-01-10 15:20:34.001000",
        "methodology_url": "https://www.nass.usda.gov/",
        "schema_version": "quickstats-crop-v1",
    }
    session = _RecordingSession(rows=[row])
    body = _client(session).get("/api/v1/usda-nass/measures").json()

    assert body["total"] == 1
    assert body["items"][0]["unit"] == "TONS / ACRE"
    assert body["items"][0]["additive_behavior"] == "non_additive"


def test_source_notes_are_derived_from_the_ingested_contract() -> None:
    """Covers: API-013 — source notes cannot drift from the registry."""
    session = _RecordingSession()
    body = _client(session).get("/api/v1/usda-nass/source-notes").json()

    topics = {item["topic"]: item for item in body["items"]}
    assert {
        "units",
        "suppression",
        "release_status",
        "source_program",
        "county_coverage",
        "aggregation",
    } <= set(topics)
    suppression_detail = topics["suppression"]["detail"]
    for symbol in SUPPRESSION_SYMBOLS:
        assert symbol in suppression_detail
    assert "never zero" in topics["suppression"]["summary"]
    assert "BU / ACRE" in topics["units"]["detail"]
    assert "TONS" in topics["units"]["detail"]
    assert "SURVEY" in topics["source_program"]["detail"]
    assert "CENSUS" in topics["source_program"]["detail"]
    assert "COUNTY" in topics["county_coverage"]["detail"]
    assert session.statements == []


def test_source_notes_report_every_registered_symbol_state() -> None:
    """Covers: API-013 — the published symbol table matches the parser."""
    notes = usda_nass_service.source_notes()
    detail = next(item for item in notes.items if item.topic == "suppression").detail

    assert "(Z) = below_rounding_unit" in detail
    assert "(D) = withheld" in detail
    assert notes.total == len(notes.items)


#: The column that makes each NASS list statement's order total, and why it
#: is unique in the relation the statement reads.
NASS_TIE_BREAKERS = {
    "/api/v1/usda-nass/observations": (
        "observation_sk",
        "the BIGSERIAL primary key of silver_nass.fact_crop_observation, "
        "carried through gold_nass.crop_observation and the latest-release view",
    ),
    "/api/v1/usda-nass/series": (
        "series_id",
        "an MD5 over the exact tuple gold_nass.crop_series groups by",
    ),
}


@pytest.mark.parametrize("path", sorted(NASS_TIE_BREAKERS))
def test_nass_list_routes_page_a_total_order(path: str) -> None:
    """Covers: API-080 — a tie a page boundary can fall inside is a repeat.

    The Quick Stats grain is multidimensional, which is what these routes
    exist to preserve: a commodity published across several domain categories
    answers several rows carrying one ``short_desc``. Ordering by a list that
    cannot separate them left the page boundary to PostgreSQL, which promises
    nothing about it. CDC's queries already end in ``observation_sk`` with a
    comment saying exactly this.
    """
    session = _RecordingSession(rows=[], total=0)
    response = _client(session).get(path, params={"limit": 5, "offset": 10})

    assert response.status_code == 200
    listing = [
        statement
        for statement in session.statements
        if "ORDER BY" in statement and "COUNT(*)" not in statement
    ]
    assert listing, "no list statement was issued"
    column, _why = NASS_TIE_BREAKERS[path]
    rendered = " ".join(listing[0].split())
    order = rendered.split("ORDER BY", 1)[1].split("LIMIT", 1)[0].strip()
    assert order.endswith(column), order


# ---------------------------------------------------------------------------
# API-116 — the grain parameter speaks the published vocabulary
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("requested", "bound"),
    [
        ("COUNTY", "COUNTY"),
        # The catalog's word in another case. A grain read from the catalog
        # can be sent straight back, which is what the guide promises.
        ("county", "COUNTY"),
        ("  State  ", "STATE"),
        ("NATIONAL", "NATIONAL"),
        # The two aliases the guide guarantees for the national grain.
        ("NATION", "NATIONAL"),
        ("us", "NATIONAL"),
    ],
)
def test_the_grain_parameter_takes_the_vocabulary_in_any_case(
    requested: str, bound: str
) -> None:
    """Covers: API-116 — `agg_level_desc` is the grain under NASS's own name.

    API-092 and API-094 made every `geo_level` parameter normalise its value.
    This route takes the grain under another name and was not swept: it
    compared the request to the upper-case registry words by exact match, so
    `county` was a 422 and so was `NATION`, which the guide guarantees.
    """
    session = _RecordingSession()
    response = _client(session).get(
        "/api/v1/usda-nass/observations", params={"agg_level_desc": requested}
    )

    assert response.status_code == 200, response.text
    bound_values = {
        parameters.get("agg_level_desc")
        for parameters in session.parameters
        if "agg_level_desc" in parameters
    }
    assert bound_values == {bound}


def test_an_unknown_grain_is_refused_with_the_vocabulary() -> None:
    """Covers: API-116 — the refusal names the words, not NASS's own list."""
    session = _RecordingSession()
    response = _client(session).get(
        "/api/v1/usda-nass/observations", params={"agg_level_desc": "WATERSHED"}
    )

    assert response.status_code == 422
    detail = str(response.json())
    assert "NATIONAL, STATE, COUNTY" in detail
    assert session.statements == []
