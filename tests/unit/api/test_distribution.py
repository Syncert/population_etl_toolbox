"""API unit tests: the registry-dispatched distribution endpoint.

Covers: API-003 (required metric input), API-014 (distribution bins),
        API-052 (distribution dispatches to the metric's owning source's
        latest relation with one newest value per geography, declines
        stratified sources with their declared restriction, answers a
        stable 404 for unknown codes, and labels its bins as API-derived).
"""

from __future__ import annotations

import re
from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.registry import ALLOWED_OBSERVATION_RELATIONS

pytestmark = [pytest.mark.unit, pytest.mark.api]

_FRED_METRIC = {
    "metric_code": "FRED:UNRATE",
    "source_code": "FRED",
    "units": "Percent",
    "valid_time_grains": ["MONTHLY"],
    "valid_geo_grains": ["NATIONAL"],
    "aggregation_characteristic": None,
    "physical_lineage": {},
}

_NASS_METRIC = {
    "metric_code": "USDA_NASS:corn_survey_annual:41",
    "source_code": "USDA_NASS",
    "units": "BU",
    "valid_time_grains": ["ANNUAL"],
    "valid_geo_grains": ["COUNTY"],
    "aggregation_characteristic": "not_established",
    "physical_lineage": {
        "schema": "gold_nass",
        "relation": "crop_observation",
        "product_id": "corn_survey_annual",
        "statistic_sk": 41,
        "statisticcat_desc": "PRODUCTION",
        "unit_desc": "BU",
    },
}


class _FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = rows or []
        self._scalar_value = scalar_value

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def one(self):
        return self._rows[0]

    def scalar(self):
        return self._scalar_value


class _DistributionSession:
    """Resolves the glossary lookup, then records dispatched queries."""

    def __init__(self, metric_row=None, stats=None, bins=None):
        self._metric_row = metric_row
        # The period columns the same statement measures, so the harness
        # models the answer the service actually reads (API-097).
        self._stats = stats or {
            "total": 3,
            "min_value": 10.0,
            "max_value": 40.0,
            "period_count": 1,
            "binned_period": "2023-01-01",
        }
        self._bins = bins
        self.statements: list[str] = []
        self.parameters: list[dict[str, Any]] = []

    def execute(self, query, params=None):
        rendered = str(query)
        self.statements.append(rendered)
        self.parameters.append(dict(params or {}))
        if "gold_glossary.dim_metric" in rendered:
            return _FakeResult(rows=[self._metric_row] if self._metric_row else [])
        if "width_bucket" in rendered:
            # One statement answers the range and the bins together, so every
            # row carries the stats and its own bin; a measure with nothing
            # published still answers one row, with a null bin (API-084).
            bin_count = int((params or {})["bin_count"])
            bins = self._bins
            if bins is None:
                if not self._stats["total"]:
                    bins = []
                elif self._stats["min_value"] == self._stats["max_value"]:
                    bins = [{"bin_index": 1, "count": self._stats["total"]}]
                elif bin_count == 1:
                    bins = [{"bin_index": 1, "count": 3}]
                else:
                    bins = [
                        {"bin_index": 1, "count": 1},
                        {"bin_index": bin_count, "count": 2},
                    ]
            if not bins:
                return _FakeResult(
                    rows=[{**self._stats, "bin_index": None, "count": None}]
                )
            return _FakeResult(rows=[{**self._stats, **entry} for entry in bins])
        return _FakeResult(rows=[])


def _client_with(session) -> TestClient:
    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    return TestClient(app)


def _dispatched(session: _DistributionSession) -> list[str]:
    return [
        statement
        for statement in session.statements
        if "gold_glossary.dim_metric" not in statement
        and "to_regclass" not in statement
    ]


def _relations_in(sql: str) -> set[str]:
    return set(re.findall(r"(?:FROM|JOIN)\s+([a-z_]+\.[a-z_]+)", sql))


def test_distribution_requires_metric_code() -> None:
    """Covers: API-003 — metric_code is required; the metric_id alias is gone."""
    client = _client_with(_DistributionSession())
    try:
        response = client.get("/api/v1/distribution/bins", params={"bin_count": 7})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    body = response.json()["detail"]
    assert any(
        error["loc"][-1] == "metric_code" and error["type"] == "missing"
        for error in body
    ), body


@pytest.mark.parametrize("bin_count", [1, 20])
def test_distribution_bin_boundaries_and_counts(bin_count: int) -> None:
    """Covers: API-014 — supported bin boundaries reconcile counts."""
    session = _DistributionSession(metric_row=dict(_FRED_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": "FRED:UNRATE", "bin_count": bin_count},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["bin_count"] == bin_count
    assert sum(item["count"] for item in payload["items"]) == payload["total"] == 3
    assert payload["items"][0]["lower_bound"] == 10.0
    assert payload["items"][-1]["upper_bound"] == 40.0


def test_every_bin_asked_for_is_reported() -> None:
    """Covers: API-079 — an empty bin is a measured zero, not an absence.

    ``GROUP BY bin_index`` returns no row for a bin nothing falls into, so a
    request for 7 bins over a long-tailed measure could answer with 2 while
    still declaring ``bin_count: 7``. Every consumer then had to rebuild the
    gaps from ``min_value``/``max_value`` -- or draw a histogram whose bars
    sit adjacent where empty ranges belong.
    """
    session = _DistributionSession(
        metric_row=dict(_FRED_METRIC),
        stats={
            "total": 3,
            "min_value": 0.0,
            "max_value": 100.0,
            "period_count": 1,
            "binned_period": "2023-01-01",
        },
        bins=[{"bin_index": 1, "count": 2}, {"bin_index": 5, "count": 1}],
    )
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": "FRED:UNRATE", "bin_count": 5},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["bin_index"] for item in items] == [1, 2, 3, 4, 5]
    assert [item["count"] for item in items] == [2, 0, 0, 0, 1]
    assert sum(item["count"] for item in items) == response.json()["total"] == 3
    # Contiguous, and the last bin closes on the observed maximum.
    assert [item["lower_bound"] for item in items] == [0.0, 20.0, 40.0, 60.0, 80.0]
    assert [item["upper_bound"] for item in items] == [20.0, 40.0, 60.0, 80.0, 100.0]


def test_degenerate_distributions_are_unchanged() -> None:
    """Covers: API-079 — no range to bin stays one bin, or none at all."""
    empty = _DistributionSession(
        metric_row=dict(_FRED_METRIC),
        stats={
            "total": 0,
            "min_value": None,
            "max_value": None,
            "period_count": 0,
            "binned_period": None,
        },
    )
    client = _client_with(empty)
    try:
        no_values = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": "FRED:UNRATE", "bin_count": 5},
        ).json()
    finally:
        app.dependency_overrides.clear()
    assert no_values["total"] == 0
    assert no_values["items"] == []
    assert no_values["min_value"] is None and no_values["max_value"] is None

    single = _DistributionSession(
        metric_row=dict(_FRED_METRIC),
        stats={
            "total": 4,
            "min_value": 7.5,
            "max_value": 7.5,
            "period_count": 1,
            "binned_period": "2023-01-01",
        },
    )
    client = _client_with(single)
    try:
        one_value = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": "FRED:UNRATE", "bin_count": 5},
        ).json()
    finally:
        app.dependency_overrides.clear()
    assert one_value["items"] == [
        {"bin_index": 1, "lower_bound": 7.5, "upper_bound": 7.5, "count": 4}
    ]


@pytest.mark.parametrize("bin_count", [0, 21])
def test_distribution_invalid_bin_counts_are_rejected(bin_count: int) -> None:
    """Covers: API-014 — invalid bin counts fail before database work."""
    client = _client_with(_DistributionSession(metric_row=dict(_FRED_METRIC)))
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": "FRED:UNRATE", "bin_count": bin_count},
        )
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 422


def test_unknown_distribution_metric_is_a_stable_404() -> None:
    """Covers: API-052 — an unknown code is explained, no query runs."""
    session = _DistributionSession(metric_row=None)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins", params={"metric_code": "NO:SUCH"}
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "metric_code not found"}
    assert not _dispatched(session)


def test_stratified_source_distribution_is_declined_with_its_reason() -> None:
    """Covers: API-052 — NASS bins would collapse domains; the API says so."""
    session = _DistributionSession(metric_row=dict(_NASS_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": _NASS_METRIC["metric_code"]},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert "multi-dimensional" in response.json()["detail"]
    assert not _dispatched(session), "a declined source must not reach SQL"


_UNREGISTERED_METRIC = {
    "metric_code": "NEWSRC:THING",
    "source_code": "NEWSRC",
    "units": "people",
    "valid_time_grains": ["ANNUAL"],
    "valid_geo_grains": ["STATE"],
    "aggregation_characteristic": "additive",
    "physical_lineage": {},
}


def test_metric_from_an_unregistered_source_is_explained_not_a_500() -> None:
    """Covers: API-078 — the one analysis route that crashed now explains.

    The glossary can publish a metric whose source has no reviewed dispatch
    entry: warehouse work lands before API registry work by design, and
    ``catalog_service.get_metric_capability`` documents exactly that state.
    ``/observations``, ``/comparison``, ``/comparison/preflight``, and
    ``/catalog/metrics/{code}`` all answer it honestly; this route raised
    ``UnknownObservationDispatch`` -- a ``KeyError`` no handler caught.
    """
    session = _DistributionSession(metric_row=_UNREGISTERED_METRIC)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins", params={"metric_code": "NEWSRC:THING"}
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "NEWSRC" in detail
    assert "/catalog/capabilities" in detail
    assert _dispatched(session) == [], "no query runs for a source with no dispatch"


def test_the_unregistered_explanation_is_the_one_observations_gives() -> None:
    """Covers: API-078 — one helper, so the two routes cannot drift apart."""
    session = _DistributionSession(metric_row=_UNREGISTERED_METRIC)
    client = _client_with(session)
    try:
        bins = client.get(
            "/api/v1/distribution/bins", params={"metric_code": "NEWSRC:THING"}
        )
        observations = client.get(
            "/api/v1/observations", params={"metric_code": "NEWSRC:THING"}
        )
    finally:
        app.dependency_overrides.clear()

    assert bins.status_code == observations.status_code == 422
    assert bins.json()["detail"] == observations.json()["detail"]


def test_distribution_dispatches_to_the_owning_sources_latest_relation() -> None:
    """Covers: API-052 — bins compute over one newest value per geography."""
    session = _DistributionSession(metric_row=dict(_FRED_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={
                "metric_code": "FRED:UNRATE",
                "geo_level": "NATIONAL",
                "bin_count": 7,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_code"] == "FRED"
    assert payload["units"] == "Percent"
    assert payload["derived"] is True

    queries = _dispatched(session)
    assert queries
    for sql in queries:
        assert "FROM gold_fred.mv_fred_latest" in sql
        assert "recency_rank = 1" in sql
        assert "value IS NOT NULL" in sql
        assert _relations_in(sql) <= ALLOWED_OBSERVATION_RELATIONS
    bound = session.parameters[-1]
    assert bound["metric_code_value"] == "FRED:UNRATE"
    assert bound["geo_level"] == "NATIONAL"


def test_distribution_filter_unsupported_by_the_source_is_rejected() -> None:
    """Covers: API-052 — PEP declares no state_fips filter; the API says so."""
    pep_metric = {
        "metric_code": "CENSUS_PEP:POP",
        "source_code": "CENSUS_PEP",
        "units": "people",
        "valid_time_grains": ["ANNUAL"],
        "valid_geo_grains": ["STATE"],
        "aggregation_characteristic": None,
        "physical_lineage": {
            "schema": "gold_pep",
            "relation": "population_estimate_revision",
            "key": "POP",
        },
    }
    session = _DistributionSession(metric_row=pep_metric)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": "CENSUS_PEP:POP", "state_fips": "06"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "state_fips" in detail
    assert "CENSUS_PEP" in detail
    assert not _dispatched(session)


def test_range_and_bins_are_measured_in_one_statement() -> None:
    """Covers: API-084 — one reading of the warehouse, not two.

    The range came from one statement and every count from a second, each
    taking its own snapshot. A refresh of the materialized view between them
    -- which is what that relation is for -- left `min_value` describing rows
    the counts no longer measured: a value published below it buckets to 0,
    which `items` never asks for, so the geography vanishes from the bins
    while `total` still counts it.
    """
    session = _DistributionSession(metric_row=dict(_FRED_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": "FRED:UNRATE", "bin_count": 5},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    serving = _dispatched(session)
    assert len(serving) == 1, (
        "the range and the bins must be measured in one statement: "
        f"{len(serving)} were issued"
    )
    sql = serving[0]
    assert "width_bucket" in sql and "MIN(value)" in sql
    assert _relations_in(sql) <= ALLOWED_OBSERVATION_RELATIONS, sql
    # A range whose bounds are equal is not a range; the database rejects it
    # outright, so the statement must never hand width_bucket one.
    ranked = sql.split("width_bucket", 1)[1]
    assert "CASE" in ranked, sql


def test_every_counted_value_is_inside_the_reported_range() -> None:
    """Covers: API-084 — the bins reconcile with the total they report."""
    session = _DistributionSession(metric_row=dict(_FRED_METRIC))
    client = _client_with(session)
    try:
        payload = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": "FRED:UNRATE", "bin_count": 4},
        ).json()
    finally:
        app.dependency_overrides.clear()

    assert sum(item["count"] for item in payload["items"]) == payload["total"]
    assert payload["items"][0]["lower_bound"] == payload["min_value"]
    assert payload["items"][-1]["upper_bound"] == payload["max_value"]


def test_a_distribution_reports_the_period_its_bins_describe() -> None:
    """Covers: API-097 — the bins say which period they are of.

    `/distribution/bins` and `/comparison` reduce through the same CTE, which
    ranks each geography's own newest period, so two geographies in one answer
    can be describing two different years. The comparison treats that as
    load-bearing and publishes `period_a`/`period_b` on every row; the
    distribution published no period at all, and a histogram mixing 2023 and
    2019 county estimates was indistinguishable from one that did not. The
    explorer feeds that answer to the map legend, so the bins decide the
    colour scale a choropleth is painted with.
    """
    session = _DistributionSession(
        metric_row=dict(_FRED_METRIC),
        stats={
            "total": 3,
            "min_value": 10.0,
            "max_value": 40.0,
            "period_count": 1,
            "binned_period": "2023-01-01",
        },
    )
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": _FRED_METRIC["metric_code"], "bin_count": 4},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["period"] == "2023-01-01"
    assert payload["periods_differ"] is False

    statement = _dispatched(session)[-1]
    # Measured in the same statement as the counts, from the same reduced
    # rows: a period taken separately could describe a different set than the
    # bins it labels (API-084 is the same reasoning for the range).
    assert "period_start" in statement.split("WITH", 1)[1].split("binned", 1)[0]


def test_a_distribution_that_mixes_periods_says_so() -> None:
    """Covers: API-097 — the answer names the mismatch instead of hiding it."""
    session = _DistributionSession(
        metric_row=dict(_FRED_METRIC),
        stats={
            "total": 3,
            "min_value": 10.0,
            "max_value": 40.0,
            "period_count": 2,
            "binned_period": "2019-01-01",
        },
    )
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": _FRED_METRIC["metric_code"], "bin_count": 4},
        )
    finally:
        app.dependency_overrides.clear()

    payload = response.json()
    assert payload["periods_differ"] is True
    # No single period is published, because there is not one: naming the
    # earliest or the latest would label the whole histogram with a year most
    # of it is not from.
    assert payload["period"] is None


def test_a_distribution_with_nothing_published_reports_no_period() -> None:
    """Covers: API-097 — unpublished stays unpublished."""
    session = _DistributionSession(
        metric_row=dict(_FRED_METRIC),
        stats={
            "total": 0,
            "min_value": None,
            "max_value": None,
            "period_count": 0,
            "binned_period": None,
        },
    )
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": _FRED_METRIC["metric_code"], "bin_count": 4},
        )
    finally:
        app.dependency_overrides.clear()

    payload = response.json()
    assert payload["total"] == 0
    assert payload["period"] is None
    assert payload["periods_differ"] is False


def test_a_distribution_says_what_uncertainty_it_could_not_carry() -> None:
    """Covers: API-098 — the same note the comparison publishes, here too.

    API-096 gave the comparison a caveat naming the uncertainty an aligned
    analysis cannot carry. The distribution reads the same rows through the
    same reduction, bins Census ACS county estimates into a histogram, and had
    no `caveats` array at all -- so one analysis said what it dropped and the
    other, reading the same published figures, did not.

    Source-agnostic: the note comes from the same helper and the same
    registry, so the two cannot drift into describing one source differently.
    """
    from apps.api.registry import OBSERVATION_DISPATCH
    from apps.api.services.compatibility import uncertainty_caveat

    acs = OBSERVATION_DISPATCH["CENSUS_ACS"]
    assert acs.uncertainty_expressions, "the fixture assumes ACS publishes one"

    metric = {
        "metric_code": "CENSUS_ACS:acs5:B01003_001E",
        "source_code": "CENSUS_ACS",
        "units": "people",
        "valid_time_grains": ["ANNUAL"],
        "valid_geo_grains": ["COUNTY"],
        "aggregation_characteristic": None,
        "physical_lineage": {},
    }
    session = _DistributionSession(metric_row=dict(metric))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": metric["metric_code"], "bin_count": 4},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200, response.text
    expected = uncertainty_caveat(metric)
    assert expected is not None
    assert response.json()["caveats"] == [expected]


def test_a_distribution_of_a_source_publishing_none_carries_no_caveat() -> None:
    """Covers: API-098 — read from the registry, so silence stays silence."""
    session = _DistributionSession(metric_row=dict(_FRED_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/distribution/bins",
            params={"metric_code": "FRED:UNRATE", "bin_count": 4},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.json()["caveats"] == []
