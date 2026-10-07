"""API unit tests: the registry-dispatched neutral observation resource.

Covers: API-042 (a metric resolves to its owning source and is answered from
        that source's own reviewed relations, with lineage identity bound and
        a publication/registry disagreement failing as a sanitized fault),
        API-043 (the declared per-source filter contract: unsupported filters
        are rejected with an explanation, release requires as_released, and a
        reversed year window is rejected),
        API-044 (the neutral envelope preserves source semantics: value
        status, suppression, uncertainty, coverage, and dimensions survive,
        and a non-numeric value is never coerced),
        API-045 (the release discovery resource lists a metric's published
        releases deterministically, newest first),
        API-046 (every neutral path the discovery registry declares is
        actually served, so all seven completed sources are queryable),
        API-047 (dispatch queries name only allowlisted relations and order
        deterministically).
"""

from __future__ import annotations

import re
from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import SERVICE_UNAVAILABLE_DETAIL, get_db_session_dep
from apps.api.main import app
from apps.api.registry import (
    ALLOWED_OBSERVATION_RELATIONS,
    OBSERVATION_DISPATCH,
    SOURCE_DISCOVERY,
)
from apps.api.versioning import VERSIONED_ROOT
from data_ingestion_toolbox.fbi_ucr.registry import ALL_PRODUCTS as FBI_PRODUCTS

pytestmark = [pytest.mark.unit, pytest.mark.api]


_CDC_METRIC = {
    "metric_code": "CDC:cdi:ALC1_1:crude",
    "metric_display_name": "Alcohol use among youth",
    "source_code": "CDC",
    "units": "percent",
    "physical_lineage": {
        "schema": "gold_cdc",
        "relation": "health_observation",
        "asset_id": "cdi",
        "measure_id": "ALC1_1",
        "value_type_id": "crude",
    },
}

_BLS_METRIC = {
    # LAUS is published per measure, so one BLS metric spans every state and
    # county the program covers; the series id rides along as a dimension.
    "metric_code": "BLS:LAU:UNEMP_RATE",
    "metric_display_name": "Unemployment rate",
    "source_code": "BLS",
    "units": "Percent",
    "physical_lineage": {
        "schema": "gold_bls",
        "relation": "fact_bls_observation",
        "key": "LAU:UNEMP_RATE",
    },
}

_ACS_METRIC = {
    "metric_code": "CENSUS_ACS:acs5:B01003_001E",
    "metric_display_name": "Total population",
    "source_code": "CENSUS_ACS",
    "units": None,
    "physical_lineage": {
        "schema": "gold_census",
        "relation": "fact_acs_observation",
        "key": "acs5:B01003_001E",
    },
}

_PEP_METRIC = {
    "metric_code": "CENSUS_PEP:POP",
    "metric_display_name": "Resident population",
    "source_code": "CENSUS_PEP",
    "units": "people",
    "physical_lineage": {
        "schema": "gold_pep",
        "relation": "population_estimate_revision",
        "key": "POP",
    },
}

_FBI_METRIC = {
    "metric_code": "FBI_UCR:summarized_violent_crime:actual",
    "metric_display_name": "Violent crime, actual count",
    "source_code": "FBI_UCR",
    "units": "offenses",
    "physical_lineage": {
        "schema": "gold_fbi",
        "relation": "crime_observation",
        "product_id": "summarized_violent_crime",
        "measure_id": "actual",
    },
}

_FRED_METRIC = {
    "metric_code": "FRED:UNRATE",
    "metric_display_name": "Unemployment rate",
    "source_code": "FRED",
    "units": "Percent",
    "physical_lineage": {
        "schema": "gold_fred",
        "relation": "fact_fred_observation",
        "key": "UNRATE",
    },
}


class _FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = rows or []
        self._scalar = scalar_value

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar


class _DispatchSession:
    """Answers the glossary lookup, then records the dispatched queries."""

    def __init__(self, metric_row=None, rows=None, total=0):
        self._metric_row = metric_row
        self._rows = rows or []
        self._total = total
        self.statements: list[str] = []
        self.parameters: list[dict[str, Any]] = []

    def execute(self, query, params=None):
        rendered = str(query)
        self.statements.append(rendered)
        self.parameters.append(dict(params or {}))
        if "gold_glossary.dim_metric" in rendered:
            return _FakeResult(rows=[self._metric_row] if self._metric_row else [])
        if rendered.lstrip().startswith("SELECT COUNT("):
            return _FakeResult(scalar_value=self._total)
        return _FakeResult(rows=self._rows)


def _client_with(session) -> TestClient:
    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    return TestClient(app, raise_server_exceptions=False)


def _clear_overrides() -> None:
    app.dependency_overrides.clear()


def _relations_in(sql: str) -> set[str]:
    return set(re.findall(r"(?:FROM|JOIN)\s+([a-z_]+\.[a-z_]+)", sql))


def _dispatched(session: _DispatchSession) -> list[str]:
    return [
        statement
        for statement in session.statements
        if "gold_glossary.dim_metric" not in statement
        and "to_regclass" not in statement
    ]


@pytest.mark.parametrize(
    ("metric", "period"),
    [
        (_BLS_METRIC, "2023-01-01"),
        (_ACS_METRIC, "2023-01-01"),
        (_PEP_METRIC, "2023-07-01"),
        (_CDC_METRIC, "2021"),
        (_FBI_METRIC, "2020-01-01"),
        (_FRED_METRIC, "2023-01-01"),
    ],
)
def test_exact_published_period_is_bound_before_latest_reduction(
    metric: dict[str, Any], period: str
) -> None:
    """Covers: API-159 — exact served periods select rows for every source shape."""
    session = _DispatchSession(metric_row=dict(metric))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": metric["metric_code"],
                "period_start": period,
                "newest_per_geography": "true"
                if OBSERVATION_DISPATCH[metric["source_code"]].analysis_ready
                else "false",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    assert response.json()["total"] == 0  # unknown but well-formed: empty page
    for sql in _dispatched(session):
        assert (
            f"{OBSERVATION_DISPATCH[metric['source_code']].period_start_expression} = :period_start"
            in sql
        )
    assert session.parameters[-1]["period_start"] == period


@pytest.mark.parametrize("period", ["2023-02-29", "2023-1-01", "bad", "2023-13-01"])
def test_malformed_published_period_is_refused(period: str) -> None:
    """Covers: API-159 — malformed periods are requests, not empty evidence."""
    session = _DispatchSession(metric_row=dict(_BLS_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _BLS_METRIC["metric_code"], "period_start": period},
        )
    finally:
        _clear_overrides()
    assert response.status_code == 422
    assert "period_start" in str(response.json())


@pytest.mark.parametrize("release", [None, "2023"])
def test_latest_period_pin_refuses_as_released_scope(release: str | None) -> None:
    """Covers: API-159 — latest-publication selector cannot imply a release."""
    session = _DispatchSession(metric_row=dict(_BLS_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _BLS_METRIC["metric_code"],
                "scope": "as_released",
                "period_start": "2023-01-01",
                "release": release,
            },
        )
    finally:
        _clear_overrides()
    assert response.status_code == 422
    assert "period_start" in str(response.json())


def test_published_periods_are_paged_from_the_latest_relation() -> None:
    """Covers: API-160 — discover exact served period pairs before painting."""
    session = _DispatchSession(
        metric_row=dict(_CDC_METRIC),
        rows=[{"period_start": "2021", "period_end": "2021", "observation_count": 3}],
        total=2,
    )
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations/periods",
            params={
                "metric_code": _CDC_METRIC["metric_code"],
                "scope": "latest",
                "limit": 1,
                "offset": 1,
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    assert response.json() == {
        "metric_code": _CDC_METRIC["metric_code"],
        "source_code": "CDC",
        "total": 2,
        "limit": 1,
        "offset": 1,
        "items": [
            {"period_start": "2021", "period_end": "2021", "observation_count": 3}
        ],
    }
    assert response.headers["cache-control"].startswith("public")
    for sql in _dispatched(session):
        assert "FROM gold_cdc.latest_release_observation" in sql
        assert "period_start::TEXT" in sql
        assert "GROUP BY" in sql
    assert (
        "ORDER BY period_start DESC, period_end DESC NULLS LAST"
        in _dispatched(session)[-1]
    )


def test_period_listing_refuses_as_released_scope() -> None:
    """Covers: API-160 — a latest-only selector cannot label released history."""
    client = _client_with(_DispatchSession(metric_row=dict(_CDC_METRIC)))
    try:
        response = client.get(
            "/api/v1/observations/periods",
            params={"metric_code": _CDC_METRIC["metric_code"], "scope": "as_released"},
        )
    finally:
        _clear_overrides()
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# API-042 — registry dispatch and identity binding
# ---------------------------------------------------------------------------


def test_metric_code_source_dispatches_to_its_own_latest_relation() -> None:
    """Covers: API-042 — a BLS metric reads gold_bls with the requested code."""
    session = _DispatchSession(metric_row=dict(_BLS_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _BLS_METRIC["metric_code"]},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    queries = _dispatched(session)
    assert queries, "no dispatched query was issued"
    for sql in queries:
        assert "FROM gold_bls.mv_bls_latest" in sql
    bound = session.parameters[-1]
    assert bound["metric_code_value"] == _BLS_METRIC["metric_code"]


def test_lineage_identity_source_binds_the_published_identity() -> None:
    """Covers: API-042 — CDC identity comes from physical_lineage, bound."""
    session = _DispatchSession(metric_row=dict(_CDC_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _CDC_METRIC["metric_code"]},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    queries = _dispatched(session)
    for sql in queries:
        assert "FROM gold_cdc.latest_release_observation" in sql
        assert "asset_id = :identity_asset_id" in sql
        assert "measure_id = :identity_measure_id" in sql
        assert "value_type_id = :identity_value_type_id" in sql
        assert "cdi" not in sql, "identity values must be bound, not inlined"
    bound = session.parameters[-1]
    assert bound["identity_asset_id"] == "cdi"
    assert bound["identity_measure_id"] == "ALC1_1"
    assert bound["identity_value_type_id"] == "crude"


def test_acs_binds_the_published_catalog_code_without_a_rewrite() -> None:
    """Covers: API-042 — ACS serving rows answer the code the catalog publishes.

    Covers: ARC-005 — the ACS serving relations carried ``ACS:<dataset>:
    <variable>`` while the glossary published ``CENSUS_ACS:<dataset>:
    <variable>``, and the dispatch spanned the gap with a rewriting
    ``lineage_key_prefix``. The serving relations now spell the catalog's own
    identity, so the requested code binds directly and no prefix rewrite is
    declared for the source.
    """
    session = _DispatchSession(metric_row=dict(_ACS_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _ACS_METRIC["metric_code"]},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    assert any("FROM gold_census.mv_acs_latest" in sql for sql in _dispatched(session))
    bound = session.parameters[-1]
    assert "lineage_key" not in bound
    assert bound["metric_code_value"] == "CENSUS_ACS:acs5:B01003_001E"
    assert OBSERVATION_DISPATCH["CENSUS_ACS"].lineage_key_prefix == ""


def test_pep_dispatch_reads_the_revision_relations_with_the_bare_key() -> None:
    """Covers: API-042 — PEP's neutral reach uses its published lineage key."""
    session = _DispatchSession(metric_row=dict(_PEP_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _PEP_METRIC["metric_code"]},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    assert any(
        "FROM gold_pep.population_estimate_latest" in sql
        for sql in _dispatched(session)
    )
    assert session.parameters[-1]["lineage_key"] == "POP"


def test_lineage_registry_disagreement_is_a_sanitized_fault() -> None:
    """Covers: API-042 — drifted lineage fails loudly, without leaking names."""
    row = dict(_CDC_METRIC)
    row["physical_lineage"] = {
        "schema": "gold_cdc",
        "relation": "some_other_relation",
        "asset_id": "cdi",
        "measure_id": "ALC1_1",
        "value_type_id": "crude",
    }
    session = _DispatchSession(metric_row=row)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations", params={"metric_code": row["metric_code"]}
        )
    finally:
        _clear_overrides()

    assert response.status_code == 503
    assert response.json() == {"detail": SERVICE_UNAVAILABLE_DETAIL}
    assert "some_other_relation" not in response.text
    assert not _dispatched(session), "no serving query may run after the fault"


@pytest.mark.parametrize(
    "product_id",
    [product.product_id for product in FBI_PRODUCTS],
)
def test_every_fbi_dataset_binds_its_own_product_identity(product_id: str) -> None:
    """Covers: API-042 — each FBI dataset reads only its own product's rows."""
    offense = next(
        product.offense_code
        for product in FBI_PRODUCTS
        if product.product_id == product_id
    )
    measure_id = f"{offense}:offense:absolute_total"
    row = dict(_FBI_METRIC)
    row.update(
        {
            "metric_code": f"FBI_UCR:{product_id}:{measure_id}",
            "physical_lineage": {
                "schema": "gold_fbi",
                "relation": "crime_observation",
                "product_id": product_id,
                "measure_id": measure_id,
            },
        }
    )
    session = _DispatchSession(metric_row=row)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations", params={"metric_code": row["metric_code"]}
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    for sql in _dispatched(session):
        assert "product_id = :identity_product_id" in sql
        assert product_id not in sql, "identity values must be bound, not inlined"
    bound = session.parameters[-1]
    assert bound["identity_product_id"] == product_id
    assert bound["identity_measure_id"] == measure_id


def test_missing_lineage_identity_is_a_sanitized_fault() -> None:
    """Covers: API-042 — a lineage without its identity fields cannot be read."""
    row = dict(_FBI_METRIC)
    row["physical_lineage"] = {
        "schema": "gold_fbi",
        "relation": "crime_observation",
        "product_id": "summarized_violent_crime",
    }
    session = _DispatchSession(metric_row=row)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations", params={"metric_code": row["metric_code"]}
        )
    finally:
        _clear_overrides()

    assert response.status_code == 503
    assert response.json() == {"detail": SERVICE_UNAVAILABLE_DETAIL}
    assert not _dispatched(session)


def test_a_source_without_a_dispatch_entry_is_explained_not_guessed() -> None:
    """Covers: API-042 — an undeclared source is a 422 explanation, not a 500."""
    row = dict(_BLS_METRIC)
    row.update({"metric_code": "NEW_SOURCE:thing", "source_code": "NEW_SOURCE"})
    session = _DispatchSession(metric_row=row)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations", params={"metric_code": "NEW_SOURCE:thing"}
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422
    assert "NEW_SOURCE" in response.json()["detail"]
    assert "capabilities" in response.json()["detail"]
    assert not _dispatched(session)


def test_unknown_metric_code_is_a_stable_404() -> None:
    """Covers: API-042 — an unknown metric is explained, not an empty page."""
    session = _DispatchSession(metric_row=None)
    client = _client_with(session)
    try:
        observations = client.get(
            "/api/v1/observations", params={"metric_code": "NO:SUCH"}
        )
        releases = client.get(
            "/api/v1/observations/releases", params={"metric_code": "NO:SUCH"}
        )
    finally:
        _clear_overrides()

    assert observations.status_code == 404
    assert observations.json() == {"detail": "metric_code not found"}
    assert releases.status_code == 404
    assert releases.json() == {"detail": "metric_code not found"}


# ---------------------------------------------------------------------------
# API-043 — the declared per-source filter contract
# ---------------------------------------------------------------------------


def test_a_filter_the_source_does_not_declare_is_rejected_with_help() -> None:
    """Covers: API-043 — an unsupported filter is explained, never ignored."""
    session = _DispatchSession(metric_row=dict(_BLS_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _BLS_METRIC["metric_code"],
                "stratum_id": "abc123",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "stratum_id" in detail
    assert "BLS" in detail
    assert "geo_id" in detail, "the rejection names the supported filters"
    assert not _dispatched(session), "a rejected filter must not reach SQL"


def test_supported_filters_bind_their_declared_conditions() -> None:
    """Covers: API-043 — a declared filter becomes its reviewed condition."""
    session = _DispatchSession(metric_row=dict(_CDC_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _CDC_METRIC["metric_code"],
                "geo_level": "state",
                "stratum_id": "s1",
                "year_from": 2019,
                "year_to": 2021,
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    sql = _dispatched(session)[-1]
    assert "gold_glossary.geo_grain(geo_type) = UPPER(:geo_level)" in sql
    assert "stratum_id = :stratum_id" in sql
    assert "period_end >= :year_from" in sql
    assert "period_start <= :year_to" in sql
    bound = session.parameters[-1]
    # Bound as the vocabulary word: the served rows carry STATE, and a
    # lower-case request is the same grain, not a different one.
    assert bound["geo_level"] == "STATE"
    assert bound["year_from"] == 2019


def test_a_grain_alias_binds_the_vocabulary_word() -> None:
    """Covers: API-073 — a word the catalog once published keeps answering.

    CDC, PEP, and NASS published ``NATION`` before the grain vocabulary was
    unified; a saved configuration or a shared link holding it must not stop
    answering, per the versioning decision record. The filter binds ``NATIONAL``, the word the served
    rows carry, and never the alias itself.
    """
    session = _DispatchSession(metric_row=dict(_CDC_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _CDC_METRIC["metric_code"], "geo_level": "nation"},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    assert session.parameters[-1]["geo_level"] == "NATIONAL"


def test_release_pin_requires_the_as_released_scope() -> None:
    """Covers: API-043 — a pinned release under scope=latest is contradictory."""
    session = _DispatchSession(metric_row=dict(_CDC_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _CDC_METRIC["metric_code"],
                "release": "1700000000",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422
    assert "as_released" in response.json()["detail"]


def test_reversed_year_window_is_rejected() -> None:
    """Covers: API-043 — a reversed window is a 422, not an empty page."""
    client = _client_with(_DispatchSession(metric_row=dict(_BLS_METRIC)))
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _BLS_METRIC["metric_code"],
                "year_from": 2024,
                "year_to": 2020,
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422
    assert response.json()["detail"] == (
        "year_from must be less than or equal to year_to"
    )


def test_as_released_scope_reads_the_released_relation_with_the_pin() -> None:
    """Covers: API-043 — as_released reads every release; the pin binds."""
    session = _DispatchSession(metric_row=dict(_CDC_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _CDC_METRIC["metric_code"],
                "scope": "as_released",
                "release": "1700000000",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    payload = response.json()
    assert payload["scope"] == "as_released"
    assert payload["release"] == "1700000000"
    sql = _dispatched(session)[-1]
    assert "FROM gold_cdc.health_observation" in sql
    assert "release_watermark = :release" in sql
    assert session.parameters[-1]["release"] == "1700000000"


# ---------------------------------------------------------------------------
# API-044 — envelope fidelity
# ---------------------------------------------------------------------------


def _cdc_suppressed_row() -> dict[str, Any]:
    return {
        "release": "1700000000",
        "as_of": None,
        "period_start": "2021",
        "period_end": "2021",
        "geo_id": "state:06",
        "geo_level": "state",
        "value": None,
        "value_status": "suppressed",
        "unit": "percent",
        "dim_asset_id": "cdi",
        "dim_dataset_title": "Chronic Disease Indicators",
        "dim_measure_label": "Alcohol use among youth",
        "dim_value_type_label": "Crude Prevalence",
        "dim_topic": "Alcohol",
        "dim_stratum_id": "s1",
        "dim_strata": {"sex": "Female"},
        "dim_adjustment_status": "crude",
        "dim_estimate_method": None,
        "dim_population_basis": None,
        "dim_total_population": "12345",
        "dim_population_18_plus": None,
        "dim_footnote_code": "S",
        "dim_footnote_text": "Suppressed by provider",
        "u_confidence_lower": None,
        "u_confidence_upper": None,
        "source_record_id": "row-1",
        "capture_id": "11111111-1111-1111-1111-111111111111",
    }


def test_suppressed_values_stay_null_with_their_published_status() -> None:
    """Covers: API-044 — suppression survives; nothing becomes zero."""
    session = _DispatchSession(
        metric_row=dict(_CDC_METRIC), rows=[_cdc_suppressed_row()], total=1
    )
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _CDC_METRIC["metric_code"]},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    (item,) = payload["items"]
    assert item["value"] is None
    assert item["value_status"] == "suppressed"
    assert item["dimensions"]["footnote_code"] == "S"
    assert item["dimensions"]["strata"] == {"sex": "Female"}
    assert item["uncertainty"] == {
        "margin_of_error": None,
        "margin_of_error_pct": None,
        "confidence_lower": None,
        "confidence_upper": None,
        "cv_value": None,
        "cv_status": None,
        "cv_symbol": None,
    }
    assert item["coverage"] is None, "CDC publishes no participation coverage"
    assert item["source_record_id"] == "row-1"
    assert item["capture_id"] == "11111111-1111-1111-1111-111111111111"


def test_fbi_rows_carry_their_participation_coverage() -> None:
    """Covers: API-044 — a not-reported month keeps null value with context."""
    row = {
        "release": "2026-06-01",
        "as_of": "2026-06-01",
        "period_start": "2020-01-01",
        "period_end": "2020-01-31",
        "geo_id": None,
        "geo_level": "agency",
        "value": None,
        "value_status": "not_reported",
        "unit": "offenses",
        "dim_product_id": "summarized_violent_crime",
        "dim_offense_code": "V",
        "dim_offense_label": "Violent crime",
        "dim_ucr_program": "SRS",
        "dim_measure_form": "absolute_total",
        "dim_counted_entity_basis": "offenses",
        "dim_subject_type": "agency",
        "dim_subject_code": "CA0010100",
        "dim_subject_label": "Alameda County Sheriff's Office",
        "dim_period": "2020-01",
        "dim_max_data_month": "2020-12",
        "dim_geography_basis": "agency-reported for one law-enforcement agency",
        "u_confidence_lower": None,
        "c_population": "1670834",
        "c_participated_population": "0",
        "c_coverage_percent": "0",
        "c_coverage_basis": "population",
        "c_participation_status": "did_not_report",
        "c_population_denominator": None,
        "source_record_id": "fbi-row-1",
        "capture_id": "22222222-2222-2222-2222-222222222222",
    }
    session = _DispatchSession(metric_row=dict(_FBI_METRIC), rows=[row], total=1)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _FBI_METRIC["metric_code"]},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    (item,) = response.json()["items"]
    assert item["value"] is None
    assert item["value_status"] == "not_reported"
    assert item["coverage"]["participation_status"] == "did_not_report"
    assert item["coverage"]["population"] == "1670834"
    assert item["dimensions"]["subject_code"] == "CA0010100"
    assert item["uncertainty"] is None, "FBI publishes no uncertainty fields"


def test_a_source_without_value_status_serves_null_not_valid() -> None:
    """Covers: API-044 — "publishes no status" is distinguishable from valid."""
    row = {
        "release": "2026-08-01",
        "as_of": "2026-08-01",
        "period_start": "2026-07-01",
        "period_end": "2026-07-31",
        "geo_id": "county:06001",
        "geo_level": "county",
        "value": "4.2",
        "value_status": None,
        "unit": "rate",
        "dim_series_id": "LAUCN060010000000003",
        "dim_seasonal_adjustment_status": "not_seasonally_adjusted",
        "source_record_id": None,
        "capture_id": None,
    }
    session = _DispatchSession(metric_row=dict(_BLS_METRIC), rows=[row], total=1)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _BLS_METRIC["metric_code"]},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    (item,) = response.json()["items"]
    assert item["value"] == "4.2"
    assert item["value_status"] is None
    assert item["source_code"] == "BLS"
    assert item["metric_code"] == _BLS_METRIC["metric_code"]
    assert item["release"] == "2026-08-01"


def test_empty_result_is_a_stable_page_for_a_known_metric() -> None:
    """Covers: API-044 — no rows for a real metric is a page, not an error."""
    session = _DispatchSession(metric_row=dict(_BLS_METRIC), rows=[], total=0)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _BLS_METRIC["metric_code"], "geo_id": "none"},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 0
    assert payload["items"] == []
    assert payload["source_code"] == "BLS"


# ---------------------------------------------------------------------------
# API-045 — release discovery
# ---------------------------------------------------------------------------


def test_releases_resource_lists_releases_newest_first() -> None:
    """Covers: API-045 — release identities, counts, deterministic order."""
    rows = [
        {"release": "1710000000", "as_of": None, "observation_count": 40},
        {"release": "1700000000", "as_of": None, "observation_count": 38},
    ]
    session = _DispatchSession(metric_row=dict(_CDC_METRIC), rows=rows, total=2)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations/releases",
            params={"metric_code": _CDC_METRIC["metric_code"]},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_code"] == "CDC"
    assert payload["total"] == 2
    assert [item["release"] for item in payload["items"]] == [
        "1710000000",
        "1700000000",
    ]
    assert payload["items"][0]["observation_count"] == 40

    sql = _dispatched(session)[-1]
    assert "FROM gold_cdc.health_observation" in sql
    assert "GROUP BY 1" in sql
    assert "ORDER BY MAX(release_watermark::BIGINT) DESC" in sql


# ---------------------------------------------------------------------------
# API-046 — declared capability round trip
# ---------------------------------------------------------------------------


def test_every_declared_neutral_path_is_actually_served() -> None:
    """Covers: API-046 — the registry cannot advertise an unserved route."""
    served = {
        path
        for path, item in app.openapi()["paths"].items()
        if (item or {}).get("get") is not None
    }
    for discovery in SOURCE_DISCOVERY.values():
        for relative in discovery.neutral_paths:
            assert f"{VERSIONED_ROOT}{relative}" in served, (
                f"{discovery.source_code} declares unserved path {relative}"
            )


def test_every_completed_source_is_dispatchable_and_discoverable() -> None:
    """Covers: API-046 — discovery and dispatch declare the same sources."""
    assert set(OBSERVATION_DISPATCH) == set(SOURCE_DISCOVERY)
    for discovery in SOURCE_DISCOVERY.values():
        assert discovery.served_by_neutral_routes is True


def test_every_declared_filter_is_an_accepted_query_parameter() -> None:
    """Covers: API-046 — a declared filter the route ignores would be a lie."""
    operation = app.openapi()["paths"]["/api/v1/observations"]["get"]
    accepted = {
        parameter["name"]
        for parameter in operation["parameters"]
        if parameter["in"] == "query"
    }
    for dispatch in OBSERVATION_DISPATCH.values():
        declared = set(dispatch.supported_filters())
        assert declared <= accepted, (
            f"{dispatch.source_code} declares filters the route does not "
            f"accept: {sorted(declared - accepted)}"
        )


# ---------------------------------------------------------------------------
# API-047 — allowlist and deterministic ordering
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("source_code", sorted(OBSERVATION_DISPATCH))
def test_dispatch_relations_are_allowlisted_and_own_schema(source_code: str) -> None:
    """Covers: API-047 — every dispatch relation is declared and reviewed."""
    dispatch = OBSERVATION_DISPATCH[source_code]
    assert dispatch.latest_relation in ALLOWED_OBSERVATION_RELATIONS
    assert dispatch.released_relation in ALLOWED_OBSERVATION_RELATIONS
    schema = dispatch.latest_relation.split(".")[0]
    assert dispatch.released_relation.startswith(f"{schema}.")
    assert schema == dispatch.lineage_schema, (
        "serving relations and published lineage must share a schema"
    )
    strategies = [
        dispatch.metric_code_column is not None,
        dispatch.lineage_key_column is not None,
        bool(dispatch.identity_columns),
    ]
    assert sum(strategies) == 1, "exactly one metric identity strategy"
    assert dispatch.latest_order and dispatch.released_order
    assert "DESC" not in dispatch.release_order_expression


@pytest.mark.parametrize(
    ("metric_row", "scope"),
    [
        (_BLS_METRIC, "latest"),
        (_BLS_METRIC, "as_released"),
        (_CDC_METRIC, "latest"),
        (_CDC_METRIC, "as_released"),
        (_PEP_METRIC, "as_released"),
        (_FBI_METRIC, "latest"),
    ],
    ids=lambda value: value if isinstance(value, str) else value["source_code"],
)
def test_dispatched_sql_names_only_allowlisted_relations(
    metric_row: dict[str, Any], scope: str
) -> None:
    """Covers: API-047 — rendered SQL cannot reach past the allowlist."""
    session = _DispatchSession(metric_row=dict(metric_row))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": metric_row["metric_code"], "scope": scope},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    queries = _dispatched(session)
    assert queries
    for sql in queries:
        assert _relations_in(sql) <= ALLOWED_OBSERVATION_RELATIONS, sql
    list_sql = queries[-1]
    assert "ORDER BY" in list_sql
    assert "LIMIT :limit OFFSET :offset" in list_sql


# ---------------------------------------------------------------------------
# API-066 — one row per geography, ranked inside the source relation
# ---------------------------------------------------------------------------


def test_newest_per_geography_ranks_inside_the_source_relation() -> None:
    """Covers: API-066 — the reduction happens where the ordering is known.

    ``scope=latest`` answers a source's whole latest publication, which for
    Census PEP is every estimated year of the current vintage. A client that
    wants one value per geography would otherwise have to page the whole
    publication and reduce it itself.
    """
    session = _DispatchSession(metric_row=dict(_PEP_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _PEP_METRIC["metric_code"],
                "newest_per_geography": "true",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    dispatched = _dispatched(session)
    assert dispatched, "the request reached the source relation"
    for sql in dispatched:
        assert "ROW_NUMBER() OVER" in sql
        assert "PARTITION BY geo_id" in sql
        assert "ORDER BY estimate_date::TEXT DESC" in sql
        assert "newest_period_rank = 1" in sql
        # Ranked before projection, over the source's own relation.
        assert "FROM gold_pep.population_estimate_latest" in sql
    # The count answers over the reduced set, so total and page agree.
    assert any(sql.lstrip().startswith("SELECT COUNT(") for sql in dispatched)


def test_newest_per_geography_keeps_the_declared_filters() -> None:
    """Covers: API-066 — the reduction composes, it does not replace."""
    session = _DispatchSession(metric_row=dict(_PEP_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _PEP_METRIC["metric_code"],
                "geo_level": "COUNTY",
                "year_from": 2020,
                "newest_per_geography": "true",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    for sql in _dispatched(session):
        # Filters stay inside the ranked subquery: ranking the unfiltered
        # relation and filtering afterwards would answer the newest period
        # that survived the filter, not the newest period of the selection.
        ranked = sql.split("ROW_NUMBER() OVER", 1)[1]
        # PEP carries its grain as geo_type and both projects and filters it
        # through the one vocabulary mapping, which is the condition its
        # dispatch entry declares for the neutral geo_level filter.
        assert "gold_glossary.geo_grain(geo_type) = UPPER(:geo_level)" in ranked
        assert "observation_year >= :year_from" in ranked
    assert session.parameters[-1]["geo_level"] == "COUNTY"
    assert session.parameters[-1]["year_from"] == 2020


def test_newest_per_geography_is_refused_for_an_as_released_read() -> None:
    """Covers: API-066 — an as-released read is a series per release.

    Reducing it to one row per geography would present whichever release
    sorted last as the value, which is the same reason the explorer leaves
    an unpinned as-released answer uncoloured.
    """
    session = _DispatchSession(metric_row=dict(_PEP_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _PEP_METRIC["metric_code"],
                "scope": "as_released",
                "newest_per_geography": "true",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "newest_per_geography" in detail
    assert "scope=latest" in detail


def test_default_still_answers_the_whole_latest_publication() -> None:
    """Covers: API-066 — the v1 default is unchanged by the addition."""
    session = _DispatchSession(metric_row=dict(_PEP_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _PEP_METRIC["metric_code"]},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    for sql in _dispatched(session):
        assert "ROW_NUMBER() OVER" not in sql
        assert "newest_period_rank" not in sql


def test_newest_per_geography_is_declared_on_the_neutral_route() -> None:
    """Covers: API-066 — a client discovers the parameter rather than assuming.

    The explorer only sends a parameter the capability entry declares, so an
    undeclared one would simply never be used.
    """
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/api/v1/catalog/capabilities")

    assert response.status_code == 200
    neutral_path = f"{VERSIONED_ROOT}/observations"
    for capability in response.json()["items"]:
        routes = {
            route["path"]: route["parameters"]
            for route in capability["observation_routes"]
        }
        if neutral_path in routes:
            assert "newest_per_geography" in routes[neutral_path], capability[
                "source_code"
            ]


# ---------------------------------------------------------------------------
# API-081 — one row per period, ranked by the source's own release order
# ---------------------------------------------------------------------------


def test_newest_release_per_period_ranks_by_the_declared_release_order() -> None:
    """Covers: API-081 — the API decides which release is newer, not a client.

    A source whose latest relation keeps one row per geography -- Census ACS
    holds only the newest vintage -- has a geography's history only across
    its releases. Reducing that to one row per period needs the source's own
    release order, which every dispatch entry declares and which a client
    can only guess at.
    """
    session = _DispatchSession(metric_row=dict(_PEP_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _PEP_METRIC["metric_code"],
                "scope": "as_released",
                "newest_release_per_period": "true",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    dispatched = _dispatched(session)
    assert dispatched, "the request reached the source relation"
    for sql in dispatched:
        assert "ROW_NUMBER() OVER" in sql
        # One row per geography and period, newest release first, by the
        # order the dispatch entry declares.
        assert "PARTITION BY geo_id, estimate_date::TEXT" in sql
        assert "ORDER BY pep_vintage DESC" in sql
        assert "newest_release_rank = 1" in sql
        # Ranked inside the source's own as-released relation.
        assert "FROM gold_pep.population_estimate_revision" in sql
    assert any(sql.lstrip().startswith("SELECT COUNT(") for sql in dispatched)


def test_newest_release_per_period_keeps_the_declared_filters() -> None:
    """Covers: API-081 — the reduction composes, it does not replace."""
    session = _DispatchSession(metric_row=dict(_PEP_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _PEP_METRIC["metric_code"],
                "scope": "as_released",
                "geo_level": "COUNTY",
                "newest_release_per_period": "true",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    for sql in _dispatched(session):
        ranked = sql.split("ROW_NUMBER() OVER", 1)[1]
        assert "gold_glossary.geo_grain(geo_type) = UPPER(:geo_level)" in ranked


def test_newest_release_per_period_is_refused_for_a_latest_read() -> None:
    """Covers: API-081 — only an as-released read has releases to reduce."""
    session = _DispatchSession(metric_row=dict(_PEP_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _PEP_METRIC["metric_code"],
                "newest_release_per_period": "true",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "newest_release_per_period" in detail
    assert "scope=as_released" in detail


def test_newest_release_per_period_and_a_pinned_release_are_contradictory() -> None:
    """Covers: API-081 — a contradiction is refused, never resolved silently."""
    session = _DispatchSession(metric_row=dict(_PEP_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _PEP_METRIC["metric_code"],
                "scope": "as_released",
                "release": "2024",
                "newest_release_per_period": "true",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "newest_release_per_period" in detail
    assert "release" in detail


def test_the_two_reductions_cannot_be_asked_for_together() -> None:
    """Covers: API-081 — each belongs to the scope the other refuses."""
    session = _DispatchSession(metric_row=dict(_PEP_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _PEP_METRIC["metric_code"],
                "scope": "as_released",
                "newest_per_geography": "true",
                "newest_release_per_period": "true",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422


def test_newest_release_per_period_is_declared_on_the_neutral_route() -> None:
    """Covers: API-081 — a client discovers it from the capability entry."""
    paths = app.openapi().get("paths") or {}
    neutral = paths.get("/api/v1/observations", {}).get("get", {})
    names = {
        parameter["name"]
        for parameter in neutral.get("parameters") or []
        if parameter.get("in") == "query"
    }
    assert "newest_release_per_period" in names


# ---------------------------------------------------------------------------
# API-083 — a reduction that ties picks the same row every time
# ---------------------------------------------------------------------------


def _ranking_order(sql: str, marker: str) -> str:
    """The ``ORDER BY`` of the window function that assigns ``marker``."""
    window = sql.split(marker, 1)[0].rsplit("ROW_NUMBER() OVER", 1)[1]
    ordering = window.split("ORDER BY", 1)[1].rsplit(")", 1)[0]
    return " ".join(ordering.split())


def test_newest_per_geography_breaks_ties_on_the_declared_order() -> None:
    """Covers: API-083 — ROW_NUMBER picks one row of a tie group, and SQL
    does not say which.

    Census PEP's latest publication is a series, so a geography carries
    several rows and more than one can share the newest period. Ranking on
    the period alone leaves that group undecided: the same request answers a
    different published row when the plan or the relation's physical order
    changes, with no publication in between.
    """
    session = _DispatchSession(metric_row=dict(_PEP_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _PEP_METRIC["metric_code"],
                "newest_per_geography": "true",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    dispatch = OBSERVATION_DISPATCH["CENSUS_PEP"]
    expected = ", ".join(
        (f"{dispatch.period_start_expression} DESC",) + dispatch.latest_order
    )
    for sql in _dispatched(session):
        assert _ranking_order(sql, "newest_period_rank") == expected, sql


def test_settled_history_breaks_ties_on_the_declared_order() -> None:
    """Covers: API-083 — the mirror image, over the released relation.

    Two rows of one period inside one release tie on the release order, and
    the settled history promised the row the source's own declared order
    names.
    """
    session = _DispatchSession(metric_row=dict(_PEP_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _PEP_METRIC["metric_code"],
                "scope": "as_released",
                "newest_release_per_period": "true",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    dispatch = OBSERVATION_DISPATCH["CENSUS_PEP"]
    expected = ", ".join(
        (f"{dispatch.release_order_expression} DESC",) + dispatch.released_order
    )
    for sql in _dispatched(session):
        assert _ranking_order(sql, "newest_release_rank") == expected, sql


def test_every_dispatch_entry_declares_the_order_its_reductions_need() -> None:
    """Covers: API-083 — the gap is visible, not silent.

    A reduction can only be deterministic where the entry declares a total
    order. Nothing here invents one for a source that does not; CI names the
    source instead, at the point a source is added.
    """
    missing_latest = sorted(
        code for code, entry in OBSERVATION_DISPATCH.items() if not entry.latest_order
    )
    missing_released = sorted(
        code for code, entry in OBSERVATION_DISPATCH.items() if not entry.released_order
    )
    assert missing_latest == [], (
        "these sources reduce their latest relation on an order that can tie: "
        f"{missing_latest}"
    )
    assert missing_released == [], (
        "these sources reduce their released relation on an order that can "
        f"tie: {missing_released}"
    )


# ---------------------------------------------------------------------------
# API-095 — the releases listing pages a total order
# ---------------------------------------------------------------------------


def _metric_row_for(source_code: str, dispatch) -> dict[str, Any]:
    """A glossary row that resolves and dispatches to one source.

    Built from the dispatch entry itself -- its lineage relation, its lineage
    key, its identity columns -- so a source added to the registry is
    exercised without an edit here.
    """
    lineage: dict[str, Any] = {
        "schema": dispatch.lineage_schema,
        "relation": dispatch.lineage_relation,
        "key": "RELEASE_ORDER_KEY",
    }
    lineage.update({column: "IDENTITY" for column in dispatch.identity_columns})
    return {
        "metric_code": f"{source_code}:RELEASE_ORDER",
        "source_code": source_code,
        "metric_display_name": "Release order",
        "units": None,
        "physical_lineage": lineage,
    }


def test_every_source_lists_its_releases_in_a_total_order() -> None:
    """Covers: API-095 — the releases listing cannot repeat or skip a release.

    The guide promises every paged read a total order. This one had it only by
    coincidence of the registry: it ordered by `MAX(release_order_expression)`
    alone, and every dispatch entry's ordering expression happens to be its
    release identity with a cast, so no two groups could share a value. A
    source whose release identity is a name ordered by a date -- the obvious
    next shape -- pages non-deterministically the moment two releases land on
    one date.

    The release identity is the `GROUP BY` key, so naming it as the tie-break
    makes the order total by construction rather than by inspection. Swept
    over the reviewed registry, so a source added later is covered here.
    """
    from apps.api.registry import OBSERVATION_DISPATCH

    for source_code, dispatch in sorted(OBSERVATION_DISPATCH.items()):
        row = _metric_row_for(source_code, dispatch)
        session = _DispatchSession(metric_row=row, rows=[], total=0)
        client = _client_with(session)
        try:
            response = client.get(
                "/api/v1/observations/releases",
                params={"metric_code": row["metric_code"]},
            )
        finally:
            _clear_overrides()
        assert response.status_code == 200, response.text

        listing = _dispatched(session)[-1]
        order = listing.split("ORDER BY", 1)[1].split("LIMIT", 1)[0].strip()
        assert order.startswith(f"MAX({dispatch.release_order_expression}) DESC"), (
            f"{source_code} orders its releases by {order!r}, which does not "
            "begin with the release ordering the dispatch declares"
        )
        tie_break = order.split("DESC", 1)[1].strip()
        assert dispatch.release_expression in tie_break, (
            f"{source_code} orders its releases by {order!r}, whose tie-break "
            f"does not name the release identity {dispatch.release_expression!r}; "
            "two releases sharing an ordering value could then repeat or skip "
            "across a page boundary"
        )


# ---------------------------------------------------------------------------
# API-118 — a reduction declines a source that does not reduce
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("reduction", "scope"),
    [
        ("newest_per_geography", "latest"),
        ("newest_release_per_period", "as_released"),
    ],
)
def test_a_reduction_declines_a_stratified_source(reduction: str, scope: str) -> None:
    """Covers: API-118 — the reduction is the analysis routes' own ranking.

    The guide says of `newest_per_geography` that it is "the same ranking
    `/distribution/bins` and `/comparison/preflight` already apply", and
    those routes decline CDC, USDA NASS and FBI UCR with a stated reason:
    their rows do not reduce to one number per geography. The reduction
    never consulted `analysis_ready`. It partitioned on `geo_id` alone, and
    the tie-break then resolved CDC's many rows per geography by
    `stratum_id`, so the lexicographically first stratum answered as the
    geography's value and `total` counted only the survivors -- "collapse a
    source's strata, domains, or subject grain into a single number you did
    not ask for", which the guide's own "What this API will not do" opens
    with.

    Every API-066 and API-081 node above uses the Census PEP metric, an
    analysis-ready source, so the block passed without a stratified one.
    """
    session = _DispatchSession(metric_row=dict(_CDC_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _CDC_METRIC["metric_code"],
                "scope": scope,
                reduction: "true",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert reduction in detail
    assert "CDC" in detail
    # The restriction is the dispatch entry's own, so the reader is told the
    # same thing the analysis routes tell them, and what to ask instead.
    assert "stratum_id" in detail
    assert not _dispatched(session), "no query may run for a refused reduction"


def test_a_reduction_still_answers_for_a_source_that_reduces() -> None:
    """Covers: API-118 — the refusal is narrow.

    Census PEP is analysis-ready and its latest publication is a series per
    geography, which is the case the reduction exists for.
    """
    session = _DispatchSession(metric_row=dict(_PEP_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _PEP_METRIC["metric_code"],
                "newest_per_geography": "true",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200, response.text


_NASS_METRIC = {
    "metric_code": "USDA_NASS:hay_survey_annual:3bf8ec4d",
    "metric_display_name": "HAY - YIELD, MEASURED IN TONS / ACRE",
    "source_code": "USDA_NASS",
    "units": "TONS / ACRE",
    "physical_lineage": {
        "schema": "gold_nass",
        "relation": "crop_observation",
        "product_id": "hay_survey_annual",
        "statistic_sk": "3bf8ec4d",
        "statisticcat_desc": "YIELD",
        "unit_desc": "TONS / ACRE",
    },
}


def test_nass_exact_year_period_is_bound_without_a_derived_date() -> None:
    """Covers: API-159 — NASS's year-only period remains its served string."""
    session = _DispatchSession(metric_row=dict(_NASS_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _NASS_METRIC["metric_code"], "period_start": "2023"},
        )
    finally:
        _clear_overrides()
    assert response.status_code == 200, response.text
    assert response.json()["items"] == []
    assert session.parameters[-1]["period_start"] == "2023"
    assert (
        f"{OBSERVATION_DISPATCH['USDA_NASS'].period_start_expression} = :period_start"
        in _dispatched(session)[-1]
    )


def test_nass_reference_period_is_a_declared_bound_filter() -> None:
    """Covers: API-156 — a final value and its forecasts can be told apart.

    USDA NASS publishes a year's final value beside its August and October
    forecasts for one geography, separated only by `reference_period_desc`.
    Without a filter for it, a one-value-per-geography reader -- the explorer
    map -- could only decline the whole answer.
    """
    capabilities = TestClient(app).get("/api/v1/catalog/capabilities").json()
    nass = next(
        item for item in capabilities["items"] if item["source_code"] == "USDA_NASS"
    )
    assert "reference_period_desc" in nass["observation_filters"]

    session = _DispatchSession(metric_row=dict(_NASS_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _NASS_METRIC["metric_code"],
                "geo_level": "STATE",
                "reference_period_desc": "YEAR",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    queries = _dispatched(session)
    assert queries
    for sql in queries:
        assert "reference_period_desc = :reference_period_desc" in sql
        assert "'YEAR'" not in sql, "the filter value must be bound, not inlined"
    assert session.parameters[-1]["reference_period_desc"] == "YEAR"


def test_reference_period_is_refused_where_a_source_does_not_declare_it() -> None:
    """Covers: API-156 — the filter is the declaring source's, not everyone's."""
    session = _DispatchSession(metric_row=dict(_FBI_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _FBI_METRIC["metric_code"],
                "reference_period_desc": "YEAR",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422
    assert "reference_period_desc" in response.text
    assert not _dispatched(session)


# ---------------------------------------------------------------------------
# API-168 — time_grain=quarterly|annual serves calendar windows
# ---------------------------------------------------------------------------


class _CalendarSession(_DispatchSession):
    """A dispatch session whose calendar relation holds the metric (or not)."""

    def __init__(self, *args: Any, published: bool = True, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._published = published

    def execute(self, query, params=None):
        rendered = str(query)
        if rendered.lstrip().startswith("SELECT EXISTS"):
            self.statements.append(rendered)
            self.parameters.append(dict(params or {}))
            return _FakeResult(scalar_value=self._published)
        return super().execute(query, params)


_PROVIDER_ROW = {
    "geo_id": "us:1",
    "geo_level": "NATIONAL",
    "subject_code": None,
    "unit": "index",
    "period_start": "2024-01-01",
    "period_end": "2024-12-31",
    "as_of": "2025-01-15",
    "value": "313.689",
    "value_status": "valid",
    "derivation_kind": "provider_published",
    "method": None,
    "method_version": None,
    "expected_periods": None,
    "present_periods": None,
    "refusal_reason": None,
    "component_releases": None,
}

_REFUSED_ROW = {
    **_PROVIDER_ROW,
    "period_start": "2025-01-01",
    "period_end": "2025-12-31",
    "value": None,
    "value_status": "incomplete_window",
    "derivation_kind": "derived",
    "method": "mean",
    "method_version": 1,
    "expected_periods": 12,
    "present_periods": 8,
    "refusal_reason": "incomplete_window: 8 of 12 periods reported",
    "component_releases": ["2025-09-11"],
}


@pytest.mark.parametrize(
    ("time_grain", "grain"), [("annual", "year"), ("quarterly", "quarter")]
)
def test_a_calendar_grain_reads_the_calendar_relation(
    time_grain: str, grain: str
) -> None:
    """Covers: API-168 — a BLS metric's calendar windows come from the calendar relation, labelled."""
    session = _CalendarSession(
        metric_row=dict(_BLS_METRIC), rows=[_PROVIDER_ROW, _REFUSED_ROW], total=2
    )
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _BLS_METRIC["metric_code"], "time_grain": time_grain},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["time_grain"] == time_grain and body["total"] == 2
    provider, refused = body["items"]
    assert provider["value"] == "313.689"
    assert provider["derivation"]["kind"] == "provider_published"
    assert refused["value"] is None
    assert refused["derivation"] == {
        "kind": "derived",
        "method": "mean",
        "method_version": 1,
        "expected_periods": 12,
        "present_periods": 8,
        "refusal_reason": "incomplete_window: 8 of 12 periods reported",
        "component_releases": ["2025-09-11"],
    }
    queries = _dispatched(session)
    assert queries
    for sql in queries:
        assert _relations_in(sql) == {"gold_bls.calendar_window_observation"}, sql
    calendar_reads = [
        bound for bound, sql in zip(session.parameters, session.statements)
        if "calendar_window_observation" in sql
    ]
    assert calendar_reads and all(bound["grain"] == grain for bound in calendar_reads)
    assert session.parameters[-1]["metric_code"] == _BLS_METRIC["metric_code"]


def test_native_grain_is_the_default_and_unchanged() -> None:
    """Covers: API-168 — without time_grain a read answers the source's own periods, with no derivation."""
    session = _DispatchSession(metric_row=dict(_BLS_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _BLS_METRIC["metric_code"]},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    assert response.json()["time_grain"] == "native"
    for sql in _dispatched(session):
        assert "calendar_window_observation" not in sql


def test_a_metric_with_no_window_is_refused_not_answered_empty() -> None:
    """Covers: API-168 — no provider figure and no approved method is a 422 naming ADR-0007."""
    session = _CalendarSession(metric_row=dict(_BLS_METRIC), published=False)
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _BLS_METRIC["metric_code"], "time_grain": "quarterly"},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422
    assert "ADR-0007" in str(response.json())


def test_a_source_without_calendar_windows_derives_nothing() -> None:
    """Covers: API-168 — FRED declares no calendar relation, so a year is refused before any query."""
    session = _CalendarSession(metric_row=dict(_FRED_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _FRED_METRIC["metric_code"], "time_grain": "annual"},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422
    assert "ADR-0007" in str(response.json())
    assert _dispatched(session) == []


@pytest.mark.parametrize(
    "params",
    [
        {"scope": "as_released"},
        {"newest_per_geography": "true"},
        {"time_grain": "monthly"},
        {"adjustment_status": "SA"},
    ],
)
def test_a_calendar_grain_refuses_what_it_cannot_answer(params: dict[str, str]) -> None:
    """Covers: API-168 — no release history, reduction, unknown grain or native-only filter."""
    session = _CalendarSession(metric_row=dict(_BLS_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _BLS_METRIC["metric_code"],
                "time_grain": "annual",
                **params,
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422
    assert _dispatched(session) == []


@pytest.mark.parametrize("source_code", sorted(OBSERVATION_DISPATCH))
def test_capabilities_publish_exactly_the_grains_the_route_serves(
    source_code: str,
) -> None:
    """Covers: API-168 — a source lists calendar grains only when its dispatch declares a calendar relation."""
    client = TestClient(app)
    payload = client.get("/api/v1/catalog/capabilities").json()
    entry = next(
        (item for item in payload["items"] if item["source_code"] == source_code),
        None,
    )
    if entry is None:
        pytest.fail(f"{source_code} has a dispatch entry but no capability entry")
    dispatch = OBSERVATION_DISPATCH[source_code]
    expected = (
        ["native", "quarterly", "annual"] if dispatch.calendar_relation else ["native"]
    )
    assert entry["time_grains"] == expected
    if dispatch.calendar_relation is not None:
        assert dispatch.calendar_relation in ALLOWED_OBSERVATION_RELATIONS
        assert dispatch.calendar_relation.startswith(f"{dispatch.lineage_schema}.")


# ---------------------------------------------------------------------------
# API-169 — trailing and year-to-date windows, computed on request
# ---------------------------------------------------------------------------


_WINDOW_ROW = {
    "geo_id": "us:1",
    "geo_level": "NATIONAL",
    "subject_code": None,
    "unit": "index",
    "period_start": "2025-07-01",
    "period_end": "2025-09-30",
    "expected_periods": 3,
    "present_periods": 3,
    "value": "321.5",
    "refusal_reason": None,
    "component_releases": ["2025-10-15"],
}


def _approve(monkeypatch: pytest.MonkeyPatch, code: str, method: str = "mean") -> None:
    import apps.api.services.neutral_observations_service as service
    from data_ingestion_toolbox.semantics.time_aggregation import TimeMethod

    monkeypatch.setattr(
        service,
        "authorized_method",
        lambda metric: TimeMethod(metric, method, "approved", "Nick", 1)
        if metric == code
        else None,
    )


def test_a_window_is_computed_with_the_approved_method(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-169 — one derived row per geography, bound anchor and span, labelled."""
    _approve(monkeypatch, _BLS_METRIC["metric_code"])
    session = _DispatchSession(metric_row=dict(_BLS_METRIC), rows=[_WINDOW_ROW])
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _BLS_METRIC["metric_code"],
                "window": "trailing_3",
                "period_start": "2025-09-01",
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["window"] == "trailing_3" and body["total"] == 1
    (row,) = body["items"]
    assert row["value"] == "321.5"
    assert row["derivation"]["kind"] == "derived"
    assert row["derivation"]["method"] == "mean"
    (sql,) = _dispatched(session)
    assert "gold_bls.rpt_bls_observations" in sql
    assert "%(" not in sql
    bound = session.parameters[-1]
    assert bound["anchor"] == "2025-09-01" and bound["span"] == 3
    assert bound["metric_codes"] == [_BLS_METRIC["metric_code"]]


def test_year_to_date_binds_no_span(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: API-169 — YTD runs from January to the anchor, so its span is the anchor's month."""
    _approve(monkeypatch, _BLS_METRIC["metric_code"])
    session = _DispatchSession(metric_row=dict(_BLS_METRIC), rows=[])
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _BLS_METRIC["metric_code"], "window": "ytd"},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 200
    assert session.parameters[-1]["span"] is None
    assert session.parameters[-1]["anchor"] is None


def test_a_metric_without_an_approved_method_has_no_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-169 — no approved method is a 422 naming ADR-0007, before any query."""
    _approve(monkeypatch, "BLS:SOMETHING_ELSE")
    session = _DispatchSession(metric_row=dict(_BLS_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={"metric_code": _BLS_METRIC["metric_code"], "window": "trailing_12"},
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422
    assert "ADR-0007" in str(response.json())
    assert _dispatched(session) == []


@pytest.mark.parametrize(
    "params",
    [
        {"time_grain": "annual"},
        {"scope": "as_released"},
        {"newest_per_geography": "true"},
        {"period_start": "2025-09-15"},
        {"year_from": "2020"},
        {"window": "trailing_6"},
    ],
)
def test_a_window_refuses_what_it_cannot_answer(
    monkeypatch: pytest.MonkeyPatch, params: dict[str, str]
) -> None:
    """Covers: API-169 — no calendar grain, history, reduction, mid-month anchor, year filter or unknown window."""
    _approve(monkeypatch, _BLS_METRIC["metric_code"])
    session = _DispatchSession(metric_row=dict(_BLS_METRIC))
    client = _client_with(session)
    try:
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": _BLS_METRIC["metric_code"],
                "window": "trailing_3",
                **params,
            },
        )
    finally:
        _clear_overrides()

    assert response.status_code == 422
    assert _dispatched(session) == []
