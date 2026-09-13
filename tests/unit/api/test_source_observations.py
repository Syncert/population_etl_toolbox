"""API unit tests: source-specific observation endpoints.

Migrated from apps/api/tests/test_source_observations.py.
Covers: API-013 (source-specific endpoints return only their source),
        API-003 (required metric input), API-007 (date-range validation).
"""

import pytest
from datetime import date, datetime
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.registry import OBSERVATION_DISPATCH, SERVING_CONTRACTS


class _FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = rows or []
        self._scalar_value = scalar_value

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def scalar(self):
        return self._scalar_value

    def first(self):
        return self._rows[0] if self._rows else None

    def scalars(self):
        return self

    def __iter__(self):
        return iter(self._rows)


def _observation_row(
    metric_code: str = "BLS:LAU:UNEMP_RATE", geo_id: str = "state:06"
) -> dict:
    return {
        "source_code": "BLS",
        "source": "BLS",
        "observation_date": "2024-01-01",
        "period": "2024",
        "duration_start": None,
        "duration_end": None,
        "time_sk": 20240101,
        "as_of_date": "2024-02-01",
        "release_date": "2024-02-01",
        "updated_at": datetime(2024, 2, 1),
        "geo_id": geo_id,
        "geo_level": "STATE",
        "geo_name": "California",
        "state_fips": "06",
        "county_fips": None,
        "state_name": "California",
        "county_name": None,
        "geo_latitude": 36.7783,
        "geo_longitude": -119.4179,
        "metric_code": metric_code,
        "metric_display_name": "Unemployment Rate",
        "value": "4.2",
        "value_type": "RATE",
        "units": "percent",
        "unit": "percent",
        "seasonal_adjustment_status": "SA",
        "dataset_code": None,
        "dataset": None,
        "vintage_year": None,
        "vintage": None,
        "margin_of_error": None,
        "margin_of_error_pct": None,
    }


class _SourceSchemaSession:
    def __init__(self, source_schema: str, rows: list, lineage_key: str | None = None):
        self._schema = source_schema
        self._rows = rows
        # A relation that composes its own metric identity is matched against
        # the lineage key its publisher declares, which the glossary answers
        # (API-093). Sources whose relations store the catalog's own code
        # never reach this lookup.
        self._lineage_key = lineage_key
        self.calls: list[tuple[str, dict]] = []

    def execute(self, query, params=None):
        sql = str(query).lower()
        self.calls.append((sql, params or {}))

        if "to_regclass" in sql:
            return _FakeResult(scalar_value=True)

        if "gold_glossary.dim_metric" in sql:
            requested = (params or {}).get("metric_code", "")
            return _FakeResult(
                rows=[
                    {
                        "metric_code": requested,
                        "source_code": "CENSUS_PEP",
                        "physical_lineage": {
                            "schema": "gold_pep",
                            "relation": "population_estimate_revision",
                            "key": self._lineage_key,
                        },
                    }
                ]
                if self._lineage_key
                else []
            )

        if "information_schema.columns" in sql:
            return _FakeResult(
                rows=[
                    "dataset_code",
                    "vintage_year",
                    "margin_of_error",
                    "margin_of_error_pct",
                ]
            )

        schema = self._schema.lower()
        source_tables = {
            "gold_bls": ("gold_bls.mv_bls_latest", "gold_bls.rpt_bls_observations"),
            "gold_census": (
                "gold_census.mv_acs_latest",
                "gold_census.rpt_acs_observations",
            ),
            "gold_fred": (
                "gold_fred.mv_fred_latest",
                "gold_fred.rpt_fred_observations",
            ),
            "gold_pep": (
                "gold_pep.mv_pep_latest",
                "gold_pep.rpt_pep_observations",
            ),
        }
        latest_table, timeseries_table = source_tables[schema]
        latest_relations = (f"{schema}.v_metric_latest_by_geo", latest_table)
        timeseries_relations = (
            f"{schema}.v_metric_timeseries_by_geo",
            timeseries_table,
        )

        if any(f"from {r}" in sql for r in latest_relations) and "count(*)" in sql:
            return _FakeResult(scalar_value=len(self._rows))

        if any(f"from {r}" in sql for r in latest_relations):
            return _FakeResult(rows=self._rows)

        if any(f"from {r}" in sql for r in timeseries_relations) and "count(*)" in sql:
            return _FakeResult(scalar_value=len(self._rows))

        if any(f"from {r}" in sql for r in timeseries_relations):
            return _FakeResult(rows=self._rows)

        return _FakeResult(rows=[])


@pytest.mark.unit
@pytest.mark.api
def test_bls_latest_observations_returns_data() -> None:
    """Covers: API-011, API-013 — BLS latest returns BLS contract rows."""

    def _override_db():
        yield _SourceSchemaSession("gold_bls", [_observation_row()])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/v1/bls/observations/latest",
            params={
                "metric_code": "BLS:LAU:UNEMP_RATE",
                "geo_level": "STATE",
                "limit": 10,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["source"] == "BLS"


@pytest.mark.unit
@pytest.mark.api
def test_bls_latest_requires_metric_code() -> None:
    """Covers: API-003 — BLS latest requires a metric identifier."""

    def _override_db():
        yield _SourceSchemaSession("gold_bls", [])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/api/v1/bls/observations/latest", params={"limit": 5})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    body = response.json()["detail"]
    assert any(
        error["loc"][-1] == "metric_code" and error["type"] == "missing"
        for error in body
    ), body


@pytest.mark.unit
@pytest.mark.api
def test_bls_timeseries_rejects_invalid_date_range() -> None:
    """Covers: API-007 — BLS history rejects a reversed date range."""

    def _override_db():
        yield _SourceSchemaSession("gold_bls", [])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/v1/bls/observations/timeseries",
            params={
                "metric_code": "BLS:LAU:UNEMP_RATE",
                "geo_id": "state:06",
                "start_date": "2024-06-01",
                "end_date": "2024-01-01",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert (
        response.json()["detail"] == "start_date must be less than or equal to end_date"
    )


@pytest.mark.unit
@pytest.mark.api
def test_census_latest_observations_returns_data() -> None:
    """Covers: API-011, API-013 — Census latest returns ACS contract rows."""
    row = _observation_row(metric_code="CENSUS_ACS:acs5:B01003_001", geo_id="state:06")
    row.update(
        source_code="CENSUS_ACS",
        source="CENSUS_ACS",
        dataset_code="acs5",
        dataset="acs5",
        vintage_year=2022,
        vintage="2022",
        margin_of_error="150.0",
        margin_of_error_pct="0.01",
    )

    def _override_db():
        yield _SourceSchemaSession("gold_census", [row])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/v1/census/observations/latest",
            params={"metric_code": "CENSUS_ACS:acs5:B01003_001", "limit": 10},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["dataset"] == "acs5"
    assert payload["items"][0]["vintage"] == "2022"
    assert payload["items"][0]["margin_of_error"] == "150.0"


@pytest.mark.unit
@pytest.mark.api
def test_fred_latest_observations_returns_data() -> None:
    """Covers: API-011, API-013 — FRED latest returns FRED contract rows."""
    row = _observation_row(metric_code="FRED:UNRATE", geo_id="us:1")
    row.update(source_code="FRED", source="FRED")

    def _override_db():
        yield _SourceSchemaSession("gold_fred", [row])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/v1/fred/observations/latest",
            params={"metric_code": "FRED:UNRATE", "limit": 10},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["source"] == "FRED"


@pytest.mark.unit
@pytest.mark.api
@pytest.mark.parametrize(
    ("source_path", "source_schema", "metric_code", "source_code"),
    [
        ("bls", "gold_bls", "BLS:LAU:UNEMP_RATE", "BLS"),
        ("census", "gold_census", "CENSUS_ACS:acs5:B01003_001", "CENSUS_ACS"),
        ("fred", "gold_fred", "FRED:UNRATE", "FRED"),
        (
            "pep",
            "gold_pep",
            "CENSUS_PEP:pep_nst_alldata:POPESTIMATE",
            "CENSUS_PEP",
        ),
    ],
)
def test_source_timeseries_routes_preserve_source_contract(
    source_path: str,
    source_schema: str,
    metric_code: str,
    source_code: str,
) -> None:
    """Covers: API-013 — source history routes return only their source."""
    row = _observation_row(metric_code=metric_code)
    row.update(source_code=source_code, source=source_code)

    def _override_db():
        yield _SourceSchemaSession(source_schema, [row])

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        response = TestClient(app).get(
            f"/api/v1/{source_path}/observations/timeseries",
            params={"metric_code": metric_code, "geo_id": row["geo_id"]},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert {item["source"] for item in payload["items"]} == {source_code}


@pytest.mark.unit
@pytest.mark.api
@pytest.mark.parametrize(
    ("source_path", "source_schema", "latest_table", "history_table"),
    [
        ("bls", "gold_bls", "gold_bls.mv_bls_latest", "gold_bls.rpt_bls_observations"),
        (
            "census",
            "gold_census",
            "gold_census.mv_acs_latest",
            "gold_census.rpt_acs_observations",
        ),
        (
            "fred",
            "gold_fred",
            "gold_fred.mv_fred_latest",
            "gold_fred.rpt_fred_observations",
        ),
        (
            "pep",
            "gold_pep",
            "gold_pep.mv_pep_latest",
            "gold_pep.rpt_pep_observations",
        ),
    ],
)
def test_source_filters_reach_exact_source_queries(
    source_path: str,
    source_schema: str,
    latest_table: str,
    history_table: str,
) -> None:
    """Covers: API-010 — all filters reach exact source-aware queries."""
    # The glossary publishes a lineage key for the requested code, so a
    # contract whose relation composes its own identity binds the key it is
    # matched against rather than the NULL an unpublished code binds (DB-034).
    session = _SourceSchemaSession(source_schema, [], lineage_key="METRIC")

    def _override_db():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        latest = client.get(
            f"/api/v1/{source_path}/observations/latest",
            params={
                "metric_code": "METRIC",
                "geo_level": "STATE",
                "state_fips": "06",
                "limit": 17,
                "offset": 4,
            },
        )
        history = client.get(
            f"/api/v1/{source_path}/observations/timeseries",
            params={
                "metric_code": "METRIC",
                "geo_id": "state:06",
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "limit": 19,
                "offset": 6,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert latest.status_code == history.status_code == 200
    latest_calls = [
        (sql, params)
        for sql, params in session.calls
        if f"from {latest_table}" in sql and "metric_code" in params
    ]
    history_calls = [
        (sql, params)
        for sql, params in session.calls
        if f"from {history_table}" in sql and "metric_code" in params
    ]
    assert len(latest_calls) == len(history_calls) == 2
    # A contract whose relation composes its own metric identity also binds
    # the lineage key it is matched against; one that stores the catalog's
    # own code binds nothing extra (API-093). Read from the contract so this
    # stays source-agnostic.
    from apps.api.registry import serving_contract

    identity = (
        # The lineage key the relation is matched against, and the
        # catalog's own code the row is labelled with (API-108). The fake
        # glossary answers the requested code, so both are "METRIC" here.
        {"metric_key": "METRIC", "catalog_metric_code": "METRIC"}
        if serving_contract(source_path).binds_lineage_key
        else {}
    )
    assert all(
        params
        == {
            "metric_code": "METRIC",
            "geo_level": "STATE",
            "state_fips": "06",
            "limit": 17,
            "offset": 4,
            **identity,
        }
        for _, params in latest_calls
    )
    assert all(
        params
        == {
            "metric_code": "METRIC",
            "geo_id": "state:06",
            "start_date": date(2024, 1, 1),
            "end_date": date(2024, 12, 31),
            "limit": 19,
            "offset": 6,
            **identity,
        }
        for _, params in history_calls
    )


@pytest.mark.unit
@pytest.mark.api
def test_source_timeseries_echoes_the_page_it_was_asked_for() -> None:
    """Covers: API-074 — the source-scoped history pages like every list route."""
    session = _SourceSchemaSession("gold_pep", [_observation_row()])

    def _override_db():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app)
        response = client.get(
            "/api/v1/pep/observations/timeseries",
            params={
                "metric_code": "CENSUS_PEP:pep_2020s:POPESTIMATE",
                "geo_id": "state:06",
                "limit": 25,
                "offset": 50,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["limit"] == 25
    assert payload["offset"] == 50


# ---------------------------------------------------------------------------
# API-092 — the source-scoped routes speak the warehouse's grain vocabulary
# ---------------------------------------------------------------------------


def _source_session(
    schema: str, lineage_key: str | None = None
) -> _SourceSchemaSession:
    return _SourceSchemaSession(schema, [_observation_row()], lineage_key)


def _override_with(session: _SourceSchemaSession) -> None:
    def _override_db():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override_db


def _dispatched_sql(session: _SourceSchemaSession, relation: str) -> list[str]:
    return [sql for sql, _ in session.calls if f"from {relation}" in sql]


@pytest.mark.unit
@pytest.mark.api
def test_a_source_route_filters_and_projects_the_vocabulary_word() -> None:
    """Covers: API-092 — `NATIONAL` must reach Census PEP's national rows.

    `gold_pep.rpt_pep_observations` projects `revision.geo_type AS geo_level`,
    so the served relation carries `nation`/`state`/`county`/`place` under a
    column named `geo_level`. Filtering `UPPER(geo_level) = UPPER(:geo_level)`
    meant the word the catalog publishes matched nothing and answered an empty
    page indistinguishable from a geography with no published values -- the
    defect migration 018 wrote down, on the one serving surface it did not
    reach. STATE, COUNTY and PLACE coincided, which is why it stayed hidden.
    """
    session = _source_session("gold_pep", lineage_key="POP")
    _override_with(session)
    try:
        response = TestClient(app).get(
            "/api/v1/pep/observations/latest",
            params={"metric_code": "CENSUS_PEP:x:POP", "geo_level": "NATIONAL"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    dispatched = _dispatched_sql(session, "gold_pep.mv_pep_latest")
    assert dispatched, "the request reached the PEP relation"
    for sql in dispatched:
        assert "gold_glossary.geo_grain(geo_level)" in sql, sql
        assert "upper(geo_level)" not in sql, sql
    assert session.calls[-1][1]["geo_level"] == "NATIONAL"


@pytest.mark.unit
@pytest.mark.api
def test_a_legacy_grain_word_still_answers() -> None:
    """Covers: API-092 — a shared link holding `NATION` keeps working.

    The versioning decision record requires it: a saved configuration or a
    link carrying the catalog's earlier word must keep answering, so the
    alias is resolved on the way in rather than left to match a raw value by
    accident.
    """
    session = _source_session("gold_pep", lineage_key="POP")
    _override_with(session)
    try:
        TestClient(app).get(
            "/api/v1/pep/observations/latest",
            params={"metric_code": "CENSUS_PEP:x:POP", "geo_level": "nation"},
        )
    finally:
        app.dependency_overrides.clear()

    assert session.calls[-1][1]["geo_level"] == "NATIONAL"


@pytest.mark.unit
@pytest.mark.api
def test_a_source_that_publishes_the_vocabulary_is_not_remapped() -> None:
    """Covers: API-092 — BLS, ACS and FRED already derive the vocabulary word."""
    session = _source_session("gold_bls")
    _override_with(session)
    try:
        TestClient(app).get(
            "/api/v1/bls/observations/latest",
            params={"metric_code": "BLS:LAU:UNEMP_RATE", "geo_level": "county"},
        )
    finally:
        app.dependency_overrides.clear()

    for sql in _dispatched_sql(session, "gold_bls.mv_bls_latest"):
        assert "gold_glossary.geo_grain(" not in sql, sql
    assert session.calls[-1][1]["geo_level"] == "COUNTY"


@pytest.mark.unit
@pytest.mark.api
def test_the_two_registries_agree_on_which_sources_map_the_grain() -> None:
    """Covers: API-092 — a mapping in one registry and not the other is the bug.

    The dispatch entry and the serving contract read different relations for
    the same source, so they cannot share an expression -- but they must agree
    on *whether* that source's relations store a source-shaped grain.
    """
    for segment, contract in SERVING_CONTRACTS.items():
        dispatch = OBSERVATION_DISPATCH[contract.source_code]
        maps_dispatch = "gold_glossary.geo_grain(" in dispatch.geo_level_expression
        maps_contract = "gold_glossary.geo_grain(" in contract.geo_level_expression
        assert maps_dispatch == maps_contract, (
            f"'{segment}' maps the grain on one serving surface and not the "
            f"other: dispatch={dispatch.geo_level_expression!r}, "
            f"contract={contract.geo_level_expression!r}"
        )
