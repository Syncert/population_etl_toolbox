"""API unit tests: the reviewed serving registry and its dispatch.

Covers: API-034 (serving registry is the single source of relation names),
        API-035 (per-source reads target their own declared relations),
        API-036 (an absent serving contract fails explicitly).
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from apps.api.registry import (
    ALLOWED_OBSERVATION_RELATIONS,
    OBSERVATION_DISPATCH,
    SERVING_CONTRACTS,
    UnknownServingContract,
    serving_contract,
)
from apps.api.services.observations_service import (
    ServingContractUnavailable,
    list_latest_observations_for_source,
    list_timeseries_observations_for_source,
)

pytestmark = [pytest.mark.unit, pytest.mark.api]


class _RecordingResult:
    def __init__(self, scalar_value=0, rows=None):
        self._scalar = scalar_value
        self._rows = rows or []

    def scalar(self):
        return self._scalar

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _RecordingSession:
    """A session that records statements and reports every relation present."""

    bind = object()

    def __init__(self, missing_relations: frozenset[str] = frozenset()):
        self.statements: list[str] = []
        self._missing = missing_relations

    def execute(self, query, params=None):
        rendered = str(query)
        self.statements.append(rendered)
        if "to_regclass" in rendered:
            relation = (params or {}).get("relation_name")
            return _RecordingResult(relation not in self._missing)
        return _RecordingResult(0, [])


class _UnbindableSession:
    """A stub that cannot answer a relation probe, like a deterministic double."""

    def execute(self, query, params=None):
        raise SQLAlchemyError("no bind")


def _projection_of(session: _RecordingSession) -> str:
    """The row-projection query, excluding the relation probe and the count."""
    return next(
        statement
        for statement in session.statements
        if "to_regclass" not in statement and "COUNT(*)" not in statement
    )


def test_every_contract_declares_relations_inside_its_own_schema() -> None:
    """Covers: API-034 — a contract cannot point at another source's schema."""
    for segment, contract in SERVING_CONTRACTS.items():
        assert contract.route_segment == segment
        assert contract.latest_relation.startswith(f"{contract.schema}.")
        assert contract.history_relation.startswith(f"{contract.schema}.")
        assert contract.latest_relation != contract.history_relation


def test_allowlist_is_exactly_the_declared_relations() -> None:
    """Covers: API-034 — no relation reaches SQL without being declared.

    Since API-004 the allowlist is the union of the per-source serving
    contracts and the neutral observation dispatch entries; both registries
    are reviewed constants, and nothing outside them may name a relation.
    """
    expected = {
        relation
        for contract in SERVING_CONTRACTS.values()
        for relation in (contract.latest_relation, contract.history_relation)
    } | {
        relation
        for dispatch in OBSERVATION_DISPATCH.values()
        for relation in (dispatch.latest_relation, dispatch.released_relation)
    }
    assert ALLOWED_OBSERVATION_RELATIONS == expected
    assert all(relation.startswith("gold_") for relation in expected)


def test_unknown_source_raises_instead_of_guessing() -> None:
    """Covers: API-034 — an unregistered source has no fallback relation."""
    with pytest.raises(UnknownServingContract):
        serving_contract("fbi")


@pytest.mark.parametrize("segment", sorted(SERVING_CONTRACTS))
def test_latest_read_targets_only_the_declared_latest_relation(segment: str) -> None:
    """Covers: API-035 — a source route reads that source's own relation."""
    contract = serving_contract(segment)
    session = _RecordingSession()

    list_latest_observations_for_source(
        session,
        source=segment,
        metric_code="UNEMP",
        geo_level=None,
        state_fips=None,
        limit=10,
        offset=0,
    )

    queries = [s for s in session.statements if "to_regclass" not in s]
    assert queries, "no query was issued"
    for statement in queries:
        assert f"FROM {contract.latest_relation}" in statement
        for other in SERVING_CONTRACTS.values():
            if other.route_segment != segment:
                assert other.latest_relation not in statement
                assert other.history_relation not in statement


@pytest.mark.parametrize("segment", sorted(SERVING_CONTRACTS))
def test_history_read_targets_only_the_declared_history_relation(segment: str) -> None:
    """Covers: API-035 — history comes from the durable relation, not the view."""
    contract = serving_contract(segment)
    session = _RecordingSession()

    list_timeseries_observations_for_source(
        session,
        source=segment,
        metric_code="UNEMP",
        geo_id="state:06",
        start_date=None,
        end_date=None,
        limit=10,
    )

    queries = [s for s in session.statements if "to_regclass" not in s]
    assert queries
    for statement in queries:
        assert f"FROM {contract.history_relation}" in statement
        assert contract.latest_relation not in statement


def test_projection_reflects_what_each_source_actually_publishes() -> None:
    """Covers: API-035 — an unpublished field is a typed NULL, never invented."""
    bls = _RecordingSession()
    list_latest_observations_for_source(bls, "bls", "UNEMP", None, None, 10, 0)
    bls_sql = _projection_of(bls)

    census = _RecordingSession()
    list_latest_observations_for_source(census, "census", "POP", None, None, 10, 0)
    census_sql = _projection_of(census)

    # BLS publishes seasonal adjustment and no survey vintage or margin of error.
    assert "seasonal_adjustment_status,\n" in bls_sql
    assert "NULL::TEXT AS margin_of_error" in bls_sql
    assert "NULL::INT AS vintage_year" in bls_sql

    # Census ACS is the mirror image.
    assert "NULL::TEXT AS seasonal_adjustment_status" in census_sql
    assert "margin_of_error::TEXT AS margin_of_error" in census_sql
    assert "vintage_year::TEXT AS vintage" in census_sql


def test_place_names_are_selected_only_where_they_are_published() -> None:
    """Covers: API-035 — selecting place_name elsewhere would be a SQL error."""
    pep = _RecordingSession()
    list_latest_observations_for_source(pep, "pep", "POP", None, None, 10, 0)
    assert any("place_name" in statement for statement in pep.statements)

    fred = _RecordingSession()
    list_latest_observations_for_source(fred, "fred", "GDP", None, None, 10, 0)
    assert not any("place_name" in statement for statement in fred.statements)


def test_absent_serving_relation_fails_explicitly() -> None:
    """Covers: API-036 — a missing contract is a fault, not an empty page.

    The retired behaviour answered a source-specific route from the cross-source
    union when the source's schema looked absent, so ``/api/v1/bls/...`` could
    return rows from another source under a name that said otherwise.
    """
    contract = serving_contract("bls")
    session = _RecordingSession(missing_relations=frozenset({contract.latest_relation}))

    with pytest.raises(ServingContractUnavailable) as raised:
        list_latest_observations_for_source(session, "bls", "UNEMP", None, None, 10, 0)

    assert contract.latest_relation in str(raised.value)
    assert not [s for s in session.statements if "to_regclass" not in s], (
        "no query may run once the declared relation is known to be missing"
    )


def test_a_session_that_cannot_answer_the_probe_is_not_treated_as_absence() -> None:
    """Covers: API-036 — an unanswerable probe is not evidence of a fault."""
    session = _UnbindableSession()
    with pytest.raises(SQLAlchemyError):
        # The probe stays silent, so the read proceeds and fails on its own
        # query rather than on a fabricated ServingContractUnavailable.
        list_latest_observations_for_source(session, "bls", "UNEMP", None, None, 10, 0)


def test_probe_binds_the_relation_name_rather_than_interpolating_it() -> None:
    """Covers: API-036 — the existence check is itself parameterized."""
    session = _RecordingSession()
    list_latest_observations_for_source(session, "fred", "GDP", None, None, 10, 0)

    probe = next(s for s in session.statements if "to_regclass" in s)
    assert probe == str(text("SELECT to_regclass(:relation_name) IS NOT NULL"))
    assert "gold_fred" not in probe


def test_missing_serving_contract_answers_a_sanitized_503() -> None:
    """Covers: API-036 — the fault is reported without leaking warehouse state."""
    from fastapi.testclient import TestClient

    from apps.api.dependencies import SERVICE_UNAVAILABLE_DETAIL, get_db_session_dep
    from apps.api.main import app

    contract = serving_contract("bls")

    def _override_db():
        yield _RecordingSession(missing_relations=frozenset({contract.latest_relation}))

    app.dependency_overrides[get_db_session_dep] = _override_db
    try:
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get(
            "/api/v1/bls/observations/latest", params={"metric_code": "UNEMP"}
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json() == {"detail": SERVICE_UNAVAILABLE_DETAIL}
    body = response.text
    assert contract.latest_relation not in body
    assert "gold_bls" not in body
