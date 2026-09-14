"""API unit tests: the N-measure aligned matrix.

Covers: API-132 (``GET /comparison/matrix`` answers 2-8 measures as pairwise
        verdicts, pairwise statistics and aligned wide rows, declining a pair
        inside the answer and a source for the whole request), API-133 (the
        wide rows page a declared total order over the union of geographies,
        every published value carries its own period and release, and the
        statistics are measured over the join rather than over the page).

The session double answers the two statements the route executes -- the
one-row-per-fact statistics union and the page of wide rows -- which is what
lets these tests assert the shape of both.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.registry import ALLOWED_OBSERVATION_RELATIONS
from apps.api.services.compatibility import (
    CORRELATION_CAUSATION_CAVEAT,
    CORRELATION_DERIVATIONS,
)

pytestmark = [pytest.mark.unit, pytest.mark.api]

MATRIX = "/api/v1/comparison/matrix"


def _metric(code: str, source: str, **overrides) -> dict:
    metric = {
        "metric_code": code,
        "source_code": source,
        "units": "Percent",
        "valid_time_grains": ["MONTHLY"],
        "valid_geo_grains": ["COUNTY"],
        "aggregation_characteristic": None,
        "physical_lineage": {},
    }
    metric.update(overrides)
    return metric


def _fred_three() -> dict[str, dict]:
    return {
        "FRED:UNRATE": _metric("FRED:UNRATE", "FRED"),
        "FRED:CIVPART": _metric("FRED:CIVPART", "FRED"),
        "FRED:EMRATIO": _metric("FRED:EMRATIO", "FRED"),
    }


def _total(n: int = 3) -> dict[str, Any]:
    return {
        "kind": "total",
        "code_a": None,
        "code_b": None,
        "n": n,
        "contemporaneous": None,
        "distinct_a": None,
        "distinct_b": None,
        "pearson_r": None,
        "spearman_rho": None,
        "period_count": None,
        "period_min": None,
    }


def _metric_fact(
    code: str, geographies: int = 3, period: str = "2023"
) -> dict[str, Any]:
    return {
        "kind": "metric",
        "code_a": code,
        "code_b": None,
        "n": geographies,
        "contemporaneous": None,
        "distinct_a": None,
        "distinct_b": None,
        "pearson_r": None,
        "spearman_rho": None,
        "period_count": 1,
        "period_min": period,
    }


def _pair_fact(
    code_a: str,
    code_b: str,
    n: int = 3,
    contemporaneous: Optional[int] = None,
    distinct_a: int = 3,
    distinct_b: int = 3,
    pearson_r: Optional[float] = 0.5,
    spearman_rho: Optional[float] = 0.4,
) -> dict[str, Any]:
    return {
        "kind": "pair",
        "code_a": code_a,
        "code_b": code_b,
        "n": n,
        "contemporaneous": n if contemporaneous is None else contemporaneous,
        "distinct_a": distinct_a,
        "distinct_b": distinct_b,
        "pearson_r": pearson_r,
        "spearman_rho": spearman_rho,
        "period_count": None,
        "period_min": None,
    }


def _wide_row(**overrides) -> dict[str, Any]:
    row = {
        "geo_id": "county:06001",
        "geo_level": "COUNTY",
        "state_fips": "06",
        "county_fips": "001",
        "state_name": "California",
        "county_name": "Alameda",
        "v0": 4.2,
        "p0": "2023-01-01",
        "r0": "2024-01-05",
        "v1": 62.1,
        "p1": "2023-01-01",
        "r1": "2024-01-05",
        "v2": None,
        "p2": None,
        "r2": None,
    }
    row.update(overrides)
    return row


class _MatrixSession:
    """Resolves glossary lookups per code, records the dispatched SQL."""

    def __init__(self, metric_rows: dict[str, dict], facts=None, rows=None):
        self._metric_rows = metric_rows
        self._facts = facts
        self._rows = rows if rows is not None else [_wide_row()]
        self.statements: list[str] = []
        self.parameters: list[dict[str, Any]] = []

    def execute(self, query, params=None):
        rendered = str(query)
        self.statements.append(rendered)
        self.parameters.append(dict(params or {}))
        if "gold_glossary.dim_metric" in rendered:
            row = self._metric_rows.get((params or {}).get("metric_code"))
            return _FakeResult(rows=[row] if row else [])
        if "'total'" in rendered:
            return _FakeResult(rows=list(self._facts or []))
        return _FakeResult(rows=list(self._rows))


class _FakeResult:
    def __init__(self, rows=None):
        self._rows = rows or []

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def one(self):
        return self._rows[0]

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return None


def _client_with(session) -> TestClient:
    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    return TestClient(app)


def _dispatched(session: _MatrixSession) -> list[str]:
    return [
        statement
        for statement in session.statements
        if "gold_glossary.dim_metric" not in statement
        and "to_regclass" not in statement
    ]


def _three_comparable_facts() -> list[dict[str, Any]]:
    return [
        _total(3),
        _metric_fact("FRED:UNRATE"),
        _metric_fact("FRED:CIVPART"),
        _metric_fact("FRED:EMRATIO"),
        _pair_fact("FRED:UNRATE", "FRED:CIVPART"),
        _pair_fact("FRED:UNRATE", "FRED:EMRATIO", pearson_r=-0.8, spearman_rho=-0.7),
        _pair_fact("FRED:CIVPART", "FRED:EMRATIO", pearson_r=0.9, spearman_rho=0.9),
    ]


# --------------------------------------------------------------------------
# The bound, and what refuses the whole request (criteria 1 and 2)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "codes",
    ["FRED:UNRATE", "A,B,C,D,E,F,G,H,I"],
)
def test_fewer_than_two_or_more_than_eight_measures_is_refused(codes: str) -> None:
    """Covers: API-132 — the 2-8 bound is enforced before any lookup."""
    session = _MatrixSession(_fred_three(), facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        response = client.get(MATRIX, params={"metric_codes": codes})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert not _dispatched(session)


def test_a_repeated_measure_is_refused_rather_than_correlated_with_itself() -> None:
    """Covers: API-132 — the codes must be distinct."""
    session = _MatrixSession(_fred_three(), facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        response = client.get(
            MATRIX, params={"metric_codes": "FRED:UNRATE,FRED:UNRATE"}
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert "distinct" in response.json()["detail"]
    assert not _dispatched(session)


def test_an_unknown_code_is_a_404_naming_it() -> None:
    """Covers: API-132 — an unknown metric refuses the whole request."""
    session = _MatrixSession(_fred_three(), facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        response = client.get(MATRIX, params={"metric_codes": "FRED:UNRATE,NO:SUCH"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert "NO:SUCH" in response.json()["detail"]
    assert not _dispatched(session)


def test_an_analysis_refused_source_refuses_the_whole_request() -> None:
    """Covers: API-132 — a hole for a declined source is not the honest shape.

    A declined *pair* is a cell, because the pair is what was declined. A
    declined *source* is the whole request, because the refusal is about the
    measure itself: a matrix with a row of holes labelled "stratified" invites
    exactly the reading the refusal exists to prevent.
    """
    metrics = _fred_three()
    metrics["CDC:cdi:X:crude"] = _metric("CDC:cdi:X:crude", "CDC")
    session = _MatrixSession(metrics, facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        response = client.get(
            MATRIX, params={"metric_codes": "FRED:UNRATE,CDC:cdi:X:crude"}
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "CDC:cdi:X:crude" in detail
    assert "stratified" in detail
    assert not _dispatched(session)


def test_every_pair_declined_answers_422_naming_each_failure() -> None:
    """Covers: API-132 — a matrix with no comparable cell is not an answer."""
    metrics = {
        "FRED:UNRATE": _metric("FRED:UNRATE", "FRED", units="Percent"),
        "BLS:LNS14000000": _metric("BLS:LNS14000000", "BLS", units="Persons"),
    }
    session = _MatrixSession(metrics, facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        response = client.get(
            MATRIX, params={"metric_codes": "FRED:UNRATE,BLS:LNS14000000"}
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "units differ" in detail
    assert "preflight" in detail
    assert not _dispatched(session)


def test_one_declined_pair_is_a_cell_and_the_request_still_answers() -> None:
    """Covers: API-132 — a 3x3 with one declined cell is still an answer."""
    metrics = _fred_three()
    metrics["BLS:LNS14000000"] = _metric("BLS:LNS14000000", "BLS", units="Persons")
    facts = [
        _total(3),
        _metric_fact("FRED:UNRATE"),
        _metric_fact("FRED:CIVPART"),
        _metric_fact("BLS:LNS14000000"),
        _pair_fact("FRED:UNRATE", "FRED:CIVPART"),
    ]
    session = _MatrixSession(metrics, facts=facts)
    client = _client_with(session)
    try:
        response = client.get(
            MATRIX,
            params={"metric_codes": "FRED:UNRATE,FRED:CIVPART,BLS:LNS14000000"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    pairs = {(p["metric_code_a"], p["metric_code_b"]): p for p in payload["pairs"]}
    assert len(pairs) == 3, "every unordered pair is a cell, declined or not"

    served = pairs[("FRED:UNRATE", "FRED:CIVPART")]
    assert served["comparable"] is True
    assert served["statistic"]["pearson_r"] == 0.5

    declined = pairs[("FRED:UNRATE", "BLS:LNS14000000")]
    assert declined["comparable"] is False
    assert declined["statistic"] is None
    assert any(
        rule["status"] == "fail" and "units differ" in rule["reason"]
        for rule in declined["rules"]
    )


# --------------------------------------------------------------------------
# The answer's shape (criteria 3 and 4)
# --------------------------------------------------------------------------


def test_the_matrix_answers_every_pair_and_every_measure() -> None:
    """Covers: API-132 — three measures make three cells and three summaries."""
    session = _MatrixSession(_fred_three(), facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        response = client.get(
            MATRIX,
            params={
                "metric_codes": "FRED:UNRATE,FRED:CIVPART,FRED:EMRATIO",
                "geo_level": "county",
            },
        )
    finally:
        app.dependency_overrides.clear()

    payload = response.json()
    assert payload["derived"] is True
    assert payload["geo_level"] == "COUNTY"
    assert [m["metric_code"] for m in payload["metrics"]] == [
        "FRED:UNRATE",
        "FRED:CIVPART",
        "FRED:EMRATIO",
    ]
    assert payload["metrics"][0]["source_code"] == "FRED"
    assert payload["metrics"][0]["units"] == "Percent"
    assert payload["metrics"][0]["valid_geo_grains"] == ["COUNTY"]
    assert payload["metrics"][0]["geographies"] == 3
    assert payload["metrics"][0]["period"] == "2023"

    assert len(payload["pairs"]) == 3
    for pair in payload["pairs"]:
        assert pair["statistic"]["derivations"] == list(CORRELATION_DERIVATIONS)
    assert payload["caveats"][0] == CORRELATION_CAUSATION_CAVEAT


def test_every_published_value_on_a_wide_row_carries_its_period_and_release() -> None:
    """Covers: API-133 — a cell is a value, its period and its release."""
    session = _MatrixSession(_fred_three(), facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        response = client.get(
            MATRIX,
            params={"metric_codes": "FRED:UNRATE,FRED:CIVPART,FRED:EMRATIO"},
        )
    finally:
        app.dependency_overrides.clear()

    (row,) = response.json()["items"]
    assert row["geo_id"] == "county:06001"
    cells = {cell["metric_code"]: cell for cell in row["values"]}
    assert len(cells) == 3, "one cell per requested measure, published or not"
    assert cells["FRED:UNRATE"] == {
        "metric_code": "FRED:UNRATE",
        "value": 4.2,
        "period": "2023-01-01",
        "release": "2024-01-05",
    }
    # A geography with no published value for one measure carries a null cell,
    # not an absent one: the shape says "asked and not published".
    assert cells["FRED:EMRATIO"]["value"] is None
    assert cells["FRED:EMRATIO"]["period"] is None


def test_the_wide_rows_page_a_declared_total_order() -> None:
    """Covers: API-133 — the union of geographies, ordered and paged."""
    session = _MatrixSession(_fred_three(), facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        response = client.get(
            MATRIX,
            params={
                "metric_codes": "FRED:UNRATE,FRED:CIVPART,FRED:EMRATIO",
                "limit": 2,
                "offset": 4,
            },
        )
    finally:
        app.dependency_overrides.clear()

    payload = response.json()
    assert payload["limit"] == 2
    assert payload["offset"] == 4
    assert payload["total"] == 3

    page_sql = _dispatched(session)[-1]
    assert "ORDER BY geo_level, geo_id" in page_sql
    assert "LIMIT :limit OFFSET :offset" in page_sql
    assert session.parameters[-1]["limit"] == 2
    assert session.parameters[-1]["offset"] == 4


def test_the_statistics_are_measured_over_the_join_not_the_page() -> None:
    """Covers: API-133 — the statistics statement takes no paging bound."""
    session = _MatrixSession(_fred_three(), facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        client.get(
            MATRIX,
            params={
                "metric_codes": "FRED:UNRATE,FRED:CIVPART,FRED:EMRATIO",
                "limit": 1,
            },
        )
    finally:
        app.dependency_overrides.clear()

    statistics_sql = next(sql for sql in _dispatched(session) if "'total'" in sql)
    assert "LIMIT" not in statistics_sql.upper()
    assert "corr(" in statistics_sql


def test_the_union_of_geographies_is_the_key_not_an_inner_join() -> None:
    """Covers: API-133 — a geography one measure publishes is a row.

    ``/comparison`` joins its two sides inner, and that is right for a pair:
    a row with one side missing has no difference and no ratio. A matrix is
    the opposite question -- which measures is this geography published for --
    so its rows are the union, and an unpublished measure is a null cell.
    """
    session = _MatrixSession(_fred_three(), facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        client.get(
            MATRIX,
            params={"metric_codes": "FRED:UNRATE,FRED:CIVPART,FRED:EMRATIO"},
        )
    finally:
        app.dependency_overrides.clear()

    page_sql = _dispatched(session)[-1]
    assert "UNION" in page_sql, "the keys are the union of every side"
    assert page_sql.count("LEFT JOIN") == 3, "each side joins onto the keys"
    assert page_sql.count("recency_rank = 1") == 3


def test_every_relation_read_is_a_reviewed_one() -> None:
    """Covers: API-132 — the matrix reaches no relation the registry omits."""
    import re

    session = _MatrixSession(_fred_three(), facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        client.get(
            MATRIX,
            params={"metric_codes": "FRED:UNRATE,FRED:CIVPART,FRED:EMRATIO"},
        )
    finally:
        app.dependency_overrides.clear()

    for sql in _dispatched(session):
        relations = set(re.findall(r"(?:FROM|JOIN)\s+([a-z_]+\.[a-z_]+)", sql))
        assert relations <= ALLOWED_OBSERVATION_RELATIONS


# --------------------------------------------------------------------------
# Cell semantics carried over from the correlation route
# --------------------------------------------------------------------------


def test_a_cell_whose_pairs_cannot_carry_a_coefficient_is_null_with_its_reason() -> (
    None
):
    """Covers: API-133 — the WB-3 null rule holds cell by cell."""
    facts = [
        _total(3),
        _metric_fact("FRED:UNRATE"),
        _metric_fact("FRED:CIVPART"),
        _metric_fact("FRED:EMRATIO"),
        _pair_fact("FRED:UNRATE", "FRED:CIVPART", n=2, pearson_r=1.0, spearman_rho=1.0),
        _pair_fact(
            "FRED:UNRATE",
            "FRED:EMRATIO",
            distinct_b=1,
            pearson_r=None,
            spearman_rho=None,
        ),
        _pair_fact("FRED:CIVPART", "FRED:EMRATIO"),
    ]
    session = _MatrixSession(_fred_three(), facts=facts)
    client = _client_with(session)
    try:
        response = client.get(
            MATRIX,
            params={"metric_codes": "FRED:UNRATE,FRED:CIVPART,FRED:EMRATIO"},
        )
    finally:
        app.dependency_overrides.clear()

    pairs = {
        (p["metric_code_a"], p["metric_code_b"]): p for p in response.json()["pairs"]
    }
    too_few = pairs[("FRED:UNRATE", "FRED:CIVPART")]
    assert too_few["statistic"]["pearson_r"] is None
    assert any("2 paired geographies" in c for c in too_few["caveats"])

    constant = pairs[("FRED:UNRATE", "FRED:EMRATIO")]
    assert constant["statistic"]["spearman_rho"] is None
    assert any(
        "metric_code_b publishes one distinct value" in c for c in constant["caveats"]
    )

    served = pairs[("FRED:CIVPART", "FRED:EMRATIO")]
    assert served["statistic"]["pearson_r"] == 0.5


def test_a_cell_reports_its_own_contemporaneity() -> None:
    """Covers: API-133 — periods are reported per cell, never aligned away."""
    facts = [
        _total(10),
        _metric_fact("FRED:UNRATE", geographies=10),
        _metric_fact("FRED:CIVPART", geographies=10),
        _pair_fact("FRED:UNRATE", "FRED:CIVPART", n=10, contemporaneous=6),
    ]
    session = _MatrixSession(_fred_three(), facts=facts)
    client = _client_with(session)
    try:
        response = client.get(
            MATRIX, params={"metric_codes": "FRED:UNRATE,FRED:CIVPART"}
        )
    finally:
        app.dependency_overrides.clear()

    (pair,) = response.json()["pairs"]
    assert pair["statistic"]["contemporaneous_pairs"] == 6
    assert pair["statistic"]["periods_differ"] is True
    assert any("4 of 10" in caveat for caveat in pair["caveats"])


def test_the_year_pin_reaches_every_side() -> None:
    """Covers: API-133 — one pin, applied to all of them."""
    session = _MatrixSession(_fred_three(), facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        response = client.get(
            MATRIX,
            params={
                "metric_codes": "FRED:UNRATE,FRED:CIVPART,FRED:EMRATIO",
                "year": 2023,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.json()["year"] == 2023


# ---------------------------------------------------------------------------
# API-136 — the refusals and the pin agree with the routes beside them
# ---------------------------------------------------------------------------


def test_a_source_with_no_dispatch_entry_is_refused_not_a_failure() -> None:
    """Covers: API-136 — a 422 here, as `/comparison` gives for the same code.

    The glossary can publish a metric whose source has no reviewed dispatch
    entry: warehouse work lands before API registry work by design. The pair
    routes answer that with the 422 `compatibility._source_finding` composes,
    because they evaluate the pair before touching the registry. This route
    resolved the dispatch first, so `observation_dispatch` raised and the same
    measure answered a sanitized 500 here and a 422 there.
    """
    metrics = _fred_three()
    metrics["MYSTERY:X"] = _metric("MYSTERY:X", "MYSTERY")
    session = _MatrixSession(metrics, facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        response = client.get(MATRIX, params={"metric_codes": "MYSTERY:X,FRED:UNRATE"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "MYSTERY" in detail
    assert "no reviewed observation dispatch entry" in detail
    assert not _dispatched(session)


def test_the_matrix_pins_a_year_the_way_each_source_filters_one() -> None:
    """Covers: API-136 — the same pin the correlation uses, for every side."""
    from apps.api.registry import OBSERVATION_DISPATCH

    session = _MatrixSession(_fred_three(), facts=_three_comparable_facts())
    client = _client_with(session)
    try:
        response = client.get(
            MATRIX,
            params={
                "metric_codes": "FRED:UNRATE,FRED:CIVPART,FRED:EMRATIO",
                "year": 2023,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    page_sql = _dispatched(session)[-1]
    declared = dict(OBSERVATION_DISPATCH["FRED"].filter_conditions)
    for condition in (declared["year_from"], declared["year_to"]):
        assert page_sql.count(condition) == 3, "every side must carry the pin"
    assert "FROM 1 FOR 4" not in page_sql
    assert session.parameters[-1]["year_from"] == 2023
    assert session.parameters[-1]["year_to"] == 2023
