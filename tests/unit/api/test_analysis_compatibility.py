"""API unit tests: the declared comparison compatibility policy and preflight.

Covers: API-049 (three-valued rule evaluation over published glossary
        semantics: units, time grains, geography grains, aggregation, and
        source analysis readiness — with unknown distinguished from
        incompatible),
        API-050 (the preflight resource explains the full verdict for any
        known pair without moving data, and answers a stable 404 naming the
        unknown parameter).
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.registry import OBSERVATION_DISPATCH
from apps.api.services.compatibility import (
    RULE_AGGREGATION,
    RULE_GEO_GRAINS,
    RULE_SOURCES,
    RULE_TIME_GRAINS,
    RULE_UNITS,
    STATUS_FAIL,
    STATUS_PASS,
    STATUS_UNKNOWN,
    evaluate_comparison,
)

from apps.api.registry import GEO_GRAIN_ALIASES

pytestmark = [pytest.mark.unit, pytest.mark.api]


def _metric(**overrides) -> dict:
    metric = {
        "metric_code": "FRED:UNRATE",
        "source_code": "FRED",
        "units": "Percent",
        "valid_time_grains": ["MONTHLY"],
        "valid_geo_grains": ["NATIONAL"],
        "aggregation_characteristic": None,
        "physical_lineage": {},
    }
    metric.update(overrides)
    return metric


def _finding(decision, rule):
    return next(finding for finding in decision.findings if finding.rule == rule)


# ---------------------------------------------------------------------------
# API-049 — rule evaluation
# ---------------------------------------------------------------------------


def test_identical_published_semantics_are_comparable() -> None:
    """Covers: API-049 — matching units and grains pass every checkable rule."""
    decision = evaluate_comparison(_metric(), _metric(metric_code="FRED:CIVPART"))
    assert decision.comparable is True
    assert decision.derivations == ("difference", "ratio")
    assert _finding(decision, RULE_UNITS).status == STATUS_PASS
    assert _finding(decision, RULE_TIME_GRAINS).status == STATUS_PASS
    assert _finding(decision, RULE_GEO_GRAINS).status == STATUS_PASS


def test_differing_units_are_incompatible_not_served() -> None:
    """Covers: API-049 — unlike units positively fail; no derivations remain."""
    decision = evaluate_comparison(_metric(), _metric(units="Persons"))
    assert decision.comparable is False
    assert decision.derivations == ()
    finding = _finding(decision, RULE_UNITS)
    assert finding.status == STATUS_FAIL
    assert "Percent" in finding.reason and "Persons" in finding.reason
    assert "units differ" in decision.failure_summary()


def test_unpublished_units_are_unknown_with_a_caveat_not_a_rejection() -> None:
    """Covers: API-049 — ACS publishes no units; unknown is not incompatible."""
    decision = evaluate_comparison(_metric(units=None), _metric())
    assert decision.comparable is True
    finding = _finding(decision, RULE_UNITS)
    assert finding.status == STATUS_UNKNOWN
    assert any("units" in caveat for caveat in decision.caveats)


def test_disjoint_time_grains_are_incompatible() -> None:
    """Covers: API-049 — an annual and a monthly metric never compare."""
    decision = evaluate_comparison(
        _metric(valid_time_grains=["ANNUAL"]),
        _metric(valid_time_grains=["MONTHLY"]),
    )
    assert decision.comparable is False
    assert _finding(decision, RULE_TIME_GRAINS).status == STATUS_FAIL


def test_disjoint_geo_grains_are_incompatible() -> None:
    """Covers: API-049 — no shared geography grain means no aligned rows."""
    decision = evaluate_comparison(
        _metric(valid_geo_grains=["COUNTY"]),
        _metric(valid_geo_grains=["NATIONAL"]),
    )
    assert decision.comparable is False
    assert _finding(decision, RULE_GEO_GRAINS).status == STATUS_FAIL


def test_grain_case_differences_do_not_defeat_the_intersection() -> None:
    """Covers: API-049 — grains compare case-insensitively."""
    decision = evaluate_comparison(
        _metric(valid_time_grains=["annual"]),
        _metric(valid_time_grains=["ANNUAL"]),
    )
    assert _finding(decision, RULE_TIME_GRAINS).status == STATUS_PASS


def test_aggregation_disagreement_is_a_caveat_never_a_rejection() -> None:
    """Covers: API-049 — additivity differences warn about summing, only."""
    decision = evaluate_comparison(
        _metric(aggregation_characteristic="additive_within_subject"),
        _metric(aggregation_characteristic="non_additive"),
    )
    assert decision.comparable is True
    assert _finding(decision, RULE_AGGREGATION).status == STATUS_UNKNOWN
    assert any("summed" in caveat or "sum" in caveat for caveat in decision.caveats)


def test_stratified_sources_fail_with_their_declared_restriction() -> None:
    """Covers: API-049 — the declined sources carry reviewed reasons."""
    decision = evaluate_comparison(
        _metric(source_code="CDC", metric_code="CDC:cdi:X:crude"), _metric()
    )
    assert decision.comparable is False
    finding = _finding(decision, RULE_SOURCES)
    assert finding.status == STATUS_FAIL
    assert "stratified" in finding.reason
    assert finding.reason == (
        "metric_code_a: " + (OBSERVATION_DISPATCH["CDC"].analysis_restriction or "")
    )


def test_a_source_without_a_dispatch_entry_fails_the_source_rule() -> None:
    """Covers: API-049 — an undeclared source is a verdict, not a crash."""
    decision = evaluate_comparison(_metric(source_code="NEW_SOURCE"), _metric())
    assert decision.comparable is False
    assert "NEW_SOURCE" in _finding(decision, RULE_SOURCES).reason


def test_registry_declares_readiness_and_restrictions_coherently() -> None:
    """Covers: API-049 — every declined source explains itself, none both."""
    for dispatch in OBSERVATION_DISPATCH.values():
        if dispatch.analysis_ready:
            assert dispatch.analysis_restriction is None, dispatch.source_code
        else:
            assert dispatch.analysis_restriction, dispatch.source_code
    ready = {
        code
        for code, dispatch in OBSERVATION_DISPATCH.items()
        if dispatch.analysis_ready
    }
    assert ready == {"BLS", "CENSUS_ACS", "FRED", "CENSUS_PEP"}


# ---------------------------------------------------------------------------
# API-050 — the preflight resource
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, rows=None):
        self._rows = rows or []

    def mappings(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None


class _GlossarySession:
    """Resolves metric detail lookups from a code-keyed row map."""

    def __init__(self, metric_rows: dict[str, dict]):
        self._metric_rows = metric_rows

    def execute(self, query, params=None):
        if "gold_glossary.dim_metric" in str(query):
            row = self._metric_rows.get((params or {}).get("metric_code"))
            return _FakeResult(rows=[row] if row else [])
        raise AssertionError("preflight must not query beyond the glossary")


def _client_with(session) -> TestClient:
    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    return TestClient(app)


def test_preflight_explains_an_incompatible_pair_with_every_rule() -> None:
    """Covers: API-050 — the verdict is a 200 explanation, not an error."""
    rows = {
        "CDC:cdi:X:crude": _metric(
            metric_code="CDC:cdi:X:crude",
            source_code="CDC",
            units="cases",
            valid_time_grains=["ANNUAL"],
        ),
        "FRED:UNRATE": _metric(),
    }
    client = _client_with(_GlossarySession(rows))
    try:
        response = client.get(
            "/api/v1/comparison/preflight",
            params={
                "metric_code_a": "CDC:cdi:X:crude",
                "metric_code_b": "FRED:UNRATE",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["comparable"] is False
    assert payload["derivations"] == []
    assert payload["source_code_a"] == "CDC"
    assert payload["source_code_b"] == "FRED"
    by_rule: dict[str, list[dict]] = {}
    for rule in payload["rules"]:
        by_rule.setdefault(rule["rule"], []).append(rule)
    # One source finding per side: the CDC side fails, the FRED side passes.
    assert [rule["status"] for rule in by_rule[RULE_SOURCES]] == [
        STATUS_FAIL,
        STATUS_PASS,
    ]
    assert "stratified" in by_rule[RULE_SOURCES][0]["reason"]
    assert by_rule[RULE_UNITS][0]["status"] == STATUS_FAIL
    assert by_rule[RULE_TIME_GRAINS][0]["status"] == STATUS_FAIL


def test_preflight_confirms_a_compatible_pair_with_derivations() -> None:
    """Covers: API-050 — a comparable pair names its derived values."""
    rows = {
        "FRED:UNRATE": _metric(),
        "FRED:CIVPART": _metric(metric_code="FRED:CIVPART"),
    }
    client = _client_with(_GlossarySession(rows))
    try:
        response = client.get(
            "/api/v1/comparison/preflight",
            params={"metric_code_a": "FRED:UNRATE", "metric_code_b": "FRED:CIVPART"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["comparable"] is True
    assert payload["derivations"] == ["difference", "ratio"]


def test_preflight_answers_a_stable_404_naming_the_unknown_parameter() -> None:
    """Covers: API-050 — an unknown code is explained per parameter."""
    rows = {"FRED:UNRATE": _metric()}
    client = _client_with(_GlossarySession(rows))
    try:
        missing_b = client.get(
            "/api/v1/comparison/preflight",
            params={"metric_code_a": "FRED:UNRATE", "metric_code_b": "NO:SUCH"},
        )
    finally:
        app.dependency_overrides.clear()

    assert missing_b.status_code == 404
    assert missing_b.json() == {"detail": "metric_code_b not found"}


def test_a_source_that_publishes_uncertainty_is_named_in_the_caveats() -> None:
    """Covers: API-096 — a comparison says when it dropped a published bound.

    `ComparisonRow` publishes no uncertainty, which is a reasonable shape: the
    difference of two intervals is a statistic neither source published, and
    propagating them is a methodological decision this API does not make. It
    is not reasonable to say nothing. Census ACS is the one analysis-ready
    source that publishes a margin of error, and it is the source most
    comparisons involve, so `/comparison` served a `difference` and a `ratio`
    from two estimates whose margins the same API publishes on
    `/observations` -- in a response that already carries a `caveats` array
    built for exactly this.

    Source-agnostic: the fields come from the dispatch entry's own
    `uncertainty_expressions`, so a source that begins publishing one is named
    without an edit here.
    """
    from apps.api.registry import OBSERVATION_DISPATCH

    acs = OBSERVATION_DISPATCH["CENSUS_ACS"]
    published = [name for name, _ in acs.uncertainty_expressions]
    assert published, "the fixture assumes Census ACS publishes an uncertainty"

    decision = evaluate_comparison(
        _metric(
            metric_code="CENSUS_ACS:acs5:B01003_001E",
            source_code="CENSUS_ACS",
            units="people",
            valid_time_grains=["ANNUAL"],
            valid_geo_grains=["COUNTY"],
        ),
        _metric(
            metric_code="CENSUS_ACS:acs5:B19013_001E",
            source_code="CENSUS_ACS",
            units="people",
            valid_time_grains=["ANNUAL"],
            valid_geo_grains=["COUNTY"],
        ),
    )

    assert decision.comparable is True
    assert decision.derivations, "a published margin does not make a pair incomparable"
    named = [caveat for caveat in decision.caveats if "CENSUS_ACS" in caveat]
    assert named, decision.caveats
    for field in published:
        assert any(field in caveat for caveat in named), (field, named)
    assert any("/observations" in caveat for caveat in named), named


def test_a_pair_publishing_no_uncertainty_earns_no_such_caveat() -> None:
    """Covers: API-096 — read from the registry, so silence stays silence."""
    decision = evaluate_comparison(_metric(), _metric(metric_code="FRED:CPI"))
    assert not [caveat for caveat in decision.caveats if "margin_of_error" in caveat], (
        decision.caveats
    )


def test_a_same_source_pair_says_it_once() -> None:
    """Covers: API-096 — one caveat per source, not one per side.

    The note names the source rather than the side, for both reasons that
    matters: a distribution has one metric and no side to name, and a
    comparison whose two sides are the same source would otherwise say the
    same thing twice under two labels. Saying it twice is noise, not
    emphasis.
    """
    same_source = evaluate_comparison(
        _metric(metric_code="CENSUS_ACS:acs5:A", source_code="CENSUS_ACS"),
        _metric(metric_code="CENSUS_ACS:acs5:B", source_code="CENSUS_ACS"),
    )
    published = [
        caveat for caveat in same_source.caveats if "margin_of_error" in caveat
    ]
    assert len(published) == 1, published
    # And it says "a derived value", not "a difference or a ratio": the same
    # sentence labels a distribution's bins, which derive neither.
    assert "a derived value" in published[0]
    assert "difference or a ratio" not in published[0]


# ---------------------------------------------------------------------------
# API-126 — one grain vocabulary decides comparability
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("alias", sorted(GEO_GRAIN_ALIASES))
def test_a_grain_the_vocabulary_replaced_is_the_grain_it_names(alias: str) -> None:
    """Covers: API-126 — comparability reads the vocabulary, not the spelling.

    The catalog published `NATION` for CDC, Census PEP and USDA NASS before
    the grains were unified, and ADR-0002 promises a value carrying one keeps
    answering. This policy upper-cased and compared, so a metric whose
    catalog row still carries the old word shared no grain with one carrying
    the new one -- and `no shared geography grains` is a `fail`, which makes
    the comparison route refuse a pair both measures publish nationally.
    """
    decision = evaluate_comparison(
        _metric(valid_geo_grains=[alias]),
        _metric(valid_geo_grains=[GEO_GRAIN_ALIASES[alias]]),
    )
    finding = _finding(decision, RULE_GEO_GRAINS)
    assert finding.status == STATUS_PASS, finding.reason
    assert GEO_GRAIN_ALIASES[alias] in finding.reason
    assert decision.comparable is True


def test_the_time_grain_rule_does_not_borrow_the_geography_aliases() -> None:
    """Covers: API-126 — each vocabulary is normalised as its own.

    `US` and `NATION` are geography words. A time grain spelled one of them
    is not a time grain, and mapping it to `NATIONAL` here would invent a
    shared grain out of two measures that publish none.
    """
    decision = evaluate_comparison(
        _metric(valid_time_grains=["NATION"]),
        _metric(valid_time_grains=["NATIONAL"]),
    )
    assert _finding(decision, RULE_TIME_GRAINS).status == STATUS_FAIL


@pytest.mark.parametrize(
    ("field", "rule", "label"),
    [
        ("valid_time_grains", RULE_TIME_GRAINS, "time grains"),
        ("valid_geo_grains", RULE_GEO_GRAINS, "geography grains"),
    ],
)
def test_an_unpublished_grain_says_which_measure_did_not_publish_it(
    field: str, rule: str, label: str
) -> None:
    """Covers: API-126 — an unknown names the side to go and look at.

    `unknown` is not incompatibility -- the comparison is served with the
    unverified rule as a caveat -- and the units rule beside this one already
    names which metric published nothing. This one said only that the grains
    were "incomplete", leaving a caller to guess which of two measures to
    check.
    """
    decision = evaluate_comparison(_metric(**{field: []}), _metric())
    finding = _finding(decision, rule)
    assert finding.status == STATUS_UNKNOWN
    assert finding.reason == (
        f"metric_code_a publish no {label}; {label} compatibility cannot be "
        f"verified from the publication"
    )
    assert finding.reason in decision.caveats
    assert decision.comparable is True, "an unknown is served with a caveat"

    both = evaluate_comparison(_metric(**{field: []}), _metric(**{field: None}))
    assert _finding(both, rule).reason.startswith("metric_code_a and metric_code_b")
