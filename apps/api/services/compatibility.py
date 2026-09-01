"""Declared comparison compatibility policy (API-005).

Whether two metrics may be compared is a reviewed decision over their
published glossary semantics, not a side effect of whichever rows happen to
join. Each rule is evaluated three-valued:

- ``pass`` -- the published semantics support the comparison.
- ``fail`` -- the published semantics contradict it; the pair is not
  comparable and the comparison route rejects it with this explanation.
- ``unknown`` -- the source publishes nothing to check (Census ACS publishes
  no units, some sources publish no aggregation characteristic). Unknown is
  not incompatibility: the comparison is served, and the unverified rule is
  stated as a caveat instead of being silently assumed to pass.

The policy reads only published contracts: the glossary row's ``units``,
``valid_time_grains``, ``valid_geo_grains``, and
``aggregation_characteristic``, plus the reviewed dispatch registry's
``analysis_ready`` declaration. It never inspects warehouse rows, so the
verdict is deterministic for a given publication.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Optional

from apps.api.registry import OBSERVATION_DISPATCH

STATUS_PASS = "pass"
STATUS_FAIL = "fail"
STATUS_UNKNOWN = "unknown"

RULE_SOURCES = "source_analysis_ready"
RULE_UNITS = "units"
RULE_TIME_GRAINS = "time_grains"
RULE_GEO_GRAINS = "geo_grains"
RULE_AGGREGATION = "aggregation"

#: The derived values the comparison route computes when a pair is comparable.
#: Both are explicitly API-derived; the provider-published inputs travel with
#: every row.
COMPARISON_DERIVATIONS = ("difference", "ratio")


@dataclass(frozen=True)
class RuleFinding:
    rule: str
    status: str
    reason: str


@dataclass(frozen=True)
class CompatibilityDecision:
    """The full verdict for one metric pair, machine-readable."""

    comparable: bool
    derivations: tuple[str, ...]
    findings: tuple[RuleFinding, ...]
    caveats: tuple[str, ...]

    def failure_summary(self) -> str:
        """One sentence naming every failed rule, for the 422 detail."""
        reasons = "; ".join(
            finding.reason for finding in self.findings if finding.status == STATUS_FAIL
        )
        return f"metrics are not comparable: {reasons}"


def _units_of(metric: Mapping[str, Any]) -> Optional[str]:
    units = metric.get("units")
    if units is None or not str(units).strip():
        return None
    return str(units).strip()


def _grains_of(metric: Mapping[str, Any], field: str) -> frozenset[str]:
    grains = metric.get(field) or ()
    return frozenset(str(grain).upper() for grain in grains if grain)


def _source_finding(metric: Mapping[str, Any], label: str) -> RuleFinding:
    source_code = str(metric.get("source_code") or "")
    dispatch = OBSERVATION_DISPATCH.get(source_code)
    if dispatch is None:
        return RuleFinding(
            RULE_SOURCES,
            STATUS_FAIL,
            f"{label} belongs to source '{source_code}', which has no "
            "reviewed observation dispatch entry",
        )
    if not dispatch.analysis_ready:
        restriction = dispatch.analysis_restriction or (
            f"source '{source_code}' is not served by the aligned analysis routes"
        )
        return RuleFinding(RULE_SOURCES, STATUS_FAIL, f"{label}: {restriction}")
    return RuleFinding(
        RULE_SOURCES,
        STATUS_PASS,
        f"{label} is served by source '{source_code}', whose latest surface "
        "reduces to one newest value per geography",
    )


def evaluate_comparison(
    metric_a: Mapping[str, Any], metric_b: Mapping[str, Any]
) -> CompatibilityDecision:
    """Evaluate every declared rule for the pair; nothing short-circuits.

    All findings are returned even when an early rule fails, so the preflight
    explains the whole disagreement instead of one symptom at a time.
    """
    findings: list[RuleFinding] = []
    caveats: list[str] = []

    source_a = _source_finding(metric_a, "metric_code_a")
    source_b = _source_finding(metric_b, "metric_code_b")
    findings.extend(finding for finding in (source_a, source_b))

    units_a = _units_of(metric_a)
    units_b = _units_of(metric_b)
    if units_a is not None and units_b is not None:
        if units_a.casefold() == units_b.casefold():
            findings.append(
                RuleFinding(
                    RULE_UNITS, STATUS_PASS, f"both metrics publish '{units_a}'"
                )
            )
        else:
            findings.append(
                RuleFinding(
                    RULE_UNITS,
                    STATUS_FAIL,
                    f"units differ ('{units_a}' vs '{units_b}'); a difference "
                    "or ratio of unlike units would present incomparable "
                    "quantities as comparable",
                )
            )
    else:
        unpublished = [
            label
            for label, units in (
                ("metric_code_a", units_a),
                ("metric_code_b", units_b),
            )
            if units is None
        ]
        reason = (
            f"{' and '.join(unpublished)} publish no units; unit "
            "compatibility cannot be verified from the publication"
        )
        findings.append(RuleFinding(RULE_UNITS, STATUS_UNKNOWN, reason))
        caveats.append(reason)

    for rule, field, label in (
        (RULE_TIME_GRAINS, "valid_time_grains", "time grains"),
        (RULE_GEO_GRAINS, "valid_geo_grains", "geography grains"),
    ):
        grains_a = _grains_of(metric_a, field)
        grains_b = _grains_of(metric_b, field)
        if grains_a and grains_b:
            shared = grains_a & grains_b
            if shared:
                findings.append(
                    RuleFinding(
                        rule,
                        STATUS_PASS,
                        f"shared {label}: {', '.join(sorted(shared))}",
                    )
                )
            else:
                findings.append(
                    RuleFinding(
                        rule,
                        STATUS_FAIL,
                        f"no shared {label} "
                        f"({', '.join(sorted(grains_a))} vs "
                        f"{', '.join(sorted(grains_b))})",
                    )
                )
        else:
            reason = (
                f"published {label} are incomplete; {label} compatibility "
                "cannot be verified"
            )
            findings.append(RuleFinding(rule, STATUS_UNKNOWN, reason))
            caveats.append(reason)

    aggregation_a = metric_a.get("aggregation_characteristic")
    aggregation_b = metric_b.get("aggregation_characteristic")
    if aggregation_a and aggregation_b:
        if aggregation_a == aggregation_b:
            findings.append(
                RuleFinding(
                    RULE_AGGREGATION,
                    STATUS_PASS,
                    f"both metrics declare '{aggregation_a}'",
                )
            )
        else:
            reason = (
                f"aggregation characteristics differ ('{aggregation_a}' vs "
                f"'{aggregation_b}'); derived values must not be summed "
                "across geographies"
            )
            findings.append(RuleFinding(RULE_AGGREGATION, STATUS_UNKNOWN, reason))
            caveats.append(reason)
    else:
        reason = (
            "aggregation characteristics are not fully published; do not sum "
            "derived values across geographies"
        )
        findings.append(RuleFinding(RULE_AGGREGATION, STATUS_UNKNOWN, reason))
        caveats.append(reason)

    comparable = all(finding.status != STATUS_FAIL for finding in findings)
    return CompatibilityDecision(
        comparable=comparable,
        derivations=COMPARISON_DERIVATIONS if comparable else (),
        findings=tuple(findings),
        caveats=tuple(caveats),
    )
