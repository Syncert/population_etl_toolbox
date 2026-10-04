"""Static guards on the declared-derived county roll-up publication.

Covers: ETL-053 — the one gold aggregate is derived, labelled, coverage-
honest, and multi-county-explicit. The provider-faithful views keep their
never-sum rule in ``test_fbi_aggregation_boundary.py``; this file pins the
contract of the single carved-out exception.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
GOLD_DDL = (
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/fbi_ucr/gold_fbi/DDL/gold_fbi.sql"
)


def _rollup_body() -> str:
    sql = GOLD_DDL.read_text(encoding="utf-8")
    match = re.search(
        r"CREATE OR REPLACE VIEW gold_fbi\.county_rollup AS(?P<body>.*?);\s*\n",
        sql,
        re.DOTALL,
    )
    assert match is not None, "gold_fbi.county_rollup is not defined"
    return match.group("body")


def test_the_rollup_declares_itself_derived() -> None:
    """Covers: ETL-053 — a consumer can tell this from a provider fact."""
    body = _rollup_body()

    assert "TRUE AS derived" in body
    assert "'sum_of_agency_reported_totals'::TEXT AS derivation_method" in body
    assert (
        "'derived county roll-up of agency-reported totals'::TEXT" in body
    )
    assert "not additive to state totals" in body


def test_the_rollup_reads_only_resolved_effective_county_relationships() -> None:
    """Covers: ETL-053 — identity comes from the resolved mapping only."""
    body = _rollup_body()

    assert "relationship.relationship_type = 'county'" in body
    assert "relationship.resolution_status = 'resolved'" in body
    # Contributions and coverage both follow the observation's own period
    # through the effective-dated relationship window.
    assert "observation.period_start >= link.effective_start" in body
    assert "observation.period_end <= link.effective_end" in body
    assert "summed.period_start >= county_link.effective_start" in body
    assert "summed.period_end <= county_link.effective_end" in body


def test_the_rollup_sums_only_absolute_offense_and_clearance_totals() -> None:
    """Covers: ETL-053 — no rate, percentage, or trend is aggregated."""
    body = _rollup_body()

    assert "observation.subject_type = 'agency'" in body
    assert "observation.measure_form = 'absolute_total'" in body
    assert (
        "observation.counted_entity_basis IN ('offense', 'clearance')" in body
    )
    assert "population_denominator" not in body
    assert "rate" not in body.lower().replace("derivation_method", "")


def test_the_rollup_never_turns_a_missing_report_into_zero() -> None:
    """Covers: ETL-053 — only reported values sum; no empty period row."""
    body = _rollup_body()

    sum_clause = re.search(
        r"SUM\(contribution\.value\)\s*\n?\s*FILTER \(WHERE "
        r"contribution\.value_status = 'reported'\)",
        body,
    )
    assert sum_clause is not None
    assert re.search(
        r"HAVING COUNT\(\*\) FILTER \(WHERE "
        r"contribution\.value_status = 'reported'\) > 0",
        body,
    )
    assert "COALESCE(contribution.value" not in body
    assert "COALESCE(observation.value" not in body
    assert "COALESCE(summed.value" not in body


def test_the_rollup_states_its_coverage_and_contributors() -> None:
    """Covers: ETL-053 — non-reporting agencies stay visible."""
    body = _rollup_body()

    assert "AS contributing_oris" in body
    assert "AS reporting_agency_count" in body
    assert "AS mapped_agency_count" in body
    # Reporting counts come from reported contributions only.
    assert re.search(
        r"COUNT\(DISTINCT contribution\.ori\)\s*\n?\s*FILTER \(WHERE "
        r"contribution\.value_status = 'reported'\)",
        body,
    )


def test_the_rollup_flags_multi_county_contributors() -> None:
    """Covers: ETL-053 — a multi-county agency counts in full and says so."""
    body = _rollup_body()

    assert "AS includes_multi_county_agency" in body
    assert re.search(
        r"COUNT\(DISTINCT sibling\.geo_id\) > 1", body
    ), "multi-county detection must count distinct resolved counties"
