"""Static guards against synthesizing an area total from agency observations."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from data_ingestion_toolbox.fbi_ucr.registry import (
    FbiSubject,
    SUMMARIZED_VIOLENT_CRIME,
)

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
MIGRATION = REPOSITORY_ROOT / "sql/migrations/011_fbi_ucr_pipeline.sql"
PACKAGE = REPOSITORY_ROOT / "src/data_ingestion_toolbox/fbi_ucr"
DAG = REPOSITORY_ROOT / "dags/fbi_ucr_ingest_dag.py"

#: Labels the plan forbids for any product built from agency observations.
FORBIDDEN_LABELS = ("county_total", "city_total", "place_total")


def _gold_view_bodies() -> dict[str, str]:
    sql = MIGRATION.read_text(encoding="utf-8")
    pattern = re.compile(
        r"CREATE OR REPLACE VIEW gold_fbi\.(?P<name>\w+) AS(?P<body>.*?);\s*\n",
        re.DOTALL,
    )
    return {match.group("name"): match.group("body") for match in pattern.finditer(sql)}


def test_the_expected_gold_products_exist() -> None:
    """Covers: ETL-042 — the published product set is explicit."""
    assert set(_gold_view_bodies()) == {
        "crime_observation",
        "reporting_coverage",
        "agency_geography",
        "agency_observation_area_filter",
        "latest_release_observation",
        "measure_export",
        "metric_publisher",
    }


def test_no_gold_view_aggregates_observation_values() -> None:
    """Covers: ETL-042 — agency values are never summed into an area total."""
    aggregate = re.compile(
        r"\b(SUM|AVG|MIN|MAX)\s*\(\s*[\w.]*\b"
        r"(value|population|participated_population|coverage_percent"
        r"|population_denominator)\b",
        re.IGNORECASE,
    )
    offenders = {
        name: aggregate.findall(body)
        for name, body in _gold_view_bodies().items()
        if aggregate.search(body)
    }

    assert offenders == {}


def test_area_filter_view_declares_agency_grain_and_never_a_total() -> None:
    """Covers: ETL-042 — a county or place filter keeps the agency grain."""
    body = _gold_view_bodies()["agency_observation_area_filter"]

    assert "'agency' AS observation_grain" in body
    assert "agency-reported for agencies associated with this county" in body
    assert "agency-reported for agencies mapped to this place" in body
    assert "GROUP BY" not in body.upper()
    assert "observation.observation_sk" in body


def test_area_filter_uses_relationship_effectivity_not_evidence_release() -> None:
    """Covers: ETL-042 — later refreshes retain effective area filters."""
    body = _gold_view_bodies()["agency_observation_area_filter"]

    assert "observation.period_start >= relationship.effective_start" in body
    assert "observation.period_end <= relationship.effective_end" in body
    assert "relationship.release_key = observation.release_key" not in body


def test_no_forbidden_area_total_label_is_published() -> None:
    """Covers: ETL-042 — the banned area-total labels never appear."""
    sources = [MIGRATION, DAG, *sorted(PACKAGE.rglob("*.py"))]
    offenders = [
        f"{path.relative_to(REPOSITORY_ROOT).as_posix()}:{label}"
        for path in sources
        for label in FORBIDDEN_LABELS
        if label in path.read_text(encoding="utf-8")
    ]

    assert offenders == []


def test_national_and_state_totals_are_read_only_from_their_endpoints() -> None:
    """Covers: ETL-042 — provider totals are never reconstructed locally."""
    transform = (PACKAGE / "silver_fbi/transform.py").read_text(encoding="utf-8")

    assert SUMMARIZED_VIOLENT_CRIME.observation_endpoint(
        FbiSubject("national", "US")
    ) == "/summarized/national/V"
    assert SUMMARIZED_VIOLENT_CRIME.observation_endpoint(
        FbiSubject("state", "WI")
    ) == "/summarized/state/WI/V"
    # Conformance never derives one subject's value from another's rows.
    assert "SUM(" not in transform.upper()
