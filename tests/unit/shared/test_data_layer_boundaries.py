"""Static enforcement for the data-layer ownership decision."""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SOURCE_ROOT = REPOSITORY_ROOT / "src/data_ingestion_toolbox"

LEGACY_GOLD_DDL: dict[str, set[str]] = {}

POLICY_COLUMNS = (
    "business_definition",
    "dashboard_suitability",
    "comparability_group",
    "do_not_compare_with",
    "recommended_aggregation",
    "owner_team",
)


def _relative(path: Path) -> str:
    return path.relative_to(REPOSITORY_ROOT).as_posix()


def _shared_glossary_operations(sql: str) -> set[str]:
    pattern = re.compile(
        r"\b(?P<verb>CREATE(?:\s+OR\s+REPLACE)?\s+"
        r"(?:SCHEMA|TABLE|VIEW|PROCEDURE)|ALTER\s+TABLE|"
        r"DROP\s+(?:SCHEMA|TABLE|VIEW|PROCEDURE))"
        r"(?:\s+IF\s+(?:NOT\s+)?EXISTS)?\s+"
        r"(?P<object>gold_glossary(?:\.[a-z_][a-z0-9_]*)?)",
        re.IGNORECASE,
    )
    operations: set[str] = set()
    for match in pattern.finditer(sql):
        verb = re.sub(r"\s+OR\s+REPLACE", "", match.group("verb").upper())
        verb = re.sub(r"\s+", " ", verb)
        operations.add(f"{verb}:{match.group('object').lower()}")
    return operations


def test_source_ddl_cannot_expand_shared_glossary_ownership() -> None:
    """Covers: ARC-001 — only the frozen legacy DDL may define shared objects."""
    actual: dict[str, set[str]] = {}
    for path in sorted(SOURCE_ROOT.rglob("*.sql")):
        operations = _shared_glossary_operations(path.read_text(encoding="utf-8"))
        if operations:
            actual[_relative(path)] = operations

    assert actual == LEGACY_GOLD_DDL


def test_new_raw_ddl_requires_a_lossless_append_only_capture_contract() -> None:
    """Covers: ARC-002 — new raw DDL declares the lossless capture envelope."""
    required_terms = {
        "capture_id",
        "request_fingerprint",
        "retrieved_at",
        "checksum",
        "payload",
        "media_type",
    }

    source_sql = "\n".join(
        path.read_text(encoding="utf-8").lower()
        for path in sorted(SOURCE_ROOT.rglob("*.sql"))
    )
    assert not re.search(
        r"create\s+table[^;]+raw_(?:census|bls|fred)\.\w+_long", source_sql
    )

    foundation = (
        (REPOSITORY_ROOT / "sql/migrations/001_raw_capture_control_foundation.sql")
        .read_text(encoding="utf-8")
        .lower()
    )
    assert all(term in foundation for term in required_terms)
    assert "create trigger response_capture_reject_mutation" in foundation


def test_gold_ddl_cannot_add_dashboard_or_governance_policy_columns() -> None:
    """Covers: ARC-003 — new gold DDL cannot add consumer-policy columns."""
    failures: list[str] = []

    for path in sorted(SOURCE_ROOT.rglob("gold_*.sql")):
        relative = _relative(path)
        sql = path.read_text(encoding="utf-8")
        declarations = Counter(
            match.group("column").lower()
            for match in re.finditer(
                rf"^\s*(?P<column>{'|'.join(POLICY_COLUMNS)})\s+TEXT(?:\[\])?\b",
                sql,
                re.IGNORECASE | re.MULTILINE,
            )
        )
        expected = Counter()
        if declarations != expected:
            failures.append(
                f"{relative}: expected {dict(expected)}, found {dict(declarations)}"
            )

    assert failures == [], "\n".join(failures)


def test_source_ddl_and_dags_keep_execution_ledgers_in_control() -> None:
    """Covers: ARC-001 — shared control ownership excludes source raw schemas."""
    failures: list[str] = []
    roots = (SOURCE_ROOT, REPOSITORY_ROOT / "dags")
    pattern = re.compile(r"\braw_(?:census|bls|fred)\.\w*ingestion_slices\b")
    for root in roots:
        for path in sorted((*root.rglob("*.py"), *root.rglob("*.sql"))):
            if pattern.search(path.read_text(encoding="utf-8")):
                failures.append(_relative(path))

    assert failures == [], "execution ledgers remain raw-owned: " + ", ".join(failures)


def test_shared_reference_pipeline_is_the_only_geography_owner() -> None:
    """Covers: ARC-001, ARC-002 — no source owns a competing geography dimension."""
    source_paths = [
        *sorted((SOURCE_ROOT / "census_acs").rglob("*.py")),
        *sorted((SOURCE_ROOT / "census_acs").rglob("*.sql")),
        REPOSITORY_ROOT / "dags/acs_ingest_dag.py",
    ]
    legacy = re.compile(r"raw_census\.geo_dim|census_acs\.geography|sync_geo_dim")
    failures = [
        _relative(path)
        for path in source_paths
        if legacy.search(path.read_text(encoding="utf-8"))
    ]
    assert failures == [], "legacy ACS geography ownership remains: " + ", ".join(
        failures
    )

    pipeline = (SOURCE_ROOT / "silver_ref/geography_pipeline.py").read_text(
        encoding="utf-8"
    )
    assert pipeline.index("persist_response_capture(") < pipeline.index(
        "load_captured_payload("
    )
    ddl = (SOURCE_ROOT / "silver_ref/DDL/silver_ref.sql").read_text(encoding="utf-8")
    assert "REFERENCES raw_capture.response_capture(capture_id)" in ddl
