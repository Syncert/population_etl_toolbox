"""Static enforcement for the data-layer ownership decision."""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SOURCE_ROOT = REPOSITORY_ROOT / "src/data_ingestion_toolbox"

LEGACY_GOLD_DDL = {
    "src/data_ingestion_toolbox/bls/gold_bls/DDL/gold_bls.sql": {
        "CREATE SCHEMA:gold_glossary",
        "CREATE VIEW:gold_glossary.dim_geo",
        "CREATE TABLE:gold_glossary.dim_source_system",
        "CREATE TABLE:gold_glossary.dim_metric_catalog",
        "CREATE TABLE:gold_glossary.serving_refresh_state",
        "CREATE TABLE:gold_glossary.serving_refresh_chunk_state",
        "CREATE TABLE:gold_glossary.bridge_metric_bls_series",
        "CREATE TABLE:gold_glossary.dim_geo_latest",
        "DROP PROCEDURE:gold_glossary.refresh_dim_geo_latest",
        "CREATE PROCEDURE:gold_glossary.refresh_dim_geo_latest",
    },
    "src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs.sql": {
        "CREATE SCHEMA:gold_glossary",
        "CREATE VIEW:gold_glossary.dim_geo",
        "CREATE VIEW:gold_glossary.dim_time",
        "CREATE TABLE:gold_glossary.dim_source_system",
        "CREATE TABLE:gold_glossary.dim_metric_catalog",
        "CREATE TABLE:gold_glossary.serving_refresh_state",
        "CREATE TABLE:gold_glossary.serving_refresh_chunk_state",
        "CREATE TABLE:gold_glossary.bridge_metric_acs_variable",
        "CREATE TABLE:gold_glossary.bridge_metric_bls_series",
        "CREATE TABLE:gold_glossary.bridge_metric_fred_series",
        "CREATE TABLE:gold_glossary.dim_geo_latest",
        "DROP PROCEDURE:gold_glossary.refresh_dim_geo_latest",
        "CREATE PROCEDURE:gold_glossary.refresh_dim_geo_latest",
    },
    "src/data_ingestion_toolbox/fred/gold_fred/DDL/gold_fred.sql": {
        "CREATE SCHEMA:gold_glossary",
        "CREATE VIEW:gold_glossary.dim_geo",
        "CREATE TABLE:gold_glossary.dim_source_system",
        "CREATE TABLE:gold_glossary.dim_metric_catalog",
        "CREATE TABLE:gold_glossary.serving_refresh_state",
        "CREATE TABLE:gold_glossary.serving_refresh_chunk_state",
        "CREATE TABLE:gold_glossary.bridge_metric_fred_series",
        "CREATE TABLE:gold_glossary.dim_geo_latest",
        "DROP PROCEDURE:gold_glossary.refresh_dim_geo_latest",
        "CREATE PROCEDURE:gold_glossary.refresh_dim_geo_latest",
    },
}

LEGACY_RAW_DDL = {
    "src/data_ingestion_toolbox/bls/DDL/raw_bls.sql",
    "src/data_ingestion_toolbox/census_acs/DDL/raw_census.sql",
    "src/data_ingestion_toolbox/fred/DDL/raw_fred.sql",
}

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
    failures: list[str] = []
    required_terms = {
        "capture_id",
        "request_fingerprint",
        "retrieved_at",
        "checksum",
        "payload",
        "media_type",
    }

    for path in sorted(SOURCE_ROOT.rglob("raw_*.sql")):
        relative = _relative(path)
        if relative in LEGACY_RAW_DDL:
            continue
        sql = path.read_text(encoding="utf-8").lower()
        missing = sorted(term for term in required_terms if term not in sql)
        if missing:
            failures.append(f"{relative}: missing {missing}")
        if re.search(r"\b(?:update|delete\s+from)\s+\w+\.\w*capture\w*", sql):
            failures.append(f"{relative}: capture tables must be append-only")

    assert failures == [], "\n".join(failures)


def test_gold_ddl_cannot_add_dashboard_or_governance_policy_columns() -> None:
    """Covers: ARC-003 — new gold DDL cannot add consumer-policy columns."""
    expected_legacy_columns = Counter({column: 2 for column in POLICY_COLUMNS})
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
        expected = expected_legacy_columns if relative in LEGACY_GOLD_DDL else Counter()
        if declarations != expected:
            failures.append(
                f"{relative}: expected {dict(expected)}, found {dict(declarations)}"
            )

    assert failures == [], "\n".join(failures)
