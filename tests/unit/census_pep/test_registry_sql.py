"""Static alignment contracts for the Census PEP SQL registry."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from data_ingestion_toolbox.census_pep.config import CONFIG

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_SQL = REPOSITORY_ROOT / "sql" / "migrations" / "009_census_pep_registry.sql"
WAREHOUSE_MANIFEST = REPOSITORY_ROOT / "sql" / "bootstrap" / "warehouse_manifest.json"
SILVER_SQL = (
    REPOSITORY_ROOT
    / "src"
    / "data_ingestion_toolbox"
    / "census_pep"
    / "DDL"
    / "silver_pep.sql"
)


def test_sql_registry_contains_only_configured_products_and_releases() -> None:
    """Covers: ETL-030 — Python and SQL PEP release identities remain aligned."""
    sql = REGISTRY_SQL.read_text(encoding="utf-8")

    for dataset_code in CONFIG.datasets:
        assert f"'{dataset_code}'" in sql
    for release in CONFIG.releases:
        assert f"'{release.product_code}'" in sql
        assert f"'{release.data_url}'" in sql
        assert f"'{release.layout_url}'" in sql

    assert "pep_annual_estimates" not in sql
    assert "pep_interim_estimates" not in sql
    assert "pep_aging_estimates" not in sql


def test_sql_release_key_separates_vintage_and_observation_range() -> None:
    """Covers: ETL-030 — SQL PEP keys distinguish vintage from observations."""
    sql = REGISTRY_SQL.read_text(encoding="utf-8")

    assert "PRIMARY KEY (dataset_code, vintage_year)" in sql
    assert "observation_start_year" in sql
    assert "observation_end_year" in sql
    assert "geography_basis_date" in sql
    assert "CHECK (observation_end_year = vintage_year)" in sql
    assert "CHECK (status IN ('published', 'archived'))" in sql


def test_pep_registry_is_in_authoritative_bootstrap_order() -> None:
    """Covers: ETL-030 — PEP registry participates in clean bootstrap."""
    assets = json.loads(WAREHOUSE_MANIFEST.read_text(encoding="utf-8"))["assets"]
    paths = [asset["path"] for asset in assets]

    assert "sql/migrations/009_census_pep_registry.sql" in paths
    assert "src/data_ingestion_toolbox/census_pep/DDL/silver_pep.sql" in paths
    assert paths.index(
        "sql/migrations/008_geography_reference_cutover.sql"
    ) < paths.index("sql/migrations/009_census_pep_registry.sql")
    assert paths.index("sql/migrations/009_census_pep_registry.sql") < paths.index(
        "src/data_ingestion_toolbox/census_pep/DDL/silver_pep.sql"
    )


def test_silver_revision_schema_uses_release_and_observation_keys() -> None:
    """Covers: ARC-002 — PEP silver revisions remain capture and release scoped."""
    sql = SILVER_SQL.read_text(encoding="utf-8")

    for column in (
        "capture_id",
        "dataset_code",
        "release_vintage",
        "product_code",
        "observation_year",
        "metric_code",
        "unit",
        "summary_level",
        "value_source",
        "value_status",
    ):
        assert column in sql
    assert "PRIMARY KEY (capture_id, source_row_index, source_column_index)" in sql
    assert "file_type" not in sql
    assert "census-pep-bulk-csv-v1" in sql
