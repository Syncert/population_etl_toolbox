"""Static alignment contracts for the Census PEP SQL registry."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from data_ingestion_toolbox.census_pep.config import CONFIG

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_SQL = REPOSITORY_ROOT / "sql" / "migrations" / "009_census_pep_registry.sql"
HISTORICAL_SQL = (
    REPOSITORY_ROOT / "sql" / "migrations" / "015_census_pep_historical_series.sql"
)


def registry_sql() -> str:
    """Every migration that registers PEP products, read as one text.

    The registry spans two migrations: 009 opened it on the current
    decade, 015 widened it to the closed ones. A product is aligned when
    it appears in either.
    """
    return REGISTRY_SQL.read_text(encoding="utf-8") + HISTORICAL_SQL.read_text(
        encoding="utf-8"
    )


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
    sql = registry_sql()

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
    assert "CHECK (status IN ('published', 'archived'))" in sql


def test_sql_observation_range_ends_at_the_vintage_only_when_postcensal() -> None:
    """Covers: PEH-001 — an intercensal release may close an earlier decade.

    The original contract required every release to end at its own vintage,
    which is true of a postcensal series and false of an intercensal one.
    The effective constraint is the one 015 leaves behind.
    """
    historical = HISTORICAL_SQL.read_text(encoding="utf-8")

    assert "pep_release_observation_range_check" in historical
    assert "observation_end_year <= vintage_year" in historical
    assert "series_kind <> 'postcensal'" in historical
    # The superseded constraint is dropped rather than left to contradict it.
    assert "observation_end_year = vintage_year%" in historical


def test_sql_separates_the_april_count_from_the_july_estimate() -> None:
    """Covers: PEH-001 — a decennial count is not dated as a July estimate."""
    historical = HISTORICAL_SQL.read_text(encoding="utf-8")

    assert "fact_population_estimate_date_check" in historical
    assert "WHEN metric_code = 'CENSUSPOP' THEN 4 ELSE 7" in historical
    # The 2020-only floor on the observation year is replaced, not kept.
    assert "observation_year BETWEEN 1900 AND release_vintage" in historical


def test_sql_registers_one_product_per_closed_decade() -> None:
    """Covers: PEH-001 — the closed decades reach the SQL registry too."""
    historical = HISTORICAL_SQL.read_text(encoding="utf-8")

    for dataset_code in (
        "pep_county_alldata_2010s",
        "pep_nst_alldata_2010s",
        "pep_county_alldata_2000s",
    ):
        assert f"'{dataset_code}'" in historical
    for product_code in (
        "CO-EST2020-ALLDATA",
        "NST-EST2020-ALLDATA",
        "CO-EST2009-ALLDATA",
    ):
        assert f"'{product_code}'" in historical


def test_pep_registry_is_in_authoritative_bootstrap_order() -> None:
    """Covers: ETL-030 — PEP registry participates in clean bootstrap."""
    assets = json.loads(WAREHOUSE_MANIFEST.read_text(encoding="utf-8"))["assets"]
    paths = [asset["path"] for asset in assets]

    assert "sql/migrations/009_census_pep_registry.sql" in paths
    assert "src/data_ingestion_toolbox/census_pep/DDL/silver_pep.sql" in paths
    assert "sql/migrations/015_census_pep_historical_series.sql" in paths
    # 015 relaxes constraints the silver DDL creates, so it must follow it.
    assert paths.index(
        "src/data_ingestion_toolbox/census_pep/DDL/silver_pep.sql"
    ) < paths.index("sql/migrations/015_census_pep_historical_series.sql")
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
