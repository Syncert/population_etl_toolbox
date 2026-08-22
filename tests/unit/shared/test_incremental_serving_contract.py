from pathlib import Path


import pytest


pytestmark = pytest.mark.unit
REPO_ROOT = Path(__file__).resolve().parents[3]
GLOSSARY_CONTRACT = REPO_ROOT / "sql/gold_contract/002_gold_glossary_schema.sql"
CONTROL_FOUNDATION = REPO_ROOT / "sql/migrations/001_raw_capture_control_foundation.sql"

SOURCE_FILES = {
    "acs": {
        "gold": REPO_ROOT
        / "src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs.sql",
        "silver": REPO_ROOT
        / "src/data_ingestion_toolbox/census_acs/silver_census/transform.py",
        "dag": REPO_ROOT / "dags/acs_ingest_dag.py",
        "dashboard_procedure": "refresh_dashboard_serving_layer_acs",
        "report_procedure": "refresh_rpt_acs_observations",
        "latest_procedure": "refresh_mv_acs_latest",
        "affected_keys": "gold_acs_affected_keys",
    },
    "bls": {
        "gold": REPO_ROOT / "src/data_ingestion_toolbox/bls/gold_bls/DDL/gold_bls.sql",
        "silver": REPO_ROOT / "src/data_ingestion_toolbox/bls/silver_bls/transform.py",
        "dag": REPO_ROOT / "dags/bls_ingest_dag.py",
        "dashboard_procedure": "refresh_dashboard_serving_layer_bls",
        "report_procedure": "refresh_rpt_bls_observations",
        "latest_procedure": "refresh_mv_bls_latest",
        "affected_keys": "gold_bls_affected_keys",
    },
    "fred": {
        "gold": REPO_ROOT
        / "src/data_ingestion_toolbox/fred/gold_fred/DDL/gold_fred.sql",
        "silver": REPO_ROOT
        / "src/data_ingestion_toolbox/fred/silver_fred/transform.py",
        "dag": REPO_ROOT / "dags/fred_ingest_dag.py",
        "dashboard_procedure": "refresh_dashboard_serving_layer_fred",
        "report_procedure": "refresh_rpt_fred_observations",
        "latest_procedure": "refresh_mv_fred_latest",
        "affected_keys": "gold_fred_affected_keys",
    },
}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_shared_geography_refresh_is_not_held_by_source_rebuilds() -> None:
    """Covers: ETL-037 — source rebuilds do not own shared geography refresh."""
    for source in SOURCE_FILES.values():
        sql = _read(source["gold"])
        assert "TRUNCATE TABLE gold_glossary.dim_geo_latest" not in sql
        assert "CALL gold_glossary.refresh_dim_geo_latest();" not in sql
        assert "pg_advisory_xact_lock" not in sql

    glossary_sql = _read(GLOSSARY_CONTRACT)
    assert "pg_advisory_xact_lock" in glossary_sql


def test_source_refreshes_are_watermarked_and_affected_key_scoped() -> None:
    """Covers: ETL-037 — serving refreshes use watermarks and affected keys."""
    for source in SOURCE_FILES.values():
        sql = _read(source["gold"])
        assert "control.serving_refresh_state" in sql
        assert "s.ingested_at > v_watermark" in sql
        assert source["affected_keys"] in sql
        assert "p_force_full BOOLEAN DEFAULT FALSE" in sql
        assert "SET LOCAL lock_timeout = '30s'" in sql
        assert "SET LOCAL statement_timeout = 0" not in sql


def test_chunk_checkpoint_table_is_installed_everywhere() -> None:
    """Covers: ETL-037 — every source installs durable chunk checkpoints."""
    foundation = _read(CONTROL_FOUNDATION)
    assert "control.serving_refresh_chunk_state" in foundation
    assert "completed_silver_ingested_at" in foundation
    assert "attempt_count" in foundation


def test_dags_refresh_changed_history_in_annual_chunks() -> None:
    """Covers: ETL-037 — DAGs refresh changed history in annual chunks."""
    for source in SOURCE_FILES.values():
        dag = _read(source["dag"])
        assert "get_gold_" not in dag or "_refresh_window" not in dag
        assert "SET statement_timeout = 0" not in dag
        assert "CALL gold_glossary.refresh_dim_geo_latest()" not in dag
        assert "refresh_serving_layer_in_year_chunks" in dag
        assert "MAKE_DATE" in dag
        assert source["report_procedure"] in dag
        assert source["latest_procedure"] in dag


def test_chunk_refreshes_emit_progress_and_row_count_logs() -> None:
    """Covers: ETL-037 — chunk refreshes emit status and row-count progress."""
    utility = _read(REPO_ROOT / "src/data_ingestion_toolbox/utility/gold_schema.py")
    assert "status=STARTED" in utility
    assert "status=COMPLETE" in utility
    assert "status=FAILED" in utility
    assert "status=SKIPPED" in utility
    assert "report_rows" in utility

    for source in SOURCE_FILES.values():
        sql = _read(source["gold"])
        assert "RPT CHUNK" in sql
        assert "LATEST CHUNK" in sql
        assert "GET DIAGNOSTICS v_inserted_rows = ROW_COUNT" in sql


def test_silver_upserts_preserve_watermarks_for_unchanged_rows() -> None:
    """Covers: ETL-037 — unchanged silver rows preserve their watermarks."""
    for source in SOURCE_FILES.values():
        transform = _read(source["silver"])
        assert "IS DISTINCT FROM" in transform
        assert "ingested_at = EXCLUDED.ingested_at\n        WHERE" in transform
