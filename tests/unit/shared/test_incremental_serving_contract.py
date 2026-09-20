from pathlib import Path


import pytest


pytestmark = pytest.mark.unit
REPO_ROOT = Path(__file__).resolve().parents[3]
GLOSSARY_CONTRACT = REPO_ROOT / "sql/gold_contract/002_gold_glossary_schema.sql"
CONTROL_FOUNDATION = REPO_ROOT / "sql/migrations/001_raw_capture_control_foundation.sql"
SERVING_RESERVE = REPO_ROOT / "src/data_ingestion_toolbox/utility/serving_reserve.py"

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
    """Covers: ETL-037 — DAGs refresh changed history in annual chunks.

    The chunk configuration moved to ``utility/serving_reserve.py`` so the
    operator-triggered full re-serve drives the same declarations rather than a
    second copy of them, so the annual plan and the procedure names are
    asserted there while the DAG is asserted to still drive the chunked path.
    """
    configs = _read(SERVING_RESERVE)
    for source in SOURCE_FILES.values():
        dag = _read(source["dag"])
        assert "get_gold_" not in dag or "_refresh_window" not in dag
        assert "SET statement_timeout = 0" not in dag
        assert "CALL gold_glossary.refresh_dim_geo_latest()" not in dag
        assert "refresh_serving_layer_in_year_chunks" in dag
        assert "MAKE_DATE" in configs
        assert source["report_procedure"] in configs
        assert source["latest_procedure"] in configs


def test_only_an_operator_can_select_the_full_reserve_plan() -> None:
    """Covers: ETL-037 — a scheduled run never triggers a full re-serve.

    A forced re-serve rewrites the whole relation; on ACS that is 68 million
    rows. No ingestion DAG may ask for one.
    """
    for source in SOURCE_FILES.values():
        assert "force_full" not in _read(source["dag"])

    operator_dag = _read(REPO_ROOT / "dags/serving_full_reserve_dag.py")
    assert "force_full=True" in operator_dag
    assert "schedule=None" in operator_dag


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


def test_acs_latest_refresh_uses_bounded_indexed_key_lookups() -> None:
    """Covers: ETL-037 — ACS latest refresh avoids a global historical-row sort."""
    sql = _read(SOURCE_FILES["acs"]["gold"])
    procedure = sql.split(
        "CREATE OR REPLACE PROCEDURE gold_census.refresh_mv_acs_latest(", 1
    )[1].split(
        "DROP PROCEDURE IF EXISTS gold_census.refresh_dashboard_serving_layer_acs", 1
    )[0]

    assert "ANALYZE gold_acs_affected_keys;" in procedure

    # The lookup is bounded two ways: by the affected keys, and by one
    # partition at a time. It used to be bounded only the first way, with a
    # `CROSS JOIN LATERAL ... LIMIT 1` per key -- which was a bounded lookup
    # right up until the relation was partitioned, at which point every key's
    # `LIMIT 1` became a `Merge Append` over all 37 partitions (DB-060). The
    # construct was never the property; it was a proxy for it that stopped
    # being one when the table shape changed underneath it.
    assert "gold_acs_pending_keys" in procedure
    assert "EXIT WHEN NOT EXISTS (SELECT 1 FROM gold_acs_pending_keys);" in procedure

    # And the resolve step reads *a* partition, passed in, rather than the
    # parent -- which is what "no global historical-row sort" means now.
    resolve = procedure.split("WITH resolved AS (", 1)[1].split("$resolve$", 1)[0]
    assert "FROM %1$s d" in resolve
    assert "gold_census.rpt_acs_observations" not in resolve, (
        "the resolve step reads the partitioned parent, so it ranks candidates "
        "from every partition again"
    )


def test_silver_upserts_preserve_watermarks_for_unchanged_rows() -> None:
    """Covers: ETL-037 — unchanged silver rows preserve their watermarks."""
    for source in SOURCE_FILES.values():
        transform = _read(source["silver"])
        assert "IS DISTINCT FROM" in transform
        assert "ingested_at = EXCLUDED.ingested_at\n        WHERE" in transform


def test_reporting_refreshes_never_write_the_raw_geography_vocabulary() -> None:
    """Covers: ETL-047 — served ``geo_level`` is the normalised vocabulary.

    ``silver_ref.dim_geo`` spells the national level ``us``; the served
    relations promise ``NATIONAL``. A refresh that prefers the dimension's
    column over the fact view's normalised one publishes ``us``, and
    ``UPPER(geo_level) = UPPER(:geo_level)`` in the API dispatch then answers
    nothing for ``geo_level=NATIONAL``.
    """
    offenders = []
    for name, source in SOURCE_FILES.items():
        sql = _read(source["gold"])
        if "gl.geo_level" in sql:
            offenders.append(name)
    assert not offenders, (
        "these reporting refreshes write silver_ref.dim_geo's raw geo_level "
        "vocabulary ('us'/'state'/'county') into relations that promise "
        f"NATIONAL/STATE/COUNTY: {offenders}"
    )


def test_fact_views_normalise_the_national_geography_level() -> None:
    """Covers: ETL-047, DB-037 — the normalised vocabulary has one definition.

    It now has exactly one: `gold_glossary.geo_grain`. The ACS and BLS fact
    views each carried their own `CASE ... LOWER(geo_level) = 'us' THEN
    'NATIONAL' ...` copy of it, which is the structure migration 018 exists to
    end. What stays in the views is the *other* rule beside it — a grain
    inferred from the row's identity when its producer wrote no grain word —
    and that is asserted here too, so consolidating the vocabulary cannot
    quietly take the inference with it.
    """
    for name in ("acs", "bls"):
        sql = _read(SOURCE_FILES[name]["gold"])
        assert "gold_glossary.geo_grain(s.geo_level)" in sql
        assert "LOWER(s.geo_level) = 'us'" not in sql
        assert "WHEN s.geo_id = 'us:1'             THEN 'NATIONAL'" in sql

    fred = _read(SOURCE_FILES["fred"]["gold"])
    assert "'NATIONAL'," in fred


#: A statement clock. Any of these in a published release or as-of expression
#: makes the value the moment the refresh ran rather than a fact about the row.
_STATEMENT_CLOCKS = (
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "LOCALTIMESTAMP",
    "NOW()",
    "STATEMENT_TIMESTAMP",
)


def _released_expressions() -> list[tuple[Path, str]]:
    """Every `... AS as_of_date` expression in the source gold DDL.

    Read from the files rather than listed, so a fourth source that publishes
    the column is covered the day it is written.
    """
    found = []
    for path in sorted(REPO_ROOT.glob("src/data_ingestion_toolbox/*/gold_*/DDL/*.sql")):
        for line in _read(path).splitlines():
            stripped = line.strip()
            if stripped.startswith("--"):
                continue
            if "AS as_of_date" in stripped:
                found.append((path, stripped))
    return found


def test_a_served_release_is_never_the_refresh_clock() -> None:
    """Covers: DB-039 — `as_of_date` is a fact about the row, not the refresh.

    `API_CONSUMER_GUIDE.md` says `release` and `as_of` "trace a row back to
    its publication". BLS, FRED and ACS all published `CURRENT_DATE AS
    as_of_date`, and the chunked serving refresh materialised that literal
    into the reporting table -- so a "release" was the calendar day a chunk
    was last written. The chunk driver re-serves only changed years, so
    re-serving 2019 on Monday and 2020 on Tuesday made
    `/observations/releases` list two published releases nobody published,
    and a full re-serve collapsed every release into one.

    BLS and FRED publish no release identity in their responses, so the
    honest identity is the warehouse's read, and `ingested_at` is exact about
    that: ETL-037's upsert advances it only when the row's content changed.
    Census PEP does carry a provider release date and uses it.
    """
    expressions = _released_expressions()
    assert expressions, "no gold DDL publishes as_of_date; the rule read nothing"

    offenders = [
        f"{path.name}: {expression}"
        for path, expression in expressions
        if any(clock in expression.upper() for clock in _STATEMENT_CLOCKS)
    ]
    assert not offenders, (
        "these serving views publish the refresh's clock as a release date, "
        "so a re-serve invents a release the provider never published: "
        + "; ".join(offenders)
    )


def test_no_seeded_serving_row_dates_itself_from_the_clock() -> None:
    """Covers: DB-039 — the live-stack fixtures encode what the refresh produces.

    `as_of_date` and `updated_at` on a served row are one fact: the refresh
    derives the release date from the silver row's `ingested_at`, and
    `updated_at` publishes that same value. The two live-stack seeds set
    `as_of_date` to a fixed date and `updated_at` to `NOW()`, so the fixture
    encoded a row the refresh can no longer produce -- and the seeds were not
    reproducible, since the row's timestamp moved with the day it was applied.
    That is the shape of defect the smoke tier's own header warns about: "the
    fixtures encoded a shape the real services do not serve".
    """
    offenders = []
    for path in sorted((REPO_ROOT / "tests/sql").glob("*.sql")):
        source = _read(path)
        for statement in source.split(";"):
            if "INSERT INTO" not in statement.upper():
                continue
            target = statement.upper().split("INSERT INTO", 1)[1].strip()
            if not (".RPT_" in target or ".MV_" in target):
                continue
            body = "\n".join(
                line
                for line in statement.splitlines()
                if not line.strip().startswith("--")
            )
            named = any(clock in body.upper() for clock in _STATEMENT_CLOCKS)
            if named:
                offenders.append(f"{path.name}: {target.splitlines()[0]}")
    assert not offenders, (
        "these fixtures date a served row from the clock, so the seed is not "
        "reproducible and the row is one the refresh would not write: "
        + "; ".join(offenders)
    )
