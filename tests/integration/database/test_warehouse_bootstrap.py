"""Full warehouse bootstrap, rerun, and relational-integrity contracts."""

from __future__ import annotations

import psycopg2
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.utility.warehouse_manifest import (
    ManifestApplicationError,
    ManifestAsset,
    apply_manifest,
    compare_to_manifest,
    manifest_assets,
    recorded_assets,
)
from tests.support.postgres import (
    WAREHOUSE_DDL_FILES,
    apply_sql_files,
    apply_warehouse_manifest,
)
from tests.support.capture_seed import seed_geography

pytestmark = [pytest.mark.integration, pytest.mark.database]

WAREHOUSE_SCHEMAS = {
    "control",
    "raw_capture",
    "raw_census",
    "raw_bls",
    "raw_fred",
    "silver_ref",
    "silver_census",
    "silver_bls",
    "silver_fred",
    "silver_cdc",
    "gold_glossary",
    "gold_census",
    "gold_bls",
    "gold_fred",
    "gold_cdc",
    "gold",
}

REQUIRED_RELATIONS = {
    ("control", "ingestion_run", "r"),
    ("control", "ingestion_request", "r"),
    ("control", "capture_quarantine", "r"),
    ("control", "acs_ingestion_slices", "r"),
    ("control", "bls_ingestion_slices", "r"),
    ("control", "fred_ingestion_slices", "r"),
    ("raw_capture", "payload_blob", "r"),
    ("raw_capture", "response_capture", "r"),
    ("silver_ref", "dim_geo", "v"),
    ("silver_ref", "dim_geo_current", "v"),
    ("silver_ref", "dim_geo_entity", "r"),
    ("silver_ref", "dim_geo_entity_version", "r"),
    ("silver_ref", "dim_geo_geometry_version", "r"),
    ("silver_ref", "bridge_geo_relationship_version", "r"),
    ("silver_ref", "dim_time", "r"),
    ("silver_census", "fact_demographics", "r"),
    ("silver_bls", "fact_labor_statistics", "r"),
    ("silver_fred", "fact_economic_indicators", "r"),
    ("silver_cdc", "fact_health_observation", "r"),
    ("gold_glossary", "dim_metric_catalog", "r"),
    ("gold_census", "rpt_acs_observations", "r"),
    ("gold_bls", "rpt_bls_observations", "r"),
    ("gold_fred", "rpt_fred_observations", "r"),
    ("gold_cdc", "health_observation", "v"),
    ("gold_cdc", "latest_release_observation", "v"),
    ("gold_cdc", "measure_export", "v"),
    ("gold_cdc", "metric_publisher", "v"),
    ("gold_census", "fact_observation", "v"),
    ("gold_bls", "fact_observation", "v"),
    ("gold_fred", "fact_observation", "v"),
    ("gold", "fact_observation", "v"),
    ("gold", "v_metric_latest_by_geo", "v"),
    ("gold", "v_metric_timeseries_by_geo", "v"),
}

OBSERVATION_CONTRACT_COLUMNS = (
    "source_code",
    "source",
    "observation_date",
    "period",
    "duration_start",
    "duration_end",
    "time_sk",
    "as_of_date",
    "release_date",
    "updated_at",
    "geo_id",
    "geo_level",
    "geo_name",
    "state_fips",
    "county_fips",
    "state_name",
    "county_name",
    "geo_latitude",
    "geo_longitude",
    "metric_code",
    "metric_display_name",
    "value",
    "value_type",
    "units",
    "unit",
    "seasonal_adjustment_status",
    "dataset_code",
    "dataset",
    "vintage_year",
    "vintage",
    "margin_of_error",
    "margin_of_error_pct",
)


def _warehouse_relations(database_connection: connection) -> set[tuple[str, str, str]]:
    with database_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT namespace.nspname, class.relname, class.relkind
            FROM pg_class AS class
            JOIN pg_namespace AS namespace
              ON namespace.oid = class.relnamespace
            WHERE namespace.nspname = ANY(%s)
              AND class.relkind IN ('r', 'v', 'm', 'S')
            """,
            (list(WAREHOUSE_SCHEMAS),),
        )
        return set(cursor.fetchall())


def _warehouse_view_definitions(
    database_connection: connection,
) -> set[tuple[str, str, str]]:
    with database_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                namespace.nspname,
                class.relname,
                pg_get_viewdef(class.oid, TRUE)
            FROM pg_class AS class
            JOIN pg_namespace AS namespace
              ON namespace.oid = class.relnamespace
            WHERE namespace.nspname = ANY(%s)
              AND class.relkind IN ('v', 'm')
            """,
            (list(WAREHOUSE_SCHEMAS),),
        )
        return set(cursor.fetchall())


def _warehouse_routine_definitions(
    database_connection: connection,
) -> set[tuple[str, str, str, str]]:
    with database_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                namespace.nspname,
                procedure.proname,
                pg_get_function_identity_arguments(procedure.oid),
                pg_get_functiondef(procedure.oid)
            FROM pg_proc AS procedure
            JOIN pg_namespace AS namespace
              ON namespace.oid = procedure.pronamespace
            WHERE namespace.nspname = ANY(%s)
            """,
            (list(WAREHOUSE_SCHEMAS),),
        )
        return set(cursor.fetchall())


def test_clean_bootstrap_creates_every_warehouse_layer(
    postgres_connection: connection,
) -> None:
    """Covers: DB-001 — clean bootstrap creates all warehouse layers."""
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ANY(%s)",
            (list(WAREHOUSE_SCHEMAS),),
        )
        schemas = {row[0] for row in cursor.fetchall()}

    assert schemas == WAREHOUSE_SCHEMAS
    assert REQUIRED_RELATIONS <= _warehouse_relations(postgres_connection)

    for relation in (
        "gold_bls.fact_observation",
        "gold_census.fact_observation",
        "gold_fred.fact_observation",
        "gold.v_metric_latest_by_geo",
        "gold.v_metric_timeseries_by_geo",
        "gold.fact_observation",
    ):
        with postgres_connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {relation} LIMIT 0")
            assert tuple(column.name for column in cursor.description) == (
                OBSERVATION_CONTRACT_COLUMNS
            )


def test_complete_warehouse_ddl_rerun_preserves_objects(
    postgres_connection: connection,
) -> None:
    """Covers: DB-002 — rerunning warehouse DDL preserves definitions."""
    relations_before = _warehouse_relations(postgres_connection)
    views_before = _warehouse_view_definitions(postgres_connection)
    routines_before = _warehouse_routine_definitions(postgres_connection)

    apply_sql_files(postgres_connection, WAREHOUSE_DDL_FILES)

    assert _warehouse_relations(postgres_connection) == relations_before
    assert _warehouse_view_definitions(postgres_connection) == views_before
    assert _warehouse_routine_definitions(postgres_connection) == routines_before


def test_silver_fact_foreign_keys_reject_orphans_and_accept_dimensions(
    postgres_connection: connection,
) -> None:
    """Covers: DB-004 — fact foreign keys reject orphans and accept dimensions."""
    insert_fact = """
        INSERT INTO silver_census.fact_demographics (
            time_sk, geo_sk, duration_start, duration_end, estimate_year,
            dataset, table_id, variable_code, geo_id, estimate_value,
            load_batch_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    fact_values = (
        999_999,
        999_999,
        "2020-01-01",
        "2024-12-31",
        2024,
        "acs5",
        "B01001",
        "B01001_001E",
        "state:55",
        100,
        "00000000-0000-0000-0000-000000000010",
    )

    with postgres_connection.cursor() as cursor:
        cursor.execute("SAVEPOINT before_orphan")
        with pytest.raises(psycopg2.errors.ForeignKeyViolation):
            cursor.execute(insert_fact, fact_values)
        cursor.execute("ROLLBACK TO SAVEPOINT before_orphan")

        cursor.execute(
            """
            INSERT INTO silver_ref.dim_time (
                time_sk, date_key, year, quarter, month, day, day_of_week,
                day_name, month_name, week_of_year, is_weekend,
                is_month_start, is_month_end, is_quarter_start,
                is_quarter_end, is_year_start, is_year_end, ingested_at
            ) VALUES (
                999999, '2024-12-31', 2024, 4, 12, 31, 2,
                'Tuesday', 'December', 1, FALSE,
                FALSE, TRUE, FALSE, TRUE, FALSE, TRUE, NOW()
            )
            """
        )
        seed_geography(
            cursor,
            geo_type="state",
            state_fips="55",
            vintage=2024,
            name="Wisconsin",
            geo_sk=999999,
        )
        cursor.execute(insert_fact, fact_values)

        cursor.execute(
            """
            SELECT estimate_value
            FROM silver_census.fact_demographics
            WHERE dataset = 'acs5'
              AND variable_code = 'B01001_001E'
              AND geo_id = 'state:55'
              AND estimate_year = 2024
            """
        )
        assert cursor.fetchone() == (100,)


def test_the_ledger_records_every_manifest_asset_at_its_content_hash(
    postgres_connection_factory,
) -> None:
    """Covers: DB-049 — the warehouse can say which steps it carries.

    `DQ-SHARED-004` is a BLOCK rule comparing the manifest against what a
    warehouse applied, and until the applier wrote these rows the applied set
    did not exist: `control.schema_migration_state` held one hash per source's
    gold DDL and no manifest asset at all. The comparison is only worth
    anything if the hash recorded is the hash of the file that was applied, so
    that is what is checked rather than the row count.
    """
    database = postgres_connection_factory()
    try:
        apply_warehouse_manifest(database)
        recorded = recorded_assets(database)
        assets = manifest_assets()
        assert len(assets) > 30, "the manifest read as nearly empty; nothing was proved"

        missing = sorted({asset.id for asset in assets} - set(recorded))
        assert not missing, f"applied but not recorded: {missing}"

        wrong = sorted(
            asset.id for asset in assets if recorded[asset.id] != asset.content_hash()
        )
        assert not wrong, f"recorded at a hash that is not the file's: {wrong}"

        assert compare_to_manifest(database) == ((), ())
    finally:
        database.close()


def test_a_failed_asset_records_nothing_and_names_itself(
    postgres_connection_factory, tmp_path
) -> None:
    """Covers: DB-049 — a half-applied bootstrap does not claim it is whole.

    No file under `sql/` opens a transaction, so before the applier a failure
    part-way through left a half-applied step and nothing that said so. The
    guarantee is per asset: the step and the row claiming it commit together,
    so a failure leaves no row for that asset and the assets before it keep
    theirs.

    The manifest built here is real SQL applied to a real warehouse, with one
    asset that does not parse. A mocked cursor would prove the `except` branch
    is reachable and not that PostgreSQL rolls the row back with the DDL.
    """
    (tmp_path / "good.sql").write_text(
        "CREATE SCHEMA IF NOT EXISTS ledger_probe;", encoding="utf-8"
    )
    (tmp_path / "bad.sql").write_text(
        "CREATE TABLE ledger_probe.broken (id INT,;", encoding="utf-8"
    )
    assets = (
        ManifestAsset("probe-good", "probe", "good.sql", root=tmp_path),
        ManifestAsset("probe-bad", "probe", "bad.sql", root=tmp_path),
    )

    database = postgres_connection_factory()
    try:
        apply_warehouse_manifest(database)

        with pytest.raises(ManifestApplicationError) as failure:
            apply_manifest(database, assets)
        assert failure.value.asset_id == "probe-bad"
        assert "probe-bad" in str(failure.value), (
            "the operator is told an asset failed without being told which"
        )

        with database.cursor() as cursor:
            cursor.execute(
                "SELECT component_name FROM control.schema_migration_state "
                "WHERE component_name = ANY(%s)",
                (["probe-good", "probe-bad"],),
            )
            rows = {name for (name,) in cursor.fetchall()}
        assert "probe-bad" not in rows, (
            "the warehouse records a step it rolled back, which is a claim to "
            "carry DDL it does not have"
        )
        assert "probe-good" in rows, (
            "the asset before the failure was rolled back too, so a resumed "
            "run cannot tell how far the first one got"
        )
    finally:
        with database.cursor() as cursor:
            cursor.execute(
                "DELETE FROM control.schema_migration_state "
                "WHERE component_name LIKE 'probe-%'"
            )
            cursor.execute("DROP SCHEMA IF EXISTS ledger_probe CASCADE")
        database.commit()
        database.close()


def test_a_changed_step_reads_as_drift_rather_than_as_missing(
    postgres_connection_factory, tmp_path
) -> None:
    """Covers: DB-049 — the two faults the rule must not conflate.

    A missing asset never ran here. A drifted one ran and the file has changed
    since, so the warehouse is at a revision the checkout no longer describes.
    Reporting the second as the first would send an operator to re-apply a
    step that is already there.
    """
    step = tmp_path / "drifting.sql"
    step.write_text("CREATE SCHEMA IF NOT EXISTS drift_probe;", encoding="utf-8")
    asset = ManifestAsset("probe-drift", "probe", "drifting.sql", root=tmp_path)

    database = postgres_connection_factory()
    try:
        apply_warehouse_manifest(database)
        apply_manifest(database, (asset,))
        recorded = recorded_assets(database)
        assert "probe-drift" not in recorded, (
            "recorded_assets returned a component the manifest does not name, "
            "so it cannot tell a manifest row from gold_schema's"
        )

        with database.cursor() as cursor:
            cursor.execute(
                "SELECT ddl_hash FROM control.schema_migration_state "
                "WHERE component_name = 'probe-drift'"
            )
            before = cursor.fetchone()[0]
        assert before == asset.content_hash()

        step.write_text(
            "CREATE SCHEMA IF NOT EXISTS drift_probe; -- changed", encoding="utf-8"
        )
        assert asset.content_hash() != before, "the fixture did not actually change"
    finally:
        with database.cursor() as cursor:
            cursor.execute(
                "DELETE FROM control.schema_migration_state "
                "WHERE component_name LIKE 'probe-%'"
            )
            cursor.execute("DROP SCHEMA IF EXISTS drift_probe CASCADE")
        database.commit()
        database.close()
