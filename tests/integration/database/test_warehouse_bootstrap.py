"""Full warehouse bootstrap, rerun, and relational-integrity contracts."""

from __future__ import annotations

import psycopg2
import pytest
from psycopg2.extensions import connection

from tests.support.postgres import WAREHOUSE_DDL_FILES, apply_sql_files

pytestmark = [pytest.mark.integration, pytest.mark.database]

WAREHOUSE_SCHEMAS = {
    "raw_census",
    "raw_bls",
    "raw_fred",
    "silver_ref",
    "silver_census",
    "silver_bls",
    "silver_fred",
    "gold_glossary",
    "gold_census",
    "gold_bls",
    "gold_fred",
    "gold",
}

REQUIRED_RELATIONS = {
    ("silver_ref", "dim_geo", "r"),
    ("silver_ref", "dim_time", "r"),
    ("silver_census", "fact_demographics", "r"),
    ("silver_bls", "fact_labor_statistics", "r"),
    ("silver_fred", "fact_economic_indicators", "r"),
    ("gold_glossary", "dim_metric_catalog", "r"),
    ("gold_census", "rpt_acs_observations", "r"),
    ("gold_bls", "rpt_bls_observations", "r"),
    ("gold_fred", "rpt_fred_observations", "r"),
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
    "dashboard_suitability",
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
        cursor.execute(
            """
            INSERT INTO silver_ref.dim_geo (
                geo_sk, geo_level, geo_id, state_fips, name,
                is_active, source, source_year, ingested_at
            ) VALUES (
                999999, 'state', 'state:55', '55', 'Wisconsin',
                TRUE, 'test', 2024, NOW()
            )
            """
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
