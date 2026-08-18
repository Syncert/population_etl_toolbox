"""Helpers for disposable PostgreSQL integration tests."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import psycopg2
from psycopg2.extras import register_uuid
from psycopg2.extensions import connection

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE_DATABASE_IMAGE = (
    "postgis/postgis:16-3.5-alpine@"
    "sha256:b193e996618e9e632e2c6e268462b350c28a9c871cb0352b32905fc01e0299bd"
)
EXPECTED_POSTGRES_MAJOR = 16
EXPECTED_POSTGIS_MAJOR_MINOR = "3.5"
REFERENCE_DDL_FILES = (
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/silver_ref/DDL/silver_ref.sql",
)
RAW_DDL_FILES = (
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/census_acs/DDL/raw_census.sql",
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/bls/DDL/raw_bls.sql",
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/fred/DDL/raw_fred.sql",
)
SILVER_DDL_FILES = (
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/census_acs/DDL/silver_census.sql",
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/bls/DDL/silver_bls.sql",
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/fred/DDL/silver_fred.sql",
)
GOLD_DDL_FILES = (
    REPOSITORY_ROOT
    / "src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs.sql",
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/bls/gold_bls/DDL/gold_bls.sql",
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/fred/gold_fred/DDL/gold_fred.sql",
)
PUBLISHER_DDL_FILES = (
    REPOSITORY_ROOT
    / "src/data_ingestion_toolbox/census_acs/gold_census/DDL/publisher.sql",
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/bls/gold_bls/DDL/publisher.sql",
    REPOSITORY_ROOT / "src/data_ingestion_toolbox/fred/gold_fred/DDL/publisher.sql",
)
CONTRACT_DDL_FILES = (
    REPOSITORY_ROOT / "sql/gold_contract/002_gold_glossary_schema.sql",
    REPOSITORY_ROOT / "sql/gold_contract/001_gold_contract_views.sql",
)
FOUNDATION_MIGRATION_DDL_FILES = (
    REPOSITORY_ROOT / "sql/migrations/001_raw_capture_control_foundation.sql",
)
GLOSSARY_MIGRATION_DDL_FILES = (
    REPOSITORY_ROOT / "sql/migrations/002_gold_glossary_decoupling.sql",
)
SOURCE_CUTOVER_DDL_FILES = (
    REPOSITORY_ROOT / "sql/migrations/004_fred_capture_cutover.sql",
    REPOSITORY_ROOT / "sql/migrations/005_census_acs_capture_cutover.sql",
    REPOSITORY_ROOT / "sql/migrations/006_bls_capture_cutover.sql",
)
WAREHOUSE_DDL_FILES = (
    *FOUNDATION_MIGRATION_DDL_FILES,
    *GLOSSARY_MIGRATION_DDL_FILES,
    *REFERENCE_DDL_FILES,
    *RAW_DDL_FILES,
    *SILVER_DDL_FILES,
    *SOURCE_CUTOVER_DDL_FILES,
    *GOLD_DDL_FILES,
    *PUBLISHER_DDL_FILES,
    *CONTRACT_DDL_FILES,
)


@dataclass(frozen=True)
class PostgresTestConfig:
    """Connection settings accepted only for an explicitly named test database."""

    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_environment(cls) -> "PostgresTestConfig | None":
        values = {
            name: os.environ.get(f"TEST_POSTGRES_{name.upper()}")
            for name in ("host", "port", "user", "password", "database")
        }
        if not any(values.values()):
            return None

        missing = [name for name, value in values.items() if not value]
        if missing:
            names = ", ".join(f"TEST_POSTGRES_{name.upper()}" for name in missing)
            raise RuntimeError(f"Incomplete test PostgreSQL configuration: {names}")

        database = str(values["database"])
        if not database.endswith("_test"):
            raise RuntimeError(
                "Refusing database integration tests because TEST_POSTGRES_DATABASE "
                "does not end with '_test'."
            )

        try:
            port = int(str(values["port"]))
        except ValueError as exc:
            raise RuntimeError("TEST_POSTGRES_PORT must be an integer") from exc

        return cls(
            host=str(values["host"]),
            port=port,
            user=str(values["user"]),
            password=str(values["password"]),
            database=database,
        )

    def connect(self) -> connection:
        """Open a short-timeout connection without exposing a DSN in test output."""
        database_connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
            connect_timeout=5,
            application_name="population_etl_integration_tests",
        )
        register_uuid(conn_or_curs=database_connection)
        return database_connection


def apply_sql_files(database_connection: connection, paths=WAREHOUSE_DDL_FILES) -> None:
    """Apply repository SQL files as one transaction."""
    with database_connection.cursor() as cursor:
        for path in paths:
            cursor.execute(path.read_text(encoding="utf-8"))


class ClosingConnection:
    """Connection context that always closes the underlying test connection."""

    def __init__(self, database_connection: connection) -> None:
        self._connection = database_connection

    def __enter__(self) -> connection:
        return self._connection

    def __getattr__(self, name: str):
        return getattr(self._connection, name)

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            if exc_type is None:
                self._connection.commit()
            else:
                self._connection.rollback()
        finally:
            self._connection.close()


class PostgresHookStub:
    """Minimal Airflow PostgresHook surface backed by disposable connections."""

    def __init__(self, connection_factory: Callable[[], connection]) -> None:
        self._connection_factory = connection_factory

    def get_conn(self) -> ClosingConnection:
        return ClosingConnection(self._connection_factory())
