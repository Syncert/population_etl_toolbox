"""Helpers for disposable PostgreSQL integration tests."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import psycopg2
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
CONTRACT_DDL_FILES = (
    REPOSITORY_ROOT / "sql/gold_contract/002_gold_glossary_schema.sql",
    REPOSITORY_ROOT / "sql/gold_contract/001_gold_contract_views.sql",
)
WAREHOUSE_DDL_FILES = (
    *REFERENCE_DDL_FILES,
    *RAW_DDL_FILES,
    *SILVER_DDL_FILES,
    *GOLD_DDL_FILES,
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
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
            connect_timeout=5,
            application_name="population_etl_integration_tests",
        )


def apply_sql_files(database_connection: connection, paths=WAREHOUSE_DDL_FILES) -> None:
    """Apply repository SQL files as one transaction."""
    with database_connection.cursor() as cursor:
        for path in paths:
            cursor.execute(path.read_text(encoding="utf-8"))
