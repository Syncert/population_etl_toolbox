"""Helpers for disposable PostgreSQL integration tests."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import psycopg2
from psycopg2.extras import register_uuid
from psycopg2.extensions import connection

from data_ingestion_toolbox.utility import warehouse_manifest as _manifest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE_DATABASE_IMAGE = (
    "postgis/postgis:16-3.5-alpine@"
    "sha256:b193e996618e9e632e2c6e268462b350c28a9c871cb0352b32905fc01e0299bd"
)
EXPECTED_POSTGRES_MAJOR = 16
EXPECTED_POSTGIS_MAJOR_MINOR = "3.5"

# The manifest is read by `data_ingestion_toolbox.utility.warehouse_manifest`
# and not a second time here. A test warehouse built from a different reading
# of the bootstrap order than a deployment's is the one thing the manifest
# exists to prevent, and it is also what makes the ledger comparable: the rows
# this tier's warehouse carries are written by the same applier a deployment
# runs (DB-049).
WAREHOUSE_MANIFEST_PATH = _manifest.MANIFEST_PATH
WAREHOUSE_MANIFEST = _manifest.load_manifest()
WAREHOUSE_ASSET_RECORDS = _manifest.manifest_assets()
WAREHOUSE_ASSETS = tuple(WAREHOUSE_MANIFEST["assets"])
WAREHOUSE_DDL_FILES = _manifest.asset_paths(WAREHOUSE_ASSET_RECORDS)
RAW_DDL_FILES = _manifest.asset_paths(
    tuple(asset for asset in WAREHOUSE_ASSET_RECORDS if asset.phase == "raw")
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
    """Apply repository SQL files as one transaction.

    Still one transaction, and still taking paths: several callers apply a
    subset (the raw phase alone, or a fixture's own file) where a ledger row
    would be a claim about a manifest step that did not run. Building a whole
    warehouse goes through `apply_warehouse_manifest` below instead.
    """
    with database_connection.cursor() as cursor:
        for path in paths:
            cursor.execute(path.read_text(encoding="utf-8"))


def apply_warehouse_manifest(database_connection: connection) -> tuple[str, ...]:
    """Build the warehouse the way every other environment builds it.

    One transaction per asset, each committed with the ledger row that claims
    it, so this tier's warehouse can answer `DQ-SHARED-004` exactly as a
    deployment's does (DB-049).
    """
    return _manifest.apply_manifest(database_connection)


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
