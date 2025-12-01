# airflow/dags/utility/db_connection.py

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any


try:
    # These imports will only succeed inside an Airflow environment
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    _AIRFLOW_AVAILABLE = True
except Exception:  # ImportError or airflow not installed
    PostgresHook = None  # type: ignore
    _AIRFLOW_AVAILABLE = False


@dataclass
class PostgresConnectionDetails:
    host: str
    port: int
    user: str
    password: str
    database: str

    def sqlalchemy_url(self, driver: str = "psycopg2") -> str:
        """
        Build a SQLAlchemy connection URL.

        Example:
            postgresql+psycopg2://user:pass@host:5432/dbname
        """
        return (
            f"postgresql+{driver}://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def psycopg_kwargs(self) -> Dict[str, Any]:
        """
        Build kwargs dict for psycopg / psycopg2.connect.
        """
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "dbname": self.database,
        }


class PostgresConnectionFactory:
    """
    Central place to get Postgres connection details from either:

    - Local dev (environment variables)
    - Airflow connection (via PostgresHook)

    This keeps your DAG code and local scripts using the same interface.
    """

    @staticmethod
    def from_env(
        prefix: str = "POSTGRES_",
        database: Optional[str] = None,
    ) -> PostgresConnectionDetails:
        """
        Read connection details from environment variables.

        Expected env vars (with default prefix 'POSTGRES_'):

            POSTGRES_HOST
            POSTGRES_PORT
            POSTGRES_USER
            POSTGRES_PASSWORD
            POSTGRES_DB   (or override via `database` arg)

        `database` argument overrides POSTGRES_DB if provided.
        """
        host = os.getenv(f"{prefix}HOST", "localhost")
        port_str = os.getenv(f"{prefix}PORT", "5432")
        user = os.getenv(f"{prefix}USER", "postgres")
        password = os.getenv(f"{prefix}PASSWORD", "")
        db_env = os.getenv(f"{prefix}DB", "postgres")

        db_name = database or db_env

        return PostgresConnectionDetails(
            host=host,
            port=int(port_str),
            user=user,
            password=password,
            database=db_name,
        )

    @staticmethod
    def from_airflow(
        conn_id: str,
        database: Optional[str] = None,
    ) -> PostgresConnectionDetails:
        """
        Read connection details from an Airflow Postgres connection.

        - `conn_id`: the Airflow connection id (e.g. 'public_datasets')
        - `database`: optional override for the database name

        If `database` is provided, it overrides the schema/database
        defined in the Airflow connection so you can point to a
        different database in the same Postgres instance.
        """
        if not _AIRFLOW_AVAILABLE or PostgresHook is None:
            raise RuntimeError(
                "Airflow is not available in this environment. "
                "Use PostgresConnectionFactory.from_env() instead."
            )

        # schema argument overrides the connection's schema/database
        hook = PostgresHook(postgres_conn_id=conn_id, schema=database)
        conn = hook.get_connection(conn_id)

        host = conn.host or "localhost"
        port = conn.port or 5432
        user = conn.login or "postgres"
        password = conn.password or ""
        # Prefer the override (schema passed into PostgresHook), then connection schema
        db_name = database or conn.schema or "postgres"

        return PostgresConnectionDetails(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=db_name,
        )

    @staticmethod
    def auto(
        conn_id: Optional[str] = None,
        prefix: str = "POSTGRES_",
        database: Optional[str] = None,
    ) -> PostgresConnectionDetails:
        """
        Convenience: try Airflow first (if conn_id given and Airflow present),
        otherwise fall back to environment variables.

        - In Airflow: use `conn_id` to get connection details.
        - On dev box with no Airflow: read env vars with `prefix`.
        """
        if conn_id and _AIRFLOW_AVAILABLE:
            return PostgresConnectionFactory.from_airflow(
                conn_id=conn_id,
                database=database,
            )

        return PostgresConnectionFactory.from_env(
            prefix=prefix,
            database=database,
        )