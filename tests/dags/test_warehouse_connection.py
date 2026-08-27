"""Warehouse connection resolution contracts for DAG task runtime."""

from __future__ import annotations

import warnings

import pytest

pytestmark = pytest.mark.dag


def test_hook_construction_emits_no_deprecated_argument_warning() -> None:
    """Covers: DAG-017 — hook arguments survive strict deprecation filters."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        hook = PostgresHook(postgres_conn_id="public_data", database="override_db")

    assert hook.database == "override_db"


def test_from_airflow_honors_the_database_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: DAG-017 — the override reaches the resolved connection details."""
    from data_ingestion_toolbox.utility import db_connection

    class _Connection:
        host = "warehouse.internal"
        port = 6543
        login = "etl"
        password = "secret"
        schema = "connection_default_db"

    class _Hook:
        def __init__(self, *args: object, **kwargs: object) -> None:
            assert "schema" not in kwargs, (
                "from_airflow must not pass the deprecated 'schema' argument"
            )
            self.kwargs = kwargs

        def get_connection(self, _conn_id: str) -> _Connection:
            return _Connection()

    monkeypatch.setattr(db_connection, "PostgresHook", _Hook)
    monkeypatch.setattr(db_connection, "_AIRFLOW_AVAILABLE", True)

    details = db_connection.PostgresConnectionFactory.from_airflow(
        conn_id="public_data", database="override_db"
    )

    assert details.database == "override_db"
    assert details.host == "warehouse.internal"
    assert details.port == 6543
