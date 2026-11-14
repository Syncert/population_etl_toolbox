"""A deploy updates the ``public_data`` connection in place, never deletes it.

Covers: DAG-024
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.dag

pytest.importorskip("airflow")

_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _ROOT / "scripts" / "airflow_upsert_connection.py"
_COMPOSE_FILES = [
    _ROOT / "infra/docker/docker-compose.yml",
    _ROOT / "infra/docker/docker-compose.airflow.yml",
    _ROOT / "infra/docker/docker-compose.external.yml",
]


def _script():
    spec = importlib.util.spec_from_file_location("airflow_upsert_connection", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def session():
    from airflow.models import Connection
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    engine = create_engine("sqlite://")
    Connection.__table__.create(engine)
    with Session(engine) as session:
        yield session


def test_upsert_creates_then_updates_the_same_row(session) -> None:
    """Covers: DAG-024 — an existing connection keeps its row and gains the new settings."""
    from airflow.models import Connection
    from sqlalchemy import select

    upsert = _script().upsert_connection

    first = Connection(
        conn_id="public_data", conn_type="postgres", host="old", port=5432
    )
    assert upsert(session, first) == "created"
    row_id = session.scalar(
        select(Connection.id).where(Connection.conn_id == "public_data")
    )

    second = Connection(
        conn_id="public_data", conn_type="postgres", host="new", port=5435, schema="db"
    )
    assert upsert(session, second) == "updated"

    rows = session.scalars(
        select(Connection).where(Connection.conn_id == "public_data")
    ).all()
    assert len(rows) == 1
    assert rows[0].id == row_id
    assert (rows[0].host, rows[0].port, rows[0].schema) == ("new", 5435, "db")


def test_cli_arguments_map_to_the_connection_fields() -> None:
    """Covers: DAG-024 — the compose command's flags parse."""
    args = _script().parse_args(
        [
            "public_data",
            "--conn-type",
            "postgres",
            "--host",
            "h",
            "--port",
            "5435",
            "--login",
            "u",
            "--password",
            "p",
            "--schema",
            "s",
        ]
    )
    assert (args.conn_id, args.conn_type, args.host, args.port) == (
        "public_data",
        "postgres",
        "h",
        5435,
    )
    assert (args.login, args.password, args.schema) == ("u", "p", "s")


@pytest.mark.parametrize("compose_file", _COMPOSE_FILES, ids=lambda p: p.name)
def test_airflow_init_never_deletes_public_data(compose_file: Path) -> None:
    """Covers: DAG-024 — no deploy opens a window where ``public_data`` is undefined.

    ``delete`` then ``add`` left the connection missing for the seconds the
    second CLI call took to boot; on OFFICE_LAB (2026-11-05 15:45 UTC) a
    ``bls_ingest`` batch failed on "The conn_id `public_data` isn't defined".
    """
    text = compose_file.read_text(encoding="utf-8")
    assert "connections delete public_data" not in text
    assert "connections add public_data" not in text
    assert re.search(
        r"python /opt/data_ingestion_toolbox/scripts/airflow_upsert_connection\.py public_data "
        r"--conn-type postgres --host \S+ --port \S+ --login \S+ --password \S+ --schema \S+;",
        text,
    )
