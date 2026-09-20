"""Airflow environment settings that work on every host the suite runs on.

Airflow refuses a SQLite metadata database it considers relative, and the test
tier has to hand it one Airflow accepts on Linux *and* on Windows. The check
(`airflow.settings._is_sqlite_db_path_relative`) strips the literal prefix
``sqlite:///`` -- three slashes -- and asks `os.path.isabs` about the rest:

    sqlite:////tmp/airflow.db   -> isabs('/tmp/airflow.db')     -> True on POSIX
    sqlite:///C:/x/airflow.db   -> isabs('C:/x/airflow.db')     -> True on Windows

So the number of slashes is not the contract; the remainder being absolute
*for the host* is. Building the four-slash form unconditionally, as this
helper used to, produces ``sqlite:////G:/...`` on Windows, whose remainder is
``/G:/...`` -- and since Python 3.13 `ntpath.isabs` calls a leading slash with
no drive *drive-relative* rather than absolute, so Airflow rejects it and the
whole DAG tier fails at `from airflow.models import DagBag`.

The failure surfaces at import, so it does not skip a module -- it aborts
collection of the whole tier, and an engineer on Windows gets no result at all
rather than a partial one.
"""

from __future__ import annotations

import os
from pathlib import Path


def sqlite_connection_string(airflow_home: Path | str) -> str:
    """Airflow's SQLite URL for a metadata database under ``airflow_home``.

    Uses the POSIX rendering of the resolved path, which on Windows keeps the
    drive letter (``G:/x``) and on POSIX keeps the leading slash (``/tmp/x``).
    Appending either to ``sqlite:///`` leaves a remainder this host calls
    absolute, which is what Airflow checks.
    """
    database = Path(airflow_home).resolve() / "airflow.db"
    connection = f"sqlite:///{database.as_posix()}"
    assert os.path.isabs(connection.removeprefix("sqlite:///")), (
        f"{connection} is a path Airflow will reject as relative; the DAG "
        f"tier would abort at collection rather than skip"
    )
    return connection


def require_airflow_dag_imports() -> None:
    """Skip the calling module when a production DAG cannot be imported here.

    ``pytest.importorskip("airflow")`` is not enough, and neither is
    ``airflow.models``: both succeed in environments where importing a DAG does
    not, because the declarative mappers are only built when something pulls in
    ``airflow.decorators`` -- which every DAG module does. The failures are
    also not ``ImportError``:

    - The warehouse coverage job installs the API extra, whose SQLAlchemy 2 pin
      cannot coexist with Airflow 2.9.3 (SQLAlchemy < 2). Loading the ORM
      models raises ``MappedAnnotationError``.
    - A misconfigured metadata connection raises ``AirflowConfigException``.

    Either one at module scope aborts collection of the entire tier rather than
    skipping one module, so an engineer gets no result at all instead of a
    partial one. This turns that into a skip that names why.
    """
    import pytest

    try:
        # What a DAG module imports, and what actually triggers the failure.
        import airflow.decorators  # noqa: F401
    except BaseException as error:  # noqa: BLE001 - any failure means "not here"
        pytest.skip(
            "a production DAG cannot be imported in this environment "
            f"({type(error).__name__}); the postgres-integration job installs "
            ".[airflow-dev] and runs this module there.",
            allow_module_level=True,
        )
