"""Airflow environment settings that work on every host the suite runs on.

Airflow validates its SQLite connection string by requiring the literal prefix
``sqlite:////`` -- four slashes -- and rejects anything else as a relative
path. The obvious construction, ``f"sqlite:///{path}"``, produces that prefix
only when the path itself begins with ``/``. It does on POSIX. It does not on
Windows, where an absolute path begins with a drive letter, so the string comes
out as ``sqlite:///C:\\Users\\...`` and Airflow raises
``AirflowConfigException: Cannot use relative path`` before anything runs.

The failure surfaces as an *import* error, so it does not skip a module -- it
aborts collection of the whole tier, and an engineer on Windows gets no result
at all rather than a partial one.
"""

from __future__ import annotations

from pathlib import Path


def sqlite_connection_string(airflow_home: Path | str) -> str:
    """Airflow's required four-slash SQLite URL for a metadata database.

    Uses the POSIX rendering of the resolved path, so a Windows drive letter
    becomes ``C:/...`` rather than ``C:\\...`` and the leading slash Airflow
    insists on is supplied rather than borrowed from the path.
    """
    database = Path(airflow_home).resolve() / "airflow.db"
    return f"sqlite:////{database.as_posix().lstrip('/')}"


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
