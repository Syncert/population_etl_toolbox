"""DAG-tier test configuration.

Provides a ``dagbag`` fixture that loads the repository dag folder using
Airflow's DagBag.  All tests in this directory carry ``@pytest.mark.dag``
and are silently skipped when Airflow is not installed so the default unit
suite never needs the Airflow environment.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from typing import Any, Generator

import pytest

_DAGS_FOLDER = str(Path(__file__).resolve().parents[2] / "dags")

# ---------------------------------------------------------------------------
# Skip the entire dag suite gracefully if Airflow is absent
# ---------------------------------------------------------------------------
_AIRFLOW_AVAILABLE = importlib.util.find_spec("airflow") is not None


@pytest.fixture(scope="session", autouse=True)
def isolated_airflow_environment(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[Path, None, None]:
    """Configure Airflow before its first import and restore the environment."""
    airflow_home = tmp_path_factory.mktemp("airflow-home")
    original = {
        key: os.environ.get(key)
        for key in (
            "AIRFLOW_HOME",
            "AIRFLOW__CORE__LOAD_EXAMPLES",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        )
    }
    os.environ["AIRFLOW_HOME"] = str(airflow_home)
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
        f"sqlite:///{airflow_home / 'airflow.db'}"
    )
    try:
        yield airflow_home
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@pytest.fixture(scope="session")
def dagbag(isolated_airflow_environment: Path) -> Any:
    """Return an Airflow DagBag loaded from the repository dags/ folder.

    Uses a temporary AIRFLOW_HOME so no production state is touched.
    LOAD_EXAMPLES is always disabled.
    """
    if not _AIRFLOW_AVAILABLE:
        pytest.fail(
            "Airflow is required for the DAG tier; install .[airflow-dev] "
            "in the Python 3.11 environment."
        )

    from airflow.models import DagBag

    return DagBag(dag_folder=_DAGS_FOLDER, include_examples=False)
