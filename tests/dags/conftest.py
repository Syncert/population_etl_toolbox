"""DAG-tier test configuration.

Provides a ``dagbag`` fixture that loads the repository dag folder using
Airflow's DagBag.  All tests in this directory carry ``@pytest.mark.dag``
and are silently skipped when Airflow is not installed so the default unit
suite never needs the Airflow environment.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Generator

import pytest

_DAGS_FOLDER = str(Path(__file__).resolve().parents[2] / "dags")

# ---------------------------------------------------------------------------
# Skip the entire dag suite gracefully if Airflow is absent
# ---------------------------------------------------------------------------
try:
    import airflow  # noqa: F401

    _AIRFLOW_AVAILABLE = True
except ModuleNotFoundError:
    _AIRFLOW_AVAILABLE = False

_skip_no_airflow = pytest.mark.skipif(
    not _AIRFLOW_AVAILABLE,
    reason="Airflow is not installed in this environment; run in the airflow-dev venv.",
)


@pytest.fixture(scope="module")
def dagbag() -> Generator[Any, None, None]:
    """Return an Airflow DagBag loaded from the repository dags/ folder.

    Uses a temporary AIRFLOW_HOME so no production state is touched.
    LOAD_EXAMPLES is always disabled.
    """
    if not _AIRFLOW_AVAILABLE:
        pytest.skip("Airflow not installed")

    from airflow.models import DagBag

    with tempfile.TemporaryDirectory() as tmp_home:
        old_home = os.environ.get("AIRFLOW_HOME")
        os.environ["AIRFLOW_HOME"] = tmp_home
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        try:
            db_path = Path(tmp_home) / "airflow.db"
            os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
                f"sqlite:///{db_path}"
            )
            # Initialise the Airflow metadata DB (required before DagBag)
            from airflow.utils.db import initdb

            initdb()
            bag = DagBag(dag_folder=_DAGS_FOLDER, include_examples=False)
            yield bag
        finally:
            if old_home is None:
                os.environ.pop("AIRFLOW_HOME", None)
            else:
                os.environ["AIRFLOW_HOME"] = old_home
            os.environ.pop("AIRFLOW__CORE__LOAD_EXAMPLES", None)
            os.environ.pop("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", None)
