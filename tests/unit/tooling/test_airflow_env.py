"""The DAG tier's metadata URL is one Airflow accepts on this host.

Covers: ENV-023 -- `sqlite_connection_string` builds the URL every DAG test
        starts from. When Airflow rejects it the failure lands at
        `from airflow.models import DagBag`, which aborts collection of the
        whole tier rather than skipping a module, so the tier reports nothing
        at all.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.support.airflow_env import sqlite_connection_string

pytestmark = pytest.mark.unit


def _airflow_calls_it_relative(connection: str) -> bool:
    """`airflow.settings._is_sqlite_db_path_relative`, as it is actually written.

    Reimplemented rather than imported: the guard has to hold in the unit tier,
    which does not install Airflow. It strips three slashes -- not four -- and
    asks the host's `os.path.isabs` about the remainder.
    """
    if connection == "sqlite://":
        return False
    prefix = "sqlite:///"
    return not (
        connection.startswith(prefix) and os.path.isabs(connection[len(prefix) :])
    )


def test_the_metadata_url_is_absolute_on_this_host(tmp_path: Path) -> None:
    """Covers: ENV-023 — Airflow's own predicate accepts what we build.

    This is asserted through the predicate rather than through a slash count,
    because the count is not the contract and believing it was is what broke.
    The helper built `sqlite:////` unconditionally; on Windows that leaves
    `/G:/...`, which Python 3.13's `ntpath.isabs` calls drive-relative.
    """
    connection = sqlite_connection_string(tmp_path)
    assert not _airflow_calls_it_relative(connection), (
        f"Airflow would refuse {connection}, and the DAG tier would abort at "
        f"collection instead of running"
    )


def test_the_url_names_the_database_inside_the_given_home(tmp_path: Path) -> None:
    """Covers: ENV-023 — the URL points at the isolated home it was given."""
    connection = sqlite_connection_string(tmp_path)
    database = connection.removeprefix("sqlite:///")
    assert Path(database).name == "airflow.db"
    assert Path(database).resolve().parent == tmp_path.resolve(), (
        "the metadata database is outside the temporary AIRFLOW_HOME, so a "
        "test run would touch state it does not own"
    )


@pytest.mark.parametrize(
    ("connection", "relative"),
    [
        ("sqlite://", False),
        ("sqlite:///relative/airflow.db", True),
    ],
)
def test_the_reimplemented_predicate_matches_the_cases_it_claims_to(
    connection: str, relative: bool
) -> None:
    """Covers: ENV-023 — the local copy of Airflow's check is not vacuous.

    A predicate that answered "acceptable" to everything would make the guard
    above pass on any string, so it is exercised on the two cases whose answer
    does not depend on the host.
    """
    assert _airflow_calls_it_relative(connection) is relative
