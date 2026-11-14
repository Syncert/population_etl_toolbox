"""Airflow's own logging configuration keeps provider keys out of task logs."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.dag

REPOSITORY = Path(__file__).resolve().parents[2]
CONFIG_CLASS = "data_ingestion_toolbox.airflow_logging.LOGGING_CONFIG"
FAKE_KEY = "fakeKeyValueForLoggingTest0000"


def test_stack_env_file_selects_the_redacting_logging_config() -> None:
    """Covers: ETL-038, DAG-014 — every compose stack runs Airflow with redaction."""
    env_file = (REPOSITORY / "infra/airflow/airflow.env.example").read_text()
    assert f"AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS={CONFIG_CLASS}" in env_file


def test_config_quiets_httpx_and_filters_every_handler() -> None:
    """Covers: ETL-038 — httpx stays above INFO and each handler masks secrets."""
    from data_ingestion_toolbox.airflow_logging import (
        FILTER_NAME,
        LOGGING_CONFIG,
        RedactSecretsFilter,
    )

    for name in ("httpx", "httpcore"):
        assert LOGGING_CONFIG["loggers"][name]["level"] == "WARNING"
    assert LOGGING_CONFIG["handlers"]
    for handler in LOGGING_CONFIG["handlers"].values():
        assert FILTER_NAME in handler["filters"]

    record = logging.LogRecord(
        "airflow.task",
        logging.INFO,
        __file__,
        1,
        "GET %s",
        (f"https://api.eia.gov/v2/x?api_key={FAKE_KEY}",),
        None,
    )
    assert RedactSecretsFilter().filter(record)
    assert FAKE_KEY not in record.getMessage()


def test_airflow_applies_the_config_and_task_logs_carry_no_key(
    tmp_path: Path,
) -> None:
    """Covers: ETL-038, DAG-014 — a real Airflow process masks a logged key URL."""
    script = f"""
import logging
from airflow.logging_config import configure_logging
configure_logging()
url = "https://api.census.gov/data?get=NAME&key={FAKE_KEY}"
logging.getLogger("httpx").info("HTTP Request: GET %s", url)
logging.getLogger("airflow.task").info("fetching %s", url)
logging.getLogger("data_ingestion_toolbox.census").warning("retry %s", url)
"""
    env = {
        **os.environ,
        "AIRFLOW_HOME": str(tmp_path),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{tmp_path}/airflow.db",
        "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS": CONFIG_CLASS,
        "AIRFLOW__LOGGING__BASE_LOG_FOLDER": str(tmp_path / "logs"),
        "PYTHONPATH": os.pathsep.join(
            [str(REPOSITORY / "src"), str(REPOSITORY), os.environ.get("PYTHONPATH", "")]
        ),
    }
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert FAKE_KEY not in output
    assert "fetching https://api.census.gov" in output
    assert "HTTP Request" not in output
