"""The ACS backfill driver's plan, without Docker or Airflow (ETL-090)."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "run_acs_backfill.py"
_spec = importlib.util.spec_from_file_location("run_acs_backfill", SCRIPT)
driver = importlib.util.module_from_spec(_spec)
sys.modules["run_acs_backfill"] = driver
_spec.loader.exec_module(driver)


def test_years_run_newest_first_however_they_are_written() -> None:
    """Covers: ETL-090 -- a stop leaves a contiguous recent history."""
    assert driver.parse_years("2005-2009") == [2009, 2008, 2007, 2006, 2005]
    assert driver.parse_years("2009-2005") == [2009, 2008, 2007, 2006, 2005]
    assert driver.parse_years("2017, 2019") == [2019, 2017]
    with pytest.raises(ValueError):
        driver.parse_years(" ")


def test_each_year_is_one_run_carrying_its_backfill_conf() -> None:
    """Covers: ETL-090 -- the run is the one-year-one-grain request ETL-083 reads."""
    run_id = driver.run_id_for("place", 2019, datetime(2026, 10, 26, 16, 0, 0))
    assert run_id == "backfill_place_2019_20261026T160000"
    command = driver.trigger_command("place", 2019, run_id)
    assert command[3:7] == ["airflow", "dags", "trigger", "acs_ingest"]
    conf = json.loads(command[command.index("--conf") + 1])
    assert conf == {"acs_backfill": {"year": 2019, "geo_level": "place"}}
