"""Offline contracts for the EPA AirData annual monitor adapter.

Covers: ETL-068
"""

from __future__ import annotations

import csv
import importlib
import io
import sys
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest

from data_ingestion_toolbox.epa_aqs.client import AqsPayloadError, csv_text
from data_ingestion_toolbox.epa_aqs.registry import get_file, registered_files
from data_ingestion_toolbox.epa_aqs.silver_epa_aqs.parse import parse_file

pytestmark = pytest.mark.unit

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "epa_aqs"
    / "annual_conc_by_monitor_2024.zip"
)
Y2024 = get_file("annual_conc_by_monitor:2024")


def _text() -> str:
    return csv_text(FIXTURE.read_bytes(), Y2024)


def _zip(text: str) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        archive.writestr(Y2024.member, text)
    return out.getvalue()


def test_configuration_imports_without_io_and_holds_no_credential() -> None:
    """Covers: ETL-068 — the AirData files need no key; the AQS API key is not used."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.epa_aqs"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.epa_aqs.config")
    assert not {
        field
        for field in module.AqsConfig.model_fields
        if "key" in field or "token" in field or "email" in field
    }
    assert [item.year for item in registered_files()] == [2020, 2021, 2022, 2023, 2024]


def test_only_the_registered_standards_are_read() -> None:
    """Covers: ETL-068 — PM2.5 annual 2024 and ozone 8-hour 2015; everything else counted."""
    parsed = parse_file(FIXTURE.read_bytes(), item=Y2024)
    assert (
        parsed.quarantined == ()
        and parsed.row_count == 499
        and parsed.in_scope_row_count == 26
    )
    assert {(obs.measure, obs.pollutant_standard) for obs in parsed.observations} == {
        ("pm25_annual_mean", "PM25 Annual 2024"),
        ("ozone_8hour_4th_max", "Ozone 8-hour 2015"),
    }
    rows = {(obs.monitor_id, obs.event_type): obs for obs in parsed.observations}
    sussex = rows[("10005-1002-88101-3", "No Events")]
    assert (sussex.value, sussex.completeness, sussex.geo_id, sussex.units) == (
        Decimal("6.188827"),
        "Y",
        "state:10|county:005",
        "Micrograms/cubic meter (LC)",
    )
    assert any(obs.event_type == "Events Included" for obs in parsed.observations)
    assert all(
        obs.geo_id.startswith(("state:10|", "state:09|")) for obs in parsed.observations
    )


def test_empty_statistics_are_missing_and_bad_rows_quarantined() -> None:
    """Covers: ETL-068 — an empty mean is missing, never zero; a bad FIPS, year or event is refused."""
    reader = csv.DictReader(io.StringIO(_text(), newline=""))
    columns = list(reader.fieldnames or [])
    in_scope = [
        row for row in reader if row["Pollutant Standard"] == "PM25 Annual 2024"
    ]

    def with_cell(row: dict[str, str], column: str, value: str) -> dict[str, str]:
        return {**row, column: value}

    damaged = [
        with_cell(in_scope[0], "Arithmetic Mean", ""),
        with_cell(in_scope[1], "State Code", "99"),
        with_cell(in_scope[2], "Year", "2023"),
        with_cell(in_scope[3], "Event Type", "Some Events"),
        in_scope[4],
        in_scope[4],
        with_cell(in_scope[5], "State Code", "80"),
    ]
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=columns, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    writer.writerows(damaged)
    parsed = parse_file(_zip(out.getvalue()), item=Y2024)
    assert sorted(q.error_code for q in parsed.quarantined) == [
        "duplicate_row",
        "unknown_event_type",
        "unreadable_fips",
        "wrong_year",
    ]
    (missing,) = [obs for obs in parsed.observations if obs.value is None]
    assert (missing.value_status, missing.value_source) == ("missing", "")
    with pytest.raises(AqsPayloadError, match="member_missing"):
        csv_text(b"<html>", Y2024)
    with pytest.raises(AqsPayloadError, match="unexpected_header"):
        csv_text(_zip('"State Code","County Code"\n'), Y2024)
