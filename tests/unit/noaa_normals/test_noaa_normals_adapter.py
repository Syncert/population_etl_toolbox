"""Offline contracts for the NOAA climate normals adapter.

Covers: ETL-069
"""

from __future__ import annotations

import csv
import importlib
import io
import sys
import tarfile
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.noaa_normals.client import (
    NormalsFetchError,
    NormalsPayloadError,
    check_archive,
    fetch_archive,
)
from data_ingestion_toolbox.noaa_normals.config import NormalsConfig
from data_ingestion_toolbox.noaa_normals.silver_noaa_normals.parse import parse_archive

pytestmark = pytest.mark.unit

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "noaa_normals"
    / "annualseasonal_by_station.tar.gz"
)


def _archive(files: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, text in files.items():
            content = text.encode()
            member = tarfile.TarInfo(name)
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
    return buffer.getvalue()


def _station(station_id: str = "USC00099999", **variables: tuple[str, str, str]) -> str:
    """One station file: each variable is (value, measurement flag, completeness flag)."""
    header = [
        "STATION",
        "LATITUDE",
        "LONGITUDE",
        "ELEVATION",
        "NAME",
        "month",
        "day",
        "hour",
    ]
    row = [station_id, "39.1", "-75.5", "10.0", "SOMEWHERE, DE US", "99", "99", "99"]
    for variable, (value, flag, completeness) in variables.items():
        name = variable.replace("_", "-")
        header += [name, f"meas_flag_{name}", f"comp_flag_{name}", f"years_{name}"]
        row += [value, flag, completeness, "30"]
    out = io.StringIO()
    csv.writer(out, quoting=csv.QUOTE_ALL, lineterminator="\n").writerows([header, row])
    return out.getvalue()


def test_configuration_imports_without_io_and_holds_no_credential() -> None:
    """Covers: ETL-069 — the normals archive needs no key."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.noaa_normals"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.noaa_normals.config")
    assert not {
        field
        for field in module.NormalsConfig.model_fields
        if "key" in field or "token" in field or "email" in field
    }


def test_the_reviewed_archive_parses_every_station() -> None:
    """Covers: ETL-069 — eleven stations, absent elements absent, nothing quarantined."""
    parsed = parse_archive(FIXTURE.read_bytes())
    assert (len(parsed.stations), len(parsed.observations), parsed.quarantined) == (
        11,
        61,
        (),
    )
    felton = [obs for obs in parsed.observations if obs.station_id == "US1DEKN0001"]
    assert [(obs.variable, obs.completeness_flag) for obs in felton] == [
        ("ANN-PRCP-NORMAL", "E")
    ]
    dover = next(
        station for station in parsed.stations if station.station_id == "USC00072730"
    )
    assert (dover.latitude, dover.longitude) == (
        Decimal("39.1467"),
        Decimal("-75.5056"),
    )


def test_flags_keep_their_meaning() -> None:
    """Covers: ETL-069 — `X` keeps a valid zero with its flag; `M`, `V`, `Y` publish no number."""
    parsed = parse_archive(
        _archive(
            {
                "USC00099999.csv": _station(
                    ANN_TAVG_NORMAL=("-9999", "M", "S"),
                    ANN_HTDD_NORMAL=("-7777", "V", "S"),
                    ANN_TMAX_NORMAL=("", "Y", "S"),
                    ANN_CLDD_NORMAL=("0.0", "X", "S"),
                    ANN_PRCP_NORMAL=("0.00", "", "S"),
                )
            }
        )
    )
    states = {
        obs.variable: (
            obs.value,
            obs.value_status,
            obs.missing_reason,
            obs.measurement_flag,
        )
        for obs in parsed.observations
    }
    assert states == {
        "ANN-TAVG-NORMAL": (None, "missing", "missing", "M"),
        "ANN-HTDD-NORMAL": (None, "not_applicable", "too_cold_to_compute", "V"),
        "ANN-TMAX-NORMAL": (None, "missing", "insufficient_values", "Y"),
        "ANN-CLDD-NORMAL": (Decimal("0.0"), "valid", None, "X"),
        "ANN-PRCP-NORMAL": (Decimal("0.00"), "valid", None, None),
    }


@pytest.mark.parametrize(
    ("text", "code"),
    [
        (_station(ANN_TAVG_NORMAL=("-9999", "", "S")), "sentinel_value"),
        (_station(ANN_TAVG_NORMAL=("55.0", "Q", "S")), "unknown_flag"),
        (_station(ANN_TAVG_NORMAL=("55.0", "", "W")), "unknown_flag"),
        (_station(ANN_TAVG_NORMAL=("warm", "", "S")), "unreadable_value"),
        (
            _station("USC00011111", ANN_TAVG_NORMAL=("55.0", "", "S")),
            "unreadable_station",
        ),
        ('"STATION","NAME"\n"USC00099999","X"\n', "unexpected_header"),
    ],
)
def test_an_unreadable_station_file_is_quarantined_alone(text: str, code: str) -> None:
    """Covers: ETL-069 — the bad file is quarantined; its neighbour still parses."""
    parsed = parse_archive(
        _archive(
            {
                "USC00099999.csv": text,
                "USC00088888.csv": _station(
                    "USC00088888", ANN_TAVG_NORMAL=("50.0", "", "S")
                ),
            }
        )
    )
    assert [q.error_code for q in parsed.quarantined] == [code]
    assert [station.station_id for station in parsed.stations] == ["USC00088888"]


def test_a_payload_that_is_not_the_archive_is_refused() -> None:
    """Covers: ETL-069 — HTML, an empty archive, or foreign CSVs never reach capture."""
    with pytest.raises(NormalsPayloadError, match="not_an_archive"):
        check_archive(b"<html>moved</html>")
    with pytest.raises(NormalsPayloadError, match="empty_archive"):
        check_archive(_archive({}))
    with pytest.raises(NormalsPayloadError, match="unexpected_header"):
        check_archive(_archive({"a.csv": '"A","B"\n"1","2"\n'}))
    check_archive(FIXTURE.read_bytes())


class _Scripted:
    def __init__(self, *responses: httpx.Response) -> None:
        self.responses = list(responses)

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        return self.responses.pop(0)


def test_server_errors_retry_and_client_errors_do_not() -> None:
    """Covers: ETL-069 — 503 retries then succeeds; 404 fails at once with the path only."""
    request = httpx.Request("GET", "https://example.test")
    config = NormalsConfig(min_spacing_seconds=0, max_attempts=2)
    response = fetch_archive(
        config=config,
        client=_Scripted(
            httpx.Response(503, request=request),
            httpx.Response(200, content=FIXTURE.read_bytes(), request=request),
        ),
        sleep=lambda _seconds: None,
    )
    assert response.http_status == 200
    with pytest.raises(NormalsFetchError) as raised:
        fetch_archive(
            config=config,
            client=_Scripted(httpx.Response(404, request=request)),
            sleep=lambda _seconds: None,
        )
    assert (raised.value.code, raised.value.status) == ("non_retryable_http", 404)
    assert "https://" not in str(raised.value)


def test_a_cocorahs_station_id_with_lower_case_letters_is_admitted() -> None:
    """Covers: ETL-069 — NCEI's own ids include `US10adam002`; the table must accept them."""
    import re
    from pathlib import Path

    ddl = (
        Path(__file__).resolve().parents[3]
        / "src/data_ingestion_toolbox/noaa_normals/DDL/silver_noaa_normals.sql"
    ).read_text(encoding="utf-8")
    patterns = re.findall(r"station_id ~ '([^']+)'", ddl)
    assert patterns and all(p == "^[A-Za-z0-9_]{11}$" for p in patterns)
    assert re.fullmatch(patterns[0], "US10adam002")
    assert re.fullmatch(patterns[0], "USW00014837")
    assert re.fullmatch(patterns[0], "US10box_001")
    assert not re.fullmatch(patterns[0], "US10adam00")
