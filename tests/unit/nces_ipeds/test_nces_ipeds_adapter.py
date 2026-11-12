"""Offline contracts for the NCES IPEDS adapter.

Covers: ETL-131
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.nces_ipeds.client import (
    IpedsFetchError,
    IpedsNotPublished,
    IpedsPayloadError,
    fetch_file,
)
from data_ingestion_toolbox.nces_ipeds.config import NcesIpedsConfig
from data_ingestion_toolbox.nces_ipeds.plan import files_to_capture
from data_ingestion_toolbox.nces_ipeds.registry import EFA, HD, IC
from data_ingestion_toolbox.nces_ipeds.silver_nces_ipeds.parse import (
    choose_member,
    parse_file,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "nces_ipeds"
UW = "240444"


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _zip(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


class _ScriptedClient:
    def __init__(self, responses: list[httpx.Response]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, str]]] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        self.calls.append((url, dict(headers)))
        response = self.responses.pop(0)
        return httpx.Response(
            response.status_code,
            content=response.content,
            request=httpx.Request("GET", url),
        )


def _config() -> NcesIpedsConfig:
    return NcesIpedsConfig(min_spacing_seconds=0, max_attempts=2)


def test_the_directory_places_each_institution_by_its_published_county_code() -> None:
    """Covers: ETL-131 -- every Wisconsin institution, with county code and point as published."""
    parsed = parse_file(HD, _fixture("HD2023_wi.zip"))
    assert parsed.csv_member == "HD2023.csv"
    assert parsed.row_count == 89
    assert parsed.quarantined == ()
    uw = next(item for item in parsed.institutions if item.unitid == UW)
    assert (uw.county_code, uw.state_fips, uw.county_name) == (
        "55025",
        "55",
        "Dane County",
    )
    assert (uw.longitude, uw.latitude) == (-89.404098, 43.075409)


def test_housing_capacity_is_read_from_the_revised_member_and_marks_are_not_zero() -> (
    None
):
    """Covers: ETL-131 -- the _rv member is read; "." is not applicable, never zero."""
    assert choose_member(["IC2023.csv", "IC2023_RV.csv"]) == "IC2023_RV.csv"
    assert choose_member(["a.csv", "b.csv"]) is None
    parsed = parse_file(IC, _fixture("IC2023_wi.zip"))
    assert parsed.csv_member == "IC2023_RV.csv"
    figures = {item.unitid: item for item in parsed.figures}
    assert (figures[UW].value, figures[UW].value_status) == (8949, "valid")
    assert figures[UW].imputation_code == "R"
    madison_college = figures["238263"]
    assert (madison_college.value, madison_college.value_status) == (
        None,
        "not_applicable",
    )


def test_ic2024_without_housing_columns_is_refused_whole() -> None:
    """Covers: ETL-131 -- IPEDS 2024-25 moved housing out of IC; nothing is guessed."""
    parsed = parse_file(IC, _fixture("IC2024_wi.zip"))
    (refused,) = parsed.quarantined
    assert (refused.source_row_index, refused.error_code) == (0, "unexpected_header")
    assert parsed.figures == ()


def test_enrolment_reads_only_the_attendance_totals() -> None:
    """Covers: ETL-131 -- all, full-time and part-time headcount; other levels skipped."""
    parsed = parse_file(EFA, _fixture("EF2023A_wi.zip"))
    uw = {item.measure_id: item.value for item in parsed.figures if item.unitid == UW}
    assert uw == {
        "fall_enrolment_total": 49605,
        "fall_enrolment_full_time": 44928,
        "fall_enrolment_part_time": 4677,
    }
    assert (
        parsed.stored_row_count + parsed.skipped_row_count == parsed.row_count == 1819
    )
    history = parse_file(EFA, _fixture("EF2012A_uw.zip"))
    assert history.csv_member == "ef2012a_rv.csv"
    assert {item.measure_id: item.value for item in history.figures}[
        "fall_enrolment_total"
    ] == 42269


def test_a_lower_case_header_reads_and_a_bad_figure_sets_its_row_aside() -> None:
    """Covers: ETL-131 -- header case does not matter; a non-number is set aside alone."""
    payload = _zip(
        {
            "ic2099.csv": b"unitid,room,roomcap,xroomcap\n240444,1,8949,R\n"
            b"238661,1,5x,R\n238263,2,.,A\n240444,1,1,R\n"
        }
    )
    parsed = parse_file(IC, payload)
    assert [item.error_code for item in parsed.quarantined] == [
        "unreadable_value",
        "repeated_institution",
    ]
    assert [(item.unitid, item.value_status) for item in parsed.figures] == [
        ("240444", "valid"),
        ("238263", "not_applicable"),
    ]


def test_a_404_is_not_published_and_a_non_zip_is_refused() -> None:
    """Covers: ETL-131 -- NCES's 404 is asked once and reported; HTML is never kept."""
    client = _ScriptedClient([httpx.Response(404)])
    with pytest.raises(IpedsNotPublished):
        fetch_file("https://nces.ed.gov/x/EF2024A.zip", config=_config(), client=client)
    assert len(client.calls) == 1
    assert client.calls[0][1]["User-Agent"].startswith("population-etl-toolbox")
    client = _ScriptedClient([httpx.Response(200, content=b"<html>")])
    with pytest.raises(IpedsPayloadError) as refused:
        fetch_file("https://nces.ed.gov/x/HD2023.zip", config=_config(), client=client)
    assert refused.value.code == "not_zip"
    client = _ScriptedClient([httpx.Response(403)])
    with pytest.raises(IpedsFetchError) as forbidden:
        fetch_file("https://nces.ed.gov/x/HD2023.zip", config=_config(), client=client)
    assert forbidden.value.status == 403
    assert len(client.calls) == 1


def test_a_run_reads_unanswered_years_directory_first_and_the_next_year() -> None:
    """Covers: ETL-131 -- 2012 onward; housing stops at 2023; a new year is looked for."""
    fresh = files_to_capture({})
    assert fresh[0][0] is HD and fresh[0][1] == 2012
    assert [component.component_id for component, year in fresh if year == 2023] == [
        "HD",
        "IC",
        "EFA",
    ]
    assert {component.component_id for component, year in fresh if year == 2024} == {
        "HD",
        "EFA",
    }
    answered = {
        (component.component_id, year): {"published"} for component, year in fresh
    }
    del answered[("EFA", 2024)]
    later = files_to_capture(answered)
    assert [(component.component_id, year) for component, year in later] == [
        ("EFA", 2024),
        ("HD", 2025),
    ]
    assert len(files_to_capture(answered, {"history": True})) > len(fresh)


def test_configuration_imports_without_io_or_credential() -> None:
    """Covers: ETL-131 -- no key, and the client says who it is plainly."""
    config = NcesIpedsConfig()
    assert config.data_base_url == "https://nces.ed.gov/ipeds/datacenter/data/"
    assert "@" not in config.user_agent
    assert "Mozilla" not in config.user_agent
