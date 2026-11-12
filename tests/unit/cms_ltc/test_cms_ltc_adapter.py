"""Offline contracts for the CMS long-term care facility adapter.

Covers: ETL-130
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.cms_ltc.catalog import CatalogError, Release, releases
from data_ingestion_toolbox.cms_ltc.client import (
    CmsFetchError,
    CmsPayloadError,
    fetch_file,
)
from data_ingestion_toolbox.cms_ltc.config import CmsLtcConfig
from data_ingestion_toolbox.cms_ltc.plan import releases_to_capture
from data_ingestion_toolbox.cms_ltc.registry import CHARACTERISTICS, COST_REPORT
from data_ingestion_toolbox.cms_ltc.silver_cms_ltc.parse import (
    parse_file,
    place_county,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "cms_ltc"


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


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


def _config() -> CmsLtcConfig:
    return CmsLtcConfig(min_spacing_seconds=0, max_attempts=2)


def test_the_catalog_lists_every_release_with_its_period_and_csv() -> None:
    """Covers: ETL-130 -- 12 Form 671 quarters and 14 cost-report years, each with its CSV."""
    found, skipped = releases(_fixture("data_json_excerpt.json"))
    assert skipped == []
    by_series: dict[str, list[Release]] = {}
    for item in found:
        by_series.setdefault(item.series_id, []).append(item)
    assert len(by_series["characteristics"]) == 12
    assert len(by_series["cost_report"]) == 14
    newest = by_series["characteristics"][-1]
    assert (newest.period_start, newest.period_end) == (
        date(2026, 7, 1),
        date(2026, 9, 30),
    )
    assert newest.download_url.endswith("/Form_671_Q3_2026.csv")
    cost = by_series["cost_report"][-1]
    assert cost.period_start == date(2024, 1, 1)
    assert cost.download_url.endswith("/CostReportsnf_Final_24.csv")


def test_an_entry_without_a_single_csv_or_period_is_set_aside() -> None:
    """Covers: ETL-130 -- a release CMS lists without a CSV is reported, not guessed."""
    document = json.loads(_fixture("data_json_excerpt.json"))
    document["dataset"][0]["distribution"] = [
        item
        for item in document["dataset"][0]["distribution"]
        if item.get("mediaType") != "text/csv"
    ]
    document["dataset"][1]["temporal"] = []
    found, skipped = releases(json.dumps(document).encode())
    assert {item.reason for item in skipped} == {
        "no_single_csv_distribution",
        "no_single_period",
    }
    assert len(found) == 24
    with pytest.raises(CatalogError):
        releases(b"<html>")


def test_a_file_without_the_registered_header_is_refused_and_404_is_not_retried() -> (
    None
):
    """Covers: ETL-130 -- a changed layout is refused before capture; a 404 asks once."""
    client = _ScriptedClient([httpx.Response(200, content=b"a,b\n1,2\n")])
    with pytest.raises(CmsPayloadError) as refused:
        fetch_file(
            "https://data.cms.gov/x/Form.csv",
            CHARACTERISTICS,
            config=_config(),
            client=client,
        )
    assert refused.value.code == "unexpected_header"
    client = _ScriptedClient([httpx.Response(404)])
    with pytest.raises(CmsFetchError) as missing:
        fetch_file(
            "https://data.cms.gov/x/Form.csv",
            CHARACTERISTICS,
            config=_config(),
            client=client,
        )
    assert missing.value.status == 404
    assert len(client.calls) == 1
    assert client.calls[0][1]["User-Agent"].startswith("population-etl-toolbox")


def test_the_cms_671_fixture_parses_every_wisconsin_facility() -> None:
    """Covers: ETL-130 -- one row per facility, every figure a whole number as published."""
    parsed = parse_file(CHARACTERISTICS, _fixture("Form_671_Q3_2026_wi.csv"))
    assert parsed.row_count == 323
    assert parsed.quarantined == ()
    assert {row.state_fips for row in parsed.rows} == {"55"}
    first = parsed.rows[0]
    assert first.county_name is None
    assert set(first.counts) == {
        "total_residents",
        "medicare_census",
        "medicaid_census",
        "other_census",
    }


def test_a_blank_cost_report_figure_is_missing_and_a_bad_one_is_set_aside() -> None:
    """Covers: ETL-130 -- a blank is missing, never zero; a non-number sets its row aside."""
    lines = _fixture("CostReportsnf_Final_24_wi.csv").split(b"\r\n")
    header = lines[0].decode().split(",")
    beds = header.index('"Number of Beds"')
    row = lines[1].decode().split(",")
    row[beds] = '""'
    bad = lines[2].decode().split(",")
    bad[beds] = '"12.5"'
    payload = b"\r\n".join(
        [lines[0], ",".join(row).encode(), ",".join(bad).encode(), b""]
    )
    parsed = parse_file(COST_REPORT, payload)
    assert parsed.row_count == 2
    assert [item.error_code for item in parsed.quarantined] == ["unreadable_value"]
    (kept,) = parsed.rows
    assert kept.counts["number_of_beds"] == ("", None)
    assert kept.fiscal_year_end is not None


def test_a_county_name_places_a_facility_only_on_one_exact_match() -> None:
    """Covers: ETL-130 -- case and periods aside, one county or none."""
    counties = {
        "55": [
            ("state:55|county:025", 25, "Dane County"),
            ("state:55|county:109", 109, "St. Croix County"),
        ],
        "51": [
            ("state:51|county:159", 159, "Richmond County"),
            ("state:51|county:760", 760, "Richmond city"),
        ],
    }
    assert place_county("DANE", "55", counties) == (
        "resolved",
        "state:55|county:025",
        25,
    )
    assert place_county("ST. CROIX", "55", counties)[1] == "state:55|county:109"
    assert place_county("ST CROIX", "55", counties)[1] == "state:55|county:109"
    assert place_county("OKALOOSA", "55", counties) == ("unmapped", None, None)
    assert place_county("RICHMOND", "51", counties) == ("ambiguous", None, None)
    assert place_county(None, "55", counties) == ("unmapped", None, None)


def test_a_run_reads_only_releases_not_yet_published_cost_reports_first() -> None:
    """Covers: ETL-130 -- the same URL is not read twice; history reads all."""
    found, _ = releases(_fixture("data_json_excerpt.json"))
    newest_quarter = next(
        item
        for item in found
        if item.series_id == "characteristics" and item.release_key == "2026-07-01"
    )
    answered = {
        (item.series_id, item.release_key, item.download_url)
        for item in found
        if item is not newest_quarter
    }
    assert releases_to_capture(found, answered) == [newest_quarter]
    everything = releases_to_capture(found, answered, {"history": True})
    assert len(everything) == len(found)
    assert everything[0].series_id == "cost_report"
    assert everything[-1].series_id == "characteristics"


def test_configuration_imports_without_io_or_credential() -> None:
    """Covers: ETL-130 -- no key, and the client says who it is plainly."""
    config = CmsLtcConfig()
    assert config.catalog_url == "https://data.cms.gov/data.json"
    assert "@" not in config.user_agent
    assert "Mozilla" not in config.user_agent
