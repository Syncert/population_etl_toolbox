"""Offline contracts for the IRS SOI county and state income adapter.

Covers: ETL-114
"""

from __future__ import annotations

import importlib
import io
import sys
import zipfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from data_ingestion_toolbox.capture import CaptureReceipt
from data_ingestion_toolbox.irs_soi.capture import PARSER_CONTRACT_VERSION, capture_file
from data_ingestion_toolbox.irs_soi.client import (
    IrsSoiFetchError,
    IrsSoiPayloadError,
    fetch_file,
)
from data_ingestion_toolbox.irs_soi.config import IrsSoiConfig
from data_ingestion_toolbox.irs_soi.plan import RECENT_YEARS, files_to_capture
from data_ingestion_toolbox.irs_soi.registry import (
    AGI_BANDS,
    ALL_RETURNS,
    BAND_SCHEMES,
    COUNTY_AGI,
    COUNTY_TOTAL,
    MEASURES,
    STATE,
    SoiFile,
    band_scheme,
    get_file,
    metric_key,
    registered_files,
    registered_years,
)
from data_ingestion_toolbox.irs_soi.silver_irs_soi.load import replay_run
from data_ingestion_toolbox.irs_soi.silver_irs_soi.parse import parse_file, read_number

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "irs_soi"
TOTAL_2023 = SoiFile(COUNTY_TOTAL, 2023)
AGI_2023 = SoiFile(COUNTY_AGI, 2023)
STATE_2023 = SoiFile(STATE, 2023)
KENT = "state:10|county:001"


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


class _Client:
    def __init__(self, outcomes: list[httpx.Response]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        self.calls.append(url)
        return self.outcomes.pop(0)

    def close(self) -> None:
        return None


def _response(status: int, content: bytes = b"") -> httpx.Response:
    return httpx.Response(
        status,
        content=content,
        request=httpx.Request("GET", "https://www.irs.gov/pub/irs-soi"),
    )


def _values(parsed, geo_id: str, band: str = ALL_RETURNS) -> dict[str, Decimal | None]:
    return {
        obs.measure_id: obs.value
        for obs in parsed.observations
        if obs.geo_id == geo_id and obs.agi_band == band
    }


def test_configuration_imports_without_io_and_holds_no_credential() -> None:
    """Covers: ETL-114 — the SOI files need no key, so there is none to leak."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.irs_soi"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.irs_soi.config")
    fields = set(module.IrsSoiConfig.model_fields)
    assert not {field for field in fields if "key" in field or "token" in field}
    assert module.SOURCE_CODE == "IRS_SOI"


def test_the_registry_names_every_file_band_and_item() -> None:
    """Covers: ETL-114 — two county files a year from 2011, the state file where it is a CSV."""
    files = registered_files()
    assert registered_years() == tuple(range(1989, 2024))
    assert {item.key for item in files if item.tax_year == 2011} == {
        "county_total:2011",
        "county_agi:2011",
    }
    # Before 2010 there is one workbook zip a year, all returns only.
    assert {item.key for item in files if item.tax_year == 1989} == {
        "county_total:1989"
    }
    assert get_file("county_total:1989").path == "/1989countyincome.zip"
    assert get_file("county_total:1989").container == "zip_xls"
    assert get_file("county_agi:2010").path == "/2010countydata.zip"
    assert get_file("county_agi:2010").csv_name == "10incyallagi.csv"
    assert get_file("county_total:2023").path == "/23incyallnoagi.csv"
    assert get_file("county_agi:2011").path == "/11incyallagi.csv"
    assert get_file("state:2020").path == "/20in55cmcsv.csv"
    assert "state:2019" not in {item.key for item in files}
    assert len({item.key for item in files}) == len(files)
    for _kind, _first, _last, stubs in BAND_SCHEMES:
        assert set(stubs.values()) <= set(AGI_BANDS)
        assert 0 not in stubs
    assert band_scheme(COUNTY_TOTAL, 2015) == {0: ALL_RETURNS}
    assert band_scheme(STATE, 2023)[10] == "agi_1m_plus"
    assert len({measure.column for measure in MEASURES}) == len(MEASURES)
    assert metric_key("income_tax_amount", ALL_RETURNS) == "income_tax_amount"
    assert metric_key("returns", "agi_50k_75k") == "returns:agi_50k_75k"
    with pytest.raises(KeyError):
        band_scheme(STATE, 2001)


def test_a_client_error_fails_once_a_server_error_retries_and_html_is_refused() -> None:
    """Covers: ETL-114 — a 404 is not retried; a moved file answered with a page is refused."""
    config = IrsSoiConfig(min_spacing_seconds=0, max_attempts=3)
    missing = _Client([_response(404)])
    with pytest.raises(IrsSoiFetchError) as error:
        fetch_file(TOTAL_2023, config=config, client=missing, sleep=lambda _s: None)
    assert error.value.code == "non_retryable_http"
    assert len(missing.calls) == 1

    flaky = _Client([_response(503), _response(200, _fixture("23incyallnoagi.csv"))])
    response = fetch_file(
        TOTAL_2023, config=config, client=flaky, sleep=lambda _s: None
    )
    assert response.http_status == 200
    assert flaky.calls == [
        "https://www.irs.gov/pub/irs-soi/23incyallnoagi.csv",
        "https://www.irs.gov/pub/irs-soi/23incyallnoagi.csv",
    ]

    page = _Client([_response(200, b"<!DOCTYPE html><html>moved</html>")])
    with pytest.raises(IrsSoiPayloadError) as refused:
        fetch_file(TOTAL_2023, config=config, client=page, sleep=lambda _s: None)
    assert refused.value.code == "not_a_csv"
    assert "html" not in str(refused.value).lower()


def test_the_county_totals_file_reads_counties_and_the_state_total() -> None:
    """Covers: ETL-114 — county code 000 is the state; every item is read by its column name."""
    parsed = parse_file(TOTAL_2023, _fixture("23incyallnoagi.csv"))
    assert parsed.quarantined == ()
    assert parsed.row_count == 4
    # Every item but the 1989-2009 workbooks' interest received.
    assert parsed.measures_read == tuple(
        measure.measure_id
        for measure in MEASURES
        if measure.measure_id != "interest_received_amount"
    )
    assert len(parsed.observations) == 4 * len(parsed.measures_read)
    state = _values(parsed, "state:10")
    kent = _values(parsed, KENT)
    assert kent["returns"] == Decimal("89390.0000")
    assert state["returns"] == Decimal("507370.0000")
    counties = sum(
        _values(parsed, geo)["returns"]
        for geo in (KENT, "state:10|county:003", "state:10|county:005")
    )
    # SOI rounds county counts to tens; the sum is within DQ-SOI-003's 1 percent.
    assert abs(counties - state["returns"]) <= state["returns"] / 100
    assert {obs.geo_type for obs in parsed.observations} == {"state", "county"}
    assert {obs.agi_band for obs in parsed.observations} == {ALL_RETURNS}
    labels = {obs.geo_id: obs.geo_source_label for obs in parsed.observations}
    assert labels[KENT] == "Kent County"


def test_the_band_file_maps_stubs_to_dollar_ranges_and_refuses_an_unregistered_stub() -> (
    None
):
    """Covers: ETL-114 — a stub outside the year's scheme refuses the whole file."""
    parsed = parse_file(AGI_2023, _fixture("23incyallagi.csv"))
    assert parsed.quarantined == ()
    bands = {obs.agi_band for obs in parsed.observations}
    assert bands == set(band_scheme(COUNTY_AGI, 2023).values()) - {ALL_RETURNS}
    kent_bands = sum(
        obs.value
        for obs in parsed.observations
        if obs.geo_id == KENT and obs.measure_id == "returns"
    )
    assert kent_bands == Decimal("89390")

    # 2011 splits returns into seven bands, $1 under $25,000 being one.
    old = parse_file(SoiFile(COUNTY_AGI, 2011), _fixture("11incyallagi.csv"))
    assert old.quarantined == ()
    assert {obs.agi_band for obs in old.observations} == {
        "agi_under_1",
        "agi_1_25k",
        "agi_25k_50k",
        "agi_50k_75k",
        "agi_75k_100k",
        "agi_100k_200k",
        "agi_200k_plus",
    }
    # A file read under the other scheme is refused whole, in both
    # directions: eight stubs under the seven-stub scheme, and seven
    # under the eight-stub one (which would shift every band).
    for item, name in (
        (SoiFile(COUNTY_AGI, 2011), "23incyallagi.csv"),
        (SoiFile(COUNTY_AGI, 2023), "11incyallagi.csv"),
    ):
        refused = parse_file(item, _fixture(name))
        assert refused.rejected
        assert refused.observations == ()
        assert refused.quarantined[0].error_code == "unregistered_agi_stub"


def test_the_state_file_reads_separators_the_nation_and_other_areas() -> None:
    """Covers: ETL-114 — `"159,949,000"` is a number; `US` is the nation; `OA` is unmapped."""
    parsed = parse_file(STATE_2023, _fixture("23in55cmcsv.csv"))
    assert parsed.quarantined == ()
    nation = _values(parsed, "us:1")
    assert nation["returns"] == Decimal("159949000")
    assert _values(parsed, "state:10")["returns"] == Decimal("507390")
    other = {
        obs.geo_id: obs.geo_type
        for obs in parsed.observations
        if obs.geo_source_code in {"OA", "PR"}
    }
    assert other == {"irs_soi_area:OA": "other", "irs_soi_area:PR": "other"}
    assert {obs.agi_band for obs in parsed.observations if obs.geo_id == "us:1"} == set(
        band_scheme(STATE, 2023).values()
    )
    assert read_number('"2,324,150"'.strip('"')) == Decimal("2324150")


def test_a_cell_that_is_not_a_number_is_missing_never_zero() -> None:
    """Covers: ETL-114 — SOI's own text is kept beside a missing value."""
    payload = _fixture("23incyallnoagi.csv").decode()
    lines = payload.splitlines()
    cells = lines[2].split(",")
    cells[5] = "d"
    lines[2] = ",".join(cells)
    parsed = parse_file(TOTAL_2023, ("\n".join(lines) + "\n").encode())
    returns = next(
        obs
        for obs in parsed.observations
        if obs.geo_id == KENT and obs.measure_id == "returns"
    )
    assert returns.value is None
    assert returns.value_status == "missing"
    assert returns.value_source == "d"


def test_unreadable_rows_and_a_wrong_header_are_quarantined() -> None:
    """Covers: ETL-114 — a short row, a bad code and a repeated place are set aside; no N1 refuses the file."""
    payload = _fixture("23incyallnoagi.csv").decode()
    lines = payload.splitlines()
    lines.append(lines[2])  # Kent again
    lines.append("10,DE,0X1,Nowhere,0" + ",1" * (len(lines[0].split(",")) - 5))
    lines.append("10,DE,007")
    parsed = parse_file(TOTAL_2023, ("\n".join(lines) + "\n").encode())
    codes = {(item.source_row_index, item.error_code) for item in parsed.quarantined}
    assert codes == {
        (5, "duplicate_row"),
        (6, "unreadable_geography"),
        (7, "short_row"),
    }
    assert len(parsed.observations) == 4 * len(parsed.measures_read)

    header = lines[0].replace(",N1,", ",N_ONE,")
    refused = parse_file(TOTAL_2023, ("\n".join([header, *lines[1:5]]) + "\n").encode())
    assert refused.rejected
    assert refused.quarantined[0].error_code == "unexpected_header"
    assert "N1" in refused.quarantined[0].error_summary


def test_a_scheduled_run_plans_every_year_never_answered_and_the_window() -> None:
    """Covers: ETL-114 — the loader rule: no manual history run is needed."""
    years = registered_years()
    fresh = files_to_capture(None, set())
    assert [item.key for item in fresh] == [item.key for item in registered_files()]

    loaded = set(years)
    steady = files_to_capture(None, loaded)
    assert sorted({item.tax_year for item in steady}) == list(years[-RECENT_YEARS:])

    gap = files_to_capture(None, loaded - {2015})
    assert sorted({item.tax_year for item in gap}) == [2015, *years[-RECENT_YEARS:]]

    history = files_to_capture({"history": True}, loaded)
    assert len(history) == len(registered_files())


#: The six items the 1989-2009 state workbooks carry.
WORKBOOK_ITEMS = (
    "returns",
    "individuals",
    "agi",
    "wages_amount",
    "interest_received_amount",
    "ordinary_dividends_amount",
)


@pytest.mark.parametrize(
    ("year", "state_returns"),
    [(1989, "308166"), (1996, None), (2003, None), (2009, "371587")],
)
def test_each_era_workbook_reads_its_six_items_by_label(year, state_returns) -> None:
    """Covers: ETL-114 — every 1989-2009 workbook layout reads the state, its counties and six items."""
    item = SoiFile(COUNTY_TOTAL, year)
    assert item.path == f"/{year}countyincome.zip"
    parsed = parse_file(item, _fixture(f"{year}countyincome.zip"))
    assert parsed.quarantined == ()
    assert parsed.row_count == 4
    assert parsed.measures_read == WORKBOOK_ITEMS
    assert len(parsed.observations) == 4 * len(WORKBOOK_ITEMS)
    geos = {obs.geo_id for obs in parsed.observations}
    assert geos == {"state:10", KENT, "state:10|county:003", "state:10|county:005"}
    state = _values(parsed, "state:10")
    assert all(value is not None for value in state.values())
    if state_returns is not None:
        assert state["returns"] == Decimal(state_returns)
    counties = sum(_values(parsed, geo)["returns"] for geo in geos if geo != "state:10")
    assert abs(counties - state["returns"]) <= state["returns"] / 100


def test_the_1989_workbook_matches_its_published_county_values() -> None:
    """Covers: ETL-114 — Delaware's 1989 counties as SOI printed them."""
    parsed = parse_file(SoiFile(COUNTY_TOTAL, 1989), _fixture("1989countyincome.zip"))
    assert _values(parsed, KENT)["returns"] == Decimal("48600")
    assert _values(parsed, "state:10|county:003")["returns"] == Decimal("205299")
    assert _values(parsed, "state:10|county:005")["returns"] == Decimal("54267")
    assert _values(parsed, "state:10")["agi"] == Decimal("9567700")


def test_a_revised_workbook_is_read_before_the_original_it_replaces() -> None:
    """Covers: ETL-114 — a ``cir`` workbook wins; the original's repeated rows are set aside."""
    original = zipfile.ZipFile(io.BytesIO(_fixture("1989countyincome.zip"))).read(
        "delaware89ci.xls"
    )
    revised = zipfile.ZipFile(io.BytesIO(_fixture("2009countyincome.zip"))).read(
        "2009 County Income/county income 2009 DE.xls"
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("delaware89ci.xls", original)
        archive.writestr("Delaware89cir.xls", revised)
    parsed = parse_file(SoiFile(COUNTY_TOTAL, 1989), buffer.getvalue())
    assert _values(parsed, "state:10")["returns"] == Decimal("371587")
    assert parsed.row_count == 8
    assert {q.error_code for q in parsed.quarantined} == {"duplicate_row"}
    assert sorted(q.source_row_index for q in parsed.quarantined) == [5, 6, 7, 8]


def test_the_2010_zip_reads_both_county_files_and_a_missing_member_is_refused() -> None:
    """Covers: ETL-114 — 2010's zip holds the two CSVs; a zip without the named member is refused whole."""
    payload = _fixture("2010countydata.zip")
    totals = parse_file(SoiFile(COUNTY_TOTAL, 2010), payload)
    assert totals.quarantined == ()
    assert _values(totals, "state:10")["returns"] == Decimal("427479")
    bands = parse_file(SoiFile(COUNTY_AGI, 2010), payload)
    assert bands.quarantined == ()
    assert {obs.agi_band for obs in bands.observations} == set(
        band_scheme(COUNTY_AGI, 2010).values()
    ) - {ALL_RETURNS}
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("readme.txt", b"no county data here")
    refused = parse_file(SoiFile(COUNTY_TOTAL, 2010), buffer.getvalue())
    assert refused.rejected and refused.observations == ()
    assert [q.error_code for q in refused.quarantined] == ["member_not_found"]
    no_workbook = parse_file(SoiFile(COUNTY_TOTAL, 2009), buffer.getvalue())
    assert [q.error_code for q in no_workbook.quarantined] == ["member_not_found"]


class _UnchangedDatabase:
    """A connection whose last publication of the file is ``published``."""

    def __init__(self, published: tuple | None) -> None:
        self.published = published
        self.control_rows: list[tuple] = []

    def connect(self) -> _UnchangedDatabase:
        return self

    def cursor(self) -> _UnchangedDatabase:
        return self

    def __enter__(self) -> _UnchangedDatabase:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, sql: str, parameters: tuple = ()) -> None:
        text = " ".join(sql.split())
        if "FROM silver_ref.dim_geo_entity WHERE created_at" in text:
            self._result = (False,)
        elif "status = 'published'" in text and "LIMIT 1" in text:
            assert parameters == (COUNTY_TOTAL, 2023)
            self._result = self.published
        elif text.startswith("INSERT INTO control.irs_soi_file"):
            self.control_rows.append(parameters)
            self._result = None
        elif "FROM control.irs_soi_file" in text:
            row = self.control_rows[-1]
            self._result = (row[1], row[2], row[3], row[4])
        else:
            raise AssertionError(f"unexpected statement: {text}")

    def fetchone(self):
        return self._result

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class _Control:
    def start_run(self, **_kwargs):
        return uuid4()

    def start_request(self, **_kwargs):
        return SimpleNamespace(request_id=uuid4())

    def record_request_retry(self, *_args, **_kwargs) -> None:
        return None

    def finish_request(self, *_args, **_kwargs) -> None:
        return None

    def finish_run(self, *_args, **_kwargs) -> None:
        return None


@pytest.mark.parametrize(
    ("published", "expected"),
    [
        (None, "captured"),
        (
            ("a" * 64, PARSER_CONTRACT_VERSION, datetime(2026, 10, 7, tzinfo=UTC)),
            "unchanged",
        ),
        (
            ("b" * 64, PARSER_CONTRACT_VERSION, datetime(2026, 10, 7, tzinfo=UTC)),
            "captured",
        ),
        (
            ("a" * 64, "irs_soi_income:v0", datetime(2026, 10, 7, tzinfo=UTC)),
            "captured",
        ),
    ],
    ids=["never-published", "same-bytes", "new-bytes", "new-parser"],
)
def test_a_read_that_repeats_the_last_publication_is_unchanged_and_replays_nothing(
    published, expected
) -> None:
    """Covers: ETL-114 — the monthly read of an unchanged SOI file stores no second copy of its rows."""
    database = _UnchangedDatabase(published)
    run_id, _capture = capture_file(
        database.connect,
        TOTAL_2023,
        config=IrsSoiConfig(min_spacing_seconds=0, max_attempts=1),
        client=_Client([_response(200, _fixture("23incyallnoagi.csv"))]),
        control=_Control(),
        persist_capture=lambda _factory, capture: CaptureReceipt(
            capture.capture_id, "a" * 64
        ),
    )
    assert database.control_rows[-1][-1] == expected
    if expected == "unchanged":
        assert replay_run(database.connect, run_id=run_id) == 0
