"""Offline contracts for the HUD Fair Market Rent and income-limit adapter.

Covers: ETL-065
"""

from __future__ import annotations

import importlib
import io
import re
import sys
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest

from data_ingestion_toolbox.hud_fmr_il.client import HudPayloadError, check_workbook
from data_ingestion_toolbox.hud_fmr_il.registry import get_file, registered_files
from data_ingestion_toolbox.hud_fmr_il.silver_hud_fmr_il.parse import parse_file
from data_ingestion_toolbox.utility.workbook import read_sheet

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "hud_fmr_il"
SHEET = "xl/worksheets/sheet1.xml"
FY26 = get_file("fmr:fy2026:original")
FY26_REVISED = get_file("fmr:fy2026:revised")
IL26 = get_file("il:fy2026:original")


def _fixture(item) -> bytes:  # noqa: ANN001
    return (FIXTURES / item.path.rsplit("/", 1)[1]).read_bytes()


def _with_sheet(item, edit) -> bytes:  # noqa: ANN001
    source = zipfile.ZipFile(io.BytesIO(_fixture(item)))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as target:
        for info in source.infolist():
            data = source.read(info)
            if info.filename == SHEET:
                data = edit(data.decode("utf-8")).encode("utf-8")
            target.writestr(info, data)
    return out.getvalue()


def _values(raw: bytes, item) -> dict[tuple[str, str], object]:  # noqa: ANN001
    return {
        (obs.geo_id, obs.measure): obs
        for obs in parse_file(raw, item=item).observations
    }


def test_configuration_imports_without_io_and_reads_the_token_only_on_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-065 — importing reads no token; only ``from_environment`` does."""
    monkeypatch.setenv("HUD_USER_API_TOKEN", "fixture-token-not-a-real-secret")
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.hud_fmr_il"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.hud_fmr_il.config")
    assert module.HudConfig().hud_user_api_token == ""
    assert (
        module.HudConfig.from_environment().hud_user_api_token
        == "fixture-token-not-a-real-secret"
    )
    assert [item.key for item in registered_files()] == [
        "fmr:fy2026:original",
        "fmr:fy2026:revised",
        "fmr:fy2027:original",
        "il:fy2026:original",
    ]


def test_every_registered_edition_replays_by_column_name() -> None:
    """Covers: ETL-065 — each edition's own columns, including the year-named ones."""
    for item in registered_files():
        parsed = parse_file(_fixture(item), item=item)
        assert parsed.quarantined == () and (
            parsed.row_count,
            parsed.county_row_count,
        ) == (5, 4), item.key
        check_workbook(_fixture(item), item)
    kent = _values(_fixture(FY26), FY26)[("state:10|county:001", "fmr_2br")]
    assert (kent.value, kent.hud_area_code, kent.metro) == (
        Decimal("1470"),
        "METRO20100M20100",
        True,
    )
    sussex = _values(_fixture(FY26), FY26)[("state:10|county:005", "fmr_0br")]
    assert (sussex.hud_area_code, sussex.metro) == ("NCNTY10005N10005", False)
    limits = _values(_fixture(IL26), IL26)
    assert limits[("state:10|county:001", "median_family_income")].value == Decimal(
        "112100"
    )
    assert limits[("state:10|county:001", "income_limit_50_8p")].value_status == "valid"
    assert len({measure for _geo, measure in limits}) == 25


def test_a_town_row_is_a_county_subdivision_never_a_county() -> None:
    """Covers: ETL-065 — a New England town keeps its subdivision code."""
    town = _values(_fixture(FY26), FY26)[
        ("state:09|county:110|cousub:01080", "fmr_2br")
    ]
    assert (town.geo_type, town.fips_code) == ("county_subdivision", "0911001080")
    assert ("state:09|county:110", "fmr_2br") not in _values(_fixture(FY26), FY26)


def test_the_revised_edition_differs_where_hud_reissued() -> None:
    """Covers: ETL-065 — Napa's FY 2026 FMRs were reissued; Delaware's were not."""
    original, revised = (
        _values(_fixture(FY26), FY26),
        _values(_fixture(FY26_REVISED), FY26_REVISED),
    )
    assert (
        original[("state:06|county:055", "fmr_2br")].value,
        revised[("state:06|county:055", "fmr_2br")].value,
    ) == (
        Decimal("2773"),
        Decimal("3315"),
    )
    assert (
        original[("state:10|county:001", "fmr_2br")].value
        == revised[("state:10|county:001", "fmr_2br")].value
    )


def test_empty_cells_are_missing_and_malformed_rows_quarantined() -> None:
    """Covers: ETL-065 — an empty cell is missing, not zero; a bad row or a wrong header is refused."""

    first, second, third, fourth = (
        number for number, _cells in list(read_sheet(_fixture(FY26), FY26.sheet))[1:5]
    )

    def damage(sheet: str) -> str:
        sheet = re.sub(rf'<c r="L{first}"[^>]*><v>[^<]*</v></c>', "", sheet, count=1)
        sheet = re.sub(
            rf'(<c r="M{second}"[^>]*?)(?: t="\w+")?><v>[^<]*</v>',
            r'\1 t="str"><v>n/a</v>',
            sheet,
            count=1,
        )
        sheet = re.sub(
            rf'(<c r="H{third}"[^>]*?) t="s"><v>\d+</v>',
            r'\1 t="str"><v>12345</v>',
            sheet,
            count=1,
        )
        row = re.search(rf'<row r="{fourth}" .*?</row>', sheet, flags=re.S).group(0)
        return sheet.replace(
            "</sheetData>",
            re.sub(rf'r="(\D*){fourth}"', r'r="\g<1>9999"', row) + "</sheetData>",
        )

    parsed = parse_file(_with_sheet(FY26, damage), item=FY26)
    assert sorted((q.source_row_index, q.error_code) for q in parsed.quarantined) == [
        (second, "unreadable_row"),
        (third, "unreadable_row"),
        (9999, "duplicate_row"),
    ]
    blank = next(
        obs
        for obs in parsed.observations
        if obs.source_row_index == first and obs.measure == "fmr_2br"
    )
    assert (blank.value, blank.value_status, blank.missing_reason) == (
        None,
        "missing",
        "provider_missing",
    )

    def rename(sheet: str) -> str:
        return re.sub(
            r'(<c r="J1"[^>]*?) t="s"><v>\d+</v>',
            r'\1 t="str"><v>rent_0</v>',
            sheet,
            count=1,
        )

    refused = parse_file(_with_sheet(FY26, rename), item=FY26)
    assert [(q.source_row_index, q.error_code) for q in refused.quarantined] == [
        (0, "unexpected_header")
    ]
    with pytest.raises(HudPayloadError, match="unexpected_header"):
        check_workbook(_with_sheet(FY26, rename), FY26)
    with pytest.raises(HudPayloadError, match="sheet_missing"):
        check_workbook(_fixture(IL26), FY26)


def test_an_empty_202_challenge_retries_and_is_not_a_layout_change() -> None:
    """Covers: ETL-065 — HUD User's empty 202 is retried, then reported as unavailable."""
    import httpx

    from data_ingestion_toolbox.hud_fmr_il.client import HudFetchError, fetch_file
    from data_ingestion_toolbox.hud_fmr_il.config import HudConfig
    from tests.support.external import classify_external_failure

    answers = [
        httpx.Response(202, content=b""),
        httpx.Response(200, content=_fixture(FY26)),
    ]

    class Client:
        def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
            response = answers.pop(0)
            response.request = httpx.Request("GET", url)
            return response

    slept: list[float] = []
    response = fetch_file(
        FY26, config=HudConfig(max_attempts=2), client=Client(), sleep=slept.append
    )
    assert response.http_status == 200 and len(slept) == 1

    class Challenged:
        def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
            return httpx.Response(202, content=b"", request=httpx.Request("GET", url))

    with pytest.raises(HudFetchError, match="retry_exhausted") as raised:
        fetch_file(
            FY26,
            config=HudConfig(max_attempts=2),
            client=Challenged(),
            sleep=lambda _seconds: None,
        )
    assert classify_external_failure(raised.value) == "upstream-unavailable"
