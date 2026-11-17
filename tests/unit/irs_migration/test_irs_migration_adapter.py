"""Offline contracts for the IRS SOI county migration adapter.

Covers: ETL-059
"""

from __future__ import annotations

import importlib
import sys
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.irs_migration.client import (
    IrsMigrationFetchError,
    IrsMigrationPayloadError,
    check_header,
    fetch_file,
)
from data_ingestion_toolbox.irs_migration.config import IrsMigrationConfig
from data_ingestion_toolbox.irs_migration.plan import (
    answered_year_pairs,
    file_keys_to_capture,
)
from data_ingestion_toolbox.irs_migration.registry import (
    CATEGORY_LABELS,
    get_file,
    metric_key,
    people_basis,
    recent_year_pairs,
    registered_files,
)
from data_ingestion_toolbox.irs_migration.silver_irs_migration.conform import (
    conform_flows,
)
from data_ingestion_toolbox.irs_migration.silver_irs_migration.parse import parse_file

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "irs_migration"
INFLOW_2223 = get_file("inflow", "2022-2023")
OUTFLOW_2223 = get_file("outflow", "2022-2023")
INFLOW_2122 = get_file("inflow", "2021-2022")
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


def test_configuration_imports_without_io_and_holds_no_credential() -> None:
    """Covers: ETL-059 — the SOI files need no key, so there is none to leak."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.irs_migration"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.irs_migration.config")
    fields = set(module.IrsMigrationConfig.model_fields)
    assert not {field for field in fields if "key" in field or "token" in field}
    assert module.IRS_SOI_BASE_URL == "https://www.irs.gov/pub/irs-soi"


def test_the_registry_names_every_file_and_labels_every_category() -> None:
    """Covers: ETL-059 — one inflow and one outflow per pair of filing years, labelled as SOI does."""
    files = registered_files()
    assert len(files) == 66
    assert {item.year_pair for item in files} == {
        f"{year}-{year + 1}" for year in range(1990, 2023)
    }
    assert INFLOW_2223.path == "/countyinflow2223.csv"
    assert get_file("outflow", "2011-2012").path == "/countyoutflow1112.csv"
    assert get_file("inflow", "2010-2011").path == "/county1011.zip"
    assert get_file("outflow", "2004-2005").path == "/county0405.zip"
    assert get_file("inflow", "2003-2004").path == "/2003to2004countymigration.zip"
    assert get_file("inflow", "1990-1991").path == "/1990to1991countymigration.zip"
    assert [get_file("inflow", f"{y}-{y + 1}").layout for y in (1991, 1992, 2010)] == [
        "text",
        "workbooks",
        "workbooks",
    ]
    assert not get_file("inflow", "1994-1995").has_agi
    assert get_file("inflow", "1995-1996").has_agi
    assert OUTFLOW_2223.year_pair == "2022-2023"
    assert metric_key("inflow", "total_us", "returns") == "inflow:total_us:returns"
    assert CATEGORY_LABELS["other_flows_same_state"] == "Other flows, same state"
    with pytest.raises(KeyError):
        get_file("inflow", "1989-1990")


def test_n2_counts_exemptions_until_exemptions_ended() -> None:
    """Covers: ETL-059 — before 2018-2019 the individuals count is exemptions claimed."""
    assert get_file("inflow", "2017-2018").people_basis == "exemptions"
    assert INFLOW_2223.people_basis == "individuals"
    assert people_basis(2018) == "exemptions" and people_basis(2019) == "individuals"


def test_a_scheduled_run_captures_the_newest_pairs_and_every_pair_never_published() -> (
    None
):
    """Covers: ETL-059 — loader rule: a pair never published is captured on a scheduled run."""
    assert recent_year_pairs() == ("2021-2022", "2022-2023")
    fresh = file_keys_to_capture(None)
    assert len(fresh) == 66 and fresh[:2] == ["inflow:1990-1991", "outflow:1990-1991"]
    loaded = {f"{year}-{year + 1}" for year in range(2018, 2023)}
    assert file_keys_to_capture({}, loaded) == [
        f"{direction}:{year}-{year + 1}"
        for year in [*range(1990, 2018), 2021, 2022]
        for direction in ("inflow", "outflow")
    ]
    everything = loaded | {f"{year}-{year + 1}" for year in range(1990, 2018)}
    assert file_keys_to_capture({}, everything) == [
        "inflow:2021-2022",
        "outflow:2021-2022",
        "inflow:2022-2023",
        "outflow:2022-2023",
    ]
    assert len(file_keys_to_capture({"history": True}, everything)) == 66


def test_a_pair_is_answered_only_when_both_files_are_published() -> None:
    """Covers: ETL-059 — a published inflow without its outflow is captured again."""

    class _Cursor:
        def __enter__(self) -> _Cursor:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, query: str, params: tuple) -> None:
            assert "control.irs_migration_file" in query
            assert params == (["published", "empty"],)

        def fetchall(self) -> list[tuple[str, str, str]]:
            csv = "irs_soi_county_migration_csv:v1"
            text = "irs_soi_county_migration_state_text"
            return [
                ("2018-2019", "inflow", csv),
                ("2018-2019", "outflow", csv),
                ("2017-2018", "inflow", csv),
                # Published under the contract the parser has since revised.
                ("1990-1991", "inflow", f"{text}:v1"),
                ("1990-1991", "outflow", f"{text}:v1"),
                ("1991-1992", "inflow", f"{text}:v2"),
                ("1991-1992", "outflow", f"{text}:v2"),
            ]

    class _Connection:
        def cursor(self) -> _Cursor:
            return _Cursor()

    assert answered_year_pairs(_Connection()) == {"2018-2019", "1991-1992"}


def test_a_2011_layout_file_counts_state_summary_rows_and_loads_its_counties() -> None:
    """Covers: ETL-059 — subject county 000 is the state's own total: counted, never a county."""
    item = get_file("inflow", "2011-2012")
    parsed = parse_file(
        (FIXTURES / "csv_2011_2018" / "countyinflow1112.csv").read_bytes(), item=item
    )
    assert parsed.quarantined == ()
    assert (parsed.row_count, parsed.summary_row_count, len(parsed.flows)) == (
        11,
        5,
        6,
    )
    assert parsed.subject_count == 1
    autauga = {flow.counterpart_code: flow for flow in parsed.flows}
    assert autauga["96:000"].returns == 2006
    assert autauga["01:001"].category == "non_migrants"
    assert autauga["01:001"].individuals == 41757


def test_a_repeated_row_is_quarantined_not_replaced() -> None:
    """Covers: ETL-059 — a word-for-word repeat is repeated_row; other figures for the same pair are duplicate_row."""
    lines = _fixture("countyinflow2223.csv").rstrip(b"\n").split(b"\n")
    payload = b"\n".join([*lines, lines[1]]) + b"\n"
    parsed = parse_file(payload, item=INFLOW_2223)
    assert [item.error_code for item in parsed.quarantined] == ["repeated_row"]
    changed = lines[1].rsplit(b",", 1)[0] + b",1"
    conflict = parse_file(b"\n".join([*lines, changed]) + b"\n", item=INFLOW_2223)
    assert [item.error_code for item in conflict.quarantined] == ["duplicate_row"]
    assert parsed.quarantined[0].source_row_index == len(lines)
    assert len(parsed.flows) + 1 == parsed.row_count


def test_a_client_error_fails_once_and_a_wrong_header_is_refused() -> None:
    """Covers: ETL-059 — 404 fails without retry; an outflow header is not an inflow file."""
    client = _Client([_response(404)])
    with pytest.raises(IrsMigrationFetchError) as raised:
        fetch_file(
            INFLOW_2223, config=IrsMigrationConfig(max_attempts=3), client=client
        )
    assert raised.value.code == "non_retryable_http" and len(client.calls) == 1
    with pytest.raises(IrsMigrationPayloadError, match="unexpected_header"):
        check_header(_fixture("countyoutflow2223.csv"), INFLOW_2223.path, INFLOW_2223)
    retries: list[BaseException] = []
    client = _Client([_response(503), _response(200, _fixture("countyinflow2223.csv"))])
    fetched = fetch_file(
        INFLOW_2223,
        config=IrsMigrationConfig(max_attempts=2),
        client=client,
        on_retry=retries.append,
        sleep=lambda _: None,
    )
    assert fetched.raw_bytes == _fixture("countyinflow2223.csv") and len(retries) == 1


def test_an_inflow_file_parses_flows_totals_and_categories() -> None:
    """Covers: ETL-059 — a flow names its origin and destination; a category names neither."""
    parsed = parse_file(_fixture("countyinflow2223.csv"), item=INFLOW_2223)
    assert parsed.quarantined == ()
    assert parsed.subject_count == 3
    kent = {
        flow.counterpart_code: flow
        for flow in parsed.flows
        if flow.subject_geo_id == KENT
    }
    philadelphia = kent["42:101"]
    assert philadelphia.category == "county"
    assert (philadelphia.origin_geo_id, philadelphia.destination_geo_id) == (
        "state:42|county:101",
        KENT,
    )
    assert (philadelphia.returns, philadelphia.individuals, philadelphia.agi) == (
        273,
        588,
        Decimal("17321"),
    )
    total = kent["96:000"]
    assert (total.category, total.returns, total.origin_geo_id) == (
        "total_us_and_foreign",
        5357,
        None,
    )
    assert kent["10:001"].category == "non_migrants"
    assert kent["58:000"].category == "other_flows_same_state"
    assert kent["57:005"].category == "foreign_apo_fpo"


def test_an_outflow_file_points_the_other_way_and_old_codes_are_padded() -> None:
    """Covers: ETL-059 — outflows originate at the subject; `10,1` reads as county 001."""
    outflow = parse_file(_fixture("countyoutflow2223.csv"), item=OUTFLOW_2223)
    philadelphia = next(
        flow
        for flow in outflow.flows
        if flow.subject_geo_id == KENT and flow.counterpart_code == "42:101"
    )
    assert (philadelphia.origin_geo_id, philadelphia.destination_geo_id) == (
        KENT,
        "state:42|county:101",
    )
    older = parse_file(_fixture("countyinflow2122.csv"), item=INFLOW_2122)
    assert older.quarantined == ()
    sussex = next(
        flow
        for flow in older.flows
        if flow.subject_geo_id == KENT and flow.counterpart_code == "10:005"
    )
    assert (sussex.category, sussex.returns) == ("county", 770)


def test_a_deleted_category_is_withheld_and_never_a_number() -> None:
    """Covers: ETL-059 — SOI's -1 is withheld in all three measures, not a count of minus one."""
    parsed = parse_file(_fixture("countyinflow2122.csv"), item=INFLOW_2122)
    withheld = [flow for flow in parsed.flows if flow.value_status == "withheld"]
    assert [flow.category for flow in withheld] == ["foreign_other_flows"] * 3
    assert all(
        flow.returns is None
        and flow.individuals is None
        and flow.agi is None
        and flow.value_source == "-1,-1,-1"
        for flow in withheld
    )


def test_unreadable_rows_and_a_wrong_header_are_quarantined() -> None:
    """Covers: ETL-059 — a short row, a bad code, an unknown category and a half-suppressed row."""
    lines = _fixture("countyinflow2223.csv").split(b"\n")
    lines[1] = b"10,001,96,000,DE,Kent County Total Migration-US and Foreign,5357"
    lines[2] = b"10,0X1,97,000,DE,Kent County Total Migration-US,5247,9479,298268"
    lines[3] = b"10,001,99,000,ZZ,Somewhere,20,30,40"
    lines[4] = b"10,001,59,001,DS,Other flows - Northeast,-1,801,31440"
    parsed = parse_file(b"\n".join(lines), item=INFLOW_2223)
    assert sorted(item.error_code for item in parsed.quarantined) == [
        "ragged_row",
        "unexpected_category",
        "unreadable_code",
        "unreadable_value",
    ]
    assert len(parsed.flows) + len(parsed.quarantined) == parsed.row_count
    wrong = parse_file(_fixture("countyoutflow2223.csv"), item=INFLOW_2223)
    assert [item.error_code for item in wrong.quarantined] == [
        "unexpected_header"
    ] and wrong.flows == ()


def test_a_flow_whose_origin_or_destination_does_not_resolve_is_refused() -> None:
    """Covers: ETL-059 — ADR-0008: both ends of a flow resolve, or the flow is refused."""
    parsed = parse_file(_fixture("countyinflow2223.csv"), item=INFLOW_2223)
    kent = [flow for flow in parsed.flows if flow.subject_geo_id == KENT]
    known = {KENT: 1, "state:42|county:101": 2}
    admitted, refused = conform_flows(kent, known)
    admitted_codes = {item.flow.counterpart_code for item in admitted}
    assert (
        "42:101" in admitted_codes
        and "96:000" in admitted_codes
        and "10:001" in admitted_codes
    )
    assert "36:047" not in admitted_codes
    assert {item.error_code for item in refused} == {"origin_unresolved"}
    assert len(admitted) + len(refused) == len(kent)
    philadelphia = next(
        item for item in admitted if item.flow.counterpart_code == "42:101"
    )
    assert (
        philadelphia.origin_geo_sk,
        philadelphia.destination_geo_sk,
        philadelphia.subject_geo_sk,
    ) == (2, 1, 1)
    category = next(item for item in admitted if item.flow.counterpart_code == "58:000")
    assert (category.origin_geo_sk, category.destination_geo_sk) == (None, None)
    outflow = [
        flow
        for flow in parse_file(
            _fixture("countyoutflow2223.csv"), item=OUTFLOW_2223
        ).flows
        if flow.subject_geo_id == KENT
    ]
    _admitted, refused_out = conform_flows(outflow, known)
    assert {item.error_code for item in refused_out} == {"destination_unresolved"}
    _none, unknown_subject = conform_flows(kent[:1], {})
    assert [item.error_code for item in unknown_subject] == ["subject_unresolved"]


def _zip(source_pair: str, extra: dict[str, bytes] | None = None) -> bytes:
    import io
    import zipfile

    from tests.support.irs_migration_zips import state_member_zip

    payload = state_member_zip(source_pair)
    if not extra:
        return payload
    buffer = io.BytesIO()
    with (
        zipfile.ZipFile(io.BytesIO(payload)) as source,
        zipfile.ZipFile(buffer, "w") as target,
    ):
        for name in source.namelist():
            target.writestr(name, source.read(name))
        for name, data in extra.items():
            target.writestr(name, data)
    return buffer.getvalue()


def _parts_sum_to_total(parsed) -> None:  # noqa: ANN001
    """Every subject's flows, foreign rows and remaining flows sum to its total."""
    by_subject: dict[str, list] = {}
    for flow in parsed.flows:
        by_subject.setdefault(flow.subject_geo_id, []).append(flow)
    regions = {
        "other_flows_northeast",
        "other_flows_midwest",
        "other_flows_south",
        "other_flows_west",
    }
    for flows in by_subject.values():
        total = next(f.returns for f in flows if f.category == "total_us_and_foreign")
        split = any(
            f.category in {"other_flows_same_state", "other_flows_different_state"}
            for f in flows
        )
        parts = sum(
            f.returns or 0
            for f in flows
            if f.category == "county"
            or f.category.startswith("foreign_")
            or f.category in {"other_flows_same_state", "other_flows_different_state"}
            or (not split and f.category in regions)
        )
        assert parts == total


def test_a_workbook_zip_reads_each_direction_from_its_title_with_agi() -> None:
    """Covers: ETL-059 — 2003-2004 workbooks: state rows counted, AGI read, parts sum to the total."""
    payload = _zip("2003-2004")
    inflow = parse_file(payload, item=get_file("inflow", "2003-2004"))
    outflow = parse_file(payload, item=get_file("outflow", "2003-2004"))
    assert inflow.quarantined == () and outflow.quarantined == ()
    assert (inflow.summary_row_count, inflow.subject_count) == (5, 3)
    kent = {f.counterpart_code: f for f in inflow.flows if f.subject_geo_id == KENT}
    assert (kent["96:000"].returns, kent["96:000"].agi) == (4395, Decimal("162859"))
    assert kent["10:001"].category == "non_migrants"
    assert kent["10:001"].counterpart_geo_id == KENT
    assert all(
        f.origin_geo_id == KENT
        for f in outflow.flows
        if f.category == "county" and f.subject_geo_id == KENT
    )
    _parts_sum_to_total(inflow)
    _parts_sum_to_total(outflow)


def test_a_1993_workbook_uses_the_early_codes_and_loads_no_agi() -> None:
    """Covers: ETL-059 — 00 001 is the total, 63 0xx are regions and non-migrants; money income is not AGI."""
    parsed = parse_file(_zip("1993-1994"), item=get_file("inflow", "1993-1994"))
    assert parsed.quarantined == () and parsed.summary_row_count == 0
    kent = {f.counterpart_code: f for f in parsed.flows if f.subject_geo_id == KENT}
    total = kent["00:001"]
    assert (total.category, total.returns, total.agi) == (
        "total_us_and_foreign",
        2943,
        None,
    )
    assert total.value_source == "2943,6161,72934"
    assert kent["63:011"].category == "other_flows_northeast"
    assert kent["63:050"].category == "non_migrants"
    assert kent["63:050"].counterpart_geo_id == KENT
    assert all(flow.agi is None for flow in parsed.flows)
    _parts_sum_to_total(parsed)


def test_a_1990_text_zip_reads_its_lines_as_the_early_codes() -> None:
    """Covers: ETL-059 — fixed-width text: total, flows, foreign, regions and non-migrants per county."""
    payload = _zip("1990-1991")
    inflow = parse_file(payload, item=get_file("inflow", "1990-1991"))
    outflow = parse_file(payload, item=get_file("outflow", "1990-1991"))
    assert inflow.quarantined == () and outflow.quarantined == ()
    assert inflow.subject_count == 3 and inflow.row_count == len(inflow.flows)
    kent = {f.counterpart_code: f for f in inflow.flows if f.subject_geo_id == KENT}
    assert (kent["00:001"].returns, kent["00:001"].individuals) == (3094, 6483)
    new_castle = kent["10:003"]
    assert (new_castle.category, new_castle.returns, new_castle.individuals) == (
        "county",
        449,
        910,
    )
    assert new_castle.origin_geo_id == "state:10|county:003"
    assert kent["57:001"].category == "foreign_overseas"
    assert kent["63:013"].category == "other_flows_south"
    assert kent["63:050"].returns == 38904
    _parts_sum_to_total(inflow)
    _parts_sum_to_total(outflow)


def test_a_repeated_member_is_read_once_and_an_unreadable_zip_is_refused() -> None:
    """Covers: ETL-059 — co0304OH0.xls beside co0304OHo.xls; a workbook naming no direction refuses the file."""
    item = get_file("inflow", "2003-2004")
    once = parse_file(_zip("2003-2004"), item=item)
    copy = _fixture("state_members/co0304DEi.xls")
    twice = parse_file(_zip("2003-2004", {"co0304DE0.xls": copy}), item=item)
    assert twice.row_count == once.row_count and twice.quarantined == ()
    broken = parse_file(
        _zip("2003-2004", {"co0304XXi.xls": b"not a workbook"}), item=item
    )
    assert [q.error_code for q in broken.quarantined] == ["unexpected_header"]
    assert broken.flows == ()
    with pytest.raises(IrsMigrationPayloadError, match="not_a_zip"):
        check_header(b"<html>", item.path, item)


def _members_zip(members: dict[str, str]) -> bytes:
    """A zip of checked-in members at SOI's own paths: {zip path: fixture name}."""
    import io
    import zipfile

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for path, name in members.items():
            archive.writestr(path, _fixture(f"state_members/{name}"))
    return buffer.getvalue()


_REGIONS = {
    "other_flows_northeast",
    "other_flows_midwest",
    "other_flows_south",
    "other_flows_west",
}


def _remaining_flows_sum_to_total(parsed) -> int:  # noqa: ANN001
    """DQ-IRS-004's sum: flows, foreign rows and every remaining-flow group.

    The regions count unless the file splits "different state" into them.
    Returns how many subjects were checked.
    """
    by_subject: dict[str, list] = {}
    for flow in parsed.flows:
        by_subject.setdefault(flow.subject_geo_id, []).append(flow)
    checked = 0
    for flows in by_subject.values():
        total = next(
            (f.returns for f in flows if f.category == "total_us_and_foreign"), None
        )
        if total is None:
            continue
        split = any(f.category == "other_flows_different_state" for f in flows)
        parts = sum(
            f.returns or 0
            for f in flows
            if f.category == "county"
            or f.category.startswith("foreign_")
            or (f.category.startswith("other_flows_") and f.category not in _REGIONS)
            or (not split and f.category in _REGIONS)
        )
        assert parts == total, flows[0].subject_geo_id
        checked += 1
    return checked


def test_a_1990_text_total_line_is_any_unindented_line_and_every_group_loads() -> None:
    """Covers: ETL-059 — Alaska 1990-1991: a cut "...Borough T" total starts its county; Same State to All Migration Flows load."""
    folder = "1990to1991CountyMigration/1990to1991CountyMigration"
    payload = _members_zip(
        {
            f"{folder}Inflow/C9091aki.txt": "C9091aki.txt",
            f"{folder}Outflow/C9091ako.txt": "C9091ako.txt",
        }
    )
    for direction in ("inflow", "outflow"):
        parsed = parse_file(payload, item=get_file(direction, "1990-1991"))
        assert parsed.quarantined == ()
        assert parsed.row_count == len(parsed.flows)
        assert _remaining_flows_sum_to_total(parsed) == parsed.subject_count
    inflow = parse_file(payload, item=get_file("inflow", "1990-1991"))
    anchorage = {
        f.counterpart_code: f
        for f in inflow.flows
        if f.subject_geo_id == "state:02|county:020"
    }
    # "02 020   Anchorage Borough Total  Ak  9510  18970": its own county,
    # not more rows of the county before it.
    assert anchorage["00:001"].returns == 9510
    assert anchorage["02:170"].returns == 453
    east = {
        f.counterpart_code: f
        for f in inflow.flows
        if f.subject_geo_id == "state:02|county:013"
    }
    assert east["00:001"].returns == 145
    assert east["63:020"].category == "other_flows_same_state"
    assert east["63:021"].category == "other_flows_same_region_different_state"
    assert east["63:022"].category == "other_flows_different_region"
    codes = {f.counterpart_code for f in inflow.flows}
    assert {"63:010", "63:030"} <= codes


def test_a_1992_workbook_loads_every_remaining_flow_group() -> None:
    """Covers: ETL-059 — Vermont 1992-1993: 63 010, 63 020-022 and Suppress All Flows (63 030) are categories, parts sum to totals."""
    payload = _members_zip(
        {"C9293Vti.xls": "C9293Vti.xls", "C9293Vto.xls": "C9293Vto.xls"}
    )
    for direction in ("inflow", "outflow"):
        parsed = parse_file(payload, item=get_file(direction, "1992-1993"))
        assert parsed.quarantined == ()
        assert _remaining_flows_sum_to_total(parsed) == parsed.subject_count
        categories = {f.counterpart_code: f.category for f in parsed.flows}
        assert categories["63:010"] == "other_flows_same_state"
        assert categories["63:020"] == "other_flows_same_state"
        assert categories["63:021"] == "other_flows_same_region_different_state"
        assert categories["63:022"] == "other_flows_different_region"
        assert categories["63:030"] == "other_flows_all"
    assert CATEGORY_LABELS["other_flows_all"] == "Other flows, all combined"


def test_the_1998_zip_reads_its_bare_excel4_us_members_and_revised_marks() -> None:
    """Covers: ETL-059 — co989usi.xls is no OLE2 file and has no title: read, direction from its folder, US rows not counties; "r  128" is 128."""
    folder = "1998to1999CountyMigration/1998to1999CountyMigration"
    payload = _members_zip(
        {
            f"{folder}Inflow/co989idi.xls": "co989idi.xls",
            f"{folder}Inflow/co989usi.xls": "co989usi.xls",
            f"{folder}Outflow/co989uso.xls": "co989uso.xls",
        }
    )
    inflow = parse_file(payload, item=get_file("inflow", "1998-1999"))
    assert {q.error_code for q in inflow.quarantined} == {"subject_not_county"}
    assert len(inflow.quarantined) == 5  # the US member's own five totals
    bear_lake = {
        f.counterpart_code: f
        for f in inflow.flows
        if f.subject_geo_id == "state:16|county:007"
    }
    assert bear_lake["96:000"].returns == 128
    assert bear_lake["96:000"].value_source.startswith("r")
    outflow = parse_file(payload, item=get_file("outflow", "1998-1999"))
    assert [q.error_code for q in outflow.quarantined] == ["subject_not_county"] * 5
    assert outflow.flows == ()


def test_a_trailing_state_name_is_layout_not_a_ragged_row() -> None:
    """Covers: ETL-059 — North Dakota 2002-2003: Adams County's total row carries "North Dakota" in a twelfth cell and still loads."""
    payload = _members_zip({"co203ndi.xls": "co203ndi.xls"})
    parsed = parse_file(payload, item=get_file("inflow", "2002-2003"))
    assert parsed.quarantined == ()
    adams = {
        f.counterpart_code: f
        for f in parsed.flows
        if f.subject_geo_id == "state:38|county:001"
    }
    assert adams["96:000"].returns == 50


def test_a_file_the_parser_revised_carries_a_newer_contract() -> None:
    """Covers: ETL-059 — revised pairs are v2 and are captured again by a scheduled run; others stay v1."""
    assert get_file("inflow", "1990-1991").parser_contract == (
        "irs_soi_county_migration_state_text:v2"
    )
    assert get_file("outflow", "1993-1994").parser_contract == (
        "irs_soi_county_migration_state_workbooks:v2"
    )
    assert get_file("inflow", "1998-1999").parser_contract == (
        "irs_soi_county_migration_state_workbooks:v1"
    )
    assert get_file("inflow", "2022-2023").parser_contract == (
        "irs_soi_county_migration_csv:v1"
    )


class _PendingCursor:
    def __init__(self, row: tuple | None) -> None:
        self.row = row

    def __enter__(self) -> _PendingCursor:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, query: str, params: tuple) -> None:
        assert "control.irs_migration_file" in query
        assert params == ("outflow", "2019-2020")

    def fetchone(self) -> tuple | None:
        return self.row


class _PendingConnection:
    def __init__(self, row: tuple | None) -> None:
        self.row = row

    def cursor(self) -> _PendingCursor:
        return _PendingCursor(self.row)

    def close(self) -> None:
        return None


def test_a_retry_replays_the_capture_its_failed_attempt_committed() -> None:
    """Covers: ETL-059 — the newest row captured and never replayed is resumed, not fetched again; anything else captures afresh."""
    from uuid import UUID

    from data_ingestion_toolbox.irs_migration.capture import pending_run

    item = get_file("outflow", "2019-2020")
    run = "c112d5bc-ab1b-42c0-9c9f-b52e9bbdbced"
    contract = item.parser_contract
    assert pending_run(
        lambda: _PendingConnection((run, "captured", contract)), item
    ) == UUID(run)
    for row in (
        None,
        (run, "published", contract),
        (run, "quarantined", contract),
        (run, "captured", "irs_soi_county_migration_csv:v0"),
    ):
        assert pending_run(lambda row=row: _PendingConnection(row), item) is None
