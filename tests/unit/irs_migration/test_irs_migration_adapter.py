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
from data_ingestion_toolbox.irs_migration.registry import (
    CATEGORY_LABELS,
    get_file,
    metric_key,
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
    assert len(files) == 10
    assert INFLOW_2223.path == "/countyinflow2223.csv"
    assert OUTFLOW_2223.year_pair == "2022-2023"
    assert metric_key("inflow", "total_us", "returns") == "inflow:total_us:returns"
    assert CATEGORY_LABELS["other_flows_same_state"] == "Other flows, same state"
    with pytest.raises(KeyError):
        get_file("inflow", "2017-2018")


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
