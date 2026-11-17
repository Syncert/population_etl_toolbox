"""Offline contracts for the 2020 Census redistricting data adapter.

Covers: ETL-129
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.census_decennial import plan
from data_ingestion_toolbox.census_decennial.client import (
    DecennialFetchError,
    DecennialPayloadError,
    fetch_slice,
    validate_payload,
)
from data_ingestion_toolbox.census_decennial.config import DecennialConfig
from data_ingestion_toolbox.census_decennial.registry import (
    BLOCK_COUNTIES,
    PL,
    TRACT_PARENT_FIPS,
    DecennialSlice,
    get_dataset,
    slices,
)
from data_ingestion_toolbox.census_decennial.silver_census_decennial.parse import (
    parse_slice,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "census_decennial"
KEY = "unit-test-census-key-0123456789"


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


class _ScriptedClient:
    def __init__(self, outcomes: list[httpx.Response | BaseException]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, dict[str, str]]] = []

    def get(self, url: str, *, params: dict[str, str]) -> httpx.Response:
        self.calls.append((url, dict(params)))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def close(self) -> None:
        return None


def _response(status: int, content: bytes = b"") -> httpx.Response:
    return httpx.Response(
        status,
        content=content,
        headers={"content-type": "application/json", "set-cookie": "secret"},
        request=httpx.Request("GET", "https://api.census.gov/data/2020/dec/pl"),
    )


def _config(**overrides: object) -> DecennialConfig:
    values: dict[str, object] = {
        "census_api_key": KEY,
        "min_spacing_seconds": 0,
        "max_attempts": 3,
    }
    values.update(overrides)
    return DecennialConfig(**values)


def test_configuration_and_registry_import_without_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-129 -- Importing reads no key; a request without one is refused when it runs."""
    monkeypatch.delenv("CENSUS_API_KEY", raising=False)
    for name in (
        "data_ingestion_toolbox.census_decennial.config",
        "data_ingestion_toolbox.census_decennial.registry",
    ):
        sys.modules.pop(name, None)
        importlib.import_module(name)
    with pytest.raises(ValueError, match="CENSUS_API_KEY"):
        DecennialConfig.from_environment().require_api_key()


def test_no_race_ethnicity_income_or_poverty_variable_is_requested() -> None:
    """Covers: ETL-129 -- The outbreak guardrail: P1's total is the only cell of the race table."""
    requested = PL.get_variables()
    assert requested[0] == "NAME"
    assert [v for v in requested if v.startswith("P1_")] == ["P1_001N"]
    assert not [v for v in requested if v.startswith(("P2_", "P3_", "P4_"))]
    assert {v for v in requested if v.startswith("P5_")} == {
        f"P5_{index:03d}N" for index in range(1, 11)
    }


def test_registered_slices_and_their_parameters() -> None:
    """Covers: ETL-129 -- Every grain is one request; blocks are asked only for declared counties."""
    assert BLOCK_COUNTIES == ()
    registered = slices()
    assert registered[:3] == (
        DecennialSlice("us", "1"),
        DecennialSlice("state", "*"),
        DecennialSlice("county", "*"),
    )
    assert [s.scope for s in registered if s.geo_level == "tract"] == list(
        TRACT_PARENT_FIPS
    )
    assert not [s for s in registered if s.geo_level == "block"]
    assert PL.request_parameters(DecennialSlice("tract", "55"))["in"] == "state:55"
    block = PL.request_parameters(DecennialSlice("block", "55025"))
    assert block["for"] == "block:*"
    assert block["in"] == "state:55 county:025 tract:*"
    assert "key" not in block
    for bad in (
        DecennialSlice("tract", "99"),
        DecennialSlice("block", "55"),
        DecennialSlice("place", "*"),
    ):
        with pytest.raises(ValueError):
            PL.request_parameters(bad)
    assert get_dataset("pl") is PL


def test_key_rides_only_on_the_request_and_never_in_the_slice_or_error() -> None:
    """Covers: ETL-129 -- The key rides on the request alone, never in the slice or an error."""
    client = _ScriptedClient([_response(200, _fixture("pl_us_1.json"))])
    response = fetch_slice(
        PL, slice_=DecennialSlice("us", "1"), config=_config(), client=client
    )
    ((url, params),) = client.calls
    assert url == "https://api.census.gov/data/2020/dec/pl"
    assert params["key"] == KEY
    assert "key" not in response.request_parameters
    assert "set-cookie" not in {name.lower() for name in response.response_headers}

    failing = _ScriptedClient([httpx.ConnectError(f"boom ?key={KEY}")] * 3)
    with pytest.raises(DecennialFetchError) as raised:
        fetch_slice(
            PL,
            slice_=DecennialSlice("us", "1"),
            config=_config(),
            client=failing,
            sleep=lambda _s: None,
        )
    assert KEY not in str(raised.value)


def test_an_unpublished_area_is_empty_and_a_client_error_is_not_retried() -> None:
    """Covers: ETL-129 -- A 204 is an empty slice; a 4xx is not retried."""
    empty = fetch_slice(
        PL,
        slice_=DecennialSlice("tract", "72"),
        config=_config(),
        client=_ScriptedClient([_response(204)]),
    )
    assert not empty.published and empty.raw_bytes == b""
    refused = _ScriptedClient([_response(400, b"error: unknown variable")])
    with pytest.raises(DecennialFetchError) as raised:
        fetch_slice(
            PL, slice_=DecennialSlice("us", "1"), config=_config(), client=refused
        )
    assert raised.value.code == "non_retryable_http"
    assert len(refused.calls) == 1


def test_validate_payload_refuses_a_changed_shape() -> None:
    """Covers: ETL-129 -- A body whose header is not the registered variables is refused."""
    rows = json.loads(_fixture("pl_us_1.json"))
    rows[0] = [name for name in rows[0] if name != "P5_005N"]
    with pytest.raises(DecennialPayloadError) as raised:
        validate_payload(json.dumps(rows).encode(), PL.api_path, PL.get_variables())
    assert raised.value.code in {"missing_columns", "ragged_rows"}


def test_fixtures_parse_at_exact_geographies_with_dane_county_as_published() -> None:
    """Covers: ETL-129 -- The fixtures parse at exact geographies; Dane County as published."""
    county = parse_slice(
        PL,
        slice_=DecennialSlice("county", "*"),
        payload=_fixture("pl_county_all.json"),
    )
    assert county.row_count == 72 and not county.quarantined
    dane = {
        c.variable_id: c.value
        for c in county.counts
        if c.geo_id == "state:55|county:025"
    }
    assert dane["P5_001N"] == 17465
    assert dane["P5_005N"] == 1806
    assert dane["P5_008N"] == 9925
    assert dane["P5_003N"] == 1541
    assert dane["P1_001N"] == 561504
    # The parts add to the totals as published.
    assert dane["P5_002N"] + dane["P5_007N"] == dane["P5_001N"]
    assert sum(dane[f"P5_00{i}N"] for i in (3, 4, 5, 6)) == dane["P5_002N"]
    assert dane["H1_002N"] + dane["H1_003N"] == dane["H1_001N"]

    tracts = parse_slice(
        PL, slice_=DecennialSlice("tract", "55"), payload=_fixture("pl_tract_55.json")
    )
    assert tracts.row_count == 125 and not tracts.quarantined
    assert {c.geo_type for c in tracts.counts} == {"tract"}
    first = tracts.counts[0]
    assert first.geo_id == "state:55|county:025|tract:000100"
    assert first.geo_source_code == "55025000100"
    tract_total = sum(c.value for c in tracts.counts if c.variable_id == "P5_001N")
    assert tract_total == dane["P5_001N"]

    nation = parse_slice(
        PL, slice_=DecennialSlice("us", "1"), payload=_fixture("pl_us_1.json")
    )
    assert {c.geo_id for c in nation.counts} == {"us:1"}
    assert len(nation.counts) == len(PL.variables)


def test_a_null_count_is_missing_never_zero() -> None:
    """Covers: ETL-129 -- A null count is missing, never zero."""
    rows = json.loads(_fixture("pl_us_1.json"))
    header = rows[0]
    rows[1][header.index("P5_009N")] = None
    rows[1][header.index("P5_010N")] = "-"
    parsed = parse_slice(
        PL, slice_=DecennialSlice("us", "1"), payload=json.dumps(rows).encode()
    )
    by_variable = {c.variable_id: c for c in parsed.counts}
    assert by_variable["P5_009N"].value is None
    assert by_variable["P5_009N"].value_status == "missing"
    assert by_variable["P5_009N"].value_source is None
    assert by_variable["P5_010N"].value_status == "missing"
    assert by_variable["P5_010N"].value_source == "-"


def test_a_row_outside_its_slice_or_with_an_unreadable_code_is_quarantined() -> None:
    """Covers: ETL-129 -- A row outside its slice, or with an unreadable code, is set aside alone."""
    rows = json.loads(_fixture("pl_tract_55.json"))
    header = rows[0]
    rows[1][header.index("state")] = "17"
    rows[2][header.index("tract")] = "12"
    parsed = parse_slice(
        PL, slice_=DecennialSlice("tract", "55"), payload=json.dumps(rows).encode()
    )
    assert [(q.source_row_index, q.error_code) for q in parsed.quarantined] == [
        (1, "unreadable_geography"),
        (2, "unreadable_geography"),
    ]
    assert parsed.row_count == 125
    assert len(parsed.counts) == (125 - 2) * len(PL.variables)

    refused = parse_slice(PL, slice_=DecennialSlice("us", "1"), payload=b"not json")
    assert [(q.source_row_index, q.error_code) for q in refused.quarantined] == [
        (0, "invalid_json")
    ]
    assert parse_slice(PL, slice_=DecennialSlice("us", "1"), payload=b"").counts == ()


def test_a_run_asks_only_for_slices_never_answered() -> None:
    """Covers: ETL-129 -- A first run reads every slice; a later run only those never answered."""
    registered = slices()
    answered = set(registered[:3])
    assert plan.slices_to_capture(None, answered) == list(registered[3:])
    assert plan.slices_to_capture(None, set(registered)) == []
    assert plan.slices_to_capture({"history": True}, set(registered)) == list(
        registered
    )
    with_block = slices(("55025",))
    assert plan.slices_to_capture(None, set(registered), with_block) == [
        DecennialSlice("block", "55025")
    ]
    assert "control.census_decennial_slice" in plan.ANSWERED_SLICES_SQL
