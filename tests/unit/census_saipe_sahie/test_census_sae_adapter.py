"""Offline contracts for the Census SAIPE and SAHIE adapter.

Covers: ETL-054
"""

from __future__ import annotations

import importlib
import json
import re
import sys
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.census_saipe_sahie.client import (
    SaeFetchError,
    SaePayloadError,
    fetch_slice,
    validate_payload,
)
from data_ingestion_toolbox.census_saipe_sahie.config import SaeConfig
from data_ingestion_toolbox.census_saipe_sahie.registry import (
    DATASETS,
    GEO_LEVELS,
    SAHIE,
    SAIPE,
    get_dataset,
)
from data_ingestion_toolbox.census_saipe_sahie.silver_census_sae.parse import (
    parse_slice,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "census_saipe_sahie"
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
        request=httpx.Request("GET", "https://api.census.gov/data/timeseries"),
    )


def _config(**overrides: object) -> SaeConfig:
    values: dict[str, object] = {
        "census_api_key": KEY,
        "min_spacing_seconds": 0,
        "max_attempts": 3,
    }
    values.update(overrides)
    return SaeConfig(**values)


def test_configuration_and_registry_import_without_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-054 -- importing reads no key and opens no connection."""
    monkeypatch.delenv("CENSUS_API_KEY", raising=False)
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.census_saipe_sahie"):
            monkeypatch.delitem(sys.modules, name)
    module = importlib.import_module("data_ingestion_toolbox.census_saipe_sahie.config")
    config = module.SaeConfig.from_environment()
    with pytest.raises(ValueError, match="CENSUS_API_KEY"):
        config.require_api_key()


def test_registered_parameters_are_credential_free_and_fixed() -> None:
    """Covers: ETL-054 -- the captured parameters never carry the key."""
    parameters = SAHIE.request_parameters(year=2023, geo_level="county")
    assert parameters["for"] == "county:*"
    assert parameters["time"] == "2023"
    assert {parameters[name] for name in ("AGECAT", "IPRCAT", "SEXCAT", "RACECAT")} == {
        "0"
    }
    assert "key" not in parameters
    assert SAIPE.request_parameters(year=1989, geo_level="us")["get"].startswith(
        "NAME,SAEPOVRTALL_PT,"
    )
    with pytest.raises(ValueError, match="does not register year"):
        SAHIE.request_parameters(year=2005, geo_level="state")
    with pytest.raises(ValueError, match="unregistered geography"):
        SAIPE.request_parameters(year=2023, geo_level="tract")
    assert get_dataset("saipe") is SAIPE
    with pytest.raises(KeyError):
        get_dataset("acs")
    assert GEO_LEVELS == ("us", "state", "county")


def test_metric_identity_is_distinct_from_acs_variables() -> None:
    """Covers: ETL-054 -- a SAIPE/SAHIE metric key cannot collide with an ACS variable."""
    acs_variable = re.compile(r"^[A-Z]{1,2}\d{5}[A-Z]?_\d{3}[EM]?$")
    keys = {
        f"{dataset.dataset_id}:{measure.measure_id}"
        for dataset in DATASETS
        for measure in dataset.measures
    }
    assert len(keys) == sum(len(dataset.measures) for dataset in DATASETS)
    for key in keys:
        dataset_id, measure_id = key.split(":")
        assert dataset_id in {"saipe", "sahie"}
        assert not acs_variable.match(measure_id) and not acs_variable.match(key)


def test_key_rides_only_on_the_request_and_never_in_the_slice_or_error() -> None:
    """Covers: ETL-054 -- the key is on the wire only."""
    client = _ScriptedClient([_response(200, _fixture("saipe_2023_county.json"))])
    captured = fetch_slice(
        SAIPE, year=2023, geo_level="county", config=_config(), client=client
    )
    assert client.calls[0][1]["key"] == KEY
    assert "key" not in captured.request_parameters
    assert KEY.encode() not in captured.raw_bytes
    assert "set-cookie" not in {name.lower() for name in captured.response_headers}
    assert captured.published

    leaking = httpx.ConnectError(f"failed https://api.census.gov/data?key={KEY}")
    client = _ScriptedClient([leaking, leaking, leaking])
    with pytest.raises(SaeFetchError) as raised:
        fetch_slice(
            SAIPE,
            year=2023,
            geo_level="state",
            config=_config(),
            client=client,
            sleep=lambda _: None,
        )
    assert KEY not in str(raised.value)
    assert raised.value.code == "retry_exhausted"


def test_unpublished_slice_is_empty_and_a_client_error_is_not_retried() -> None:
    """Covers: ETL-054 -- 204 is an empty slice, 400 fails once."""
    client = _ScriptedClient([_response(204)])
    empty = fetch_slice(
        SAIPE, year=1990, geo_level="county", config=_config(), client=client
    )
    assert empty.raw_bytes == b"" and not empty.published

    client = _ScriptedClient([_response(400, b"error: unknown variable")])
    with pytest.raises(SaeFetchError) as raised:
        fetch_slice(
            SAIPE, year=2023, geo_level="state", config=_config(), client=client
        )
    assert raised.value.code == "non_retryable_http" and len(client.calls) == 1

    retries: list[BaseException] = []
    client = _ScriptedClient(
        [_response(503), _response(200, _fixture("sahie_2023_us.json"))]
    )
    fetch_slice(
        SAHIE,
        year=2023,
        geo_level="us",
        config=_config(),
        client=client,
        on_retry=retries.append,
        sleep=lambda _: None,
    )
    assert len(retries) == 1 and len(client.calls) == 2


def test_validate_payload_refuses_a_changed_shape() -> None:
    """Covers: ETL-054 -- a header or row shape change is a payload error."""
    header = SAIPE.get_variables()
    with pytest.raises(SaePayloadError, match="invalid_json"):
        validate_payload(b"<html>", SAIPE.api_path, header)
    with pytest.raises(SaePayloadError, match="missing_columns"):
        validate_payload(b'[["NAME","time"]]', SAIPE.api_path, header)
    rows = json.loads(_fixture("saipe_2023_state.json"))
    rows[1] = rows[1][:-1]
    with pytest.raises(SaePayloadError, match="ragged_rows"):
        validate_payload(json.dumps(rows).encode(), SAIPE.api_path, header)


@pytest.mark.parametrize(
    ("dataset", "name", "geo_level", "rows"),
    [
        (SAIPE, "saipe_2023_us.json", "us", 1),
        (SAIPE, "saipe_2023_state.json", "state", 51),
        (SAIPE, "saipe_2023_county.json", "county", 3),
        (SAHIE, "sahie_2023_us.json", "us", 1),
        (SAHIE, "sahie_2023_state.json", "state", 51),
        (SAHIE, "sahie_2023_county.json", "county", 3),
    ],
)
def test_fixtures_parse_with_bounds_and_exact_geography(
    dataset, name, geo_level, rows
) -> None:
    """Covers: ETL-054 -- every fixture row yields one estimate per measure with its bounds."""
    parsed = parse_slice(
        dataset, geo_level=geo_level, estimate_year=2023, payload=_fixture(name)
    )
    assert parsed.quarantined == ()
    assert parsed.row_count == rows
    assert len(parsed.estimates) == rows * len(dataset.measures)
    for estimate in parsed.estimates:
        assert estimate.value_status == "valid"
        assert (
            estimate.confidence_lower is not None
            and estimate.confidence_upper is not None
        )
        assert estimate.confidence_lower <= estimate.value <= estimate.confidence_upper
        assert estimate.margin_of_error is not None
    geo_ids = {estimate.geo_id for estimate in parsed.estimates}
    if geo_level == "us":
        assert geo_ids == {"us:1"}
    elif geo_level == "county":
        assert geo_ids == {
            "state:10|county:001",
            "state:10|county:003",
            "state:10|county:005",
        }
    else:
        assert "state:10" in geo_ids and len(geo_ids) == 51
    # Deterministic identity: a replay of the same bytes produces the same records.
    again = parse_slice(
        dataset, geo_level=geo_level, estimate_year=2023, payload=_fixture(name)
    )
    assert [e.source_record_id for e in again.estimates] == [
        e.source_record_id for e in parsed.estimates
    ]


def test_non_numeric_estimate_is_missing_never_zero() -> None:
    """Covers: ETL-054 -- a suppressed or blank estimate keeps the provider text."""
    rows = json.loads(_fixture("saipe_2023_county.json"))
    header = rows[0]
    rows[1][header.index("SAEMHI_PT")] = None
    rows[1][header.index("SAEPOVALL_PT")] = "N/A"
    parsed = parse_slice(
        SAIPE, geo_level="county", estimate_year=2023, payload=json.dumps(rows).encode()
    )
    first_row = {e.measure_id: e for e in parsed.estimates if e.source_row_index == 1}
    assert (
        first_row["SAEMHI"].value is None
        and first_row["SAEMHI"].value_status == "missing"
    )
    assert (
        first_row["SAEPOVALL"].value is None
        and first_row["SAEPOVALL"].value_source == "N/A"
    )
    assert first_row["SAEPOVALL"].confidence_lower is None
    assert first_row["SAEPOVRTALL"].value == Decimal(
        str(rows[1][header.index("SAEPOVRTALL_PT")])
    )


def test_malformed_rows_and_payloads_are_quarantined_with_reasons() -> None:
    """Covers: ETL-054 -- a row outside the registered scope is set aside, not loaded."""
    rows = json.loads(_fixture("sahie_2023_county.json"))
    header = rows[0]
    rows[1][header.index("time")] = "2022"
    rows[2][header.index("IPRCAT")] = "3"
    rows[3][header.index("county")] = "1"
    parsed = parse_slice(
        SAHIE, geo_level="county", estimate_year=2023, payload=json.dumps(rows).encode()
    )
    assert [
        (item.source_row_index, item.error_code) for item in parsed.quarantined
    ] == [
        (1, "unexpected_year"),
        (2, "category_mismatch"),
        (3, "unreadable_geography"),
    ]
    assert parsed.estimates == () and parsed.row_count == 3

    rejected = parse_slice(
        SAHIE, geo_level="state", estimate_year=2023, payload=b'{"error": "x"}'
    )
    assert [
        (item.source_row_index, item.error_code) for item in rejected.quarantined
    ] == [(0, "expected_json_array_of_rows")]
    assert parse_slice(
        SAIPE, geo_level="county", estimate_year=1990, payload=b""
    ) == parse_slice(SAIPE, geo_level="county", estimate_year=1990, payload=b"")
    assert (
        parse_slice(
            SAIPE, geo_level="county", estimate_year=1990, payload=b""
        ).row_count
        == 0
    )
