"""Offline, checksum-backed replay of a complete FBI UCR release."""

from __future__ import annotations

import hashlib
from uuid import UUID, uuid4

import pytest

from data_ingestion_toolbox.fbi_ucr.registry import (
    SUMMARIZED_VIOLENT_CRIME,
    agency_directory_endpoint,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.replay import (
    CapturedSlice,
    FbiReplayError,
    load_captured_slices,
    replay_slices,
    slice_input_count,
)

from .conftest import load_bytes

pytestmark = pytest.mark.unit

PRODUCT = SUMMARIZED_VIOLENT_CRIME
RELEASE = "2026-08-15"

#: Reviewed capture fixture per registered endpoint.
FIXTURE_BY_ENDPOINT = {
    agency_directory_endpoint("WI"): "agency_directory_WI",
    "/summarized/national/V": "summarized_national_V",
    "/summarized/state/WI/V": "summarized_state_WI_V",
    **{
        f"/summarized/agency/{ori}/V": f"summarized_agency_{ori}_V"
        for ori in PRODUCT.agency_scope
    },
}


def _slice(endpoint: str, name: str) -> CapturedSlice:
    payload = load_bytes(name)
    return CapturedSlice(
        uuid4(), endpoint, payload, hashlib.sha256(payload).hexdigest()
    )


def _slices() -> dict[str, CapturedSlice]:
    return {
        endpoint: _slice(endpoint, name)
        for endpoint, name in FIXTURE_BY_ENDPOINT.items()
    }


def test_complete_release_replays_without_network_access() -> None:
    """Covers: ETL-040 — a full release rebuilds from stored bytes alone."""
    result = replay_slices(PRODUCT, _slices(), release_key=RELEASE)

    subjects = len(PRODUCT.subjects)
    assert len(result.observations) + len(result.participation) == subjects * (
        slice_input_count(PRODUCT)
    )
    assert len(result.agencies) == len(PRODUCT.agency_scope)
    assert not result.quarantined
    assert result.input_count == (
        len(result.observations)
        + len(result.participation)
        + len(result.agencies)
        + len(result.quarantined)
    )


def test_every_replayed_row_carries_its_capture_lineage() -> None:
    """Covers: ETL-040 — each silver row points at the bytes it came from."""
    slices = _slices()
    result = replay_slices(PRODUCT, slices, release_key=RELEASE)

    national = slices["/summarized/national/V"].capture_id
    national_rows = [
        item for item in result.observations if item.subject_type == "national"
    ]

    assert national_rows
    assert {item.capture_id for item in national_rows} == {national}
    assert all(item.capture_id is not None for item in result.agencies)


@pytest.mark.parametrize(
    "endpoint",
    ["/agency/byStateAbbr/WI", "/summarized/state/WI/V", "/summarized/agency/WI0130000/V"],
)
def test_missing_required_slice_blocks_the_release(endpoint: str) -> None:
    """Covers: ETL-040 — an incomplete release cannot replay at all."""
    slices = _slices()
    del slices[endpoint]

    with pytest.raises(FbiReplayError, match="missing required capture slices"):
        replay_slices(PRODUCT, slices, release_key=RELEASE)


def test_agency_observation_without_its_reference_slice_is_quarantined() -> None:
    """Covers: ETL-024 — an agency slice cannot publish without its reference."""
    slices = _slices()
    slices[agency_directory_endpoint("WI")] = _slice(
        agency_directory_endpoint("WI"), "provider_error_body"
    )

    result = replay_slices(PRODUCT, slices, release_key=RELEASE)

    agency_codes = {
        item.slice_key
        for item in result.quarantined
        if item.error_code == "agency_reference_missing"
    }

    assert not result.agencies
    assert agency_codes == {f"agency:{ori}" for ori in PRODUCT.agency_scope}
    assert not [item for item in result.observations if item.subject_type == "agency"]
    assert result.input_count == (
        len(result.observations)
        + len(result.participation)
        + len(result.agencies)
        + len(result.quarantined)
    )


def test_changed_bytes_fail_the_checksum_before_parsing() -> None:
    """Covers: ETL-040 — replay verifies the checksum before it parses."""
    slices = _slices()
    original = slices["/summarized/national/V"]
    slices["/summarized/national/V"] = CapturedSlice(
        original.capture_id,
        original.endpoint,
        original.payload + b" ",
        original.payload_checksum,
    )

    with pytest.raises(FbiReplayError, match="checksum mismatch"):
        replay_slices(PRODUCT, slices, release_key=RELEASE)


def test_invalid_capture_bytes_fail_deterministically() -> None:
    """Covers: RES-002 — undecodable capture bytes raise a typed error."""
    payload = b"{not json"
    slices = _slices()
    slices["/summarized/national/V"] = CapturedSlice(
        uuid4(),
        "/summarized/national/V",
        payload,
        hashlib.sha256(payload).hexdigest(),
    )

    with pytest.raises(FbiReplayError, match="not valid JSON"):
        replay_slices(PRODUCT, slices, release_key=RELEASE)


def test_replay_is_deterministic_across_repeated_runs() -> None:
    """Covers: DB-006 — replaying the same capture set produces the same rows."""
    slices = _slices()

    first = replay_slices(PRODUCT, slices, release_key=RELEASE)
    second = replay_slices(PRODUCT, slices, release_key=RELEASE)

    assert [item.source_record_id for item in first.observations] == [
        item.source_record_id for item in second.observations
    ]
    assert len({item.source_record_id for item in first.observations}) == len(
        first.observations
    )


class _Cursor:
    def __init__(self, rows: list[tuple]) -> None:
        self.rows = rows
        self.statement = ""
        self.parameters: object = None

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: str, parameters: object = None) -> None:
        self.statement = statement
        self.parameters = parameters

    def fetchall(self) -> list[tuple]:
        return self.rows


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> _Cursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


def test_the_newest_capture_wins_while_earlier_bytes_stay_stored() -> None:
    """Covers: DB-022 — a revised response is retained as its own capture."""
    newest = UUID(int=2)
    cursor = _Cursor(
        [
            (newest, "/summarized/national/V", b"{}", hashlib.sha256(b"{}").hexdigest()),
            (
                UUID(int=1),
                "/summarized/national/V",
                b"{ }",
                hashlib.sha256(b"{ }").hexdigest(),
            ),
        ]
    )
    connection = _Connection(cursor)

    slices = load_captured_slices(lambda: connection, run_id=UUID(int=9))

    assert set(slices) == {"/summarized/national/V"}
    assert slices["/summarized/national/V"].capture_id == newest
    assert "retrieved_at DESC" in cursor.statement
    assert connection.closed
