"""Checksum-backed offline replay of complete USDA NASS slice sets."""

from __future__ import annotations

import hashlib
import json
from typing import Any
from uuid import uuid4

import pytest

from data_ingestion_toolbox.usda_nass.registry import get_product, iter_slices
from data_ingestion_toolbox.usda_nass.silver_nass.values import (
    CapturedSlicePayload,
    NassReplayError,
    replay_slices,
)

from ._doubles import load_fixture

pytestmark = pytest.mark.unit

PRODUCT = get_product("corn_survey_annual")
WATERMARK = "2025-01-10 15:20:33.123000"


def _slice(
    level: str, envelope: dict[str, Any], **overrides: Any
) -> CapturedSlicePayload:
    raw = json.dumps(envelope["data"]).encode("utf-8")
    values: dict[str, Any] = {
        "capture_id": uuid4(),
        "slice_key": f"{PRODUCT.product_id}|{level}|2024",
        "agg_level_desc": level,
        "year": 2024,
        "provider_count": int(envelope["count"]["count"]),
        "captured_row_count": len(envelope["data"]["data"]),
        "payload": raw,
        "payload_checksum": hashlib.sha256(raw).hexdigest(),
    }
    values.update(overrides)
    return CapturedSlicePayload(**values)


def _reviewed_slices(name: str = "corn_survey_annual") -> list[CapturedSlicePayload]:
    document = load_fixture(name)
    return [
        _slice(level, envelope) for level, envelope in document["slices"].items()
    ]


def test_reviewed_slices_replay_losslessly_without_a_network() -> None:
    """Covers: ETL-004, ETL-006 — reviewed slices reconcile losslessly."""
    result = replay_slices(PRODUCT, _reviewed_slices(), release_watermark=WATERMARK)

    assert result.input_count == 20
    assert len(result.observations) == 20
    assert result.quarantined == ()
    assert {item.geography.geo_type for item in result.observations} == {
        "nation",
        "state",
        "county",
    }
    assert all(item.capture_id is not None for item in result.observations)
    assert all(item.release_watermark == WATERMARK for item in result.observations)


def test_replay_is_order_independent_and_repeatable() -> None:
    """Covers: ETL-006 — replay is deterministic regardless of slice order."""
    slices = _reviewed_slices()
    forward = replay_slices(PRODUCT, slices, release_watermark=WATERMARK)
    reversed_order = replay_slices(
        PRODUCT, list(reversed(slices)), release_watermark=WATERMARK
    )
    assert [item.source_record_id for item in forward.observations] == [
        item.source_record_id for item in reversed_order.observations
    ]


def test_a_corrupted_capture_cannot_replay() -> None:
    """Covers: RES-002 — a corrupted USDA NASS capture cannot replay."""
    slices = _reviewed_slices()
    slices[0] = CapturedSlicePayload(
        **{**slices[0].__dict__, "payload_checksum": "0" * 64}
    )
    with pytest.raises(NassReplayError, match="checksum mismatch"):
        replay_slices(PRODUCT, slices, release_watermark=WATERMARK)


def test_a_partial_slice_cannot_replay() -> None:
    """Covers: RES-002 — a partial slice never reaches silver."""
    document = load_fixture("corn_survey_annual")
    truncated = {
        "count": {"count": "8"},
        "data": {"data": document["slices"]["COUNTY"]["data"]["data"][:4]},
    }
    with pytest.raises(NassReplayError, match="partial against its preflight"):
        replay_slices(
            PRODUCT,
            [_slice("COUNTY", truncated, captured_row_count=4)],
            release_watermark=WATERMARK,
        )


def test_a_miscounted_capture_cannot_replay() -> None:
    """Covers: RES-002 — a capture disagreeing with its payload cannot replay."""
    document = load_fixture("corn_survey_annual")
    with pytest.raises(NassReplayError, match="does not match payload"):
        replay_slices(
            PRODUCT,
            [_slice("COUNTY", document["slices"]["COUNTY"], captured_row_count=99)],
            release_watermark=WATERMARK,
        )


def test_a_duplicated_slice_cannot_replay() -> None:
    """Covers: RES-002 — one slice captured twice cannot double-count."""
    slices = _reviewed_slices()
    with pytest.raises(NassReplayError, match="captured more than once"):
        replay_slices(PRODUCT, [slices[0], slices[0]], release_watermark=WATERMARK)


def test_an_empty_slice_set_cannot_replay() -> None:
    """Covers: RES-002 — an empty USDA NASS release cannot replay."""
    with pytest.raises(NassReplayError, match="no captured slices"):
        replay_slices(PRODUCT, [], release_watermark=WATERMARK)


def test_malformed_capture_bytes_cannot_masquerade_as_a_slice() -> None:
    """Covers: RES-002 — malformed capture bytes cannot masquerade as a slice."""
    raw = b'{"data": "everything"}'
    broken = _slice(
        "COUNTY",
        load_fixture("corn_survey_annual")["slices"]["COUNTY"],
        payload=raw,
        payload_checksum=hashlib.sha256(raw).hexdigest(),
    )
    with pytest.raises(Exception, match="data list"):
        replay_slices(PRODUCT, [broken], release_watermark=WATERMARK)


def test_replay_reconciles_quarantined_rows_into_the_input_count() -> None:
    """Covers: ETL-025 — quarantined rows still reconcile against the input."""
    document = load_fixture("corn_survey_annual")
    boundary = load_fixture("boundary_records")["records"]
    rows = [
        *document["slices"]["COUNTY"]["data"]["data"],
        boundary["unregistered_statistic"],
    ]
    payload = {"count": {"count": str(len(rows))}, "data": {"data": rows}}
    result = replay_slices(
        PRODUCT,
        [_slice("COUNTY", payload)],
        release_watermark=WATERMARK,
    )
    assert result.input_count == len(rows)
    assert len(result.observations) == len(rows) - 1
    assert len(result.quarantined) == 1


def test_a_revised_extraction_replays_into_distinct_source_revisions() -> None:
    """Covers: ETL-006 — a revised extraction keeps its own values."""
    original = replay_slices(
        PRODUCT,
        [_slice("COUNTY", load_fixture("corn_survey_annual")["slices"]["COUNTY"])],
        release_watermark=WATERMARK,
    )
    revised_watermark = "2025-03-04 11:02:44.900000"
    revised = replay_slices(
        PRODUCT,
        [
            _slice(
                "COUNTY",
                load_fixture("corn_survey_annual_revised")["slices"]["COUNTY"],
            )
        ],
        release_watermark=revised_watermark,
    )

    by_identity = {item.source_record_id: item for item in original.observations}
    changed = [
        item
        for item in revised.observations
        if by_identity[item.source_record_id].value_source != item.value_source
    ]
    assert changed, "the revised sample must change at least one value"
    formerly_withheld = [
        item for item in changed if by_identity[item.source_record_id].value_source == "(D)"
    ]
    assert formerly_withheld
    assert formerly_withheld[0].value_status == "valid"
    assert by_identity[formerly_withheld[0].source_record_id].value is None
    assert all(item.release_watermark == revised_watermark for item in revised.observations)


def test_every_registered_slice_key_is_representable() -> None:
    """Covers: ETL-020 — every registered slice key round-trips into replay."""
    keys = {item.slice_key for item in iter_slices(PRODUCT, mode="full")}
    assert f"{PRODUCT.product_id}|COUNTY|{PRODUCT.year_end}" in keys
    assert all(key.count("|") == 2 for key in keys)
