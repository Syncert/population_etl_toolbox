"""Pure USDA NASS release identity, preflight, and change decisions."""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from data_ingestion_toolbox.usda_nass.metadata import (
    PUBLISHABLE_DECISIONS,
    NassMetadataError,
    NassReleaseContract,
    NassSliceCount,
    ReleaseDecision,
    decide_preflight,
    decide_release,
    decode_count_payload,
    decode_data_payload,
    field_signature,
    format_watermark,
    parse_load_time,
    summarize_release,
)
from data_ingestion_toolbox.usda_nass.registry import QUICK_STATS_FIELDS, get_product

from ._doubles import deterministic_config, load_fixture

pytestmark = pytest.mark.unit

PRODUCT = get_product("corn_survey_annual")


def _counts(**values: int) -> list[NassSliceCount]:
    return [
        NassSliceCount(f"{PRODUCT.product_id}|{level}|2024", level, 2024, count)
        for level, count in values.items()
    ]


def _payloads() -> list[bytes]:
    document = load_fixture("corn_survey_annual.json")
    return [
        json.dumps(item["data"]).encode("utf-8") for item in document["slices"].values()
    ]


def test_load_time_parsing_never_guesses_a_format() -> None:
    """Covers: ETL-025 — an unparseable load_time yields no watermark."""
    assert parse_load_time("2025-01-10 15:20:33.123") == datetime(
        2025, 1, 10, 15, 20, 33, 123000
    )
    assert parse_load_time("2025-01-10 15:20:33") == datetime(2025, 1, 10, 15, 20, 33)
    assert parse_load_time("10/01/2025") is None
    assert parse_load_time("") is None
    assert parse_load_time(None) is None
    assert parse_load_time(20250110) is None


def test_watermark_formatting_is_stable_and_orderable() -> None:
    """Covers: ETL-026 — watermarks compare in publication order as text."""
    earlier = format_watermark(datetime(2025, 1, 10, 15, 20, 33, 123000))
    later = format_watermark(datetime(2025, 3, 4, 11, 2, 44, 900000))
    assert earlier < later
    assert parse_load_time(earlier) == datetime(2025, 1, 10, 15, 20, 33, 123000)


@pytest.mark.parametrize(
    "payload",
    [b"not json", b"[1]", b'{"rows": []}', b'{"data": [1]}'],
)
def test_malformed_capture_bytes_cannot_become_records(payload: bytes) -> None:
    """Covers: RES-002 — malformed capture bytes cannot become records."""
    with pytest.raises(NassMetadataError):
        decode_data_payload(payload)


@pytest.mark.parametrize(
    "payload", [b"not json", b"[1]", b'{"count": "x"}', b'{"count": -2}']
)
def test_malformed_count_bytes_cannot_become_a_preflight(payload: bytes) -> None:
    """Covers: RES-002 — malformed count bytes cannot become a preflight."""
    with pytest.raises(NassMetadataError):
        decode_count_payload(payload)


def test_field_signature_reports_the_provider_key_union() -> None:
    """Covers: RES-002 — the record field signature is exact and sorted."""
    rows = decode_data_payload(_payloads()[0])
    assert field_signature(rows) == tuple(sorted(QUICK_STATS_FIELDS))
    assert field_signature([{"b": 1}, {"a": 2}]) == ("a", "b")


def test_release_summary_is_built_from_captured_bytes_alone() -> None:
    """Covers: ETL-026 — the release contract derives from captured bytes."""
    counts = _counts(NATIONAL=4, STATE=8, COUNTY=8)
    contract = summarize_release(PRODUCT, payloads=_payloads(), slice_counts=counts)

    assert contract.product_id == PRODUCT.product_id
    assert contract.parser_contract_version == PRODUCT.parser_contract_version
    assert contract.total_row_count == 20
    assert contract.extraction_watermark == "2025-01-10 15:20:33.123000"
    assert contract.release_version == contract.extraction_watermark
    assert contract.field_signature == tuple(sorted(QUICK_STATS_FIELDS))
    assert contract.slice_counts == tuple(
        (item.slice_key, item.provider_count) for item in counts
    )


def test_an_over_limit_preflight_refuses_retrieval_before_any_request() -> None:
    """Covers: RES-002 — an over-limit partition is refused before retrieval."""
    config = deterministic_config(slice_record_limit=100)
    decision = decide_preflight(PRODUCT, config, _counts(COUNTY=101), None)
    assert decision is ReleaseDecision.OVER_LIMIT_QUARANTINE
    assert decision not in PUBLISHABLE_DECISIONS


def test_a_first_release_ingests_and_an_identical_preflight_is_unchanged() -> None:
    """Covers: ETL-026 — unchanged preflight evidence skips retrieval."""
    config = deterministic_config()
    counts = _counts(NATIONAL=4, STATE=8, COUNTY=8)
    assert decide_preflight(PRODUCT, config, counts, None) is ReleaseDecision.INGEST

    previous = summarize_release(PRODUCT, payloads=_payloads(), slice_counts=counts)
    assert (
        decide_preflight(PRODUCT, config, counts, previous) is ReleaseDecision.UNCHANGED
    )
    assert ReleaseDecision.UNCHANGED in PUBLISHABLE_DECISIONS


def test_a_large_row_count_change_is_quarantined_not_absorbed() -> None:
    """Covers: RES-002 — a large row-count change is quarantined."""
    config = deterministic_config(row_count_change_threshold=0.5)
    previous = summarize_release(
        PRODUCT,
        payloads=_payloads(),
        slice_counts=_counts(NATIONAL=4, STATE=8, COUNTY=8),
    )
    modest = decide_preflight(
        PRODUCT, config, _counts(NATIONAL=4, STATE=8, COUNTY=12), previous
    )
    drastic = decide_preflight(
        PRODUCT, config, _counts(NATIONAL=4, STATE=8, COUNTY=60), previous
    )
    assert modest is ReleaseDecision.INGEST
    assert drastic is ReleaseDecision.ROW_COUNT_DRIFT_QUARANTINE


def test_a_new_parser_contract_always_reingests() -> None:
    """Covers: ETL-026 — a parser contract change always re-ingests."""
    config = deterministic_config()
    counts = _counts(NATIONAL=4, STATE=8, COUNTY=8)
    stale = NassReleaseContract(
        PRODUCT.product_id,
        "quickstats-crop-v0",
        "2025-01-10 15:20:33.123000",
        20,
        tuple((item.slice_key, item.provider_count) for item in counts),
        tuple(sorted(QUICK_STATS_FIELDS)),
    )
    assert decide_preflight(PRODUCT, config, counts, stale) is ReleaseDecision.INGEST
    contract = summarize_release(PRODUCT, payloads=_payloads(), slice_counts=counts)
    assert decide_release(PRODUCT, contract, stale) is ReleaseDecision.INGEST


def test_schema_expansion_and_contraction_are_both_quarantined() -> None:
    """Covers: RES-002 — USDA NASS schema drift is typed, never absorbed."""
    counts = _counts(COUNTY=1)
    document = load_fixture("boundary_records.json")
    expanded = json.dumps(document["schema_expansion_payload"]).encode("utf-8")
    contracted = json.dumps(document["missing_consumed_field_payload"]).encode("utf-8")

    for payload in (expanded, contracted):
        contract = summarize_release(PRODUCT, payloads=[payload], slice_counts=counts)
        assert (
            decide_release(PRODUCT, contract, None)
            is ReleaseDecision.SCHEMA_CHANGE_QUARANTINE
        )


def test_a_release_without_a_usable_watermark_is_quarantined() -> None:
    """Covers: RES-002 — a release without a usable watermark cannot publish."""
    rows = decode_data_payload(_payloads()[0])
    for row in rows:
        row["load_time"] = "10/01/2025"
    payload = json.dumps({"data": rows}).encode("utf-8")
    contract = summarize_release(
        PRODUCT, payloads=[payload], slice_counts=_counts(NATIONAL=4)
    )
    assert contract.extraction_watermark == ""
    assert (
        decide_release(PRODUCT, contract, None)
        is ReleaseDecision.INVALID_WATERMARK_QUARANTINE
    )


def test_a_backward_watermark_is_quarantined_and_a_forward_one_ingests() -> None:
    """Covers: RES-002 — a backward provider watermark cannot overwrite state."""
    counts = _counts(COUNTY=8)
    current = summarize_release(
        PRODUCT,
        payloads=[
            json.dumps(
                load_fixture("corn_survey_annual")["slices"]["COUNTY"]["data"]
            ).encode("utf-8")
        ],
        slice_counts=counts,
    )
    newer = NassReleaseContract(
        PRODUCT.product_id,
        PRODUCT.parser_contract_version,
        "2026-01-01 00:00:00.000000",
        8,
        tuple((item.slice_key, item.provider_count) for item in counts),
        tuple(sorted(QUICK_STATS_FIELDS)),
    )
    older = NassReleaseContract(
        PRODUCT.product_id,
        PRODUCT.parser_contract_version,
        "2020-01-01 00:00:00.000000",
        8,
        tuple((item.slice_key, item.provider_count) for item in counts),
        tuple(sorted(QUICK_STATS_FIELDS)),
    )
    assert (
        decide_release(PRODUCT, current, newer)
        is ReleaseDecision.BACKWARD_WATERMARK_QUARANTINE
    )
    assert decide_release(PRODUCT, current, older) is ReleaseDecision.INGEST


def test_an_identical_watermark_and_slice_set_is_unchanged() -> None:
    """Covers: ETL-026 — an identical extraction reports no new release."""
    counts = _counts(NATIONAL=4, STATE=8, COUNTY=8)
    contract = summarize_release(PRODUCT, payloads=_payloads(), slice_counts=counts)
    assert decide_release(PRODUCT, contract, contract) is ReleaseDecision.UNCHANGED


def test_a_revised_extraction_ingests_beside_the_original() -> None:
    """Covers: ETL-006 — a revised extraction is a new release, not an update."""
    counts = _counts(COUNTY=8)
    original = summarize_release(
        PRODUCT,
        payloads=[
            json.dumps(
                load_fixture("corn_survey_annual")["slices"]["COUNTY"]["data"]
            ).encode("utf-8")
        ],
        slice_counts=counts,
    )
    revised = summarize_release(
        PRODUCT,
        payloads=[
            json.dumps(
                load_fixture("corn_survey_annual_revised")["slices"]["COUNTY"]["data"]
            ).encode("utf-8")
        ],
        slice_counts=counts,
    )
    assert revised.extraction_watermark > original.extraction_watermark
    assert decide_release(PRODUCT, revised, original) is ReleaseDecision.INGEST
