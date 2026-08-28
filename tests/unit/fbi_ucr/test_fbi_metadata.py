"""FBI CDE release identity, completeness, and revision decisions."""

from __future__ import annotations

import json
from datetime import date

import pytest

from data_ingestion_toolbox.fbi_ucr.metadata import (
    FbiRelease,
    FbiReleaseError,
    ReleaseDecision,
    decide_release,
    parse_release,
)
from data_ingestion_toolbox.fbi_ucr.registry import SUMMARIZED_VIOLENT_CRIME

pytestmark = pytest.mark.unit

PRODUCT = SUMMARIZED_VIOLENT_CRIME


def test_release_identity_comes_from_the_payload_not_the_mutable_path(
    fbi_payload,
) -> None:
    """Covers: ETL-026 — /LATEST is capture input, not a release identity."""
    release = parse_release(fbi_payload("summarized_national_V"))

    assert release.refresh_date == date(2026, 8, 15)
    assert release.release_key == "2026-08-15"
    assert release.max_data_period == "08-2026"


def test_release_parses_from_exact_capture_bytes(fbi_bytes) -> None:
    """Covers: ETL-040 — a release is identified from stored bytes alone."""
    release = parse_release(fbi_bytes("summarized_national_V"))

    assert release.release_key == "2026-08-15"


@pytest.mark.parametrize(
    "properties",
    [
        {},
        {"last_refresh_date": {"UCR": "08/15/2026"}},
        {"max_data_date": {"UCR": "08/2026"}},
        {
            "last_refresh_date": {"UCR": "2026-08-15"},
            "max_data_date": {"UCR": "08/2026"},
        },
        {
            "last_refresh_date": {"UCR": "08/15/2026"},
            "max_data_date": {"UCR": "2026-08"},
        },
        {
            "last_refresh_date": {"NIBRS": "08/15/2026"},
            "max_data_date": {"UCR": "08/2026"},
        },
    ],
)
def test_missing_or_malformed_freshness_fields_are_rejected(
    properties: dict,
) -> None:
    """Covers: RES-002 — freshness fields are validated before use."""
    with pytest.raises(FbiReleaseError):
        parse_release(json.dumps({"cde_properties": properties}).encode("utf-8"))


def test_payload_without_freshness_block_is_rejected() -> None:
    """Covers: RES-002 — a payload with no cde_properties cannot identify."""
    with pytest.raises(FbiReleaseError, match="cde_properties"):
        parse_release(b'{"offenses": {}}')


def test_invalid_json_release_probe_is_rejected() -> None:
    """Covers: RES-002 — malformed release bytes fail deterministically."""
    with pytest.raises(FbiReleaseError, match="valid JSON"):
        parse_release(b"not json")


def _release(refresh: str, latest: str = "08/2026") -> FbiRelease:
    month, day, year = refresh.split("/")
    return FbiRelease(date(int(year), int(month), int(day)), latest)


def test_first_release_ingests_and_an_identical_refresh_is_unchanged() -> None:
    """Covers: ETL-026 — an unchanged refresh does not re-ingest."""
    current = _release("08/15/2026")

    assert decide_release(PRODUCT, current, None) is ReleaseDecision.INGEST
    assert decide_release(PRODUCT, current, current) is ReleaseDecision.UNCHANGED


def test_newer_refresh_ingests_as_a_retained_revision() -> None:
    """Covers: DB-022 — a later refresh is a new release, not an overwrite."""
    decision = decide_release(PRODUCT, _release("09/15/2026"), _release("08/15/2026"))

    assert decision is ReleaseDecision.INGEST


def test_backward_refresh_is_quarantined() -> None:
    """Covers: ETL-026 — a regressed refresh date never replaces history."""
    decision = decide_release(PRODUCT, _release("07/15/2026"), _release("08/15/2026"))

    assert decision is ReleaseDecision.BACKWARD_REFRESH_QUARANTINE


def test_unpublished_period_window_is_quarantined() -> None:
    """Covers: ETL-029 — an incomplete window cannot look non-reporting."""
    decision = decide_release(PRODUCT, _release("08/15/2026", "03/2023"), None)

    assert decision is ReleaseDecision.PERIOD_UNAVAILABLE_QUARANTINE


def test_period_window_ending_exactly_at_the_latest_month_is_ingestible() -> None:
    """Covers: ETL-029 — the boundary month counts as published."""
    decision = decide_release(PRODUCT, _release("08/15/2026", "06/2023"), None)

    assert decision is ReleaseDecision.INGEST


def test_absent_release_is_quarantined_rather_than_assumed() -> None:
    """Covers: RES-002 — an unidentifiable release never publishes."""
    assert (
        decide_release(PRODUCT, None, None)
        is ReleaseDecision.MISSING_RELEASE_QUARANTINE
    )
