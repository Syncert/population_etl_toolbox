"""Pure offline replay contracts for CDC source captures."""

from __future__ import annotations

import hashlib
from pathlib import Path
from uuid import UUID

import pytest

from data_ingestion_toolbox.cdc.registry import CDI_ASSET, PLACES_COUNTY_ASSET
from data_ingestion_toolbox.cdc.silver_cdc.replay import (
    CapturedPage,
    CdcReplayError,
    replay_pages,
)

pytestmark = pytest.mark.unit

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "cdc"


def _page(name: str, *, limit: int = 100) -> CapturedPage:
    payload = (FIXTURE_DIR / name).read_bytes()
    return CapturedPage(
        capture_id=UUID("00000000-0000-0000-0000-000000000101"),
        offset=0,
        limit=limit,
        row_count=3 if name.startswith("cdi") else 4,
        payload=payload,
        payload_checksum=hashlib.sha256(payload).hexdigest(),
    )


def test_cdi_replay_preserves_values_strata_missing_and_geography() -> None:
    """Covers: ETL-004, ETL-006 — CDI fixture rows reconcile losslessly."""
    result = replay_pages(
        CDI_ASSET,
        [_page("cdi_observations.json")],
        release_watermark="1780605223",
    )

    assert result.input_count == 3
    assert len(result.observations) == 3
    assert result.quarantined == ()
    assert result.observations[0].geo_id == "us:1"
    assert str(result.observations[0].value) == "22.1"
    assert result.observations[1].geo_id == "state:01"
    assert result.observations[2].value is None
    assert result.observations[2].value_status == "missing"
    assert result.observations[2].footnote_code == "*"


def test_places_replay_keeps_modeled_population_and_suppression() -> None:
    """Covers: ETL-004, ETL-006 — PLACES county semantics remain explicit."""
    result = replay_pages(
        PLACES_COUNTY_ASSET,
        [_page("places_county_observations.json")],
        release_watermark="1764844506",
    )

    assert result.input_count == 4
    assert len(result.observations) == 4
    county = result.observations[0]
    assert county.geo_id == "state:01|county:001"
    assert county.estimate_method == "model_based_small_area_estimate"
    assert str(county.population_18_plus) == "46253"
    assert result.observations[2].geo_id == "us:1"
    assert result.observations[3].value_status == "suppressed"
    assert result.observations[3].value is None


def test_incomplete_or_checksum_changed_page_sequence_cannot_replay() -> None:
    """Covers: RES-002 — partial or corrupted CDC capture sets do not publish."""
    full_final = _page("cdi_observations.json", limit=3)
    with pytest.raises(CdcReplayError, match="terminating short page"):
        replay_pages(CDI_ASSET, [full_final], release_watermark="1")

    corrupt = full_final.__class__(
        **{**full_final.__dict__, "limit": 100, "payload_checksum": "0" * 64}
    )
    with pytest.raises(CdcReplayError, match="checksum"):
        replay_pages(CDI_ASSET, [corrupt], release_watermark="1")


def test_invalid_confidence_interval_is_quarantined_and_reconciled() -> None:
    """Covers: RES-002 — invalid CDC confidence bounds are explicit outcomes."""
    payload = (
        (FIXTURE_DIR / "cdi_observations.json")
        .read_bytes()
        .replace(b'"lowconfidencelimit": "20.5"', b'"lowconfidencelimit": "25.0"', 1)
    )
    page = CapturedPage(
        capture_id=UUID("00000000-0000-0000-0000-000000000102"),
        offset=0,
        limit=100,
        row_count=3,
        payload=payload,
        payload_checksum=hashlib.sha256(payload).hexdigest(),
    )

    result = replay_pages(CDI_ASSET, [page], release_watermark="1")

    assert result.input_count == len(result.observations) + len(result.quarantined)
    assert len(result.quarantined) == 1
    assert result.quarantined[0].error_code == "invalid_confidence_interval"
