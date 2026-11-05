"""Offline contract: a read that repeats the last publication stores nothing twice.

County Business Patterns, IRS migration, SAIPE/SAHIE, QCEW, building permits
and PEP read the same files again on every scheduled run. Their silver rows
are keyed by capture, so a replay of identical bytes stored a second copy of
every row (the pattern PR #296 removed from BEA).

Covers: ETL-115
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest

from data_ingestion_toolbox.bls_qcew import capture as qcew_capture
from data_ingestion_toolbox.bls_qcew.registry import TOTAL
from data_ingestion_toolbox.capture import CaptureReceipt
from data_ingestion_toolbox.census_bps import capture as bps_capture
from data_ingestion_toolbox.census_bps.registry import slices_for
from data_ingestion_toolbox.census_cbp import capture as cbp_capture
from data_ingestion_toolbox.census_cbp.registry import get_file as get_cbp_file
from data_ingestion_toolbox.census_cbp.silver_census_cbp.load import (
    replay_run as replay_cbp,
)
from data_ingestion_toolbox.census_pep import ingest as pep_ingest
from data_ingestion_toolbox.census_saipe_sahie import capture as sae_capture
from data_ingestion_toolbox.census_saipe_sahie.config import SaeConfig
from data_ingestion_toolbox.census_saipe_sahie.registry import get_dataset
from data_ingestion_toolbox.irs_migration import capture as irs_capture
from data_ingestion_toolbox.irs_migration.registry import registered_files
from data_ingestion_toolbox.irs_migration.silver_irs_migration.load import (
    replay_run as replay_irs,
)
from data_ingestion_toolbox.utility.unchanged import repeats_last_publication

pytestmark = pytest.mark.unit

PUBLISHED_AT = datetime(2026, 10, 15, 13, 5, tzinfo=UTC)
CHECKSUM = "a" * 64


class _Cursor:
    def __init__(self, database: _Database) -> None:
        self.database = database
        self._result: Any = None

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, sql: str, parameters: tuple = ()) -> None:
        text = " ".join(sql.split())
        self.database.statements.append((text, parameters))
        if "FROM silver_ref.dim_geo_entity WHERE created_at" in text:
            self._result = (self.database.geography_since,)
        elif "status = 'published'" in text and "LIMIT 1" in text:
            self._result = self.database.published
        elif "FROM silver_pep.release_load" in text:
            self._result = self.database.published
        elif text.startswith("INSERT INTO control."):
            self.database.control_rows.append(parameters)
            self._result = None
        elif "FROM control.census_cbp_file AS file" in text:
            row = self.database.control_rows[-1]
            self._result = (row[1], row[2], row[3], PUBLISHED_AT, row[4])
        elif "FROM control.irs_migration_file AS file" in text:
            row = self.database.control_rows[-1]
            self._result = (row[1], row[2], row[5], PUBLISHED_AT, row[6])
        else:
            raise AssertionError(f"unexpected statement: {text}")

    def fetchone(self) -> Any:
        return self._result


class _Database:
    def __init__(
        self, published: tuple | None = None, *, geography_since: bool = False
    ) -> None:
        self.published = published
        self.geography_since = geography_since
        self.statements: list[tuple[str, tuple]] = []
        self.control_rows: list[tuple] = []

    def connect(self) -> _Database:
        return self

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class _Control:
    def __init__(self) -> None:
        self.runs: list[str] = []

    def start_run(self, *, watermark: dict[str, Any]) -> UUID:
        return uuid4()

    def start_request(self, **_: Any) -> Any:
        return SimpleNamespace(request_id=uuid4())

    def record_request_retry(self, *_: Any, **__: Any) -> None:
        return None

    def finish_request(self, *_: Any, **__: Any) -> None:
        return None

    def finish_run(self, _run_id: UUID, *, status: str, **__: Any) -> None:
        self.runs.append(status)


def _response(**extra: Any) -> SimpleNamespace:
    return SimpleNamespace(
        http_status=200,
        response_headers={},
        raw_bytes=b"bytes",
        request_parameters={},
        **extra,
    )


def _receipt(_factory: Any, capture: Any) -> CaptureReceipt:
    return CaptureReceipt(capture.capture_id, CHECKSUM)


# --- the shared rule -------------------------------------------------------


@pytest.mark.parametrize(
    ("published", "geography_since", "expected"),
    [
        (None, False, False),
        ((CHECKSUM, "contract:v1", PUBLISHED_AT), False, True),
        (("b" * 64, "contract:v1", PUBLISHED_AT), False, False),
        ((CHECKSUM, "contract:v0", PUBLISHED_AT), False, False),
        ((CHECKSUM, "contract:v1", PUBLISHED_AT), True, False),
    ],
    ids=[
        "never-published",
        "same-bytes",
        "new-bytes",
        "new-parser",
        "geography-added-since",
    ],
)
def test_only_identical_bytes_under_the_same_parser_and_geography_repeat(
    published: tuple | None, geography_since: bool, expected: bool
) -> None:
    """Covers: ETL-115 — never published, new bytes, a new parser or a new geography replay."""
    database = _Database(published, geography_since=geography_since)
    assert (
        repeats_last_publication(
            database.connect,
            "SELECT ... WHERE status = 'published' LIMIT 1",
            ("key",),
            payload_checksum=CHECKSUM,
            parser_contract="contract:v1",
        )
        is expected
    )


# --- whole-file loaders ----------------------------------------------------


def test_a_repeated_county_business_patterns_file_is_unchanged_and_replays_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-115 — CBP's monthly read of every file adds no rows for the same bytes."""
    monkeypatch.setattr(cbp_capture, "fetch_file", lambda *a, **k: _response())
    database = _Database((CHECKSUM, cbp_capture.PARSER_CONTRACT_VERSION, PUBLISHED_AT))
    run_id, _ = cbp_capture.capture_file(
        database.connect,
        get_cbp_file("county", 2023),
        client=object(),
        control=_Control(),
        persist_capture=_receipt,
    )
    assert database.control_rows[-1][-1] == "unchanged"
    assert ("county", 2023) in [p for _s, p in database.statements]
    assert replay_cbp(database.connect, run_id=run_id) == 0


def test_a_new_county_business_patterns_file_is_captured_for_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-115 — a corrected CBP file is replayed beside the one it corrects."""
    monkeypatch.setattr(cbp_capture, "fetch_file", lambda *a, **k: _response())
    database = _Database(("b" * 64, cbp_capture.PARSER_CONTRACT_VERSION, PUBLISHED_AT))
    cbp_capture.capture_file(
        database.connect,
        get_cbp_file("county", 2023),
        client=object(),
        control=_Control(),
        persist_capture=_receipt,
    )
    assert database.control_rows[-1][-1] == "captured"


def test_a_repeated_migration_file_is_unchanged_and_replays_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-115 — an SOI migration file read again adds no flows."""
    monkeypatch.setattr(irs_capture, "fetch_file", lambda *a, **k: _response())
    item = registered_files()[-1]
    database = _Database((CHECKSUM, irs_capture.PARSER_CONTRACT_VERSION, PUBLISHED_AT))
    run_id, _ = irs_capture.capture_file(
        database.connect,
        item,
        client=object(),
        control=_Control(),
        persist_capture=_receipt,
    )
    assert database.control_rows[-1][-1] == "unchanged"
    assert (item.direction, item.year_pair) in [p for _s, p in database.statements]
    assert replay_irs(database.connect, run_id=run_id) == 0


# --- revision-window loaders -----------------------------------------------


def test_a_repeated_qcew_slice_is_unchanged_and_an_unpublished_one_stays_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-115 — the six-quarter QCEW window re-reads without re-storing."""
    monkeypatch.setattr(
        qcew_capture, "fetch_slice", lambda *a, **k: _response(published=True)
    )
    database = _Database((CHECKSUM, qcew_capture.PARSER_CONTRACT_VERSION, PUBLISHED_AT))
    _run, slices = qcew_capture.capture_period(
        database.connect,
        2025,
        "1",
        industries=(TOTAL,),
        client=object(),
        control=_Control(),
        persist_capture=_receipt,
        sleep=lambda _s: None,
    )
    assert [qcew_capture.slice_status(s) for s in slices] == ["unchanged"]
    assert database.control_rows[-1][-1] == "unchanged"
    empty = qcew_capture.CapturedSlice(uuid4(), 2026, "2", "10", uuid4(), False)
    assert qcew_capture.slice_status(empty) == "empty"


def test_a_repeated_permits_file_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: ETL-115 — the six-month permits window re-reads without re-storing."""
    monkeypatch.setattr(
        bps_capture, "fetch_file", lambda *a, **k: _response(published=True)
    )
    database = _Database((CHECKSUM, bps_capture.PARSER_CONTRACT_VERSION, PUBLISHED_AT))
    item = slices_for("monthly", 2026, 6)[0]
    _run, files = bps_capture.capture_period(
        database.connect,
        "monthly",
        2026,
        6,
        files=(item,),
        client=object(),
        control=_Control(),
        persist_capture=_receipt,
        sleep=lambda _s: None,
    )
    assert [bps_capture.file_status(f) for f in files] == ["unchanged"]
    assert (item.slice_key, item.frequency, 2026, 6) in [
        p for _s, p in database.statements
    ]


def test_a_repeated_saipe_slice_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: ETL-115 — the two newest SAIPE/SAHIE years re-read without re-storing."""
    monkeypatch.setattr(
        sae_capture, "fetch_slice", lambda *a, **k: _response(published=True)
    )
    dataset = get_dataset("saipe")
    database = _Database((CHECKSUM, dataset.parser_contract_version, PUBLISHED_AT))
    _run, slices = sae_capture.capture_dataset_year(
        database.connect,
        dataset,
        2024,
        config=SaeConfig(census_api_key="test", min_spacing_seconds=0, max_attempts=1),
        client=object(),
        control=_Control(),
        persist_capture=_receipt,
    )
    assert slices and {sae_capture.slice_status(s) for s in slices} == {"unchanged"}


# --- PEP --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("previous", "expected"),
    [
        (None, False),
        ((CHECKSUM, "v"), True),
        (("b" * 64, "v"), False),
        ((CHECKSUM, "w"), False),
    ],
    ids=["first-load", "same-bytes", "new-bytes", "new-parser"],
)
def test_a_pep_release_read_again_with_the_same_bytes_is_not_replayed(
    previous: tuple | None, expected: bool
) -> None:
    """Covers: ETL-115 — PEP's monthly read of each current release adds nothing unchanged."""
    database = _Database(previous)
    release = SimpleNamespace(
        dataset_code="county", vintage_year=2025, schema_version="v"
    )
    receipt = CaptureReceipt(uuid4(), CHECKSUM)
    assert (
        pep_ingest._repeats_loaded_release(database.connect, release, receipt)
        is expected
    )
