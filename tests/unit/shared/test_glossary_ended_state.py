"""A key its provider still lists but stopped publishing is `ended`.

Covers: ARC-008 — on the internal stack on 2026-11-06 the Midwest whole-milk
Average Price (BLS:APU0200709112) was `current` in the catalog with its newest
price in April 2020: BLS still lists the series, so the key never left the
publisher, and `current` only ever meant "still emitted". The harvest now
reads the publisher's optional `last_value_date` and publishes `ended` once
that date is older than the key's time grain allows.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from data_ingestion_toolbox.glossary import harvest

from tests.unit.shared.test_glossary_harvest_guard import (
    PUBLICATION,
    GuardConnection,
    _documents,
    _row,
)

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
TODAY = date(2026, 11, 6)


def _document(**overrides: Any) -> dict[str, Any]:
    last_value_date = overrides.pop("last_value_date", None)
    document = _documents(_row(**overrides))[0]
    document["last_value_date"] = last_value_date
    return document


def _state(document: dict[str, Any], **kwargs: Any) -> str:
    return harvest.freshness_of(
        document,
        today=kwargs.get("today", TODAY),
        thresholds=kwargs.get("thresholds", harvest.DEFAULT_ENDED_AFTER_DAYS),
    )


def test_a_monthly_series_last_priced_in_2020_is_ended() -> None:
    """Covers: ARC-008 — the milk series is not current."""
    assert _state(_document(last_value_date="2020-04-30")) == "ended"


def test_a_monthly_series_with_a_recent_value_is_current() -> None:
    """Covers: ARC-008 — a withheld October 2025 does not end a live series."""
    assert _state(_document(last_value_date="2026-09-30")) == "current"
    assert _state(_document(last_value_date=date(2025, 9, 30))) == "current"


def test_no_reported_date_or_an_unthresholded_grain_stays_current() -> None:
    """Covers: ARC-008 — an end is never invented from an unread signal."""
    assert _state(_document()) == "current"
    annual = _document(valid_time_grains=["ANNUAL"], last_value_date="2010-12-31")
    assert _state(annual) == "current"


def test_a_key_publishing_several_grains_takes_the_most_lenient() -> None:
    """Covers: ARC-008 — a weekly-and-quarterly key is judged as quarterly."""
    document = _document(
        valid_time_grains=["WEEKLY", "QUARTERLY"], last_value_date="2025-06-30"
    )
    assert _state(document) == "current"


def test_the_threshold_is_configurable_from_the_environment() -> None:
    """Covers: ARC-008 — an operator can change it without a release."""
    thresholds = harvest.ended_after_days(
        {harvest.ENDED_AFTER_DAYS_ENV: '{"monthly": 30, "weekly": null}'}
    )
    assert thresholds["MONTHLY"] == 30
    assert "WEEKLY" not in thresholds
    assert thresholds["DAILY"] == harvest.DEFAULT_ENDED_AFTER_DAYS["DAILY"]
    assert (
        _state(_document(last_value_date="2026-09-30"), thresholds=thresholds)
        == "ended"
    )
    assert harvest.ended_after_days({}) == harvest.DEFAULT_ENDED_AFTER_DAYS
    for bad in ("[1]", '{"MONTHLY": 0}', '{"MONTHLY": "x"}', '{"MONTHLY": true}'):
        with pytest.raises(ValueError):
            harvest.ended_after_days({harvest.ENDED_AFTER_DAYS_ENV: bad})


def _harvest_rows(rows: list[tuple[Any, ...]], state_row: Any) -> GuardConnection:
    connection = GuardConnection(rows, state_row)
    harvest.harvest_publisher(
        lambda: connection,
        harvest.Publisher("gold_bls"),
        today=TODAY,
        thresholds=harvest.DEFAULT_ENDED_AFTER_DAYS,
    )
    return connection


def test_the_publisher_read_takes_optional_columns_without_requiring_them() -> None:
    """Covers: ARC-008 — a publisher without the column still harvests."""
    connection = _harvest_rows([_row()], None)
    read = connection.cursor_instance.executed[0]
    assert "to_jsonb" in read and "last_value_date" in read


def test_an_upsert_writes_the_computed_state() -> None:
    """Covers: ARC-008 — the upsert no longer hard-codes `current`."""
    milk = _row(source_object_key="APU0200709112") + ("2020-04-30",)
    eggs = _row(source_object_key="APU0200708111") + ("2025-09-30",)
    connection = _harvest_rows([milk, eggs], None)
    upsert = next(
        statement
        for statement in connection.cursor_instance.executed
        if "INSERT INTO gold_glossary.dim_metric_catalog" in statement
    )
    assert "freshness_state = EXCLUDED.freshness_state" in upsert
    assert "freshness_state = 'current'" not in upsert
    assert "'ended'" in upsert and "'current'" in upsert


def test_a_skip_still_moves_keys_between_current_and_ended() -> None:
    """Covers: ARC-008 — age moves with the calendar, not with the publisher.

    Only the rows whose state differs are written, and only rows the
    publisher emits (a `stale` or `retired` key is the retirement count's).
    """
    rows = [_row(source_object_key="APU0200709112") + ("2020-04-30",)]
    fingerprint = harvest.content_fingerprint(
        [dict(zip(harvest.REQUIRED_COLUMNS, row)) for row in rows]
    )
    connection = _harvest_rows(rows, (PUBLICATION, fingerprint))
    executed = connection.cursor_instance.executed
    assert not any(
        "INSERT INTO gold_glossary.dim_metric_catalog" in s for s in executed
    )
    moved = next(s for s in executed if "AS computed" in s)
    assert "catalog.freshness_state IN ('current', 'ended')" in moved
    assert "catalog.freshness_state <> computed.freshness_state" in moved
    assert "'ended'" in moved
    assert connection.committed


def test_the_catalog_check_and_migration_admit_ended() -> None:
    """Covers: ARC-008 — the state the harvest writes is one the table allows."""
    schema = (REPO_ROOT / "sql/gold_contract/002_gold_glossary_schema.sql").read_text()
    migration = (
        REPO_ROOT / "sql/migrations/042_metric_catalog_ended_state.sql"
    ).read_text()
    assert "('current', 'ended', 'stale', 'retired')" in schema
    assert "('current', 'ended', 'stale', 'retired')" in migration


@pytest.mark.parametrize(
    "relative",
    [
        "src/data_ingestion_toolbox/bls/gold_bls/DDL/publisher.sql",
        "src/data_ingestion_toolbox/fred/gold_fred/DDL/publisher.sql",
        "src/data_ingestion_toolbox/eia/gold_eia/DDL/publisher.sql",
    ],
)
def test_monthly_and_weekly_publishers_report_their_newest_real_value(
    relative: str,
) -> None:
    """Covers: ARC-008 — the date is of a served value, never a withheld row."""
    text = (REPO_ROOT / relative).read_text()
    assert "last_value_date" in text
    assert "FILTER (WHERE latest.value IS NOT NULL)" in text
