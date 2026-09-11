"""The harvest skip guard, the force path, and retirement on a skip.

Covers: ARC-004 — a glossary harvest must follow what a publisher *says*, not
only when it last said it. The publication-time guard reads a watermark every
publisher derives from its facts, so a change to a metric's identity, display
name, units, grains, lineage, or the set of keys it emits moved nothing the
guard could see: the harvest wrote nothing, recorded success, and left the
catalog serving identities the warehouse no longer published. Recovering
needed an operator to edit ``publisher_harvest_state`` by hand, once per
retirement grace step, and nothing said so.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from data_ingestion_toolbox.glossary import harvest

pytestmark = pytest.mark.unit

PUBLICATION = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)


def _row(**overrides: Any) -> tuple[Any, ...]:
    """One publisher row in ``REQUIRED_COLUMNS`` order."""
    values: dict[str, Any] = {
        "source_code": "BLS",
        "publisher_contract_version": "1.0",
        "source_object_key": "LAU:UNEMP_RATE",
        "source_object_type": "measure",
        "metric_display_name": "Unemployment rate",
        "units": "Percent",
        "measure_kind": "RATE",
        "valid_geo_grains": ["COUNTY", "STATE"],
        "valid_time_grains": ["MONTHLY"],
        "aggregation_characteristic": None,
        "physical_lineage": {"schema": "gold_bls", "key": "LAU:UNEMP_RATE"},
        "source_watermark": "2026-09-10 12:00:00+00",
        "source_run_id": None,
        "publication_time": PUBLICATION,
        "source_name": "U.S. Bureau of Labor Statistics",
        "source_type": "official-statistics",
        "reference_url": "https://www.bls.gov/lau/",
    }
    values.update(overrides)
    return tuple(values[column] for column in harvest.REQUIRED_COLUMNS)


def _documents(*rows: tuple[Any, ...]) -> list[dict[str, Any]]:
    return [dict(zip(harvest.REQUIRED_COLUMNS, row)) for row in rows]


class GuardCursor:
    """A cursor that answers the publisher read and the harvest-state read.

    Every other statement is recorded so a test can assert what the harvest
    wrote without modelling the whole catalog.
    """

    def __init__(
        self,
        publisher_rows: list[tuple[Any, ...]],
        state_row: tuple[Any, ...] | None,
    ) -> None:
        self.publisher_rows = publisher_rows
        self.state_row = state_row
        self.executed: list[str] = []
        self._last = ""
        self.rowcount = 0
        # execute_values reaches back through the cursor for the connection's
        # encoding, so the fake has to carry one.
        self.connection: Any = None

    def __enter__(self) -> "GuardCursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, statement: Any, parameters: Any = None) -> None:
        # Composed SQL cannot render without a real connection; its repr still
        # names the identifiers, which is all these assertions read.
        text = statement if isinstance(statement, str) else repr(statement)
        self._last = text
        self.executed.append(text)

    def mogrify(self, template: Any, arguments: Any = None) -> bytes:
        # execute_values builds its VALUES list through mogrify before it
        # executes, so the fake has to answer it.
        return str(arguments).encode("utf-8")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.publisher_rows

    def fetchone(self) -> tuple[Any, ...] | None:
        if "publisher_harvest_state" in self._last:
            return self.state_row
        return None


class GuardConnection:
    encoding = "UTF8"

    def __init__(
        self,
        publisher_rows: list[tuple[Any, ...]],
        state_row: tuple[Any, ...] | None,
    ) -> None:
        self.cursor_instance = GuardCursor(publisher_rows, state_row)
        self.cursor_instance.connection = self
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self) -> GuardCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def _harvest(
    publisher_rows: list[tuple[Any, ...]],
    state_row: tuple[Any, ...] | None,
    **kwargs: Any,
) -> tuple[int, GuardConnection]:
    connection = GuardConnection(publisher_rows, state_row)
    written = harvest.harvest_publisher(
        lambda: connection, harvest.Publisher("gold_bls"), **kwargs
    )
    return written, connection


def _upserted(connection: GuardConnection) -> bool:
    return any(
        "dim_metric_catalog" in statement and "INSERT" in statement
        for statement in connection.cursor_instance.executed
    )


def _advanced_retirement(connection: GuardConnection) -> bool:
    return any(
        "missing_harvest_count" in statement
        for statement in connection.cursor_instance.executed
    )


# --- the fingerprint itself -------------------------------------------------


def test_fingerprint_ignores_the_publisher_view_row_order() -> None:
    """Covers: ARC-004 — an unordered view cannot look like a content change."""
    first = _row(source_object_key="LAU:UNEMP_RATE")
    second = _row(source_object_key="LAU:LFPR")

    assert harvest.content_fingerprint(
        _documents(first, second)
    ) == harvest.content_fingerprint(_documents(second, first))


def test_fingerprint_ignores_watermarks_that_move_on_every_ingestion() -> None:
    """Covers: ARC-004 — the digest is content, not a slower watermark."""
    baseline = harvest.content_fingerprint(_documents(_row()))

    assert baseline == harvest.content_fingerprint(
        _documents(
            _row(
                source_watermark="2099-01-01 00:00:00+00",
                publication_time=PUBLICATION + timedelta(days=400),
            )
        )
    )


@pytest.mark.parametrize(
    "change",
    [
        {"metric_display_name": "Unemployment rate (U)"},
        {"units": "percent"},
        {"measure_kind": "LEVEL"},
        {"valid_geo_grains": ["STATE"]},
        {"valid_time_grains": ["ANNUAL"]},
        {"aggregation_characteristic": "additive"},
        {"physical_lineage": {"schema": "gold_bls", "key": "LAU:OTHER"}},
        {"source_object_type": "series"},
        {"source_object_key": "LAU:SOMETHING_ELSE"},
        {"publisher_contract_version": "2.0"},
        {"reference_url": "https://example.invalid/"},
    ],
)
def test_fingerprint_moves_when_published_content_moves(change: dict) -> None:
    """Covers: ARC-004 — every column the catalog stores is in the digest."""
    assert harvest.content_fingerprint(
        _documents(_row())
    ) != harvest.content_fingerprint(_documents(_row(**change)))


def test_fingerprint_moves_when_the_set_of_keys_moves() -> None:
    """Covers: ARC-004 — a dropped key is a content change, so it harvests."""
    both = _documents(_row(), _row(source_object_key="LAU:LFPR"))
    one = _documents(_row())

    assert harvest.content_fingerprint(both) != harvest.content_fingerprint(one)


# --- the guard --------------------------------------------------------------


def test_unchanged_content_and_watermark_writes_nothing() -> None:
    """Covers: ARC-004 — the scheduled case stays cheap."""
    fingerprint = harvest.content_fingerprint(_documents(_row()))
    written, connection = _harvest([_row()], (PUBLICATION, fingerprint))

    assert written == 0
    assert not _upserted(connection)


def test_a_content_change_harvests_though_no_fact_moved() -> None:
    """Covers: ARC-004 — the defect this contract exists for.

    Same publication time, different published content: before the fingerprint
    this skipped, and the catalog kept the retired identity indefinitely.
    """
    stale = harvest.content_fingerprint(_documents(_row(metric_display_name="Old")))
    written, connection = _harvest([_row()], (PUBLICATION, stale))

    assert written == 1
    assert _upserted(connection)


def test_an_unrecorded_fingerprint_harvests_rather_than_skips() -> None:
    """Covers: ARC-004 — every row predating the column re-harvests once."""
    written, connection = _harvest([_row()], (PUBLICATION, None))

    assert written == 1
    assert _upserted(connection)


def test_no_prior_harvest_state_harvests() -> None:
    """Covers: ARC-004 — a source's first harvest is never skipped."""
    written, connection = _harvest([_row()], None)

    assert written == 1
    assert _upserted(connection)


def test_newer_publication_harvests_even_when_content_matches() -> None:
    """Covers: ARC-004 — the publication-time guard still does its job."""
    fingerprint = harvest.content_fingerprint(_documents(_row()))
    written, _ = _harvest([_row()], (PUBLICATION - timedelta(days=1), fingerprint))

    assert written == 1


def test_force_harvests_identical_content() -> None:
    """Covers: ARC-004 — a repair can rewrite a catalog that did not change."""
    fingerprint = harvest.content_fingerprint(_documents(_row()))
    written, connection = _harvest([_row()], (PUBLICATION, fingerprint), force=True)

    assert written == 1
    assert _upserted(connection)


def test_an_empty_publisher_is_never_treated_as_a_dropped_catalog() -> None:
    """Covers: ARC-004 — an unreadable publisher must not retire live metrics."""
    written, connection = _harvest([], None)

    assert written == 0
    assert not _advanced_retirement(connection)
    assert connection.rolled_back


# --- retirement -------------------------------------------------------------


def test_a_skipped_harvest_still_advances_retirement() -> None:
    """Covers: ARC-004 — retirement completes without an operator per step.

    The harvest that first sees a key disappear changes the fingerprint and so
    runs, marking it ``stale``. Every harvest after that sees unchanged content
    and skips, so without this the key would wait at ``stale`` forever.
    """
    fingerprint = harvest.content_fingerprint(_documents(_row()))
    written, connection = _harvest([_row()], (PUBLICATION, fingerprint))

    assert written == 0
    assert _advanced_retirement(connection)
    assert connection.committed


def test_a_harvest_that_writes_also_advances_retirement() -> None:
    """Covers: ARC-004 — the counting rule is the same on both paths."""
    _, connection = _harvest([_row()], None)

    assert _advanced_retirement(connection)


# --- targeting --------------------------------------------------------------


def test_harvest_all_forwards_force_to_each_publisher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ARC-004 — the operator's request reaches the harvest."""
    seen: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        harvest,
        "discover_publishers",
        lambda _: [harvest.Publisher("gold_bls"), harvest.Publisher("gold_fred")],
    )
    monkeypatch.setattr(
        harvest,
        "harvest_publisher",
        lambda _, publisher, force=False: seen.append((publisher.schema, force)) or 1,
    )

    harvest.harvest_all_publishers(lambda: GuardConnection([], None), force=True)

    assert seen == [("gold_bls", True), ("gold_fred", True)]


def test_harvest_all_can_target_one_publisher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ARC-004 — a repair does not rewrite every source's catalog."""
    seen: list[str] = []
    monkeypatch.setattr(
        harvest,
        "discover_publishers",
        lambda _: [harvest.Publisher("gold_bls"), harvest.Publisher("gold_fred")],
    )
    monkeypatch.setattr(
        harvest,
        "harvest_publisher",
        lambda _, publisher, force=False: seen.append(publisher.schema) or 1,
    )

    results = harvest.harvest_all_publishers(
        lambda: GuardConnection([], None), schemas=["gold_bls"]
    )

    assert seen == ["gold_bls"]
    assert set(results) == {"gold_bls"}


def test_targeting_an_unknown_publisher_is_an_error_not_a_silent_no_op(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ARC-004 — a typo in a repair request must not look like success."""
    monkeypatch.setattr(
        harvest, "discover_publishers", lambda _: [harvest.Publisher("gold_bls")]
    )

    with pytest.raises(ValueError, match="gold_typo"):
        harvest.harvest_all_publishers(
            lambda: GuardConnection([], None), schemas=["gold_typo"]
        )


# --- the operator request ---------------------------------------------------


def test_a_scheduled_run_is_never_forced_or_narrowed() -> None:
    """Covers: ARC-004 — an expensive repair cannot happen by accident."""
    for conf in (None, {}, "not-a-mapping", []):
        assert harvest.reconciliation_arguments(conf) == {
            "force": False,
            "schemas": None,
        }


def test_an_operator_request_is_read_from_the_dag_run_conf() -> None:
    """Covers: ARC-004 — force and targeting reach the harvest."""
    assert harvest.reconciliation_arguments(
        {"force": True, "schemas": ["gold_bls", "gold_fred"]}
    ) == {"force": True, "schemas": ["gold_bls", "gold_fred"]}


def test_a_single_schema_string_is_refused_rather_than_split() -> None:
    """Covers: ARC-004 — `"gold_bls"` must not silently become 8 characters."""
    with pytest.raises(ValueError, match="list of publisher schemas"):
        harvest.reconciliation_arguments({"schemas": "gold_bls"})
