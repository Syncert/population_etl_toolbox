"""Calendar rollups derive only what an approved method authorizes.

Covers: ETL-074
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.semantics.rollups import (
    ROLLUP_SOURCES,
    approved_methods,
    STAGING_RELATION,
    refresh_calendar_rollups,
    rollup_sql,
    sync_sql,
)
from data_ingestion_toolbox.semantics.time_aggregation import TimeMethod

pytestmark = pytest.mark.unit


def _method(code: str, method: str = "mean", status: str = "approved") -> TimeMethod:
    return TimeMethod(code, method, status, "Nick", 1)


REGISTRY = {
    "BLS:CUUR0000SA0": _method("BLS:CUUR0000SA0"),
    "BLS:LNS14000000": _method("BLS:LNS14000000", "not_aggregable"),
    "BLS:CES0000000001": _method("BLS:CES0000000001", status="draft"),
    "BLS:END": _method("BLS:END", "end_of_period"),
    "FBI_UCR:summarized_arson:ARS:offense:absolute_total": _method(
        "FBI_UCR:summarized_arson:ARS:offense:absolute_total", "sum"
    ),
}


def test_only_the_sources_approved_summable_methods_are_rolled_up() -> None:
    """Covers: ETL-074 — a draft, a refusal, another source or an unsupported method derives nothing."""
    assert list(approved_methods("BLS", REGISTRY)) == ["BLS:CUUR0000SA0"]
    assert list(approved_methods("FBI_UCR", REGISTRY)) == [
        "FBI_UCR:summarized_arson:ARS:offense:absolute_total"
    ]
    assert approved_methods("FRED", REGISTRY) == {}


def test_the_approved_registry_rolls_up_the_42_reviewed_metrics() -> None:
    """Covers: ETL-074 — the checked-in approvals reach the builder unchanged."""
    bls = approved_methods("BLS")
    fbi = approved_methods("FBI_UCR")
    assert len(bls) + len(fbi) == 42
    assert {method.method for method in bls.values()} == {"mean"}
    assert {method.method for method in fbi.values()} == {"sum"}


@pytest.mark.parametrize("source_code", sorted(ROLLUP_SOURCES))
def test_registry_text_reaches_the_sql_only_as_bound_parameters(
    source_code: str,
) -> None:
    """Covers: ETL-074 — codes and methods are bound; only module constants are composed."""
    methods = approved_methods(source_code, REGISTRY)
    sql = rollup_sql(ROLLUP_SOURCES[source_code], methods)
    for code in methods:
        assert code not in sql
    assert "%(metric_codes)s" in sql and "%(methods)s" in sql
    assert f"INSERT INTO {ROLLUP_SOURCES[source_code].target_relation}" in sql
    # Completeness is counted against the calendar's expected months.
    assert "incomplete_window: " in sql
    assert "INTERVAL '1 month - 1 day'" in sql


class _Cursor:
    def __init__(self, log: list) -> None:
        self.log = log
        self.rowcount = 3

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.log.append((sql, params))


class _Connection:
    def __init__(self) -> None:
        self.log: list = []
        self.commits = 0

    def cursor(self):
        return _Cursor(self.log)

    def commit(self):
        self.commits += 1


def test_a_refresh_replaces_the_derivation_in_one_transaction() -> None:
    """Covers: ETL-074 — old rows go and new rows arrive in one commit, with bound methods."""
    connection = _Connection()
    assert refresh_calendar_rollups(connection, "BLS", REGISTRY) == 3
    drop, create, (derive, params), analyze, delete, insert = connection.log
    assert drop[0] == f"DROP TABLE IF EXISTS {STAGING_RELATION}"
    assert "LIKE gold_bls.derived_calendar_rollup" in create[0]
    # Derived into staging, then only the difference reaches the served table.
    assert f"INSERT INTO {STAGING_RELATION}" in derive
    assert analyze[0] == f"ANALYZE {STAGING_RELATION}"
    assert (delete[0], insert[0]) == sync_sql(ROLLUP_SOURCES["BLS"])
    assert params == {
        "metric_codes": ["BLS:CUUR0000SA0"],
        "methods": ["mean"],
        "versions": [1],
    }
    assert connection.commits == 1


def test_a_source_with_nothing_approved_is_left_with_no_derived_rows() -> None:
    """Covers: ETL-074 — withdrawing every approval clears the source's rollups."""
    connection = _Connection()
    assert refresh_calendar_rollups(connection, "BLS", {}) == 0
    statements = [sql.strip() for sql, _ in connection.log]
    # Nothing is staged, so the sync deletes every served row and adds none.
    assert statements[-2:] == list(sync_sql(ROLLUP_SOURCES["BLS"]))
    assert not any("INSERT INTO pg_temp" in sql for sql in statements)
    assert connection.commits == 1


def test_the_sync_writes_only_rows_that_differ() -> None:
    """Covers: ETL-074 — an unchanged rollup row is compared, not rewritten."""
    delete, insert = sync_sql(ROLLUP_SOURCES["FBI_UCR"])
    assert delete.startswith(
        "DELETE FROM gold_fbi.derived_calendar_rollup AS served WHERE NOT EXISTS"
    )
    assert insert.startswith("INSERT INTO gold_fbi.derived_calendar_rollup (")
    for statement in (delete, insert):
        assert "IS NOT DISTINCT FROM" in statement
        # When a row was written is not part of what it says.
        assert "refreshed_at" not in statement
