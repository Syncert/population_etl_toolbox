"""Real PostgreSQL Monthly Treasury Statement (Table 9) capture-to-gold contract.

Covers: ETL-128
"""

from __future__ import annotations

import json
from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from data_ingestion_toolbox.quality.sources import (
    treasury_mts_function_total,
    treasury_mts_page_reconciliation,
)
from data_ingestion_toolbox.treasury_mts.schema import (
    REQUIRED_RELATIONS,
    ensure_treasury_mts_schema,
)
from tests.support import treasury_mts as mts

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def mts_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return mts.reviewed_warehouse(postgres_connection_factory, request)


def _rows(
    factory: Callable[[], connection], sql: str, parameters: tuple | None = None
) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def _outcomes(factory: Callable[[], connection], executor):  # noqa: ANN001, ANN202
    reader = factory()
    try:
        with reader.cursor() as cursor:
            return executor(cursor, {})
    finally:
        reader.close()


def test_statement_lines_load_as_published_with_headings_missing(mts_warehouse) -> None:
    """Covers: ETL-128 — amounts as Treasury wrote them; a `null` heading is missing, not zero."""
    factory = mts_warehouse
    _run_id, pages, written, published = mts.run_to_gold(factory)
    assert (pages, written, published) == (1, 60, 1)
    assert _rows(
        factory,
        """
        SELECT current_month_source, current_fytd_amount, value_status
        FROM silver_treasury_mts.statement_line
        WHERE record_date = '2026-09-30' AND line_code IN ('10', '340')
        ORDER BY line_code::INTEGER
        """,
    ) == [
        ("null", None, "missing"),
        ("584774866340.26", Decimal(mts.FY2026_TOTAL_OUTLAYS), "valid"),
    ]
    # The fiscal-year total is the September fiscal-year-to-date column.
    served = dict(
        _rows(
            factory,
            """
            SELECT metric_key, value FROM gold_treasury_mts.observation_latest
            WHERE year = 2026 AND geo_id = %s
            """,
            (mts.NATION,),
        )
    )
    assert served["outlays_total"] == Decimal(mts.FY2026_TOTAL_OUTLAYS)
    assert served["outlays_net_interest"] == Decimal(mts.FY2026_NET_INTEREST)
    assert len(served) == 30
    assert _rows(
        factory,
        """
        SELECT period_start::TEXT, period_end::TEXT, geo_level, geo_sk IS NOT NULL
        FROM gold_treasury_mts.observation_latest WHERE metric_key = 'outlays_total'
        """,
    ) == [("2025-10-01", "2026-09-30", "NATIONAL", True)]
    # August is not a fiscal-year total, so nothing of it is served there.
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_treasury_mts.observation_revision WHERE period_end <> '2026-09-30'",
    ) == [(0,)]


def test_outlays_by_function_shares_add_to_total_outlays(mts_warehouse) -> None:
    """Covers: ETL-128 — each budget function's share of line 340 for the fiscal year and months."""
    factory = mts_warehouse
    mts.run_to_gold(factory)
    fiscal_year = _rows(
        factory,
        """
        SELECT line_code, outlays_amount, total_outlays_amount, share_of_total_outlays_pct
        FROM gold_treasury_mts.outlays_by_function
        WHERE period_kind = 'fiscal_year' AND fiscal_year = 2026
        ORDER BY line_code::INTEGER
        """,
    )
    assert len(fiscal_year) == 19
    assert sum(row[1] for row in fiscal_year) == Decimal(mts.FY2026_TOTAL_OUTLAYS)
    defense = next(row for row in fiscal_year if row[0] == "140")
    assert defense[1:] == (
        Decimal(mts.FY2026_NATIONAL_DEFENSE),
        Decimal(mts.FY2026_TOTAL_OUTLAYS),
        Decimal("13.05"),
    )
    # Months come from the current-month column; the fiscal year is complete,
    # so there is no open fiscal-year-to-date row.
    assert _rows(
        factory,
        """
        SELECT period_kind, COUNT(*) FROM gold_treasury_mts.outlays_by_function
        GROUP BY period_kind ORDER BY period_kind
        """,
    ) == [("fiscal_year", 19), ("month", 38)]
    assert [
        outcome.result for outcome in _outcomes(factory, treasury_mts_function_total)
    ] == ["pass"]


def test_an_open_fiscal_year_is_served_to_date_from_its_newest_statement(
    mts_warehouse,
) -> None:
    """Covers: ETL-128 — a fiscal year with no September statement is `fiscal_year_to_date`, never annual."""
    factory = mts_warehouse
    august = [row for row in mts.fixture_rows() if row["record_date"] == "2026-08-31"]
    mts.run_to_gold(factory, client=mts.FixtureClient(august))
    assert _rows(
        factory, "SELECT COUNT(*) FROM gold_treasury_mts.observation_latest"
    ) == [(0,)]
    assert _rows(
        factory,
        """
        SELECT period_start::TEXT, period_end::TEXT, outlays_amount
        FROM gold_treasury_mts.outlays_by_function
        WHERE period_kind = 'fiscal_year_to_date' AND line_code = '140'
        """,
    ) == [("2025-10-01", "2026-08-31", Decimal("876163147483.11"))]


def test_a_reread_writes_only_restated_lines(mts_warehouse) -> None:
    """Covers: ETL-128 — the churn rule: an unchanged line is not rewritten; a restated one is kept as a revision."""
    factory = mts_warehouse
    first_run, _pages, written, _published = mts.run_to_gold(factory)
    assert written == 60
    _second, _pages, unchanged, _published = mts.run_to_gold(factory)
    assert unchanged == 0
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_treasury_mts.statement_line WHERE run_id = %s",
        (str(first_run),),
    ) == [(60,)]

    restated = mts.fixture_rows()
    for row in restated:
        if row["record_date"] == "2026-09-30" and row["line_code_nbr"] == "320":
            row["current_fytd_rcpt_outly_amt"] = "1051753969312.11"
    third, pages, changed, _published = mts.run_to_gold(
        factory, client=mts.FixtureClient(restated), page_size=25
    )
    assert (pages, changed) == (3, 1)
    assert _rows(
        factory,
        """
        SELECT run_id::TEXT, value FROM gold_treasury_mts.observation_revision
        WHERE metric_key = 'outlays_net_interest' ORDER BY published_at
        """,
    ) == [
        (str(first_run), Decimal(mts.FY2026_NET_INTEREST)),
        (str(third), Decimal("1051753969312.11")),
    ]
    assert [
        outcome.result
        for outcome in _outcomes(factory, treasury_mts_page_reconciliation)
    ] == ["pass", "pass"]


def test_a_repeated_line_is_quarantined_alone(mts_warehouse) -> None:
    """Covers: ETL-128 — a (record date, line) read twice is quarantined once; every other line loads."""
    factory = mts_warehouse
    rows = mts.fixture_rows()
    rows.append(json.loads(json.dumps(rows[-1])))
    _run, _pages, written, _published = mts.run_to_gold(
        factory, client=mts.FixtureClient(rows)
    )
    assert written == 60
    assert _rows(factory, "SELECT error_code FROM silver_treasury_mts.quarantine") == [
        ("repeated_line",)
    ]


def test_the_glossary_publishes_every_line_at_the_national_grain(mts_warehouse) -> None:
    """Covers: ETL-128 — the publisher derives NATIONAL from served rows and ANNUAL time."""
    factory = mts_warehouse
    mts.run_to_gold(factory)
    assert harvest_publisher(factory, Publisher("gold_treasury_mts")) == 30
    assert _rows(
        factory,
        """
        SELECT valid_geo_grains, valid_time_grains, source_watermark
        FROM gold_treasury_mts.metric_publisher WHERE source_object_key = 'outlays_total'
        """,
    ) == [(["NATIONAL"], ["ANNUAL"], "MTS 2026-09")]


def test_schema_is_reapplied_idempotently(mts_warehouse) -> None:
    """Covers: ETL-128 — the source-owned DDL applies twice and names every relation."""
    factory = mts_warehouse

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_treasury_mts_schema(_Hook())
    ensure_treasury_mts_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]
