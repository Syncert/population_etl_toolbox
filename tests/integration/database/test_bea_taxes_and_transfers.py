"""Transfer receipts, personal taxes and after-tax income reach gold and are checked.

The reviewed CAINC35, SAINC35, SAINC50 and SAINC51 rows are played through
the adapter's own capture path (taxes-and-public-money, T-1).

Covers: ETL-114
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Callable
from decimal import Decimal

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.quality.sources import (
    bea_component_sums,
    bea_transfer_total_agreement,
)
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support import bea
from tests.support.capture_seed import seed_capture

pytestmark = [pytest.mark.integration, pytest.mark.database]

NEW_TABLES = ("CAINC35", "SAINC35", "SAINC50", "SAINC51")


def _rezip(name: str, transform: Callable[[str, bytes], bytes]) -> bytes:
    source = zipfile.ZipFile(io.BytesIO(bea.fixture_bytes(name)))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as target:
        for member in source.namelist():
            target.writestr(member, transform(member, source.read(member)))
    return out.getvalue()


def _override(name: str, payload: bytes) -> bea.FixtureClient:
    return bea.FixtureClient(
        {
            name: httpx.Response(
                200, content=payload, request=httpx.Request("GET", "https://x")
            )
        }
    )


def _outcome(factory: Callable[[], connection], executor) -> object:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            (outcome,) = executor(cursor, {})
        return outcome
    finally:
        reader.close()


@pytest.fixture
def tax_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    factory = bea.reviewed_warehouse(postgres_connection_factory, request)
    for code in (*NEW_TABLES, "CAINC4", "SARPP"):
        _run, facts, published = bea.run_to_gold(factory, code)
        assert facts > 0 and published == 1
    return factory


def test_programs_taxes_and_income_are_served_with_their_own_release(
    tax_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-114 — each new line is a metric; the nation's 2025 figures as BEA wrote them."""
    reader = tax_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT metric_key, value, release_date::TEXT
                  FROM gold_bea.observation_latest
                 WHERE geo_id = 'us:1' AND year = 2025
                   AND metric_key IN ('SAINC35:2221', 'SAINC50:60', 'SAINC50:10', 'SAINC51:53')
                 ORDER BY metric_key
                """
            )
            assert cursor.fetchall() == [
                ("SAINC35:2221", Decimal("1023953000"), "2026-09-30"),
                ("SAINC50:10", Decimal("26326993000"), "2026-09-30"),
                ("SAINC50:60", Decimal("3341556000"), "2026-09-30"),
                ("SAINC51:53", Decimal("67251"), "2026-09-30"),
            ]
            cursor.execute(
                """
                SELECT value, release_date::TEXT FROM gold_bea.observation_latest
                 WHERE metric_key = 'CAINC4:46' AND geo_id = 'us:1' AND year = 2024
                """
            )
            assert cursor.fetchone() == (Decimal("5218016000"), "2026-02-05")
            cursor.execute(
                """
                SELECT COUNT(DISTINCT line_code) FROM silver_bea.dim_line
                 WHERE table_code = 'CAINC35'
                """
            )
            assert cursor.fetchone()[0] == 23
    finally:
        reader.close()


def test_component_sums_pass_on_bea_rows_and_warn_on_a_broken_part(
    tax_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-114 — DQ-BEA-006 passes BEA's own rows and warns past 0.5 percent."""
    outcome = _outcome(tax_warehouse, bea_component_sums)
    assert outcome.result == "pass", outcome.evidence

    def break_federal_taxes(member: str, content: bytes) -> bytes:
        if not member.startswith("SAINC50"):
            return content
        # The nation's 2025 federal taxes raised by $100 billion (3 percent of
        # personal current taxes): the four levels no longer make the total.
        return content.replace(b",2421697000,2710312000", b",2421697000,2810312000")

    broken = _rezip("SAINC", break_federal_taxes)
    assert broken != bea.fixture_bytes("SAINC")
    bea.run_to_gold(tax_warehouse, "SAINC50", client=_override("SAINC", broken))
    outcome = _outcome(tax_warehouse, bea_component_sums)
    assert outcome.result == "warn"
    assert outcome.observed_count == 1
    assert "SAINC50" in str(outcome.evidence[0]) and "2025" in str(outcome.evidence[0])


def test_the_county_total_is_compared_only_within_a_shared_release(
    tax_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-114 — DQ-BEA-007: different releases are not compared; one shared release is."""
    # CAINC35 (November 2023) and CAINC4 (February 2026) share no release.
    assert _outcome(tax_warehouse, bea_transfer_total_agreement).result == (
        "not_applicable"
    )

    def same_release(member: str, content: bytes) -> bytes:
        return content.replace(
            b"November 16, 2023-- new statistics for 2022",
            b"February 5, 2026-- new statistics for 2022",
        )

    bea.run_to_gold(
        tax_warehouse,
        "CAINC35",
        client=_override("CAINC35", _rezip("CAINC35", same_release)),
    )
    # The 2023 vintage's national 2022 transfers ($4,002 billion) differ from
    # the 2026 vintage's CAINC4 line 47 ($4,140 billion) by 3 percent, so a
    # file stamped with the wrong release is caught; its 2020 and 2021 agree.
    outcome = _outcome(tax_warehouse, bea_transfer_total_agreement)
    assert outcome.result == "warn"
    assert outcome.evidence == ["2026-02-05|us:1|2022|4002084000|4139612000"]


def test_disposable_income_is_deflated_by_national_and_state_prices(
    tax_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-114 — 2017 dollars by PCE, and by PCE and parity; no index, no figure."""
    database_connection = tax_warehouse()
    try:
        with database_connection.cursor() as db:
            db.execute(
                "INSERT INTO gold_fred.dim_fred_series (series_id, frequency) "
                "VALUES ('PCEPI', 'Monthly') ON CONFLICT (series_id) DO NOTHING"
            )
            capture_id = seed_capture(db, "FRED")
            # Twelve months of 2024; eleven of 2025, so 2025 has no level.
            for year, months in ((2024, 12), (2025, 11)):
                for month in range(1, months + 1):
                    day = f"{year}-{month:02d}-01"
                    _seed_time(db, int(day.replace("-", "")), day)
                    db.execute(
                        """
                        INSERT INTO silver_fred.fact_economic_indicators (
                            time_sk, duration_start, duration_end, observation_date,
                            series_id, value, capture_id, load_batch_id
                        ) VALUES (%s, %s, %s, %s, 'PCEPI', 125, %s, gen_random_uuid())
                        ON CONFLICT (series_id, observation_date) DO NOTHING
                        """,
                        (int(day.replace("-", "")), day, day, day, capture_id),
                    )
            db.execute(
                """
                SELECT AVG(fact.value) FROM silver_fred.fact_economic_indicators AS fact
                 WHERE fact.series_id = 'PCEPI'
                   AND fact.observation_date BETWEEN '2024-01-01' AND '2024-12-31'
                """
            )
            level = db.fetchone()[0]
            db.execute(
                """
                SELECT year, current_dollars, national_prices_2017_dollars,
                       regional_price_parity, regional_prices_2017_dollars,
                       income_release_date::TEXT, parity_release_date::TEXT
                  FROM gold_bea.disposable_income_real
                 WHERE geo_id = 'us:1'
                 ORDER BY year
                """
            )
            rows = {row[0]: row[1:] for row in db.fetchall()}
            expected = (Decimal(64825) * 100 / level).quantize(Decimal(1))
            # The nation's parity is 100, so its two views agree.
            assert rows[2024] == (
                Decimal("64825"),
                expected,
                Decimal("100.000"),
                expected,
                "2026-09-30",
                "2026-02-19",
            )
            # 2025 has eleven PCE months and BEA has no 2025 parity yet.
            assert rows[2025][1] is None and rows[2025][3] is None
    finally:
        database_connection.rollback()
        database_connection.close()
