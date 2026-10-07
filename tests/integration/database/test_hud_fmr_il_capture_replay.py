"""Real PostgreSQL HUD Fair Market Rent and income-limit capture-to-gold contract.

Covers: ETL-065
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.hud_fmr_il.client import HudPayloadError
from data_ingestion_toolbox.hud_fmr_il.registry import get_file
from data_ingestion_toolbox.hud_fmr_il.schema import (
    REQUIRED_RELATIONS,
    ensure_hud_fmr_il_schema,
)
from data_ingestion_toolbox.quality.sources import (
    hud_file_reconciliation,
    hud_value_plausibility,
)
from tests.support import hud_fmr_il as hud

pytestmark = [pytest.mark.integration, pytest.mark.database]

FY26 = get_file("fmr:fy2026:original")
FY26_REVISED = get_file("fmr:fy2026:revised")
FY27 = get_file("fmr:fy2027:original")
IL26 = get_file("il:fy2026:original")


@pytest.fixture
def hud_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return hud.reviewed_warehouse(postgres_connection_factory, request)


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


def _outcome(factory: Callable[[], connection], executor):  # noqa: ANN001, ANN202
    reader = factory()
    try:
        with reader.cursor() as cursor:
            return executor(cursor, {})[0]
    finally:
        reader.close()


def test_every_edition_reaches_gold_as_area_values_by_county(hud_warehouse) -> None:
    """Covers: ETL-065 — county rows served with their HUD area; a town row is held."""
    factory = hud_warehouse
    hud.run_all(factory)
    kent = dict(
        _rows(
            factory,
            """
            SELECT metric_key, value FROM gold_hud_fmr_il.observation_latest
            WHERE geo_id = %s AND year = 2026
            """,
            (hud.KENT,),
        )
    )
    assert kent["fmr_2br"] == Decimal("1470")
    assert kent["median_family_income"] == Decimal("112100")
    assert kent["income_limit_50_4p"] == Decimal("53900")
    assert len(kent) == 9
    sussex = _rows(
        factory,
        """
        SELECT hud_area_code, metro, effective_date::TEXT, edition, release_key, period_start::TEXT
        FROM gold_hud_fmr_il.observation_latest WHERE geo_id = %s AND metric_key = 'fmr_0br' AND year = 2027
        """,
        (hud.SUSSEX,),
    )
    assert sussex == [
        ("NCNTY10005N10005", False, "2026-10-01", "original", "FY2027", "2026-10-01")
    ]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_hud_fmr_il.observation_revision WHERE geo_id LIKE 'state:09%%'",
    ) == [(0,)]
    held = _rows(
        factory,
        "SELECT DISTINCT geo_type, geography_status FROM silver_hud_fmr_il.fact_observation WHERE geo_id = %s",
        (hud.ANDOVER,),
    )
    assert held == [("county_subdivision", "unsupported")]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_hud_fmr_il.fact_observation WHERE measure LIKE 'income_limit_%%'",
    ) == [(5 * 24,)]


def test_the_revised_edition_is_served_and_the_original_kept(hud_warehouse) -> None:
    """Covers: ETL-065 — both FY 2026 editions retained; the reissue serves by default."""
    factory = hud_warehouse
    hud.run_to_gold(factory, FY26)
    hud.run_to_gold(factory, FY26_REVISED)
    history = _rows(
        factory,
        """
        SELECT release_key, value, effective_date::TEXT FROM gold_hud_fmr_il.observation_revision
        WHERE metric_key = 'fmr_2br' AND geo_id = %s AND year = 2026 ORDER BY release_key
        """,
        (hud.NAPA,),
    )
    assert history == [
        ("FY2026", Decimal("2773"), "2025-10-01"),
        ("FY2026-revised", Decimal("3315"), "2026-05-21"),
    ]
    assert _rows(
        factory,
        """
        SELECT release_key, value FROM gold_hud_fmr_il.observation_latest
        WHERE metric_key = 'fmr_2br' AND geo_id = %s AND year = 2026
        """,
        (hud.NAPA,),
    ) == [("FY2026-revised", Decimal("3315"))]


def test_an_unchanged_read_replays_nothing(hud_warehouse) -> None:
    """Covers: ETL-065 — the same bytes add no fact; the edition's ledger shows both reads."""
    factory = hud_warehouse
    hud.run_to_gold(factory, FY27)
    _run, status, facts, published = hud.run_to_gold(factory, FY27)
    assert (status, facts, published) == ("unchanged", 0, 0)
    assert _rows(
        factory,
        "SELECT status, COUNT(*) FROM control.hud_fmr_il_file GROUP BY status ORDER BY status",
    ) == [("published", 1), ("unchanged", 1)]


def test_a_refused_workbook_and_the_rules(hud_warehouse) -> None:
    """Covers: ETL-065 — a non-workbook fails capture; DQ-HUD-002 and -004 pass then catch a fault."""
    factory = hud_warehouse
    assert _outcome(factory, hud_file_reconciliation).result == "not_applicable"
    with pytest.raises(HudPayloadError, match="not_a_workbook"):
        hud.run_to_gold(
            factory,
            IL26,
            client=hud.FixtureClient({"Section8-FY26.xlsx": b"<html>moved</html>"}),
        )
    assert _rows(factory, "SELECT COUNT(*) FROM control.hud_fmr_il_file") == [(0,)]
    run_id, _status, _facts, _published = hud.run_to_gold(factory, IL26)
    hud.run_to_gold(factory, FY26)
    assert (
        _outcome(factory, hud_file_reconciliation).result,
        _outcome(factory, hud_value_plausibility).result,
    ) == (
        "pass",
        "pass",
    )
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                UPDATE silver_hud_fmr_il.fact_observation SET value = 99999
                WHERE geo_id = %s AND measure IN ('fmr_1br', 'income_limit_30_4p')
                """,
                (hud.KENT,),
            )
        writer.commit()
    finally:
        writer.close()
    warned = _outcome(factory, hud_value_plausibility)
    assert (warned.result, warned.observed_count) == ("warn", 2)
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_hud_fmr_il.fact_observation WHERE run_id = %s",
                (str(run_id),),
            )
        writer.commit()
    finally:
        writer.close()
    lost = _outcome(factory, hud_file_reconciliation)
    assert (lost.result, lost.observed_count) == ("fail", 1)

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_hud_fmr_il_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]


def test_the_harvest_names_every_published_measure(hud_warehouse) -> None:
    """Covers: ETL-065 — nine county metrics, each stating it is an area value."""
    factory = hud_warehouse
    hud.run_all(factory)
    published = dict(
        _rows(
            factory,
            "SELECT source_object_key, valid_geo_grains FROM gold_hud_fmr_il.metric_publisher",
        )
    )
    assert len(published) == 9 and set(map(tuple, published.values())) == {("COUNTY",)}
    bases = [
        row[0]
        for row in _rows(
            factory,
            "SELECT DISTINCT observation_basis FROM gold_hud_fmr_il.measure_definition",
        )
    ]
    assert len(bases) == 2 and all("not a county estimate" in basis for basis in bases)
    assert harvest_publisher(factory, Publisher("gold_hud_fmr_il")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'HUD_FMR_IL'",
    ) == [(9,)]
