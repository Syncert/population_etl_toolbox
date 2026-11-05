"""Real PostgreSQL IRS SOI county and state income capture-to-gold contract.

Covers: ETL-115
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import httpx
import psycopg2
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.irs_soi.plan import answered_years
from data_ingestion_toolbox.irs_soi.registry import registered_years
from data_ingestion_toolbox.irs_soi.schema import (
    REQUIRED_RELATIONS,
    ensure_irs_soi_schema,
)
from data_ingestion_toolbox.quality.sources import (
    irs_soi_band_sum,
    irs_soi_county_sum,
    irs_soi_file_reconciliation,
)
from tests.support import irs_soi as soi

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def soi_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return soi.reviewed_warehouse(postgres_connection_factory, request)


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


def _outcome(factory: Callable[[], connection], executor):
    reader = factory()
    try:
        with reader.cursor() as cursor:
            return executor(cursor, {})[0]
    finally:
        reader.close()


def _latest(factory, metric_key: str, geo_id: str, tax_year: int):
    rows = _rows(
        factory,
        """
        SELECT value, product FROM gold_irs_soi.observation_latest
        WHERE metric_key = %s AND geo_id = %s AND tax_year = %s
        """,
        (metric_key, geo_id, tax_year),
    )
    return rows[0] if rows else None


def test_every_era_reaches_gold_with_delaware_resolved_and_other_areas_held_back(
    soi_warehouse,
) -> None:
    """Covers: ETL-115 — workbooks, zipped CSVs, CSVs and the state file all publish; SOI's other areas are never served."""
    factory = soi_warehouse
    soi.run_all(factory)
    statuses = _rows(
        factory,
        "SELECT DISTINCT status FROM control.irs_soi_file",
    )
    assert statuses == [("published",)]
    assert _latest(factory, "returns", soi.DELAWARE, 1989) == (
        Decimal("308166"),
        "county",
    )
    assert _latest(factory, "returns", soi.KENT, 1989) == (Decimal("48600"), "county")
    assert _latest(factory, "interest_received_amount", soi.SUSSEX, 2009) is not None
    assert _latest(factory, "income_tax_amount", soi.KENT, 2009) is None
    assert _latest(factory, "returns", soi.DELAWARE, 2010) == (
        Decimal("427479"),
        "county",
    )
    assert _latest(factory, "eitc_returns", soi.KENT, 2011) is not None
    assert _latest(factory, "returns", soi.KENT, 2023) == (Decimal("89390"), "county")
    # The county file is preferred over the state file where both speak ...
    assert _latest(factory, "returns", soi.DELAWARE, 2023) == (
        Decimal("507370"),
        "county",
    )
    # ... and the state file answers where the county files are silent.
    assert _latest(factory, "returns", soi.NATION, 2023) == (
        Decimal("159949000"),
        "state",
    )
    assert _latest(factory, "returns:agi_1m_plus", soi.DELAWARE, 2023)[1] == "state"
    bands_2011 = {
        row[0]
        for row in _rows(
            factory,
            """
            SELECT agi_band FROM gold_irs_soi.observation_latest
            WHERE geo_id = %s AND tax_year = 2011 AND measure_id = 'returns'
            """,
            (soi.KENT,),
        )
    }
    assert bands_2011 == {
        "all",
        "agi_under_1",
        "agi_1_25k",
        "agi_25k_50k",
        "agi_50k_75k",
        "agi_75k_100k",
        "agi_100k_200k",
        "agi_200k_plus",
    }
    # The by-AGI county file's all-returns rows repeat the totals file.
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM gold_irs_soi.observation_revision
        WHERE file_kind = 'county_agi' AND agi_band = 'all'
        """,
    ) == [(0,)]
    other = _rows(
        factory,
        """
        SELECT source_code, status FROM silver_ref.geography_resolution
        WHERE provider_source = 'IRS_SOI' AND source_geo_type = 'other'
        ORDER BY source_code
        """,
    )
    assert other == [("OA", "unmapped"), ("PR", "unmapped")]
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM gold_irs_soi.observation_revision
        WHERE geo_id LIKE 'irs_soi_area:%%' OR geo_sk IS NULL
        """,
    ) == [(0,)]
    # The years the fixtures answer are planned no more; the rest still are.
    answered = {1989, 1996, 2003, 2009, 2010, 2011, 2023}
    assert _answered(factory) == answered
    assert answered < set(registered_years())


def _answered(factory) -> set[int]:
    reader = factory()
    try:
        return set(answered_years(reader))
    finally:
        reader.close()


def test_a_rerun_stores_nothing_and_a_revised_file_is_kept_beside_the_old(
    soi_warehouse,
) -> None:
    """Covers: ETL-115 — the same bytes are recorded `unchanged` and store no row; a changed file keeps both captures."""
    factory = soi_warehouse
    first, facts, _p = soi.run_to_gold(factory, "county_total:2023")
    assert facts > 0
    stored = _rows(factory, "SELECT COUNT(*) FROM silver_irs_soi.fact_observation")
    second, again, published = soi.run_to_gold(factory, "county_total:2023")
    # The second read repeats the first byte for byte: recorded, not stored again.
    assert (again, published) == (0, 0)
    assert _rows(
        factory,
        "SELECT status FROM control.irs_soi_file WHERE run_id = %s",
        (str(second),),
    ) == [("unchanged",)]
    assert (
        _rows(factory, "SELECT COUNT(*) FROM silver_irs_soi.fact_observation") == stored
    )
    assert 2023 in {
        row[0]
        for row in _rows(
            factory,
            "SELECT tax_year FROM control.irs_soi_file WHERE status = 'published'",
        )
    }
    assert _outcome(factory, irs_soi_file_reconciliation).result == "pass"
    count = _rows(factory, "SELECT COUNT(*) FROM gold_irs_soi.observation_latest")
    revised = soi.fixture_bytes("23incyallnoagi.csv").replace(
        b"10,DE,001,Kent County,0,89390.0000",
        b"10,DE,001,Kent County,0,89391.0000",
    )
    assert revised != soi.fixture_bytes("23incyallnoagi.csv")
    client = soi.FixtureClient(
        {"23incyallnoagi.csv": httpx.Response(200, content=revised)}
    )
    third, revised_facts, _p = soi.run_to_gold(
        factory, "county_total:2023", client=client
    )
    assert revised_facts == facts
    # The revised bytes, read again, repeat the newest publication.
    _fourth, repeated, _p = soi.run_to_gold(factory, "county_total:2023", client=client)
    assert repeated == 0
    history = _rows(
        factory,
        """
        SELECT value, run_id::TEXT FROM gold_irs_soi.observation_revision
        WHERE metric_key = 'returns' AND geo_id = %s AND tax_year = 2023
        ORDER BY retrieved_at
        """,
        (soi.KENT,),
    )
    assert [row[1] for row in history] == [str(first), str(third)]
    assert [row[0] for row in history] == [Decimal("89390"), Decimal("89391")]
    assert _latest(factory, "returns", soi.KENT, 2023) == (Decimal("89391"), "county")
    assert (
        _rows(factory, "SELECT COUNT(*) FROM gold_irs_soi.observation_latest") == count
    )


def test_quality_rules_pass_the_fixtures_and_catch_a_loss(soi_warehouse) -> None:
    """Covers: ETL-115 — DQ-SOI-002, 003 and 004 pass, then fail or warn on a lost row and a broken sum."""
    factory = soi_warehouse
    assert _outcome(factory, irs_soi_file_reconciliation).result == "not_applicable"
    assert _outcome(factory, irs_soi_county_sum).result == "not_applicable"
    assert _outcome(factory, irs_soi_band_sum).result == "not_applicable"
    soi.run_all(factory)
    for executor in (
        irs_soi_file_reconciliation,
        irs_soi_county_sum,
        irs_soi_band_sum,
    ):
        passed = _outcome(factory, executor)
        assert (executor.__name__, passed.result, passed.observed_count) == (
            executor.__name__,
            "pass",
            0,
        )
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                UPDATE silver_irs_soi.fact_observation SET value = value * 2
                WHERE file_kind = 'county_total' AND tax_year = 2023
                  AND metric_key = 'returns' AND geo_id = %s
                """,
                (soi.KENT,),
            )
            cursor.execute(
                """
                UPDATE silver_irs_soi.fact_observation SET value = value * 10
                WHERE file_kind = 'state' AND tax_year = 2023
                  AND metric_key = 'returns:agi_1m_plus' AND geo_id = %s
                """,
                (soi.NATION,),
            )
            cursor.execute(
                """
                DELETE FROM silver_irs_soi.fact_observation
                WHERE file_kind = 'county_total' AND tax_year = 1989
                  AND metric_key = 'agi' AND geo_id = %s
                """,
                (soi.SUSSEX,),
            )
        writer.commit()
    finally:
        writer.close()
    lost = _outcome(factory, irs_soi_file_reconciliation)
    assert (lost.result, lost.observed_count) == ("fail", 1)
    county = _outcome(factory, irs_soi_county_sum)
    assert county.result == "warn"
    # Kent's doubled returns break 2023, Sussex's lost AGI breaks 1989.
    assert {tuple(row.split("|")[:3]) for row in county.evidence} == {
        ("1989", "agi", "10"),
        ("2023", "returns", "10"),
    }
    band = _outcome(factory, irs_soi_band_sum)
    assert band.result == "warn"
    # Evidence is the offender's columns joined by "|"; a county id has one too.
    assert sorted(band.evidence) == [
        f"county_agi|2023|{soi.KENT}|178780.0000|89390.0000",
        f"state|2023|{soi.NATION}|159949000|167142880",
    ]


def test_a_refused_file_is_quarantined_with_no_facts_and_asked_for_again(
    soi_warehouse,
) -> None:
    """Covers: ETL-115 — a layout SOI did not register is refused whole, never served, and stays unanswered."""
    factory = soi_warehouse
    shifted = soi.fixture_bytes("23incyallagi.csv").replace(
        b"10,DE,001,Kent County,8,", b"10,DE,001,Kent County,9,"
    )
    assert shifted != soi.fixture_bytes("23incyallagi.csv")
    run_id, facts, published = soi.run_to_gold(
        factory,
        "county_agi:2023",
        client=soi.FixtureClient(
            {"23incyallagi.csv": httpx.Response(200, content=shifted)}
        ),
    )
    assert (facts, published) == (0, 0)
    assert _rows(
        factory,
        "SELECT status FROM control.irs_soi_file WHERE run_id = %s",
        (str(run_id),),
    ) == [("quarantined",)]
    assert _rows(
        factory,
        "SELECT source_row_index, error_code FROM silver_irs_soi.observation_quarantine WHERE run_id = %s",
        (str(run_id),),
    ) == [(0, "unregistered_agi_stub")]
    soi.run_to_gold(factory, "county_total:2023")
    assert 2023 not in _answered(factory)
    assert _outcome(factory, irs_soi_file_reconciliation).result == "pass"


def test_the_publisher_harvests_and_the_schema_reapplies(soi_warehouse) -> None:
    """Covers: ETL-115 — the publisher names served metrics by grain; the CHECK refuses a resolved row without a key."""
    factory = soi_warehouse
    run_id, _f, _p = soi.run_to_gold(factory, "state:2023")
    soi.run_to_gold(factory, "county_total:2023")
    grains = dict(
        _rows(
            factory,
            "SELECT source_object_key, valid_geo_grains FROM gold_irs_soi.metric_publisher",
        )
    )
    assert sorted(grains["returns"]) == ["COUNTY", "NATIONAL", "STATE"]
    assert sorted(grains["returns:agi_1m_plus"]) == ["NATIONAL", "STATE"]
    assert harvest_publisher(factory, Publisher("gold_irs_soi")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'IRS_SOI'",
    ) == [(len(grains),)]
    writer = factory()
    try:
        with writer.cursor() as cursor, pytest.raises(psycopg2.errors.CheckViolation):
            cursor.execute(
                """
                UPDATE silver_irs_soi.fact_observation SET geo_sk = NULL
                WHERE geography_status = 'resolved' AND run_id = %s
                """,
                (str(run_id),),
            )
        writer.rollback()
    finally:
        writer.close()

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_irs_soi_schema(_Hook())
    ensure_irs_soi_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]
