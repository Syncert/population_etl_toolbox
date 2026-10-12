"""Real PostgreSQL FHFA House Price Index capture-to-gold contract.

Covers: ETL-064
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fhfa_hpi.client import HpiPayloadError
from data_ingestion_toolbox.fhfa_hpi.schema import (
    REQUIRED_RELATIONS,
    ensure_fhfa_hpi_schema,
)
from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.quality.sources import (
    hpi_file_reconciliation,
    hpi_index_plausibility,
)
from tests.support import fhfa_hpi as hpi

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def hpi_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return hpi.reviewed_warehouse(postgres_connection_factory, request)


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


def test_the_workbook_reaches_gold_with_its_vintage_and_gaps(hpi_warehouse) -> None:
    """Covers: ETL-064 — values, missing reasons, the vintage release and both measures."""
    factory = hpi_warehouse
    _run, status, facts, published = hpi.run_to_gold(factory)
    assert (status, published) == ("captured", 1) and facts == 302 * 4
    served = dict(
        _rows(
            factory,
            """
            SELECT metric_key, value FROM gold_fhfa_hpi.observation_latest
            WHERE geo_id = %s AND year = 2025
            """,
            (hpi.KENT,),
        )
    )
    assert served == {
        "annual_change_pct": Decimal("2.53"),
        "hpi_base_2000": Decimal("252.87"),
    }
    gaps = _rows(
        factory,
        """
        SELECT year, value, value_status, missing_reason FROM gold_fhfa_hpi.observation_latest
        WHERE geo_id = %s AND metric_key = 'hpi_base_2000' AND year IN (2010, 2023) ORDER BY year
        """,
        (hpi.CHUGACH,),
    )
    assert gaps == [
        (2010, None, "missing", "base_year_unavailable"),
        (2023, None, "missing", "provider_missing"),
    ]
    assert _rows(
        factory,
        "SELECT DISTINCT release_key, geo_level FROM gold_fhfa_hpi.observation_revision",
    ) == [("2026-03-31", "COUNTY")]
    assert _rows(
        factory,
        "SELECT COUNT(DISTINCT geo_id) FROM gold_fhfa_hpi.observation_latest",
    ) == [(7,)]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_fhfa_hpi.observation_revision WHERE fips_source = '10001'",
    ) == [(47 * 4,)]


def test_the_same_file_replays_nothing_and_a_new_vintage_is_kept_beside_it(
    hpi_warehouse,
) -> None:
    """Covers: ETL-064 — an unchanged read adds no fact; a revised file keeps both vintages."""
    factory = hpi_warehouse
    first, _status, _facts, _published = hpi.run_to_gold(factory)
    _second, status, facts, published = hpi.run_to_gold(factory)
    assert (status, facts, published) == ("unchanged", 0, 0)
    revised = hpi.revised_workbook(vintage="June 30, 2026", kent_2025_hpi="610.5")
    _third, status, _facts, published = hpi.run_to_gold(
        factory, client=hpi.FixtureClient(revised)
    )
    assert (status, published) == ("captured", 1)
    history = _rows(
        factory,
        """
        SELECT release_key FROM gold_fhfa_hpi.observation_revision
        WHERE metric_key = 'annual_change_pct' AND geo_id = %s AND year = 2025 ORDER BY release_key
        """,
        (hpi.KENT,),
    )
    assert history == [("2026-03-31",), ("2026-06-30",)]
    assert _rows(
        factory,
        """
        SELECT fact.value, file.provider_vintage::TEXT
        FROM silver_fhfa_hpi.fact_observation AS fact
        JOIN control.fhfa_hpi_file AS file USING (run_id)
        WHERE fact.measure = 'hpi' AND fact.geo_id = %s AND fact.year = 2025
        ORDER BY file.provider_vintage
        """,
        (hpi.KENT,),
    ) == [(Decimal("600.17"), "2026-03-31"), (Decimal("610.50"), "2026-06-30")]
    checksums = _rows(
        factory, "SELECT COUNT(DISTINCT payload_checksum) FROM control.fhfa_hpi_file"
    )
    assert checksums == [(2,)]
    assert first is not None


def test_a_refused_workbook_and_the_rules(hpi_warehouse) -> None:
    """Covers: ETL-064 — a non-workbook fails capture; DQ-HPI-002 and -004 pass then catch a fault."""
    factory = hpi_warehouse
    assert _outcome(factory, hpi_file_reconciliation).result == "not_applicable"
    with pytest.raises(HpiPayloadError, match="not_a_workbook"):
        hpi.run_to_gold(factory, client=hpi.FixtureClient(b"<html>maintenance</html>"))
    assert _rows(factory, "SELECT COUNT(*) FROM control.fhfa_hpi_file") == [(0,)]
    run_id, _status, _facts, _published = hpi.run_to_gold(factory)
    assert (
        _outcome(factory, hpi_file_reconciliation).result,
        _outcome(factory, hpi_index_plausibility).result,
    ) == (
        "pass",
        "pass",
    )
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_fhfa_hpi.fact_observation WHERE run_id = %s",
                (str(run_id),),
            )
        writer.commit()
    finally:
        writer.close()
    lost = _outcome(factory, hpi_file_reconciliation)
    assert (lost.result, lost.observed_count) == ("fail", 1)

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_fhfa_hpi_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]


def test_an_implausible_index_warns(hpi_warehouse) -> None:
    """Covers: ETL-064 — DQ-HPI-004 warns on a base year that is not 100 and an unresolved county."""
    factory = hpi_warehouse
    hpi.run_to_gold(factory)
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                UPDATE silver_fhfa_hpi.fact_observation SET value = 99.5
                WHERE geo_id = %s AND measure = 'hpi_base_2000' AND year = 2000
                """,
                (hpi.KENT,),
            )
            cursor.execute(
                """
                UPDATE silver_fhfa_hpi.fact_observation SET geography_status = 'unmapped'
                WHERE geo_id = %s AND measure = 'hpi' AND year = 2025
                """,
                (hpi.CHUGACH,),
            )
        writer.commit()
    finally:
        writer.close()
    warned = _outcome(factory, hpi_index_plausibility)
    assert (warned.result, warned.observed_count) == ("warn", 2)


def test_the_harvest_names_both_measures(hpi_warehouse) -> None:
    """Covers: ETL-064 — two metrics at county grain with FHFA's notice, harvested."""
    factory = hpi_warehouse
    hpi.run_to_gold(factory)
    published = dict(
        _rows(
            factory,
            "SELECT source_object_key, valid_geo_grains FROM gold_fhfa_hpi.metric_publisher",
        )
    )
    assert published == {"annual_change_pct": ["COUNTY"], "hpi_base_2000": ["COUNTY"]}
    basis = _rows(
        factory,
        "SELECT DISTINCT observation_basis FROM gold_fhfa_hpi.measure_definition",
    )
    assert len(basis) == 1 and "neither endorsed nor certified by FHFA" in basis[0][0]
    assert harvest_publisher(factory, Publisher("gold_fhfa_hpi")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'FHFA_HPI'",
    ) == [(2,)]
