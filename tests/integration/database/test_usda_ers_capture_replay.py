"""Real PostgreSQL USDA ERS county codes and atlas capture-to-gold contract.

Covers: ETL-066
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
from data_ingestion_toolbox.quality.sources import (
    ers_county_coverage,
    ers_file_reconciliation,
)
from data_ingestion_toolbox.usda_ers.client import ErsPayloadError
from data_ingestion_toolbox.usda_ers.registry import get_file
from data_ingestion_toolbox.usda_ers.schema import (
    REQUIRED_RELATIONS,
    ensure_usda_ers_schema,
)
from tests.support import usda_ers as ers

pytestmark = [pytest.mark.integration, pytest.mark.database]

RUCC = get_file("rucc:2023")
TYPOLOGY = get_file("typology:2025")
ATLAS = get_file("fea:2025-07")


@pytest.fixture
def ers_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return ers.reviewed_warehouse(postgres_connection_factory, request)


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


def test_every_file_reaches_gold_with_codes_flags_and_sentinels(ers_warehouse) -> None:
    """Covers: ETL-066 — RUCC with its label, flags, unset marks and Atlas sentinels."""
    factory = ers_warehouse
    ers.run_all(factory)
    kent = {
        (row[0], row[1]): row[2:]
        for row in _rows(
            factory,
            """
            SELECT metric_key, year, value, value_status, code_label FROM gold_usda_ers.observation_latest
            WHERE geo_id = %s
            """,
            (ers.KENT,),
        )
    }
    assert kent[("rural_urban_continuum_code", 2023)] == (
        Decimal("3"),
        "valid",
        "Metro - Counties in metro areas of fewer than 250,000 population",
    )
    assert kent[("farming_dependent", 2025)] == (Decimal("0"), "valid", None)
    assert kent[("snap_authorized_stores", 2023)] == (Decimal("159"), "valid", None)
    assert len(kent) == 1 + 13 + 8
    unset = _rows(
        factory,
        """
        SELECT metric_key, value, value_status, missing_reason FROM gold_usda_ers.observation_latest
        WHERE geo_id = %s AND metric_key IN ('farming_dependent', 'housing_stress') ORDER BY metric_key
        """,
        (ers.CAPITOL,),
    )
    assert unset == [
        ("farming_dependent", None, "not_applicable", "not_computed_for_geography"),
        ("housing_stress", Decimal("1"), "valid", None),
    ]
    chugach = dict(
        _rows(
            factory,
            """
            SELECT metric_key || ':' || year, missing_reason FROM gold_usda_ers.observation_latest
            WHERE geo_id = %s AND value IS NULL
            """,
            (ers.CHUGACH,),
        )
    )
    assert chugach["snap_households_low_store_access:2015"] == "county_did_not_exist"
    assert chugach["snap_households_low_store_access:2019"] == "not_available"
    assert chugach["persistent_poverty:2025"] == "not_determined"
    assert _rows(
        factory,
        "SELECT metric_key, value FROM gold_usda_ers.observation_latest WHERE geo_id = %s",
        (ers.ADJUNTAS,),
    ) == [("rural_urban_continuum_code", Decimal("6"))]
    # Hartford is a legacy Connecticut county the seeded geography does not
    # hold: recorded in the ledger, not served.
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_usda_ers.observation_latest WHERE geo_id = %s",
        (ers.HARTFORD,),
    ) == [(0,)]
    assert _rows(
        factory,
        """
        SELECT status, reason_code FROM silver_ref.geography_resolution
        WHERE provider_source = 'USDA_ERS' AND provider_dataset = 'ers_typology' AND source_code = '09001'
        """,
    ) == [("unmapped", "canonical_geography_absent")]


def test_an_unchanged_read_replays_nothing_and_a_replacement_is_kept(
    ers_warehouse,
) -> None:
    """Covers: ETL-066 — the same bytes add nothing; a replaced file keeps both captures."""
    factory = ers_warehouse
    ers.run_to_gold(factory, RUCC)
    _run, status, facts, published = ers.run_to_gold(factory, RUCC)
    assert (status, facts, published) == ("unchanged", 0, 0)
    replaced = ers.fixture_bytes(RUCC).replace(
        b"10001,DE,Kent County,RUCC_2023,3", b"10001,DE,Kent County,RUCC_2023,2"
    )
    _run, status, _facts, published = ers.run_to_gold(
        factory,
        RUCC,
        client=ers.FixtureClient({"2023-rural-urban-continuum-codes.csv": replaced}),
    )
    assert (status, published) == ("captured", 1)
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM gold_usda_ers.observation_revision
        WHERE metric_key = 'rural_urban_continuum_code' AND geo_id = %s
        """,
        (ers.KENT,),
    ) == [(2,)]
    assert _rows(
        factory,
        "SELECT value FROM gold_usda_ers.observation_latest WHERE metric_key = 'rural_urban_continuum_code' AND geo_id = %s",
        (ers.KENT,),
    ) == [(Decimal("2"),)]


def test_a_refused_file_and_the_rules(ers_warehouse) -> None:
    """Covers: ETL-066 — a moved file fails capture; DQ-ERS-002 and -004 pass then catch a fault."""
    factory = ers_warehouse
    assert _outcome(factory, ers_file_reconciliation).result == "not_applicable"
    with pytest.raises(ErsPayloadError, match="member_missing"):
        ers.run_to_gold(
            factory,
            ATLAS,
            client=ers.FixtureClient(
                {"food-environment-atlas-csv-files.zip": b"<html>moved</html>"}
            ),
        )
    assert _rows(factory, "SELECT COUNT(*) FROM control.usda_ers_file") == [(0,)]
    run_id, _status, _facts, _published = ers.run_to_gold(factory, RUCC)
    assert _outcome(factory, ers_file_reconciliation).result == "pass"
    coverage = _outcome(factory, ers_county_coverage)
    # Rose Island (American Samoa) is in the RUCC fixture but not in the
    # seeded geography, so its population row is an unmapped warning.
    assert coverage.result == "warn"
    unmapped_before = coverage.observed_count
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                UPDATE silver_usda_ers.fact_observation SET code_label = NULL
                WHERE geo_id = %s AND attribute = 'RUCC_2023'
                """,
                (ers.KENT,),
            )
        writer.commit()
    finally:
        writer.close()
    assert _outcome(factory, ers_county_coverage).observed_count == unmapped_before + 1
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_usda_ers.fact_observation WHERE run_id = %s",
                (str(run_id),),
            )
        writer.commit()
    finally:
        writer.close()
    lost = _outcome(factory, ers_file_reconciliation)
    assert (lost.result, lost.observed_count) == ("fail", 1)

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_usda_ers_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]


def test_the_harvest_names_every_published_measure(ers_warehouse) -> None:
    """Covers: ETL-066 — eighteen county metrics; classifications say so."""
    factory = ers_warehouse
    ers.run_all(factory)
    published = dict(
        _rows(
            factory,
            "SELECT source_object_key, measure_kind FROM gold_usda_ers.metric_publisher",
        )
    )
    assert len(published) == 18
    assert published["rural_urban_continuum_code"] == "classification"
    assert published["snap_authorized_stores"] == "source_fact"
    assert harvest_publisher(factory, Publisher("gold_usda_ers")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'USDA_ERS'",
    ) == [(18,)]
