"""Real PostgreSQL EIA retail gasoline capture-to-gold contract.

Covers: ETL-080
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.eia.client import EiaPayloadError
from data_ingestion_toolbox.eia.schema import REQUIRED_RELATIONS, ensure_eia_schema
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from data_ingestion_toolbox.quality.sources import eia_read_reconciliation
from tests.support import eia
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def eia_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return eia.reviewed_warehouse(postgres_connection_factory, request)


def _rows(factory: Callable[[], connection], sql: str, parameters: tuple = ()) -> list:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def test_the_schema_applies_and_names_every_relation(eia_warehouse) -> None:
    """Covers: ETL-080 — the DAG's schema task applies and its relations exist."""
    ensure_eia_schema(PostgresHookStub(eia_warehouse))
    present = {
        f"{schema}.{name}"
        for schema, name in _rows(
            eia_warehouse,
            """
            SELECT table_schema, table_name FROM information_schema.tables
            WHERE table_schema IN ('control', 'silver_eia', 'gold_eia')
            """,
        )
    }
    assert set(REQUIRED_RELATIONS) <= present


def test_weekly_prices_reach_gold_at_the_nation_states_padds_and_cities(
    eia_warehouse,
) -> None:
    """Covers: ETL-080 — every area kind by code, an unresolved state ledgered, and no key kept."""
    factory = eia_warehouse
    run_id, facts, published = eia.run_to_gold(factory)
    assert (facts, published) == (232, 1)

    served = _rows(
        factory,
        """
        SELECT geo_level, COUNT(*) FROM gold_eia.observation_latest
        GROUP BY geo_level ORDER BY 1
        """,
    )
    # The nation and two resolvable states at 4 grades x 2 weeks, and 19 PADDs
    # and cities; the seven states this warehouse has no USPS code for are not
    # served under a guessed identity.
    assert served == [("NATIONAL", 8), ("PROVIDER_AREA", 152), ("STATE", 16)]
    regular = _rows(
        factory,
        """
        SELECT geo_id, value, unit, period_start::TEXT, period_end::TEXT, value_status
        FROM gold_eia.observation_latest
        WHERE product = 'EPMR' AND geo_id IN ('us:1', 'state:06', 'area:eia:Y35NY')
          AND week_start = '2026-09-07'
        ORDER BY 1
        """,
    )
    assert [row[0] for row in regular] == ["area:eia:Y35NY", "state:06", "us:1"]
    assert all(
        row[2:] == ("U.S. dollars per gallon", "2026-09-07", "2026-09-13", "valid")
        and row[1] > Decimal("0")
        for row in regular
    )
    assert _rows(
        factory,
        """
        SELECT status, reason_code FROM silver_ref.geography_resolution
        WHERE provider_source = 'EIA' AND source_code = 'SNY'
        """,
    ) == [("unmapped", "canonical_geography_absent")]

    # The key is nowhere the warehouse keeps a request or an answer.
    for (text,) in _rows(
        factory,
        """
        SELECT request.endpoint || request.request_parameters::TEXT
        FROM control.ingestion_request AS request WHERE request.run_id = %s
        UNION ALL
        SELECT capture.endpoint || capture.request_parameters::TEXT
        FROM raw_capture.response_capture AS capture WHERE capture.run_id = %s
        """,
        (str(run_id), str(run_id)),
    ):
        assert eia.FIXTURE_KEY not in text and "api_key" not in text

    reader = factory()
    try:
        with reader.cursor() as cursor:
            (outcome,) = eia_read_reconciliation(cursor, {})
    finally:
        reader.close()
    assert outcome.result == "pass"

    assert harvest_publisher(factory, Publisher("gold_eia")) > 0
    catalog = dict(
        _rows(
            factory,
            """
            SELECT metric_code, valid_geo_grains FROM gold_glossary.dim_metric_catalog
            WHERE source_code = 'EIA'
            """,
        )
    )
    assert set(catalog) == {"EIA:EPM0", "EIA:EPMM", "EIA:EPMP", "EIA:EPMR"}
    assert sorted(catalog["EIA:EPMR"]) == ["NATIONAL", "PROVIDER_AREA", "STATE"]


def test_a_second_reading_of_a_week_is_kept_beside_the_first(eia_warehouse) -> None:
    """Covers: ETL-080 — a reread adds readings, the latest view still has one per week."""
    factory = eia_warehouse
    eia.run_to_gold(factory)
    eia.run_to_gold(factory)
    assert _rows(factory, "SELECT COUNT(*) FROM gold_eia.observation_revision") == [
        (352,)
    ]
    assert _rows(factory, "SELECT COUNT(*) FROM gold_eia.observation_latest") == [
        (176,)
    ]


def test_a_malformed_answer_is_kept_as_evidence_and_publishes_nothing(
    eia_warehouse,
) -> None:
    """Covers: ETL-080 — the bytes are captured, the run fails, and gold is unchanged."""
    factory = eia_warehouse
    bad = eia.FixtureClient(override=httpx.Response(200, content=b'{"response": {}}'))
    with pytest.raises(EiaPayloadError):
        eia.run_to_gold(factory, client=bad)
    assert _rows(
        factory,
        """
        SELECT run.status, COUNT(capture.capture_id)
        FROM control.ingestion_run AS run
        JOIN raw_capture.response_capture AS capture ON capture.run_id = run.run_id
        WHERE run.source_code = 'EIA'
        GROUP BY run.status
        """,
    ) == [("failed", 1)]
    assert _rows(factory, "SELECT COUNT(*) FROM gold_eia.observation_latest") == [(0,)]
