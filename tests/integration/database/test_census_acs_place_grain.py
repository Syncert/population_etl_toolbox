"""Census ACS place slices from capture to silver, against real PostgreSQL.

Covers: ETL-055
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import psycopg2
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.census_acs.silver_census import transform
from data_ingestion_toolbox.census_acs.silver_census.replay import (
    replay_census_capture,
)
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.postgres import PostgresHookStub
from tests.support.warehouse_scope import delete_capture_graph

pytestmark = [pytest.mark.integration, pytest.mark.database]

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "census"
    / "acs5_2023_place_10.json"
)
#: A year no other node uses, so the transform's whole-relation pass reads
#: only these rows; the bytes are the real 2023 response.
YEAR = 2097
#: acs5's period starts four years before the estimate year.
TIME_SK, TIME_DATE = 20930101, "2093-01-01"
#: Arden, Ardencroft and Ardentown: three of Delaware's 79 places. The other
#: 76 are left out of the reference on purpose.
SEEDED = ("01400", "01530", "01660")
UNSEEDED = "25840"
#: The slice ledger refuses a year after next, so its node uses this one.
SLICE_YEAR = 2026


def _capture(
    factory: Callable[[], connection],
    control: CaptureControl,
    payload: bytes,
    at: datetime,
) -> tuple[UUID, UUID]:
    run_id = control.start_run(
        watermark={"dataset": "acs5", "year": YEAR, "geo_level": "place"}
    )
    parameters = {
        "get": "B01003_001E,B01003_001M,B19013_001E,B19013_001M",
        "dataset": "acs5",
        "year": YEAR,
        "geo_level": "place",
        "for": "place:*",
        "in": "state:10",
    }
    request = control.start_request(
        run_id=run_id,
        endpoint="https://api.census.gov/data/2097/acs/acs5",
        parameters=parameters,
    )
    capture_id = uuid4()
    persist_response_capture(
        factory,
        ResponseCapture(
            capture_id=capture_id,
            request_id=request.request_id,
            run_id=run_id,
            source_code="CENSUS_ACS",
            endpoint="https://api.census.gov/data/2097/acs/acs5",
            request_parameters=parameters,
            retrieved_at=at,
            http_status=200,
            response_headers={"content-type": "application/json"},
            media_type="application/json",
            payload=payload,
            payload_schema_version="census-acs-array-v1",
            source_revision=str(YEAR),
        ),
    )
    control.finish_request(request.request_id, status="captured")
    control.finish_run(run_id, status="success")
    return run_id, capture_id


def _rows(
    factory: Callable[[], connection], sql: str, parameters: tuple = ()
) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


@pytest.fixture
def place_warehouse(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[tuple[Callable[[], connection], list[UUID]]]:
    factory = postgres_connection_factory
    writer = factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, TIME_SK, TIME_DATE)
            seed_geography(
                cursor, geo_type="state", state_fips="10", vintage=YEAR, name="Delaware"
            )
            for place in SEEDED:
                seed_geography(
                    cursor,
                    geo_type="place",
                    state_fips="10",
                    place_fips=place,
                    vintage=YEAR,
                    # Not the provider's name: identity is the code.
                    name=f"Fixture place {place}",
                )
        writer.commit()
    finally:
        writer.close()
    monkeypatch.setattr(transform, "_get_hook", lambda: PostgresHookStub(factory))
    monkeypatch.setattr(transform, "_get_approx_row_count", lambda _: 1)
    runs: list[UUID] = []
    yield factory, runs
    cleanup = factory()
    try:
        with cleanup.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_census.fact_demographics WHERE estimate_year = %s",
                (YEAR,),
            )
            cursor.execute(
                "DELETE FROM silver_census.observation_revision WHERE year = %s",
                (YEAR,),
            )
            cursor.execute(
                "DELETE FROM silver_ref.geography_resolution "
                "WHERE provider_source = 'CENSUS_ACS' AND source_vintage = %s",
                (YEAR,),
            )
            cursor.execute(
                "DELETE FROM control.acs_ingestion_slices "
                "WHERE year = %s AND geo_level = 'place' AND state_fips = '10'",
                (SLICE_YEAR,),
            )
            delete_capture_graph(cursor, list(runs))
            for place in SEEDED:
                delete_geography(cursor, f"state:10|place:{place}")
            delete_geography(cursor, "state:10")
            cursor.execute(
                "DELETE FROM silver_ref.dim_time WHERE time_sk = %s", (TIME_SK,)
            )
        cleanup.commit()
    finally:
        cleanup.close()


def test_a_place_slice_replays_into_facts_keyed_to_place_identities(
    place_warehouse,
) -> None:
    """Covers: ETL-055 — places resolve by code; an unknown place is ledgered, not dropped silently."""
    factory, runs = place_warehouse
    control = CaptureControl(factory, source_code="CENSUS_ACS")
    run_id, capture_id = _capture(
        factory, control, FIXTURE.read_bytes(), datetime.now(timezone.utc)
    )
    runs.append(run_id)
    assert (
        replay_census_capture(
            factory, capture_id=capture_id, dataset="acs5", year=YEAR, geo_level="place"
        )
        == 79 * 4
    )

    assert transform.transform_census_to_silver() == len(SEEDED) * 2
    facts = _rows(
        factory,
        """
        SELECT geo_id, geo_level, variable_code, estimate_value, margin_of_error,
               state_fips, county_fips, value_status
        FROM silver_census.fact_demographics
        WHERE estimate_year = %s ORDER BY geo_id, variable_code
        """,
        (YEAR,),
    )
    assert {row[0] for row in facts} == {f"state:10|place:{place}" for place in SEEDED}
    assert {row[1] for row in facts} == {"place"}
    arden = {row[2]: row for row in facts if row[0] == "state:10|place:01400"}
    assert arden["B01003_001"][3:7] == (Decimal("600"), Decimal("276"), "10", None)

    # The place the reference does not carry is in the ledger with the
    # vintage it was requested under, and in no fact.
    ledger = _rows(
        factory,
        """
        SELECT status, reason_code, source_vintage
        FROM silver_ref.geography_resolution
        WHERE provider_source = 'CENSUS_ACS' AND source_code = %s AND source_vintage = %s
        """,
        (f"state:10|place:{UNSEEDED}", YEAR),
    )
    assert ledger == [("unmapped", "canonical_id_not_loaded", YEAR)]
    counts = _rows(
        factory,
        """
        SELECT status, COUNT(*) FROM silver_ref.geography_resolution
        WHERE provider_source = 'CENSUS_ACS' AND source_vintage = %s
          AND source_geo_type = 'place'
        GROUP BY status ORDER BY status
        """,
        (YEAR,),
    )
    assert counts == [("resolved", 3), ("unmapped", 76)]


def test_a_rerun_is_idempotent_and_a_changed_response_keeps_both_checksums(
    place_warehouse,
) -> None:
    """Covers: ETL-055 — the same bytes change nothing; a revision updates the fact."""
    factory, runs = place_warehouse
    control = CaptureControl(factory, source_code="CENSUS_ACS")
    first_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    run_id, capture_id = _capture(factory, control, FIXTURE.read_bytes(), first_at)
    runs.append(run_id)
    replay_census_capture(
        factory, capture_id=capture_id, dataset="acs5", year=YEAR, geo_level="place"
    )
    assert transform.transform_census_to_silver() == len(SEEDED) * 2
    assert transform.transform_census_to_silver() == 0

    document = json.loads(FIXTURE.read_bytes())
    header = document[0]
    for row in document[1:]:
        if row[header.index("place")] == "01400":
            row[header.index("B01003_001E")] = "601"
    revised_run, revised_capture = _capture(
        factory, control, json.dumps(document).encode(), datetime.now(timezone.utc)
    )
    runs.append(revised_run)
    replay_census_capture(
        factory,
        capture_id=revised_capture,
        dataset="acs5",
        year=YEAR,
        geo_level="place",
    )
    # Every seeded fact now names the newer capture (DB-055 lineage); only
    # Arden's population changed value.
    assert transform.transform_census_to_silver() == len(SEEDED) * 2
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM silver_census.fact_demographics
        WHERE estimate_year = %s AND geo_id = 'state:10|place:01530'
          AND variable_code = 'B01003_001' AND estimate_value = 187
        """,
        (YEAR,),
    ) == [(1,)]
    assert _rows(
        factory,
        """
        SELECT estimate_value, capture_id::TEXT FROM silver_census.fact_demographics
        WHERE estimate_year = %s AND geo_id = 'state:10|place:01400' AND variable_code = 'B01003_001'
        """,
        (YEAR,),
    ) == [(Decimal("601"), str(revised_capture))]
    checksums = _rows(
        factory,
        "SELECT COUNT(DISTINCT payload_checksum) FROM raw_capture.response_capture WHERE capture_id = ANY(%s::UUID[])",
        ([str(capture_id), str(revised_capture)],),
    )
    assert checksums == [(2,)]


def test_the_slice_ledger_admits_a_place_slice_only_with_its_state(
    place_warehouse,
) -> None:
    """Covers: ETL-055 — migration 031: a place slice, like a county slice, names one state."""
    factory, _runs = place_warehouse
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.acs_ingestion_slices (dataset, year, geo_level, state_fips, status)
                VALUES ('acs5', %s, 'place', '10', 'planned')
                """,
                (SLICE_YEAR,),
            )
        writer.commit()
        with pytest.raises(psycopg2.errors.CheckViolation):
            with writer.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.acs_ingestion_slices (dataset, year, geo_level, status)
                    VALUES ('acs5', %s, 'place', 'planned')
                    """,
                    (SLICE_YEAR,),
                )
        writer.rollback()
    finally:
        writer.close()
