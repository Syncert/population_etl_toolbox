"""Real PostgreSQL capture-to-replay contract for Census PEP."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.census_pep.config import CONFIG
from data_ingestion_toolbox.census_pep.silver_pep.replay import replay_pep_capture
from data_ingestion_toolbox.census_pep.silver_pep.transform import (
    transform_pep_to_silver,
)
from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from tests.support.capture_seed import delete_geography
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]

FIXTURE = (
    Path(__file__).resolve().parents[2] / "fixtures" / "census_pep" / "nst_2025.csv"
)


@dataclass
class PepDatabaseScope:
    """Track committed PEP fixture state for foreign-key-safe test cleanup."""

    captures: list[ResponseCapture] = field(default_factory=list)
    event_ids: set[UUID] = field(default_factory=set)
    geo_ids: set[str] = field(default_factory=set)
    request_ids: set[UUID] = field(default_factory=set)
    run_ids: set[UUID] = field(default_factory=set)

    def track_run(self, run_id: UUID) -> None:
        self.run_ids.add(run_id)

    def track_request(self, request_id: UUID) -> None:
        self.request_ids.add(request_id)

    def track_capture(self, capture: ResponseCapture) -> None:
        self.captures.append(capture)


@pytest.fixture
def pep_database_scope(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> PepDatabaseScope:
    """Remove every committed row owned by one PEP integration test."""
    scope = PepDatabaseScope()

    def cleanup() -> None:
        capture_ids = [capture.capture_id for capture in scope.captures]
        payload_checksums: list[str] = []
        database_connection = postgres_connection_factory()
        try:
            with database_connection.cursor() as cursor:
                if scope.event_ids:
                    cursor.execute(
                        "DELETE FROM control.publisher_ready_event "
                        "WHERE event_id = ANY(%s)",
                        (list(scope.event_ids),),
                    )
                if capture_ids:
                    cursor.execute(
                        "DELETE FROM silver_ref.geography_resolution "
                        "WHERE evidence_capture_id = ANY(%s)",
                        (capture_ids,),
                    )
                    cursor.execute(
                        "DELETE FROM silver_pep.fact_population_estimate "
                        "WHERE capture_id = ANY(%s)",
                        (capture_ids,),
                    )
                    cursor.execute(
                        "DELETE FROM silver_pep.release_load "
                        "WHERE capture_id = ANY(%s)",
                        (capture_ids,),
                    )
                    cursor.execute(
                        "DELETE FROM silver_pep.observation_revision "
                        "WHERE capture_id = ANY(%s)",
                        (capture_ids,),
                    )
                    cursor.execute(
                        "DELETE FROM control.capture_quarantine "
                        "WHERE capture_id = ANY(%s)",
                        (capture_ids,),
                    )
                    cursor.execute(
                        "SELECT DISTINCT payload_checksum "
                        "FROM raw_capture.response_capture "
                        "WHERE capture_id = ANY(%s)",
                        (capture_ids,),
                    )
                    payload_checksums = [row[0] for row in cursor.fetchall()]
                    cursor.execute(
                        "ALTER TABLE raw_capture.response_capture "
                        "DISABLE TRIGGER response_capture_reject_mutation"
                    )
                    cursor.execute(
                        "DELETE FROM raw_capture.response_capture "
                        "WHERE capture_id = ANY(%s)",
                        (capture_ids,),
                    )
                    cursor.execute(
                        "ALTER TABLE raw_capture.response_capture "
                        "ENABLE TRIGGER response_capture_reject_mutation"
                    )
                    if payload_checksums:
                        cursor.execute(
                            "ALTER TABLE raw_capture.payload_blob "
                            "DISABLE TRIGGER payload_blob_reject_mutation"
                        )
                        cursor.execute(
                            "DELETE FROM raw_capture.payload_blob AS payload "
                            "WHERE payload.payload_checksum = ANY(%s) "
                            "AND NOT EXISTS ("
                            "SELECT 1 FROM raw_capture.response_capture AS capture "
                            "WHERE capture.payload_checksum = payload.payload_checksum)",
                            (payload_checksums,),
                        )
                        cursor.execute(
                            "ALTER TABLE raw_capture.payload_blob "
                            "ENABLE TRIGGER payload_blob_reject_mutation"
                        )
                if scope.request_ids:
                    cursor.execute(
                        "DELETE FROM control.ingestion_request "
                        "WHERE request_id = ANY(%s)",
                        (list(scope.request_ids),),
                    )
                if scope.run_ids:
                    cursor.execute(
                        "DELETE FROM control.ingestion_run WHERE run_id = ANY(%s)",
                        (list(scope.run_ids),),
                    )
                for geo_id in sorted(scope.geo_ids):
                    delete_geography(cursor, geo_id)
                if capture_ids:
                    cursor.execute(
                        """
                        SELECT
                            (SELECT COUNT(*) FROM silver_pep.fact_population_estimate
                             WHERE capture_id = ANY(%s))
                          + (SELECT COUNT(*) FROM silver_pep.release_load
                             WHERE capture_id = ANY(%s))
                          + (SELECT COUNT(*) FROM silver_pep.observation_revision
                             WHERE capture_id = ANY(%s))
                          + (SELECT COUNT(*) FROM silver_ref.geography_resolution
                             WHERE evidence_capture_id = ANY(%s))
                          + (SELECT COUNT(*) FROM control.capture_quarantine
                             WHERE capture_id = ANY(%s))
                          + (SELECT COUNT(*) FROM raw_capture.response_capture
                             WHERE capture_id = ANY(%s))
                        """,
                        (capture_ids,) * 6,
                    )
                    assert cursor.fetchone() == (0,)
                if payload_checksums:
                    cursor.execute(
                        "SELECT COUNT(*) FROM raw_capture.payload_blob AS payload "
                        "WHERE payload.payload_checksum = ANY(%s) "
                        "AND NOT EXISTS ("
                        "SELECT 1 FROM raw_capture.response_capture AS capture "
                        "WHERE capture.payload_checksum = payload.payload_checksum)",
                        (payload_checksums,),
                    )
                    assert cursor.fetchone() == (0,)
                if scope.request_ids:
                    cursor.execute(
                        "SELECT COUNT(*) FROM control.ingestion_request "
                        "WHERE request_id = ANY(%s)",
                        (list(scope.request_ids),),
                    )
                    assert cursor.fetchone() == (0,)
                if scope.run_ids:
                    cursor.execute(
                        "SELECT COUNT(*) FROM control.ingestion_run "
                        "WHERE run_id = ANY(%s)",
                        (list(scope.run_ids),),
                    )
                    assert cursor.fetchone() == (0,)
                if scope.event_ids:
                    cursor.execute(
                        "SELECT COUNT(*) FROM control.publisher_ready_event "
                        "WHERE event_id = ANY(%s)",
                        (list(scope.event_ids),),
                    )
                    assert cursor.fetchone() == (0,)
                if scope.geo_ids:
                    cursor.execute(
                        "SELECT COUNT(*) FROM silver_ref.dim_geo_entity "
                        "WHERE geo_id = ANY(%s)",
                        (list(scope.geo_ids),),
                    )
                    assert cursor.fetchone() == (0,)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    request.addfinalizer(cleanup)
    return scope


def test_pep_fixture_capture_replays_idempotently(
    postgres_connection_factory: Callable[[], connection],
    pep_database_scope: PepDatabaseScope,
) -> None:
    """Covers: ARC-002, DB-003 — PEP replay stays capture scoped and rerunnable."""
    release = next(
        item
        for item in CONFIG.releases
        if item.dataset_code == "pep_nst_alldata" and item.vintage_year == 2025
    )
    control = CaptureControl(
        postgres_connection_factory,
        source_code=CONFIG.source_code,
    )
    run_id = control.start_run(watermark={"product_code": release.product_code})
    pep_database_scope.track_run(run_id)
    parameters = {
        "dataset_code": release.dataset_code,
        "vintage_year": release.vintage_year,
        "product_code": release.product_code,
    }
    request = control.start_request(
        run_id=run_id,
        endpoint=release.data_url,
        parameters=parameters,
    )
    pep_database_scope.track_request(request.request_id)
    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=request.request_id,
        run_id=run_id,
        source_code=CONFIG.source_code,
        endpoint=release.data_url,
        request_parameters=parameters,
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "text/csv"},
        media_type=release.media_type,
        payload=FIXTURE.read_bytes(),
        payload_schema_version=release.schema_version,
        source_revision=release.product_code,
    )
    persist_response_capture(postgres_connection_factory, capture)
    pep_database_scope.track_capture(capture)
    control.finish_request(request.request_id, status="captured")
    control.finish_run(run_id, status="success")

    first_count = replay_pep_capture(
        postgres_connection_factory,
        capture_id=capture.capture_id,
        release=release,
    )
    second_count = replay_pep_capture(
        postgres_connection_factory,
        capture_id=capture.capture_id,
        release=release,
    )

    assert first_count > 0
    assert second_count == 0
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT release_vintage, observation_year, value
                FROM silver_pep.observation_revision
                WHERE capture_id = %s
                  AND metric_code = 'POPESTIMATE'
                  AND observation_year = 2024
                """,
                (capture.capture_id,),
            )
            assert cursor.fetchone() == (2025, 2024, 340003797)
    finally:
        reader.close()


def _capture_fixture(
    connection_factory: Callable[[], connection],
    *,
    database_scope: PepDatabaseScope,
    dataset_code: str,
    vintage_year: int,
    fixture_name: str,
) -> ResponseCapture:
    release = next(
        item
        for item in CONFIG.releases
        if item.dataset_code == dataset_code and item.vintage_year == vintage_year
    )
    control = CaptureControl(connection_factory, source_code=CONFIG.source_code)
    run_id = control.start_run(watermark={"product_code": release.product_code})
    database_scope.track_run(run_id)
    parameters = {
        "dataset_code": dataset_code,
        "vintage_year": vintage_year,
        "product_code": release.product_code,
    }
    request = control.start_request(
        run_id=run_id,
        endpoint=release.data_url,
        parameters=parameters,
    )
    database_scope.track_request(request.request_id)
    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=request.request_id,
        run_id=run_id,
        source_code=CONFIG.source_code,
        endpoint=release.data_url,
        request_parameters=parameters,
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "text/csv"},
        media_type=release.media_type,
        payload=(FIXTURE.parent / fixture_name).read_bytes(),
        payload_schema_version=release.schema_version,
        source_revision=release.product_code,
    )
    persist_response_capture(connection_factory, capture)
    database_scope.track_capture(capture)
    replay_pep_capture(
        connection_factory,
        capture_id=capture.capture_id,
        release=release,
    )
    control.finish_request(request.request_id, status="captured")
    control.finish_run(run_id, status="success")
    return capture


def test_pep_two_vintages_and_place_publish_without_losing_revision_history(
    postgres_connection_factory: Callable[[], connection],
    pep_database_scope: PepDatabaseScope,
) -> None:
    """Covers: DB-003 — revision/latest and canonical place contracts coexist."""
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_ref.dim_geo_entity (
                    geo_id, geo_type, state_fips, place_fips,
                    first_seen_version, last_seen_version
                ) VALUES
                    ('us:1', 'nation', NULL, NULL, 2020, 2025),
                    ('state:01|place:00124', 'place', '01', '00124', 2020, 2025)
                ON CONFLICT (geo_id) DO NOTHING
                RETURNING geo_id
                """
            )
            pep_database_scope.geo_ids.update(row[0] for row in cursor.fetchall())
        writer.commit()
    finally:
        writer.close()

    _capture_fixture(
        postgres_connection_factory,
        database_scope=pep_database_scope,
        dataset_code="pep_nst_alldata",
        vintage_year=2024,
        fixture_name="nst_2024.csv",
    )
    current_capture = _capture_fixture(
        postgres_connection_factory,
        database_scope=pep_database_scope,
        dataset_code="pep_nst_alldata",
        vintage_year=2025,
        fixture_name="nst_2025.csv",
    )
    _capture_fixture(
        postgres_connection_factory,
        database_scope=pep_database_scope,
        dataset_code="pep_subcounty",
        vintage_year=2025,
        fixture_name="subcounty_2025.csv",
    )

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_pep.observation_revision (
                    capture_id, source_row_index, source_column_index,
                    source_header, dataset_code, release_vintage,
                    product_code, observation_year, metric_code, unit,
                    summary_level, state_fips_source, name_source,
                    value_source, value, value_status
                ) VALUES (
                    %s, 99, 99, 'POPESTIMATE2025', 'pep_nst_alldata', 2025,
                    'NST-EST2025-ALLDATA', 2025, 'POPESTIMATE', 'persons',
                    '040', '99', 'Missing test state', '1', 1, 'valid'
                )
                ON CONFLICT (capture_id, source_row_index, source_column_index)
                DO NOTHING
                """,
                (current_capture.capture_id,),
            )
        writer.commit()
    finally:
        writer.close()

    first_insert = transform_pep_to_silver(
        PostgresHookStub(postgres_connection_factory)
    )
    second_insert = transform_pep_to_silver(
        PostgresHookStub(postgres_connection_factory)
    )

    assert first_insert > 0
    assert second_insert == 0
    event_id = emit_latest_publisher_ready(
        postgres_connection_factory,
        publisher_schema="gold_pep",
    )
    assert event_id is not None
    pep_database_scope.event_ids.add(event_id)
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT pep_vintage, value
                FROM gold_pep.population_estimate_revision
                WHERE dataset_code = 'pep_nst_alldata'
                  AND metric_code = 'POPESTIMATE'
                  AND observation_year = 2024
                  AND geo_id = 'us:1'
                ORDER BY pep_vintage
                """
            )
            assert cursor.fetchall() == [(2024, 340110988), (2025, 340003797)]
            cursor.execute(
                """
                SELECT pep_vintage, value
                FROM gold_pep.population_estimate_latest
                WHERE dataset_code = 'pep_nst_alldata'
                  AND metric_code = 'POPESTIMATE'
                  AND observation_year = 2024
                  AND geo_id = 'us:1'
                """
            )
            assert cursor.fetchone() == (2025, 340003797)
            cursor.execute(
                """
                SELECT geo_id, geo_type, functional_status_source, value
                FROM gold_pep.population_estimate_latest
                WHERE dataset_code = 'pep_subcounty'
                  AND metric_code = 'POPESTIMATE'
                  AND observation_year = 2025
                """
            )
            assert cursor.fetchone() == (
                "state:01|place:00124",
                "place",
                "A",
                2378,
            )
            cursor.execute(
                """
                SELECT status, resolution_method
                FROM silver_ref.geography_resolution
                WHERE provider_source = 'CENSUS_PEP'
                  AND provider_dataset = 'pep_subcounty'
                  AND source_geo_type = 'place'
                  AND source_code = '0100124'
                  AND source_vintage = 2025
                """
            )
            assert cursor.fetchone() == ("resolved", "exact_code")
            cursor.execute(
                """
                SELECT status, reason_code
                FROM silver_ref.geography_resolution
                WHERE provider_source = 'CENSUS_PEP'
                  AND provider_dataset = 'pep_nst_alldata'
                  AND source_geo_type = 'state'
                  AND source_code = '99'
                  AND source_vintage = 2025
                """
            )
            assert cursor.fetchone() == (
                "unmapped",
                "canonical_geography_absent",
            )
            cursor.execute(
                """
                SELECT source_code, status
                FROM control.publisher_ready_event
                WHERE event_id = %s
                """,
                (event_id,),
            )
            assert cursor.fetchone() == ("CENSUS_PEP", "pending")
    finally:
        reader.close()


def test_overlapping_products_resolve_to_one_published_value(
    postgres_connection_factory: Callable[[], connection],
    pep_database_scope: PepDatabaseScope,
) -> None:
    """Covers: ETL-044 — one row per measure, geography and year, across products.

    PEP publishes overlapping files. The state and county products both carry
    state rows, and consecutive decades both carry their shared seam year, so
    ranking within a dataset kept every one of them: a state read answered
    twice per period and July 2020 answered once per decade. The precedence
    rule picks exactly one and the revision surface still shows all of them.
    """
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_ref.dim_geo_entity (
                    geo_id, geo_type, state_fips, county_fips,
                    first_seen_version, last_seen_version
                ) VALUES
                    ('us:1', 'nation', NULL, NULL, 2000, 2025),
                    ('state:01', 'state', '01', NULL, 2000, 2025),
                    ('state:01|county:001', 'county', '01', '001', 2000, 2025)
                ON CONFLICT (geo_id) DO NOTHING
                RETURNING geo_id
                """
            )
            pep_database_scope.geo_ids.update(row[0] for row in cursor.fetchall())
        writer.commit()
    finally:
        writer.close()

    # The same decade published twice: the county file rolls Alabama up from
    # its counties, the state file publishes Alabama in its own right.
    _capture_fixture(
        postgres_connection_factory,
        database_scope=pep_database_scope,
        dataset_code="pep_county_alldata_2010s",
        vintage_year=2020,
        fixture_name="co_2010s.csv",
    )
    _capture_fixture(
        postgres_connection_factory,
        database_scope=pep_database_scope,
        dataset_code="pep_nst_alldata_2010s",
        vintage_year=2020,
        fixture_name="nst_2010s.csv",
    )
    # The seam: July 2020 is the last year of the 2010s series and the first
    # of the 2020s one, revised between them.
    _capture_fixture(
        postgres_connection_factory,
        database_scope=pep_database_scope,
        dataset_code="pep_nst_alldata",
        vintage_year=2025,
        fixture_name="nst_2025.csv",
    )

    transform_pep_to_silver(PostgresHookStub(postgres_connection_factory))

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            # Both products published Alabama for 2015, with the same value.
            cursor.execute(
                """
                SELECT dataset_code, summary_level, value
                FROM gold_pep.population_estimate_revision
                WHERE metric_code = 'POPESTIMATE'
                  AND geo_id = 'state:01'
                  AND observation_year = 2015
                ORDER BY dataset_code
                """
            )
            assert cursor.fetchall() == [
                ("pep_county_alldata_2010s", "040", 4854803),
                ("pep_nst_alldata_2010s", "040", 4854803),
            ]

            # Exactly one survives, and it is the file that publishes a state
            # in its own right rather than as a rollup of its counties.
            cursor.execute(
                """
                SELECT dataset_code, value
                FROM gold_pep.population_estimate_latest
                WHERE metric_code = 'POPESTIMATE'
                  AND geo_id = 'state:01'
                  AND observation_year = 2015
                """
            )
            assert cursor.fetchall() == [("pep_nst_alldata_2010s", 4854803)]

            # The seam year is published by both decades, and revised between
            # them; the later vintage is the currently published value.
            cursor.execute(
                """
                SELECT pep_vintage, value
                FROM gold_pep.population_estimate_revision
                WHERE metric_code = 'POPESTIMATE'
                  AND geo_id = 'us:1'
                  AND observation_year = 2020
                ORDER BY pep_vintage
                """
            )
            assert cursor.fetchall() == [(2020, 329484123), (2025, 331578104)]
            cursor.execute(
                """
                SELECT dataset_code, pep_vintage, value
                FROM gold_pep.population_estimate_latest
                WHERE metric_code = 'POPESTIMATE'
                  AND geo_id = 'us:1'
                  AND observation_year = 2020
                """
            )
            assert cursor.fetchall() == [("pep_nst_alldata", 2025, 331578104)]

            # No geography, measure and year is answered twice any more.
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT metric_code, geo_id, observation_year
                    FROM gold_pep.population_estimate_latest
                    GROUP BY metric_code, geo_id, observation_year
                    HAVING COUNT(*) > 1
                ) AS duplicated
                """
            )
            assert cursor.fetchone() == (0,)

            # The April enumeration and the July estimate share a year and
            # stay apart: different measures, different dates, both published.
            cursor.execute(
                """
                SELECT metric_code, estimate_date, value
                FROM gold_pep.population_estimate_latest
                WHERE geo_id = 'state:01'
                  AND observation_year = 2010
                  AND metric_code IN ('CENSUSPOP', 'POPESTIMATE')
                ORDER BY metric_code
                """
            )
            rows = cursor.fetchall()
            assert [(row[0], row[1].isoformat(), row[2]) for row in rows] == [
                ("CENSUSPOP", "2010-04-01", 4779736),
                ("POPESTIMATE", "2010-07-01", 4785514),
            ]

            # Coverage is read from the facts, per measure.
            cursor.execute(
                """
                SELECT first_period, last_period
                FROM gold_pep.measure_export
                WHERE source_object_key = 'POPESTIMATE'
                """
            )
            first_period, last_period = cursor.fetchone()
            assert first_period.isoformat() == "2010-07-01"
            assert last_period.isoformat() == "2025-07-01"
    finally:
        reader.close()


def test_one_county_series_spans_every_registered_decade(
    postgres_connection_factory: Callable[[], connection],
    pep_database_scope: PepDatabaseScope,
) -> None:
    """Covers: ETL-046 — a county's published history reaches back to 1970.

    Six products, four file layouts and three readers, resolved into one
    series per measure. Autauga County is deliberate: it has existed
    unchanged across every decade here, so what this asserts is the pipeline
    rather than a boundary change.
    """
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_ref.dim_geo_entity (
                    geo_id, geo_type, state_fips, county_fips,
                    first_seen_version, last_seen_version
                ) VALUES
                    ('state:01|county:001', 'county', '01', '001', 1970, 2025)
                ON CONFLICT (geo_id) DO NOTHING
                RETURNING geo_id
                """
            )
            pep_database_scope.geo_ids.update(row[0] for row in cursor.fetchall())
        writer.commit()
    finally:
        writer.close()

    for dataset_code, vintage_year, fixture_name in (
        ("pep_county_totals_1970s", 1982, "legacy_table_1970s.txt"),
        ("pep_county_totals_1980s", 1992, "legacy_table_1980s.txt"),
        ("pep_county_totals_1990s", 1999, "legacy_cells_1990s.txt"),
        ("pep_county_alldata_2000s", 2009, "co_2000s.csv"),
        ("pep_county_intercensal_2000s", 2016, "co_intercensal_2000s.csv"),
        ("pep_county_alldata_2010s", 2020, "co_2010s.csv"),
        ("pep_county_alldata", 2025, "co_2020s.csv"),
    ):
        _capture_fixture(
            postgres_connection_factory,
            database_scope=pep_database_scope,
            dataset_code=dataset_code,
            vintage_year=vintage_year,
            fixture_name=fixture_name,
        )

    transform_pep_to_silver(PostgresHookStub(postgres_connection_factory))

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT observation_year
                FROM gold_pep.population_estimate_latest
                WHERE metric_code = 'POPESTIMATE'
                  AND geo_id = 'state:01|county:001'
                ORDER BY observation_year
                """
            )
            years = [row[0] for row in cursor.fetchall()]

            # One row per year, no year answered twice, across six products.
            assert len(years) == len(set(years))
            assert years[0] == 1971
            assert years[-1] == 2025
            # July 1980 is absent because both printed tables treat April
            # 1980 as the decade boundary: the 1970s table ends at 1979 and
            # the 1980s table opens on the census rather than an estimate.
            # That is a gap in what the Bureau published here, so it is left
            # as one rather than filled by interpolation.
            assert set(range(1971, 2026)) - set(years) == {1980}

            # The decennial counts sit beside the estimates, dated to April.
            cursor.execute(
                """
                SELECT observation_year, estimate_date
                FROM gold_pep.population_estimate_latest
                WHERE metric_code = 'CENSUSPOP'
                  AND geo_id = 'state:01|county:001'
                ORDER BY observation_year
                """
            )
            counts = cursor.fetchall()
            assert [row[0] for row in counts] == [1970, 1980, 2000, 2010]
            assert {row[1].month for row in counts} == {4}

            # The 2000s are published twice: postcensal during the decade,
            # then intercensal once both censuses could close it. The
            # intercensal series wins, whatever the vintages say, because it
            # is the Bureau's settled answer rather than a projection.
            cursor.execute(
                """
                SELECT dataset_code, value
                FROM gold_pep.population_estimate_revision
                WHERE metric_code = 'POPESTIMATE'
                  AND geo_id = 'state:01|county:001'
                  AND observation_year = 2005
                ORDER BY dataset_code
                """
            )
            assert len(cursor.fetchall()) == 2
            cursor.execute(
                """
                SELECT dataset_code
                FROM gold_pep.population_estimate_latest
                WHERE metric_code = 'POPESTIMATE'
                  AND geo_id = 'state:01|county:001'
                  AND observation_year = 2005
                """
            )
            assert cursor.fetchall() == [("pep_county_intercensal_2000s",)]

            # The one overlapping year resolves to the later publication.
            cursor.execute(
                """
                SELECT dataset_code, pep_vintage
                FROM gold_pep.population_estimate_latest
                WHERE metric_code = 'POPESTIMATE'
                  AND geo_id = 'state:01|county:001'
                  AND observation_year = 2020
                """
            )
            assert cursor.fetchall() == [("pep_county_alldata", 2025)]

            # Each product is still readable as what it published.
            cursor.execute(
                """
                SELECT DISTINCT dataset_code
                FROM gold_pep.population_estimate_revision
                WHERE geo_id = 'state:01|county:001'
                ORDER BY dataset_code
                """
            )
            assert [row[0] for row in cursor.fetchall()] == [
                "pep_county_alldata",
                "pep_county_alldata_2000s",
                "pep_county_alldata_2010s",
                "pep_county_intercensal_2000s",
                "pep_county_totals_1970s",
                "pep_county_totals_1980s",
                "pep_county_totals_1990s",
            ]

            # Components begin where the Bureau began publishing them.
            cursor.execute(
                """
                SELECT MIN(observation_year), MAX(observation_year)
                FROM gold_pep.population_estimate_latest
                WHERE metric_code = 'BIRTHS'
                  AND geo_id = 'state:01|county:001'
                """
            )
            assert cursor.fetchone() == (2000, 2025)

            # Every reader that produced a row is recorded against it.
            cursor.execute(
                """
                SELECT DISTINCT parser_version
                FROM silver_pep.observation_revision
                ORDER BY parser_version
                """
            )
            assert [row[0] for row in cursor.fetchall()] == [
                "census-pep-bulk-csv-v1",
                "census-pep-fixed-width-cells-v1",
                "census-pep-fixed-width-table-v1",
            ]
    finally:
        reader.close()
