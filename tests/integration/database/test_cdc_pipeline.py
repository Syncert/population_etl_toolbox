"""Real PostgreSQL CDC capture-to-gold deployment contract."""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.cdc.gold_cdc.publisher import publish_release
from data_ingestion_toolbox.cdc.registry import CDI_ASSET, PLACES_COUNTY_ASSET
from data_ingestion_toolbox.cdc.silver_cdc.replay import (
    persist_replay_result,
    replay_captured_run,
)
from data_ingestion_toolbox.cdc.silver_cdc.transform import transform_release
from apps.api.registry import GEO_GRAINS
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.cdc_release import CDC_FIXTURE_DIR, persist_fixture_release

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_cdc_fixtures_replay_reconcile_and_publish_idempotently(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> None:
    """Covers: ARC-002, DB-003 — CDC releases reach gold without loss."""
    tracked_geo_ids = {
        "us:1",
        "state:01",
        "state:01|county:001",
        "state:48|county:301",
    }
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(tracked_geo_ids),),
            )
            preexisting_geo_ids = {row[0] for row in cursor.fetchall()}
    finally:
        reader.close()

    def cleanup() -> None:
        database_connection = postgres_connection_factory()
        try:
            with database_connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM control.publisher_ready_event "
                    "WHERE source_code = 'CDC'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution "
                    "WHERE provider_source = 'CDC'"
                )
                cursor.execute("DELETE FROM silver_cdc.fact_health_observation")
                cursor.execute("DELETE FROM silver_cdc.observation_revision")
                cursor.execute("DELETE FROM silver_cdc.observation_quarantine")
                cursor.execute("DELETE FROM silver_cdc.dim_measure")
                cursor.execute("DELETE FROM silver_cdc.dim_stratum")
                cursor.execute("DELETE FROM silver_cdc.dim_dataset_release")
                cursor.execute("DELETE FROM control.cdc_dataset_release")
                for geo_id in sorted(tracked_geo_ids - preexisting_geo_ids):
                    delete_geography(cursor, geo_id)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    request.addfinalizer(cleanup)
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            seed_geography(
                cursor, geo_type="nation", vintage=2020, name="United States"
            )
            seed_geography(
                cursor,
                geo_type="state",
                state_fips="01",
                vintage=2020,
                name="Alabama",
            )
            seed_geography(
                cursor,
                geo_type="county",
                state_fips="01",
                county_fips="001",
                vintage=2020,
                name="Autauga County",
            )
            seed_geography(
                cursor,
                geo_type="county",
                state_fips="48",
                county_fips="301",
                vintage=2020,
                name="Loving County",
            )
        writer.commit()
    finally:
        writer.close()

    fixtures = (
        (CDI_ASSET, "cdi_metadata.json", "cdi_observations.json"),
        (
            PLACES_COUNTY_ASSET,
            "places_county_metadata.json",
            "places_county_observations.json",
        ),
    )
    for asset, metadata_name, observations_name in fixtures:
        release = persist_fixture_release(
            postgres_connection_factory,
            asset=asset,
            metadata_name=metadata_name,
            observations_name=observations_name,
        )
        result = replay_captured_run(
            postgres_connection_factory,
            run_id=release.run_id,
            asset=asset,
            release_watermark=release.metadata.release_version,
        )
        persist_replay_result(
            postgres_connection_factory,
            run_id=release.run_id,
            asset=asset,
            release_watermark=release.metadata.release_version,
            result=result,
        )
        persist_replay_result(
            postgres_connection_factory,
            run_id=release.run_id,
            asset=asset,
            release_watermark=release.metadata.release_version,
            result=result,
        )
        transformed = transform_release(
            postgres_connection_factory,
            run_id=release.run_id,
            asset=asset,
            release_watermark=release.metadata.release_version,
        )
        published = publish_release(
            postgres_connection_factory,
            run_id=release.run_id,
            asset_id=asset.asset_id,
            release_watermark=release.metadata.release_version,
        )
        assert transformed == result.input_count
        assert published == result.input_count

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT asset_id, COUNT(*),
                       COUNT(*) FILTER (WHERE value_status = 'suppressed'),
                       COUNT(*) FILTER (WHERE geography_status = 'resolved')
                FROM gold_cdc.health_observation
                GROUP BY asset_id
                ORDER BY asset_id
                """
            )
            assert cursor.fetchall() == [
                ("cdi", 3, 0, 3),
                ("places_county", 4, 1, 4),
            ]
            cursor.execute(
                """
                SELECT DISTINCT estimate_method, population_basis
                FROM gold_cdc.health_observation
                WHERE asset_id = 'places_county'
                """
            )
            assert cursor.fetchone() == (
                "model_based_small_area_estimate",
                "adults age 18 years and older",
            )
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM control.publisher_ready_event
                WHERE source_code = 'CDC'
                """
            )
            assert cursor.fetchone() == (2,)
    finally:
        reader.close()


#: Shapes `jsonb` accepts and `CdcObservation.strata` -- declared `list[Any]`
#: -- cannot be validated from. Every one of them reached the response model
#: as an unhandled exception before the shape was constrained at the write.
NON_ARRAY_STRATA = (
    '{"overall": "overall"}',
    '"overall"',
    "12",
    "null",
    "true",
)


def _minimal_revision_columns(
    capture_id, run_id, stratum_id: str, strata: str
) -> tuple[str, tuple]:
    """One `silver_cdc.observation_revision` row, every NOT NULL column filled."""
    statement = """
        INSERT INTO silver_cdc.observation_revision (
            capture_id, source_row_index, run_id, asset_id, release_watermark,
            source_record_id, source_record, measure_id, measure_label, topic,
            period_start, period_end, geo_source_code, geo_type, value_status,
            value_type_id, value_type_label, adjustment_status, stratum_id,
            strata, estimate_method, population_basis
        ) VALUES (
            %s, 0, %s, 'cdi', '3975004801',
            %s, '{}'::jsonb, 'SHAPE', 'Shape measure', 'Shape topic',
            2096, 2096, 'US', 'nation', 'missing',
            'crude', 'Crude prevalence', 'crude', %s,
            %s::jsonb, 'model-based', 'adults'
        )
    """
    return statement, (capture_id, run_id, stratum_id, stratum_id, strata)


def test_a_stratum_that_is_not_a_json_array_cannot_be_written(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-033 — the warehouse refuses a stratum the API cannot serve.

    `/api/v1/cdc/observations` publishes a stratum with every value, and
    `CdcObservation.strata` is `list[Any]`. The parser produces one -- a tuple
    of `(category, category_label, value, value_label)` tuples that psycopg2
    stores as an array of arrays -- but `jsonb` accepts an object, a string, a
    number, or `null` just as happily, and neither the column nor the service
    said otherwise. A stratum stored as an object was accepted by every write
    path and then crashed the read path with a pydantic `ValidationError`,
    which the caller sees as `500 The API failed to complete this request`
    with no way to tell which row is unserveable.

    So the shape is decided where it is written. Both relations that store a
    stratum are checked: `observation_revision` is where a replay lands it,
    and `dim_stratum` is what the gold view -- and therefore the API -- reads.
    """
    from psycopg2.errors import CheckViolation

    from tests.support.capture_seed import seed_capture

    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            capture_id = seed_capture(cursor, "CDC")
            cursor.execute(
                "SELECT run_id FROM raw_capture.response_capture WHERE capture_id = %s",
                (capture_id,),
            )
            run_id = cursor.fetchone()[0]
        database_connection.commit()

        stratum_id = "f" * 64
        for strata in NON_ARRAY_STRATA:
            with database_connection.cursor() as cursor:
                with pytest.raises(CheckViolation):
                    cursor.execute(
                        "INSERT INTO silver_cdc.dim_stratum (stratum_id, strata) "
                        "VALUES (%s, %s::jsonb)",
                        (stratum_id, strata),
                    )
            database_connection.rollback()

            statement, values = _minimal_revision_columns(
                capture_id, run_id, stratum_id, strata
            )
            with database_connection.cursor() as cursor:
                with pytest.raises(CheckViolation):
                    cursor.execute(statement, values)
            database_connection.rollback()

        # The shape the parser actually produces is accepted, so the guard
        # refuses a wrong shape rather than the column's whole vocabulary.
        with database_connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO silver_cdc.dim_stratum (stratum_id, strata) "
                'VALUES (%s, \'[["OVERALL","Overall","OVR","Overall"]]\'::jsonb)',
                (stratum_id,),
            )
            cursor.execute(
                "DELETE FROM silver_cdc.dim_stratum WHERE stratum_id = %s",
                (stratum_id,),
            )
        database_connection.commit()
    finally:
        database_connection.close()


def test_an_unsupported_geography_is_kept_but_not_served(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> None:
    """Covers: DB-035 — the row stays in silver and leaves the served surface.

    `gold_fbi.crime_observation` has excluded unresolved geographies since
    011. `gold_cdc.health_observation` filtered on the release status alone,
    so a provider location outside the served vocabulary was paged out of
    `/observations` with `geo_id: null` and a `geo_level` the five-word
    vocabulary does not name — and the publisher, which aggregates the grain
    of every fact row, advertised `UNSUPPORTED` as a grain a client could send
    back.
    """

    def cleanup() -> None:
        database_connection = postgres_connection_factory()
        try:
            with database_connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM control.publisher_ready_event "
                    "WHERE source_code = 'CDC'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution "
                    "WHERE provider_source = 'CDC'"
                )
                cursor.execute("DELETE FROM silver_cdc.fact_health_observation")
                cursor.execute("DELETE FROM silver_cdc.observation_revision")
                cursor.execute("DELETE FROM silver_cdc.observation_quarantine")
                cursor.execute("DELETE FROM silver_cdc.dim_measure")
                cursor.execute("DELETE FROM silver_cdc.dim_stratum")
                cursor.execute("DELETE FROM silver_cdc.dim_dataset_release")
                cursor.execute("DELETE FROM control.cdc_dataset_release")
                delete_geography(cursor, "us:1")
                delete_geography(cursor, "state:01")
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    request.addfinalizer(cleanup)
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            seed_geography(
                cursor, geo_type="nation", vintage=2020, name="United States"
            )
            seed_geography(
                cursor,
                geo_type="state",
                state_fips="01",
                vintage=2020,
                name="Alabama",
            )
        writer.commit()
    finally:
        writer.close()

    # One reviewed CDI release, with one row's provider location changed to a
    # code the adapter does not model. CDC publishes such rows (territories,
    # regions, sub-state areas) and the parser marks them `unsupported`
    # rather than coercing them into a state.
    observations = json.loads(
        (CDC_FIXTURE_DIR / "cdi_observations.json").read_text(encoding="utf-8")
    )
    observations[-1]["locationid"] = "999"
    observations[-1]["locationabbr"] = "RGN"
    observations[-1]["locationdesc"] = "Southeast region"
    release = persist_fixture_release(
        postgres_connection_factory,
        asset=CDI_ASSET,
        metadata_name="cdi_metadata.json",
        observations_name="cdi_observations.json",
        observations_payload=json.dumps(observations).encode("utf-8"),
    )
    result = replay_captured_run(
        postgres_connection_factory,
        run_id=release.run_id,
        asset=CDI_ASSET,
        release_watermark=release.metadata.release_version,
    )
    persist_replay_result(
        postgres_connection_factory,
        run_id=release.run_id,
        asset=CDI_ASSET,
        release_watermark=release.metadata.release_version,
        result=result,
    )
    transform_release(
        postgres_connection_factory,
        run_id=release.run_id,
        asset=CDI_ASSET,
        release_watermark=release.metadata.release_version,
    )
    publish_release(
        postgres_connection_factory,
        run_id=release.run_id,
        asset_id=CDI_ASSET.asset_id,
        release_watermark=release.metadata.release_version,
    )

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            # Kept: the fact table holds every captured row, unsupported
            # geography included, and the resolution ledger says why.
            cursor.execute(
                """
                SELECT geography_status, COUNT(*)
                FROM silver_cdc.fact_health_observation
                GROUP BY geography_status
                ORDER BY geography_status
                """
            )
            assert dict(cursor.fetchall()) == {"resolved": 2, "unsupported": 1}
            cursor.execute(
                """
                SELECT status, reason_code, geo_sk
                FROM silver_ref.geography_resolution
                WHERE provider_source = 'CDC' AND status = 'unsupported'
                """
            )
            ledger = cursor.fetchall()
            assert ledger, "the ledger is where an unresolved geography lives"
            for status, reason_code, geo_sk in ledger:
                assert (status, reason_code, geo_sk) == (
                    "unsupported",
                    "unsupported_provider_code",
                    None,
                )

            # Not served: neither the row nor its grain.
            cursor.execute(
                """
                SELECT COUNT(*),
                       COUNT(*) FILTER (WHERE geo_id IS NULL)
                FROM gold_cdc.health_observation
                """
            )
            assert cursor.fetchone() == (2, 0)
            cursor.execute(
                "SELECT DISTINCT geography_status FROM gold_cdc.health_observation"
            )
            assert cursor.fetchall() == [("resolved",)]
            cursor.execute(
                """
                SELECT DISTINCT UNNEST(valid_geo_grains)
                FROM gold_cdc.metric_publisher
                ORDER BY 1
                """
            )
            grains = {row[0] for row in cursor.fetchall()}
            assert grains and grains <= set(GEO_GRAINS), grains
            assert "UNSUPPORTED" not in grains
    finally:
        reader.close()
