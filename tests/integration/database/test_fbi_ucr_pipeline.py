"""Real PostgreSQL FBI UCR capture-to-gold deployment contract."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fbi_ucr.capture import CapturedFbiRelease
from data_ingestion_toolbox.fbi_ucr.gold_fbi.publisher import (
    FbiPublicationError,
    publish_release,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.replay import (
    FbiReplayError,
    load_captured_slices,
    persist_replay_result,
    replay_captured_run,
    replay_slices,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.transform import transform_release
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import fbi_release
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.fbi_release import (
    OBSERVATIONS_PER_SUBJECT,
    PERIODS,
    PRODUCT,
    SOURCE_CODE,
    agency_directory_endpoint,
)

pytestmark = [pytest.mark.integration, pytest.mark.database]

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "fbi_ucr"

_slice_fixtures = fbi_release.slice_fixtures
_persist_fixture_release = fbi_release.persist_fixture_release
_run_pipeline = fbi_release.run_pipeline


@pytest.fixture
def fbi_warehouse(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[Callable[[], connection]]:
    """Seed the reviewed geographies and remove all FBI state afterwards."""
    yield from fbi_release.reviewed_warehouse(postgres_connection_factory)


def test_fbi_release_replays_reconciles_and_publishes_idempotently(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: ARC-002, DB-003, DB-006 — a full release reaches gold twice."""
    captured = _persist_fixture_release(fbi_warehouse)

    first = _run_pipeline(fbi_warehouse, captured)
    second = _run_pipeline(fbi_warehouse, captured)

    subjects = len(PRODUCT.subjects)
    assert (
        first
        == second
        == (
            subjects * OBSERVATIONS_PER_SUBJECT,
            subjects * OBSERVATIONS_PER_SUBJECT,
        )
    )

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT subject_type, COUNT(*)
                FROM gold_fbi.crime_observation
                GROUP BY subject_type ORDER BY subject_type
                """
            )
            assert cursor.fetchall() == [
                ("agency", len(PRODUCT.agency_scope) * OBSERVATIONS_PER_SUBJECT),
                ("national", OBSERVATIONS_PER_SUBJECT),
                ("state", OBSERVATIONS_PER_SUBJECT),
            ]
            cursor.execute("SELECT COUNT(*) FROM gold_fbi.reporting_coverage")
            assert cursor.fetchone() == (subjects * PERIODS,)
            cursor.execute(
                "SELECT COUNT(*) FROM control.publisher_ready_event "
                "WHERE source_code = 'FBI_UCR'"
            )
            assert cursor.fetchone()[0] >= 1
    finally:
        reader.close()


def test_provider_totals_and_agency_grain_stay_separable(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-012 — provider totals are never mixed with agency reports."""
    captured = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, captured)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT subject_type, geography_status, geography_basis,
                       COUNT(DISTINCT subject_code)
                FROM gold_fbi.crime_observation
                GROUP BY 1, 2, 3 ORDER BY 1, 2
                """
            )
            rows = cursor.fetchall()
            assert (
                "national",
                "provider_geo_exact",
                "provider-published national total",
                1,
            ) in rows
            assert (
                "state",
                "provider_geo_exact",
                "provider-published state total",
                1,
            ) in rows
            assert not [row for row in rows if row[0] == "agency" and row[1] != row[1]]
            cursor.execute(
                """
                SELECT DISTINCT geography_basis FROM gold_fbi.crime_observation
                WHERE subject_type = 'agency'
                """
            )
            assert cursor.fetchall() == [
                ("agency-reported for one law-enforcement agency",)
            ]
            cursor.execute(
                """
                SELECT measure_form, counted_entity_basis, unit, COUNT(*)
                FROM gold_fbi.crime_observation
                WHERE subject_type = 'national'
                GROUP BY 1, 2, 3 ORDER BY 1, 2
                """
            )
            assert cursor.fetchall() == [
                ("absolute_total", "clearance", "count", PERIODS),
                ("absolute_total", "offense", "count", PERIODS),
                ("rate", "clearance", "per_100000_population", PERIODS),
                ("rate", "offense", "per_100000_population", PERIODS),
            ]
    finally:
        reader.close()


def test_agency_geography_status_matches_its_reviewed_evidence(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-004 — bridges follow evidence, never an agency name."""
    captured = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, captured)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT subject_code, geography_status
                FROM gold_fbi.crime_observation
                WHERE subject_type = 'agency' ORDER BY subject_code
                """
            )
            assert cursor.fetchall() == [
                ("WI0050700", "agency_county_bridged"),
                ("WI0130000", "agency_county_bridged"),
                ("WI0137000", "agency_place_bridged"),
                ("WI0400100", "agency_only"),
                ("WI0540300", "agency_place_bridged"),
                ("WIWSP0000", "agency_only"),
            ]
            cursor.execute(
                """
                SELECT relationship_type, source_label, geo_id,
                       resolution_method, resolution_status, confidence_class
                FROM gold_fbi.agency_geography
                WHERE ori = 'WI0540300' ORDER BY relationship_type, source_label
                """
            )
            assert cursor.fetchall() == [
                (
                    "county",
                    "DANE",
                    "state:55|county:025",
                    "reviewed_county_name_crosswalk",
                    "resolved",
                    "reviewed",
                ),
                (
                    "county",
                    "ROCK",
                    "state:55|county:105",
                    "reviewed_county_name_crosswalk",
                    "resolved",
                    "reviewed",
                ),
                (
                    "place",
                    "Edgerton city",
                    "state:55|place:22575",
                    "reviewed_place_crosswalk",
                    "resolved",
                    "reviewed",
                ),
                (
                    "state",
                    "WI",
                    "state:55",
                    "exact_state_code",
                    "resolved",
                    "exact",
                ),
            ]
            cursor.execute(
                """
                SELECT COUNT(*) FROM gold_fbi.agency_geography
                WHERE ori IN ('WI0400100', 'WIWSP0000')
                  AND relationship_type IN ('county', 'place')
                """
            )
            assert cursor.fetchone() == (0,)
            cursor.execute(
                """
                SELECT COUNT(*) FROM silver_ref.bridge_geo_relationship_version
                AS bridge
                JOIN silver_ref.dim_geo_entity AS agency
                  ON agency.geo_sk = bridge.parent_geo_sk
                WHERE agency.geo_type = 'agency'
                """
            )
            assert cursor.fetchone()[0] > 0
    finally:
        reader.close()


def test_county_filter_keeps_agency_grain_and_deduplicates(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-012 — a county filter is never a county total."""
    captured = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, captured)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT observation_grain, result_label
                FROM gold_fbi.agency_observation_area_filter
                WHERE filter_geography_type = 'county'
                """
            )
            assert cursor.fetchall() == [
                (
                    "agency",
                    "agency-reported for agencies associated with this county",
                )
            ]
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT observation_sk)
                FROM gold_fbi.agency_observation_area_filter
                WHERE ori = 'WI0540300' AND filter_geography_type = 'county'
                """
            )
            rows, distinct_observations = cursor.fetchone()
            assert rows == 2 * OBSERVATIONS_PER_SUBJECT
            assert distinct_observations == OBSERVATIONS_PER_SUBJECT
            cursor.execute(
                """
                SELECT COUNT(DISTINCT observation_sk)
                FROM gold_fbi.agency_observation_area_filter
                WHERE filter_geo_id = 'state:55|county:025'
                  AND filter_geography_type = 'county'
                """
            )
            dane_observations = cursor.fetchone()[0]
            cursor.execute(
                """
                SELECT COUNT(*) FROM gold_fbi.crime_observation
                WHERE subject_code IN ('WI0130000', 'WI0137000', 'WI0540300')
                """
            )
            assert dane_observations == cursor.fetchone()[0]
    finally:
        reader.close()


def test_ambiguous_county_evidence_is_withheld_from_gold(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-011 — an ambiguous name match never becomes a relationship."""
    writer = fbi_warehouse()
    try:
        with writer.cursor() as cursor:
            seed_geography(
                cursor,
                geo_type="county",
                state_fips="55",
                county_fips="997",
                vintage=2023,
                name="Dane County",
            )
        writer.commit()
    finally:
        writer.close()

    try:
        captured = _persist_fixture_release(fbi_warehouse)
        _run_pipeline(fbi_warehouse, captured)

        reader = fbi_warehouse()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT resolution_status, geo_id, reason_code
                    FROM silver_fbi.agency_geography_relationship
                    WHERE ori = 'WI0130000' AND relationship_type = 'county'
                    """
                )
                assert cursor.fetchall() == [
                    ("ambiguous", None, "ambiguous_county_name")
                ]
                cursor.execute(
                    """
                    SELECT DISTINCT geography_status
                    FROM silver_fbi.fact_crime_observation
                    WHERE subject_code = 'WI0130000'
                    """
                )
                assert cursor.fetchall() == [("ambiguous",)]
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM gold_fbi.crime_observation
                    WHERE subject_code = 'WI0130000'
                    """
                )
                assert cursor.fetchone() == (0,)
        finally:
            reader.close()
    finally:
        remover = fbi_warehouse()
        try:
            with remover.cursor() as cursor:
                cursor.execute("DELETE FROM silver_fbi.fact_crime_observation")
                cursor.execute("DELETE FROM silver_fbi.agency_geography_relationship")
                delete_geography(cursor, "state:55|county:997")
            remover.commit()
        finally:
            remover.close()


def test_missing_reports_stay_distinct_from_reported_zeros(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-005 — no report and a reported zero are different rows."""
    captured = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, captured)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT period, value_status, value, participation_status
                FROM gold_fbi.crime_observation
                WHERE subject_code = 'WI0400100'
                  AND measure_form = 'absolute_total'
                  AND counted_entity_basis = 'offense'
                  AND period IN ('01-2023', '03-2023')
                ORDER BY period
                """
            )
            assert cursor.fetchall() == [
                ("01-2023", "reported", 3, "full_participation"),
                ("03-2023", "not_reported", None, "no_participation"),
            ]
            cursor.execute(
                """
                SELECT COUNT(*) FROM gold_fbi.crime_observation
                WHERE subject_code = 'WI0050700' AND value = 0
                  AND value_status = 'reported'
                """
            )
            assert cursor.fetchone()[0] > 0
    finally:
        reader.close()


def test_every_published_observation_has_a_coverage_interpretation(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-004 — an observation cannot publish without its coverage."""
    captured = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, captured)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) FROM silver_fbi.fact_crime_observation AS fact
                LEFT JOIN silver_fbi.fact_reporting_participation AS coverage
                  ON coverage.product_id = fact.product_id
                 AND coverage.release_key = fact.release_key
                 AND coverage.subject_type = fact.subject_type
                 AND coverage.subject_code = fact.subject_code
                 AND coverage.period = fact.period
                WHERE coverage.participation_sk IS NULL
                """
            )
            assert cursor.fetchone() == (0,)
            cursor.execute(
                """
                SELECT COUNT(*) FROM gold_fbi.crime_observation
                WHERE participation_status IS NULL OR coverage_basis IS NULL
                """
            )
            assert cursor.fetchone() == (0,)
    finally:
        reader.close()


def test_reference_dependency_failure_blocks_the_release(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-014 — a release without its reference slice cannot replay."""
    captured = _persist_fixture_release(
        fbi_warehouse, omit=(agency_directory_endpoint("WI"),)
    )

    with pytest.raises(FbiReplayError, match="missing required capture slices"):
        replay_captured_run(
            fbi_warehouse,
            run_id=captured.run_id,
            product=PRODUCT,
            release_key=captured.release_key,
        )

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT complete, status FROM control.fbi_ucr_release "
                "WHERE run_id = %s",
                (str(captured.run_id),),
            )
            assert cursor.fetchone() == (False, "quarantined")
            cursor.execute("SELECT COUNT(*) FROM gold_fbi.crime_observation")
            assert cursor.fetchone() == (0,)
    finally:
        reader.close()


def test_unreconciled_release_cannot_publish(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-014 — publication requires a reconciled silver release."""
    captured = _persist_fixture_release(fbi_warehouse)

    with pytest.raises(FbiPublicationError, match="not reconciled"):
        publish_release(
            fbi_warehouse,
            run_id=captured.run_id,
            product_id=PRODUCT.product_id,
            release_key=captured.release_key,
        )


def test_changed_revision_is_retained_and_latest_selection_projects_it(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: DB-013, DB-022 — a revised release keeps both refresh dates."""
    first = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, first)
    revised = _persist_fixture_release(
        fbi_warehouse, national_fixture="summarized_national_V_revised"
    )
    _run_pipeline(fbi_warehouse, revised)

    assert revised.release_key != first.release_key

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT release_key, status FROM silver_fbi.dim_ucr_dataset_release
                ORDER BY release_key
                """
            )
            assert cursor.fetchall() == [
                (first.release_key, "published"),
                (revised.release_key, "published"),
            ]
            cursor.execute(
                """
                SELECT DISTINCT release_key FROM gold_fbi.latest_release_observation
                """
            )
            assert cursor.fetchall() == [(revised.release_key,)]
            cursor.execute(
                """
                SELECT release_key, value FROM gold_fbi.crime_observation
                WHERE subject_type = 'national' AND period = '01-2023'
                  AND measure_form = 'absolute_total'
                  AND counted_entity_basis = 'offense'
                ORDER BY release_key
                """
            )
            values = cursor.fetchall()
            assert [row[0] for row in values] == [
                first.release_key,
                revised.release_key,
            ]
            assert values[1][1] == values[0][1] + 25
    finally:
        reader.close()


def test_release_replays_from_stored_bytes_with_no_provider_access(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-040 — replay reads durable captures, never the provider."""
    captured = _persist_fixture_release(fbi_warehouse)

    slices = load_captured_slices(fbi_warehouse, run_id=captured.run_id)
    result = replay_slices(PRODUCT, slices, release_key=captured.release_key)

    assert set(slices) == set(_slice_fixtures())
    assert result.observations
    assert not result.quarantined


def test_publisher_keeps_one_row_per_measure_across_published_releases(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: ARC-001 — a second published release adds no publisher row.

    The publisher view once grouped by release key, so a second published
    release doubled every measure. The glossary harvest upserts on
    (source_code, source_object_key) and failed outright -- and because
    ``harvest_all_publishers`` isolates each publisher, the FBI catalog stopped
    following the warehouse without failing the DAG.
    """
    first = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, first)
    revised = _persist_fixture_release(
        fbi_warehouse, national_fixture="summarized_national_V_revised"
    )
    _run_pipeline(fbi_warehouse, revised)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT source_object_key),
                       COUNT(DISTINCT source_watermark)
                FROM gold_fbi.metric_publisher
                """
            )
            total, distinct_keys, watermarks = cursor.fetchone()
            assert total == distinct_keys
            assert watermarks == 1
            cursor.execute(
                "SELECT DISTINCT source_watermark FROM gold_fbi.metric_publisher"
            )
            assert cursor.fetchall() == [(revised.release_key,)]
    finally:
        reader.close()

    assert harvest_publisher(fbi_warehouse, Publisher("gold_fbi")) == 4


def test_publisher_contract_exposes_measure_identity(
    fbi_warehouse: Callable[[], connection],
) -> None:
    """Covers: ARC-001 — the publisher view owns no shared glossary objects."""
    captured = _persist_fixture_release(fbi_warehouse)
    _run_pipeline(fbi_warehouse, captured)

    reader = fbi_warehouse()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_code, source_object_key, units,
                       aggregation_characteristic, valid_time_grains,
                       physical_lineage
                FROM gold_fbi.metric_publisher
                ORDER BY source_object_key
                """
            )
            rows = cursor.fetchall()
            assert {row[0] for row in rows} == {"FBI_UCR"}
            assert {row[1] for row in rows} == {
                f"{PRODUCT.product_id}:{PRODUCT.measure_id(basis, form)}"
                for basis in ("offense", "clearance")
                for form in ("absolute_total", "rate")
            }
            characteristics = {row[1]: row[3] for row in rows}
            assert (
                characteristics[f"{PRODUCT.product_id}:V:offense:absolute_total"]
                == "additive_within_subject"
            )
            assert characteristics[f"{PRODUCT.product_id}:V:offense:rate"] == (
                "non_additive"
            )
            assert all(row[4] == ["MONTHLY"] for row in rows)
            assert all(
                json.loads(json.dumps(row[5]))["schema"] == "gold_fbi" for row in rows
            )
    finally:
        reader.close()
