"""Real PostgreSQL FBI UCR capture-to-gold deployment contract."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg2.errors import ForeignKeyViolation
from psycopg2.extensions import connection

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.fbi_ucr.capture import (
    CapturedFbiRelease,
    persist_release_state,
)
from data_ingestion_toolbox.fbi_ucr.client import observation_parameters
from data_ingestion_toolbox.fbi_ucr.gold_fbi.publisher import (
    FbiPublicationError,
    publish_release,
)
from data_ingestion_toolbox.fbi_ucr.metadata import ReleaseDecision, parse_release
from data_ingestion_toolbox.fbi_ucr.registry import (
    COUNTED_ENTITY_BASES,
    MEASURE_FORMS,
    SUMMARIZED_VIOLENT_CRIME,
    agency_directory_endpoint,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.replay import (
    FbiReplayError,
    load_captured_slices,
    persist_replay_result,
    replay_captured_run,
    replay_slices,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.transform import transform_release
from tests.support.capture_seed import delete_geography, seed_geography

pytestmark = [pytest.mark.integration, pytest.mark.database]

PRODUCT = SUMMARIZED_VIOLENT_CRIME
SOURCE_CODE = "FBI_UCR"
FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "fbi_ucr"

PERIODS = len(PRODUCT.expected_periods)

#: Two measure forms x two counted entities, for every registered period. Each
#: registered period reconciles to exactly one row, so the count follows the
#: product window rather than the months the reviewed fixture reports: periods
#: the fixture leaves out land as ``not_reported`` observations.
OBSERVATIONS_PER_SUBJECT = len(MEASURE_FORMS) * len(COUNTED_ENTITY_BASES) * PERIODS

#: Geographies the reviewed Wisconsin sample resolves against.
SEEDED_GEOGRAPHIES: tuple[dict[str, object], ...] = (
    {"geo_type": "nation", "name": "United States"},
    {"geo_type": "state", "state_fips": "55", "name": "Wisconsin"},
    {
        "geo_type": "county",
        "state_fips": "55",
        "county_fips": "009",
        "name": "Brown County",
    },
    {
        "geo_type": "county",
        "state_fips": "55",
        "county_fips": "025",
        "name": "Dane County",
    },
    {
        "geo_type": "county",
        "state_fips": "55",
        "county_fips": "105",
        "name": "Rock County",
    },
    {
        "geo_type": "place",
        "state_fips": "55",
        "place_fips": "25950",
        "name": "Fitchburg city",
    },
    {
        "geo_type": "place",
        "state_fips": "55",
        "place_fips": "22575",
        "name": "Edgerton city",
    },
)

SEEDED_GEO_IDS = (
    "us:1",
    "state:55",
    "state:55|county:009",
    "state:55|county:025",
    "state:55|county:105",
    "state:55|place:25950",
    "state:55|place:22575",
)

_CLEANUP_STATEMENTS = (
    "DELETE FROM control.publisher_ready_event WHERE source_code = 'FBI_UCR'",
    "DELETE FROM silver_fbi.fact_crime_observation",
    "DELETE FROM silver_fbi.fact_reporting_participation",
    "DELETE FROM silver_fbi.observation_revision",
    "DELETE FROM silver_fbi.participation_revision",
    "DELETE FROM silver_fbi.agency_revision",
    "DELETE FROM silver_fbi.slice_quarantine",
    "DELETE FROM silver_fbi.agency_geography_relationship",
    "DELETE FROM silver_fbi.dim_agency_version",
    "DELETE FROM silver_fbi.dim_agency",
    "DELETE FROM silver_fbi.dim_offense_measure",
    "DELETE FROM silver_fbi.dim_ucr_dataset_release",
    "DELETE FROM silver_fbi.reviewed_place_crosswalk",
    "DELETE FROM silver_fbi.dim_state_code",
    "DELETE FROM control.fbi_ucr_release",
    "DELETE FROM silver_ref.geography_resolution WHERE provider_source = 'FBI_UCR'",
    """
    DELETE FROM silver_ref.bridge_geo_relationship_version
     WHERE parent_geo_sk IN (
         SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_type = 'agency'
     )
    """,
    """
    DELETE FROM silver_ref.dim_geo_entity_version
     WHERE geo_sk IN (
         SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_type = 'agency'
     )
    """,
    "DELETE FROM silver_ref.dim_geo_entity WHERE geo_type = 'agency'",
)


def _fixture_for(endpoint: str) -> str:
    if endpoint.startswith("/agency/"):
        return f"agency_directory_{endpoint.rsplit('/', 1)[-1]}"
    kind, value = endpoint.split("/")[2], endpoint.split("/")[3]
    if kind == "national":
        return f"summarized_national_{value}"
    return f"summarized_{kind}_{value}_{endpoint.split('/')[4]}"


def _slice_fixtures() -> dict[str, str]:
    slices = {
        agency_directory_endpoint(state): f"agency_directory_{state}"
        for state in PRODUCT.reference_states
    }
    for subject in PRODUCT.subjects:
        endpoint = PRODUCT.observation_endpoint(subject)
        slices[endpoint] = _fixture_for(endpoint)
    return slices


def _capture_slice(
    connection_factory: Callable[[], connection],
    control: CaptureControl,
    *,
    run_id,
    endpoint: str,
    parameters: dict,
    payload: bytes,
    source_revision: str | None,
):
    request = control.start_request(
        run_id=run_id, endpoint=endpoint, parameters=parameters
    )
    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=request.request_id,
        run_id=run_id,
        source_code=SOURCE_CODE,
        endpoint=endpoint,
        request_parameters=parameters,
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "application/json"},
        media_type="application/json",
        payload=payload,
        payload_schema_version=PRODUCT.parser_contract_version,
        source_revision=source_revision,
    )
    persist_response_capture(connection_factory, capture)
    control.finish_request(request.request_id, status="captured")
    return capture.capture_id


def _persist_fixture_release(
    connection_factory: Callable[[], connection],
    *,
    national_fixture: str = "summarized_national_V",
    omit: tuple[str, ...] = (),
) -> CapturedFbiRelease:
    """Capture every registered slice from reviewed fixtures, then record it."""
    slices = _slice_fixtures()
    slices[PRODUCT.observation_endpoint(PRODUCT.subjects[0])] = national_fixture
    control = CaptureControl(connection_factory, source_code=SOURCE_CODE)
    run_id = control.start_run(watermark={"product_id": PRODUCT.product_id})
    release = parse_release((FIXTURE_DIR / f"{national_fixture}.json").read_bytes())

    directory_captures = []
    observation_captures = []
    probe_capture = None
    for endpoint, fixture in slices.items():
        if endpoint in omit:
            continue
        payload = (FIXTURE_DIR / f"{fixture}.json").read_bytes()
        is_directory = endpoint.startswith("/agency/")
        parameters = {} if is_directory else observation_parameters(PRODUCT)
        capture_id = _capture_slice(
            connection_factory,
            control,
            run_id=run_id,
            endpoint=endpoint,
            parameters=parameters,
            payload=payload,
            source_revision=release.release_key,
        )
        if is_directory:
            directory_captures.append((endpoint.rsplit("/", 1)[-1], capture_id))
        else:
            observation_captures.append((endpoint, capture_id))
            if probe_capture is None:
                probe_capture = capture_id

    control.set_run_watermark(
        run_id,
        watermark={
            "product_id": PRODUCT.product_id,
            "refresh_date": release.release_key,
            "max_data_month": release.max_data_month,
        },
    )
    control.finish_run(run_id, status="success")
    captured = CapturedFbiRelease(
        run_id=run_id,
        product_id=PRODUCT.product_id,
        release=release,
        decision=ReleaseDecision.INGEST,
        release_capture_id=probe_capture,
        directory_capture_ids=tuple(directory_captures),
        observation_capture_ids=tuple(observation_captures),
        complete=not omit,
    )
    persist_release_state(connection_factory, captured, PRODUCT)
    return captured


def _run_pipeline(
    connection_factory: Callable[[], connection],
    captured: CapturedFbiRelease,
) -> tuple[int, int]:
    result = replay_captured_run(
        connection_factory,
        run_id=captured.run_id,
        product=PRODUCT,
        release_key=captured.release_key,
    )
    persist_replay_result(
        connection_factory,
        run_id=captured.run_id,
        product=PRODUCT,
        release_key=captured.release_key,
        result=result,
    )
    transformed = transform_release(
        connection_factory,
        run_id=captured.run_id,
        product=PRODUCT,
        release_key=captured.release_key,
    )
    published = publish_release(
        connection_factory,
        run_id=captured.run_id,
        product_id=PRODUCT.product_id,
        release_key=captured.release_key,
    )
    return transformed, published


@pytest.fixture
def fbi_warehouse(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[Callable[[], connection]]:
    """Seed the reviewed geographies and remove all FBI state afterwards."""
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(SEEDED_GEO_IDS),),
            )
            preexisting = {row[0] for row in cursor.fetchall()}
    finally:
        reader.close()

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            for entry in SEEDED_GEOGRAPHIES:
                seed_geography(cursor, vintage=2023, **entry)
        writer.commit()
    finally:
        writer.close()

    yield postgres_connection_factory

    cleanup = postgres_connection_factory()
    try:
        with cleanup.cursor() as cursor:
            for statement in _CLEANUP_STATEMENTS:
                cursor.execute(statement)
            for index, geo_id in enumerate(SEEDED_GEO_IDS):
                if geo_id in preexisting:
                    continue
                # The shared reference dimension is not owned by this suite. A
                # geography another source still references stays in place
                # rather than aborting the FBI cleanup transaction.
                savepoint = f"geo_{index}"
                cursor.execute(f"SAVEPOINT {savepoint}")
                try:
                    delete_geography(cursor, geo_id)
                except ForeignKeyViolation:
                    cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                else:
                    cursor.execute(f"RELEASE SAVEPOINT {savepoint}")
        cleanup.commit()
    except BaseException:
        cleanup.rollback()
        raise
    finally:
        cleanup.close()


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
