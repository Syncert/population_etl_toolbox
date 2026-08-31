"""Reviewed FBI UCR release capture for database and end-to-end contracts.

The capture-to-publication mechanics are identical whichever tier drives them,
and duplicating them would let the two tiers drift apart until one proved a
release shape the pipeline no longer produces. Source *semantics* stay in the
tests: this module only builds the release, runs the production functions, and
seeds the geographies the reviewed Wisconsin sample resolves against.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

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
from data_ingestion_toolbox.fbi_ucr.gold_fbi.publisher import publish_release
from data_ingestion_toolbox.fbi_ucr.metadata import ReleaseDecision, parse_release
from data_ingestion_toolbox.fbi_ucr.registry import (
    COUNTED_ENTITY_BASES,
    MEASURE_FORMS,
    SUMMARIZED_VIOLENT_CRIME,
    agency_directory_endpoint,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.replay import (
    persist_replay_result,
    replay_captured_run,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.transform import transform_release
from tests.support.capture_seed import delete_geography, seed_geography

PRODUCT = SUMMARIZED_VIOLENT_CRIME
SOURCE_CODE = "FBI_UCR"
FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "fbi_ucr"
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

SEEDED_GEO_IDS: tuple[str, ...] = (
    "us:1",
    "state:55",
    "state:55|county:009",
    "state:55|county:025",
    "state:55|county:105",
    "state:55|place:25950",
    "state:55|place:22575",
)

#: Source-owned silver and control relations, in foreign-key-safe order.
CLEANUP_STATEMENTS: tuple[str, ...] = (
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


def fixture_for(endpoint: str) -> str:
    """Map one registered endpoint to its reviewed fixture stem."""
    if endpoint.startswith("/agency/"):
        return f"agency_directory_{endpoint.rsplit('/', 1)[-1]}"
    kind, value = endpoint.split("/")[2], endpoint.split("/")[3]
    if kind == "national":
        return f"summarized_national_{value}"
    return f"summarized_{kind}_{value}_{endpoint.split('/')[4]}"


def slice_fixtures() -> dict[str, str]:
    """Return every registered slice endpoint and the fixture that answers it."""
    slices = {
        agency_directory_endpoint(state): f"agency_directory_{state}"
        for state in PRODUCT.reference_states
    }
    for subject in PRODUCT.subjects:
        endpoint = PRODUCT.observation_endpoint(subject)
        slices[endpoint] = fixture_for(endpoint)
    return slices


def _capture_slice(
    connection_factory: Callable[[], connection],
    control: CaptureControl,
    *,
    run_id: UUID,
    endpoint: str,
    parameters: dict,
    payload: bytes,
    source_revision: str | None,
) -> UUID:
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


def persist_fixture_release(
    connection_factory: Callable[[], connection],
    *,
    national_fixture: str = "summarized_national_V",
    omit: tuple[str, ...] = (),
) -> CapturedFbiRelease:
    """Capture every registered slice from reviewed fixtures, then record it."""
    slices = slice_fixtures()
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


def run_pipeline(
    connection_factory: Callable[[], connection],
    captured: CapturedFbiRelease,
) -> tuple[int, int]:
    """Replay durable bytes, reconcile silver, and publish one release."""
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


def seed_reviewed_geographies(
    connection_factory: Callable[[], connection],
) -> set[str]:
    """Seed the reviewed sample's geographies; return the ones already present."""
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(SEEDED_GEO_IDS),),
            )
            preexisting = {row[0] for row in cursor.fetchall()}
    finally:
        reader.close()

    writer = connection_factory()
    try:
        with writer.cursor() as cursor:
            for entry in SEEDED_GEOGRAPHIES:
                seed_geography(cursor, vintage=2023, **entry)  # type: ignore[arg-type]
        writer.commit()
    except BaseException:
        writer.rollback()
        raise
    finally:
        writer.close()
    return preexisting


def remove_fbi_state(
    connection_factory: Callable[[], connection], preexisting: set[str]
) -> None:
    """Delete every FBI-owned row, leaving shared geographies others still use."""
    cleanup = connection_factory()
    try:
        with cleanup.cursor() as cursor:
            for statement in CLEANUP_STATEMENTS:
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


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
) -> Iterator[Callable[[], connection]]:
    """Yield a factory whose FBI state is removed when the test finishes."""
    preexisting = seed_reviewed_geographies(connection_factory)
    try:
        yield connection_factory
    finally:
        remove_fbi_state(connection_factory, preexisting)


def fbi_warehouse_fixture() -> Callable:
    """Build the shared pytest fixture both tiers register under one name."""

    @pytest.fixture
    def fbi_warehouse(
        postgres_connection_factory: Callable[[], connection],
    ) -> Iterator[Callable[[], connection]]:
        """Seed the reviewed geographies and remove all FBI state afterwards."""
        yield from reviewed_warehouse(postgres_connection_factory)

    return fbi_warehouse
