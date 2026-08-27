"""Deterministic CDC fixture flow from raw capture through the API.

Every provider response is a checked-in reviewed fixture served by a scripted
transport double, so the run makes no network call. The reconciled counts and
values live in ``tests/fixtures/cdc/expected_e2e.json`` beside the fixtures.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from data_ingestion_toolbox.cdc.capture import (
    CapturedCdcRelease,
    capture_asset_release,
    persist_release_state,
)
from data_ingestion_toolbox.cdc.client import SocrataRetryExhausted
from data_ingestion_toolbox.cdc.config import CdcConfig
from data_ingestion_toolbox.cdc.gold_cdc.publisher import publish_release
from data_ingestion_toolbox.cdc.metadata import load_latest_accepted_metadata
from data_ingestion_toolbox.cdc.registry import (
    CDI_ASSET,
    PLACES_COUNTY_ASSET,
    CdcAsset,
)
from data_ingestion_toolbox.cdc.silver_cdc.replay import (
    CdcReplayError,
    persist_replay_result,
    replay_captured_run,
)
from data_ingestion_toolbox.cdc.silver_cdc.transform import (
    CdcReconciliationError,
    transform_release,
)
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.postgres import PostgresTestConfig
from tests.unit.cdc._doubles import ScriptedSocrataClient, socrata_response

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.database,
    pytest.mark.slow,
]

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures/cdc"
EXPECTED = json.loads((FIXTURE_DIR / "expected_e2e.json").read_text(encoding="utf-8"))
APP_TOKEN = "cdc-e2e-app-token-never-persisted"
SEEDED_GEOGRAPHIES = (
    {"geo_type": "nation", "vintage": 2020, "name": "United States"},
    {"geo_type": "state", "state_fips": "01", "vintage": 2020, "name": "Alabama"},
    {
        "geo_type": "county",
        "state_fips": "01",
        "county_fips": "001",
        "vintage": 2020,
        "name": "Autauga County",
    },
)


def _fixture_bytes(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def _test_config(**overrides: object) -> CdcConfig:
    """Build a request-time configuration with no wall-clock spacing."""
    values: dict[str, object] = {
        "socrata_app_token": APP_TOKEN,
        "socrata_min_spacing_seconds": 0.0,
        "socrata_page_size": 1000,
    }
    values.update(overrides)
    return CdcConfig(**values)


@contextmanager
def _real_api_client() -> Iterator[TestClient]:
    settings = PostgresTestConfig.from_environment()
    assert settings is not None
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": settings.host,
            "port": settings.port,
            "user": settings.user,
            "password": settings.password,
            "dbname": settings.database,
        },
        pool_pre_ping=True,
    )

    def override_db() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_db_session_dep] = override_db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
        engine.dispose()


def _capture_release(
    connection_factory: Callable[[], connection],
    asset: CdcAsset,
    *,
    metadata_payload: bytes,
    page_payloads: tuple[bytes, ...],
    config: CdcConfig,
) -> CapturedCdcRelease:
    """Run the real capture orchestration against a scripted transport."""
    client = ScriptedSocrataClient(
        [
            socrata_response(200, raw=metadata_payload),
            *(socrata_response(200, raw=payload) for payload in page_payloads),
        ]
    )
    release = capture_asset_release(
        connection_factory,
        asset,
        previous_metadata=load_latest_accepted_metadata(connection_factory, asset),
        config=config,
        client=client,
    )
    persist_release_state(connection_factory, release)
    return release


def _replay_and_publish(
    connection_factory: Callable[[], connection],
    asset: CdcAsset,
    release: CapturedCdcRelease,
) -> tuple[int, int]:
    """Replay durable bytes, reconcile silver, and publish one release."""
    watermark = release.metadata.release_version
    result = replay_captured_run(
        connection_factory,
        run_id=release.run_id,
        asset=asset,
        release_watermark=watermark,
    )
    persist_replay_result(
        connection_factory,
        run_id=release.run_id,
        asset=asset,
        release_watermark=watermark,
        result=result,
    )
    facts = transform_release(
        connection_factory,
        run_id=release.run_id,
        asset=asset,
        release_watermark=watermark,
    )
    published = publish_release(
        connection_factory,
        run_id=release.run_id,
        asset_id=asset.asset_id,
        release_watermark=watermark,
    )
    return facts, published


def _query(
    connection_factory: Callable[[], connection],
    sql: str,
    parameters: tuple[object, ...] = (),
) -> list[tuple]:
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(sql, parameters)
            return cursor.fetchall()
    finally:
        database_connection.close()


def _gold_profile(
    connection_factory: Callable[[], connection], asset_id: str
) -> dict[str, int]:
    rows = _query(
        connection_factory,
        """
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE value_status = 'valid'),
               COUNT(*) FILTER (WHERE value_status = 'missing'),
               COUNT(*) FILTER (WHERE value_status = 'suppressed'),
               COUNT(*) FILTER (WHERE geography_status = 'resolved'),
               COUNT(*) FILTER (WHERE geography_status = 'unmapped')
        FROM gold_cdc.health_observation
        WHERE asset_id = %s
        """,
        (asset_id,),
    )
    total, valid, missing, suppressed, resolved, unmapped = rows[0]
    return {
        "rows": total,
        "valid": valid,
        "missing": missing,
        "suppressed": suppressed,
        "resolved": resolved,
        "unmapped": unmapped,
    }


def _observation(items: list[dict], **match: object) -> dict:
    """Return the single API item matching every named field."""
    matched = [
        item
        for item in items
        if all(item[name] == value for name, value in match.items())
    ]
    assert len(matched) == 1, f"expected exactly one item for {match}, got {matched}"
    return matched[0]


def _assert_subset(actual: dict, expected: dict) -> None:
    assert {name: actual[name] for name in expected} == expected


@pytest.fixture
def cdc_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed only the canonical geographies this fixture flow is allowed to use."""
    tracked = {
        "us:1",
        "state:01",
        "state:01|county:001",
        EXPECTED["review"]["withheld_geography"],
    }
    preexisting = {
        row[0]
        for row in _query(
            postgres_connection_factory,
            "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
            (list(tracked),),
        )
    }
    assert EXPECTED["review"]["withheld_geography"] not in preexisting, (
        "the withheld county must be absent for the geography-miss contract"
    )

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
                for geo_id in sorted(
                    {"us:1", "state:01", "state:01|county:001"} - preexisting
                ):
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
            for geography in SEEDED_GEOGRAPHIES:
                seed_geography(cursor, **geography)
        writer.commit()
    finally:
        writer.close()
    return postgres_connection_factory


def test_cdc_fixtures_reach_the_api_and_retain_every_published_release(
    cdc_warehouse: Callable[[], connection],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers: E2E-007 — CDC fixtures flow capture-first to the API exactly.

    Covers: E2E-004 — replaying an unchanged release adds no facts and returns
        identical API JSON.
    Covers: E2E-005 — a changed release is retained while the latest projection
        advances to the newer watermark.
    Covers: E2E-006 — a county without a canonical entity stays an inspectable
        geography miss, and suppressed/missing values never become numbers.
    Covers: ETL-038 — the optional app token never reaches captures, control
        state, logs, or API responses.
    """
    caplog.set_level(logging.DEBUG)
    factory = cdc_warehouse
    config = _test_config()

    cdi_release = _capture_release(
        factory,
        CDI_ASSET,
        metadata_payload=_fixture_bytes("cdi_metadata.json"),
        page_payloads=(_fixture_bytes("cdi_observations.json"),),
        config=config,
    )
    places_release = _capture_release(
        factory,
        PLACES_COUNTY_ASSET,
        metadata_payload=_fixture_bytes("places_county_metadata.json"),
        page_payloads=(_fixture_bytes("places_county_observations.json"),),
        config=config,
    )

    assert (
        cdi_release.metadata.release_version == (EXPECTED["release_watermarks"]["cdi"])
    )
    assert (
        places_release.metadata.release_version
        == (EXPECTED["release_watermarks"]["places_county"])
    )
    for asset_id, release in (
        ("cdi", cdi_release),
        ("places_county", places_release),
    ):
        expected_capture = EXPECTED["capture"][asset_id]
        assert len(release.page_capture_ids) == expected_capture["page_captures"]
        assert release.row_count == expected_capture["captured_rows"]
        assert release.complete is True

    for asset, release in (
        (CDI_ASSET, cdi_release),
        (PLACES_COUNTY_ASSET, places_release),
    ):
        expected_silver = EXPECTED["silver"][asset.asset_id]
        facts, published = _replay_and_publish(factory, asset, release)
        assert facts == expected_silver["facts"]
        assert published == expected_silver["facts"]
        assert _query(
            factory,
            "SELECT COUNT(*) FROM silver_cdc.observation_revision WHERE run_id = %s",
            (str(release.run_id),),
        ) == [(expected_silver["revisions"],)]
        assert _query(
            factory,
            "SELECT COUNT(*) FROM silver_cdc.observation_quarantine WHERE run_id = %s",
            (str(release.run_id),),
        ) == [(expected_silver["quarantined"],)]

    for asset_id, expected_gold in EXPECTED["gold_first_release"].items():
        profile = _gold_profile(factory, asset_id)
        assert profile == {
            name: value
            for name, value in expected_gold.items()
            if name != "estimate_method"
        }

    assert _query(
        factory,
        "SELECT COUNT(*) FROM control.publisher_ready_event WHERE source_code = 'CDC'",
    ) == [(EXPECTED["publisher_ready_events"],)]

    expected_api = EXPECTED["api"]
    with _real_api_client() as client:
        latest = client.get("/api/cdc/observations", params={"limit": 100})
        assert latest.status_code == 200
        latest_payload = latest.json()
        assert latest_payload["total"] == expected_api["latest_total"]
        assert latest_payload["release_selection"] == "latest_release"
        items = latest_payload["items"]
        assert len(items) == expected_api["latest_total"]

        _assert_subset(
            _observation(items, dataset="cdi", geo_type="nation"),
            expected_api["cdi_national_observation"],
        )
        _assert_subset(
            _observation(items, dataset="cdi", value_status="missing"),
            expected_api["cdi_missing_observation"],
        )
        _assert_subset(
            _observation(items, dataset="places_county", value_status="suppressed"),
            expected_api["places_county_suppressed_observation"],
        )

        for parameters, expected_total in (
            ({"dataset": "cdi"}, expected_api["cdi_total"]),
            ({"dataset": "places_county"}, expected_api["places_county_total"]),
            ({"geo_type": "nation"}, expected_api["nation_total"]),
            ({"geo_type": "state"}, expected_api["state_total"]),
            ({"geo_type": "county"}, expected_api["county_total"]),
            ({"adjustment": "age_adjusted"}, expected_api["age_adjusted_total"]),
        ):
            filtered = client.get("/api/cdc/observations", params=parameters)
            assert filtered.status_code == 200
            assert filtered.json()["total"] == expected_total

        first_page = client.get(
            "/api/cdc/observations", params={"limit": 3, "offset": 0}
        ).json()
        second_page = client.get(
            "/api/cdc/observations", params={"limit": 3, "offset": 3}
        ).json()
        assert first_page["total"] == expected_api["latest_total"]
        assert len(first_page["items"]) == 3 and len(second_page["items"]) == 3
        paged_ids = [item["source_record_id"] for item in first_page["items"]] + [
            item["source_record_id"] for item in second_page["items"]
        ]
        assert len(set(paged_ids)) == 6

        # Covers: E2E-004 — a second replay of the same captures changes nothing.
        for asset, release in (
            (CDI_ASSET, cdi_release),
            (PLACES_COUNTY_ASSET, places_release),
        ):
            _replay_and_publish(factory, asset, release)
        replayed = client.get("/api/cdc/observations", params={"limit": 100}).json()
        assert replayed == latest_payload
        for asset_id, expected_gold in EXPECTED["gold_first_release"].items():
            assert _gold_profile(factory, asset_id)["rows"] == expected_gold["rows"]

        # Covers: E2E-005 — a newer CDI release is added, not substituted.
        second_metadata = json.loads(_fixture_bytes("cdi_metadata.json"))
        second_metadata["rowsUpdatedAt"] = int(
            EXPECTED["release_watermarks"]["cdi_second_release"]
        )
        second_release = _capture_release(
            factory,
            CDI_ASSET,
            metadata_payload=json.dumps(second_metadata).encode(),
            page_payloads=(_fixture_bytes("cdi_observations.json"),),
            config=config,
        )
        assert (
            second_release.metadata.release_version
            == (EXPECTED["release_watermarks"]["cdi_second_release"])
        )
        _replay_and_publish(factory, CDI_ASSET, second_release)

        expected_history = EXPECTED["gold_after_second_release"]
        assert (
            _gold_profile(factory, "cdi")["rows"]
            == (expected_history["cdi_history_rows"])
        )
        assert _query(
            factory,
            "SELECT COUNT(*) FROM gold_cdc.latest_release_observation "
            "WHERE asset_id = 'cdi'",
        ) == [(expected_history["cdi_latest_rows"],)]

        newest = client.get(
            "/api/cdc/observations", params={"dataset": "cdi", "limit": 100}
        ).json()
        assert newest["total"] == expected_api["cdi_total"]
        assert {item["release_watermark"] for item in newest["items"]} == {
            EXPECTED["release_watermarks"]["cdi_second_release"]
        }

        prior = client.get(
            "/api/cdc/observations",
            params={
                "dataset": "cdi",
                "release": EXPECTED["release_watermarks"]["cdi"],
                "limit": 100,
            },
        ).json()
        assert prior["release_selection"] == "single_release"
        assert prior["total"] == expected_api["cdi_total"]
        assert {item["release_watermark"] for item in prior["items"]} == {
            EXPECTED["release_watermarks"]["cdi"]
        }
        assert APP_TOKEN not in latest.text

    token_matches = _query(
        factory,
        """
        SELECT (SELECT COUNT(*) FROM raw_capture.response_capture
                 WHERE source_code = 'CDC'
                   AND (request_parameters::TEXT LIKE %s
                        OR response_headers::TEXT LIKE %s
                        OR request_fingerprint LIKE %s)),
               (SELECT COUNT(*) FROM control.ingestion_request
                 WHERE source_code = 'CDC'
                   AND (request_parameters::TEXT LIKE %s
                        OR request_fingerprint LIKE %s))
        """,
        (
            f"%{APP_TOKEN}%",
            f"%{APP_TOKEN}%",
            f"%{APP_TOKEN}%",
            f"%{APP_TOKEN}%",
            f"%{APP_TOKEN}%",
        ),
    )
    assert token_matches == [(0, 0)]
    assert APP_TOKEN not in caplog.text


def test_cdc_partial_page_capture_cannot_publish_and_reruns_to_a_clean_state(
    cdc_warehouse: Callable[[], connection],
) -> None:
    """Covers: RES-007 — a restart after a partial CDC capture reaches the same
    state as a clean successful run.

    Covers: E2E-007 — an incomplete page sequence rolls back before silver and
        never reaches the published API surface.
    """
    factory = cdc_warehouse
    expected_failure = EXPECTED["partial_page_failure"]
    partial_config = _test_config(
        socrata_page_size=expected_failure["page_size"], socrata_max_attempts=2
    )
    first_page = json.dumps(
        json.loads(_fixture_bytes("cdi_observations.json"))[
            : expected_failure["page_size"]
        ]
    ).encode()
    transport_failure = httpx.ConnectError("simulated CDC transport failure")
    client = ScriptedSocrataClient(
        [
            socrata_response(200, raw=_fixture_bytes("cdi_metadata.json")),
            socrata_response(200, raw=first_page),
            transport_failure,
            transport_failure,
        ]
    )

    with pytest.raises(SocrataRetryExhausted):
        capture_asset_release(
            factory,
            CDI_ASSET,
            previous_metadata=None,
            config=partial_config,
            client=client,
        )

    failed_runs = _query(
        factory,
        "SELECT run_id FROM control.ingestion_run "
        "WHERE source_code = 'CDC' AND status = 'failed'",
    )
    assert len(failed_runs) == 1
    failed_run_id = failed_runs[0][0]
    assert _query(
        factory,
        "SELECT COUNT(*) FROM raw_capture.response_capture "
        "WHERE run_id = %s AND endpoint = %s",
        (str(failed_run_id), CDI_ASSET.api_path),
    ) == [(expected_failure["captured_pages"],)]
    assert _query(
        factory,
        "SELECT COUNT(*) FROM control.cdc_dataset_release WHERE run_id = %s",
        (str(failed_run_id),),
    ) == [(expected_failure["control_releases"],)]

    with pytest.raises(CdcReplayError) as replay_error:
        replay_captured_run(
            factory,
            run_id=failed_run_id,
            asset=CDI_ASSET,
            release_watermark=EXPECTED["release_watermarks"]["cdi"],
        )
    assert str(replay_error.value) == expected_failure["replay_error"]

    with pytest.raises(CdcReconciliationError):
        transform_release(
            factory,
            run_id=failed_run_id,
            asset=CDI_ASSET,
            release_watermark=EXPECTED["release_watermarks"]["cdi"],
        )

    assert _query(factory, "SELECT COUNT(*) FROM silver_cdc.observation_revision") == [
        (expected_failure["silver_revisions"],)
    ]
    assert _query(factory, "SELECT COUNT(*) FROM silver_cdc.dim_dataset_release") == [
        (expected_failure["control_releases"],)
    ]
    assert _gold_profile(factory, "cdi")["rows"] == expected_failure["gold_rows"]

    rerun = _capture_release(
        factory,
        CDI_ASSET,
        metadata_payload=_fixture_bytes("cdi_metadata.json"),
        page_payloads=(_fixture_bytes("cdi_observations.json"),),
        config=_test_config(),
    )
    facts, published = _replay_and_publish(factory, CDI_ASSET, rerun)

    expected_gold = EXPECTED["gold_first_release"]["cdi"]
    assert facts == published == expected_gold["rows"]
    assert _gold_profile(factory, "cdi") == {
        name: value
        for name, value in expected_gold.items()
        if name != "estimate_method"
    }
    with _real_api_client() as api_client:
        payload = api_client.get(
            "/api/cdc/observations", params={"dataset": "cdi", "limit": 100}
        ).json()
    assert payload["total"] == EXPECTED["api"]["cdi_total"]
    assert {item["estimate_method"] for item in payload["items"]} == {
        expected_gold["estimate_method"]
    }
