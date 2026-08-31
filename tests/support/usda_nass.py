"""Shared disposable-warehouse setup and release capture for USDA NASS.

The reviewed-fixture capture, replay, transform, and publication sequence is
identical whichever tier drives it, so both the database contract tier and the
end-to-end product tier build a release through this module. Source semantics --
what a suppressed value means, which classification dimensions must survive --
stay in the tests that assert them.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.usda_nass.capture import (
    SLICE_CAPTURED,
    SLICE_OVER_LIMIT,
    CapturedNassRelease,
    CapturedNassSlice,
    persist_release_state,
)
from data_ingestion_toolbox.usda_nass.client import (
    API_COUNT_PATH,
    API_DATA_PATH,
    count_parameters,
    data_parameters,
)
from data_ingestion_toolbox.usda_nass.gold_nass.publisher import publish_release
from data_ingestion_toolbox.usda_nass.metadata import (
    NassSliceCount,
    ReleaseDecision,
    summarize_release,
)
from data_ingestion_toolbox.usda_nass.registry import NassProduct, NassSlice
from data_ingestion_toolbox.usda_nass.silver_nass.transform import (
    persist_replay_result,
    replay_captured_run,
    transform_release,
)
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPOSITORY_ROOT / "tests/fixtures/usda_nass"

#: Geographies the reviewed USDA NASS fixtures resolve against.
TRACKED_GEO_IDS: tuple[str, ...] = (
    "us:1",
    "state:01",
    "state:48",
    "state:01|county:001",
    "state:48|county:301",
)

#: Silver relations a USDA NASS contract test must leave empty.
SILVER_TABLES: tuple[str, ...] = (
    "silver_nass.fact_crop_observation",
    "silver_nass.observation_revision",
    "silver_nass.observation_quarantine",
    "silver_nass.dim_dataset_release",
    "silver_nass.dim_statistic",
    "silver_nass.dim_commodity",
    "silver_nass.dim_domain",
)

#: The reviewed fixture geographies, in the order they must be seeded.
_SEED_ARGUMENTS: tuple[dict[str, Any], ...] = (
    {"geo_type": "nation", "vintage": 2024, "name": "United States"},
    {"geo_type": "state", "state_fips": "01", "vintage": 2024, "name": "Alabama"},
    {"geo_type": "state", "state_fips": "48", "vintage": 2024, "name": "Texas"},
    {
        "geo_type": "county",
        "state_fips": "01",
        "county_fips": "001",
        "vintage": 2024,
        "name": "Autauga County",
    },
    {
        "geo_type": "county",
        "state_fips": "48",
        "county_fips": "301",
        "vintage": 2024,
        "name": "Loving County",
    },
)


def load_product_fixture(name: str) -> dict[str, Any]:
    """Load one reviewed USDA NASS fixture document by stem."""
    return json.loads((FIXTURE_DIR / f"{name}.json").read_text(encoding="utf-8"))


def _preexisting_geographies(
    connection_factory: Callable[[], connection],
) -> set[str]:
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(TRACKED_GEO_IDS),),
            )
            return {row[0] for row in cursor.fetchall()}
    finally:
        database_connection.close()


def _cleanup(
    connection_factory: Callable[[], connection],
    preexisting: set[str],
    *,
    preexisting_glossary: bool = False,
    baseline_run_ids: frozenset = frozenset(),
) -> Callable[[], None]:
    def run() -> None:
        owned_runs = sorted(
            source_run_ids(connection_factory, "USDA_NASS") - baseline_run_ids
        )
        database_connection = connection_factory()
        try:
            with database_connection.cursor() as cursor:
                delete_harvested_glossary_rows(
                    cursor, "USDA_NASS", preexisting=preexisting_glossary
                )
                cursor.execute(
                    "DELETE FROM control.publisher_ready_event "
                    "WHERE source_code = 'USDA_NASS'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution "
                    "WHERE provider_source = 'USDA_NASS'"
                )
                for table in SILVER_TABLES:
                    cursor.execute(f"DELETE FROM {table}")
                cursor.execute("DELETE FROM control.usda_nass_slice")
                cursor.execute("DELETE FROM control.usda_nass_release")
                delete_capture_graph(cursor, owned_runs)
                for geo_id in sorted(set(TRACKED_GEO_IDS) - preexisting):
                    delete_geography(cursor, geo_id)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    return run


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed the reviewed geographies and remove all USDA NASS state afterwards."""
    preexisting = _preexisting_geographies(connection_factory)
    request.addfinalizer(
        _cleanup(
            connection_factory,
            preexisting,
            preexisting_glossary=glossary_registration_exists(
                connection_factory, "USDA_NASS"
            ),
            baseline_run_ids=source_run_ids(connection_factory, "USDA_NASS"),
        )
    )

    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            for arguments in _SEED_ARGUMENTS:
                seed_geography(cursor, **arguments)
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return connection_factory


def capture_slice(
    connection_factory: Callable[[], connection],
    control: CaptureControl,
    *,
    run_id: UUID,
    product: NassProduct,
    item: NassSlice,
    endpoint: str,
    parameters: dict[str, Any],
    payload: bytes,
) -> UUID:
    request = control.start_request(
        run_id=run_id, endpoint=endpoint, parameters=parameters
    )
    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=request.request_id,
        run_id=run_id,
        source_code="USDA_NASS",
        endpoint=endpoint,
        request_parameters=parameters,
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "application/json"},
        media_type="application/json",
        payload=payload,
        payload_schema_version=product.parser_contract_version,
    )
    persist_response_capture(connection_factory, capture)
    control.finish_request(request.request_id, status="captured")
    return capture.capture_id


def persist_fixture_release(
    connection_factory: Callable[[], connection],
    *,
    product: NassProduct,
    document: dict[str, Any],
    year: int | None = None,
    over_limit_levels: tuple[str, ...] = (),
) -> CapturedNassRelease:
    """Capture one reviewed sample exactly as the production capture would."""
    control = CaptureControl(connection_factory, source_code="USDA_NASS")
    run_id = control.start_run(
        watermark={"product_id": product.product_id, "slice_mode": "recent"}
    )
    sample_year = year or int(document["sample_year"])
    counts: list[NassSliceCount] = []
    slices: list[CapturedNassSlice] = []
    payloads: list[bytes] = []

    for level, envelope in document["slices"].items():
        item = NassSlice(product.product_id, level, sample_year)
        rows = envelope["data"]["data"]
        provider_count = 10**6 if level in over_limit_levels else len(rows)
        count_payload = json.dumps({"count": str(provider_count)}).encode("utf-8")
        count_capture_id = capture_slice(
            connection_factory,
            control,
            run_id=run_id,
            product=product,
            item=item,
            endpoint=API_COUNT_PATH,
            parameters=count_parameters(product, item),
            payload=count_payload,
        )
        counts.append(
            NassSliceCount(item.slice_key, level, sample_year, provider_count)
        )
        if level in over_limit_levels:
            slices.append(
                CapturedNassSlice(
                    item.slice_key,
                    level,
                    sample_year,
                    provider_count,
                    0,
                    count_capture_id,
                    None,
                    SLICE_OVER_LIMIT,
                )
            )
            continue
        data_payload = json.dumps(envelope["data"]).encode("utf-8")
        payloads.append(data_payload)
        data_capture_id = capture_slice(
            connection_factory,
            control,
            run_id=run_id,
            product=product,
            item=item,
            endpoint=API_DATA_PATH,
            parameters=data_parameters(product, item),
            payload=data_payload,
        )
        slices.append(
            CapturedNassSlice(
                item.slice_key,
                level,
                sample_year,
                provider_count,
                len(rows),
                count_capture_id,
                data_capture_id,
                SLICE_CAPTURED,
            )
        )

    contract = summarize_release(product, payloads=payloads, slice_counts=counts)
    complete = not over_limit_levels
    decision = (
        ReleaseDecision.INGEST if complete else ReleaseDecision.OVER_LIMIT_QUARANTINE
    )
    control.set_run_watermark(
        run_id,
        watermark={
            "product_id": product.product_id,
            "extraction_watermark": contract.extraction_watermark,
        },
    )
    control.finish_run(run_id, status="success" if complete else "partial")
    release = CapturedNassRelease(
        run_id,
        product.product_id,
        "recent",
        tuple(slices),
        contract,
        decision,
        contract.total_row_count,
        complete,
    )
    persist_release_state(connection_factory, release)
    return release


def run_to_gold(
    connection_factory: Callable[[], connection],
    product: NassProduct,
    document: dict[str, Any],
    *,
    year: int | None = None,
) -> tuple[CapturedNassRelease, int, int]:
    release = persist_fixture_release(
        connection_factory, product=product, document=document, year=year
    )
    watermark = release.contract.extraction_watermark
    result = replay_captured_run(
        connection_factory,
        run_id=release.run_id,
        product=product,
        release_watermark=watermark,
    )
    persist_replay_result(
        connection_factory,
        run_id=release.run_id,
        product=product,
        release_watermark=watermark,
        result=result,
    )
    transformed = transform_release(
        connection_factory,
        run_id=release.run_id,
        product=product,
        release_watermark=watermark,
    )
    published = publish_release(
        connection_factory,
        run_id=release.run_id,
        product_id=product.product_id,
        release_watermark=watermark,
    )
    return release, transformed, published
