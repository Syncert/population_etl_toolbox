"""Shared fixtures for the CDC PLACES tract release (places-tract-release).

``tests/fixtures/cdc/places_tract_dane_offset_{0,2000,4000}.json`` are CDC's
own 4,920 Dane County rows of ``cwsq-ngmh`` (123 tracts x 40 measures, read
on 2026-11-12 by OFFICE_LAB probe 6), cut to the registered ``$select``
columns, ordered by the registered ``$order`` and split at 2,000 rows;
``places_tract_metadata.json`` keeps the view document's identity, watermark,
licence and column contract. A scripted client answers each page by its
``$offset`` and ``$limit`` (re-slicing the same rows at any other page size),
the view document, and ``count(*)`` with the fixture's row total, so database
tests run the adapter's own capture, resume, replay and publication code.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx

from data_ingestion_toolbox.cdc.config import SOCRATA_BASE_URL, CdcConfig
from data_ingestion_toolbox.cdc.registry import PLACES_TRACT_ASSET

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "cdc"
PAGE_SIZE = 2000
PAGE_OFFSETS = (0, 2000, 4000)
ROW_COUNT = 4920
DANE = "state:55|county:025"
FIRST_TRACT = "state:55|county:025|tract:000100"


def page_bytes(offset: int) -> bytes:
    return (FIXTURE_DIR / f"places_tract_dane_offset_{offset}.json").read_bytes()


def metadata_bytes() -> bytes:
    return (FIXTURE_DIR / "places_tract_metadata.json").read_bytes()


def all_rows() -> list[dict]:
    rows: list[dict] = []
    for offset in PAGE_OFFSETS:
        rows.extend(json.loads(page_bytes(offset)))
    return rows


class FixtureClient:
    """Answers the tract view document, pages and ``count(*)``."""

    def __init__(
        self,
        overrides: dict[int, httpx.Response] | None = None,
        *,
        count: int = ROW_COUNT,
    ) -> None:
        self.overrides = dict(overrides or {})
        self.count = count
        self.calls: list[tuple[str, dict]] = []

    def get(
        self, url: str, *, headers: dict | None = None, params: dict | None = None
    ) -> httpx.Response:
        parameters = dict(params or {})
        self.calls.append((url, parameters))
        request = httpx.Request("GET", url)
        if url == f"{SOCRATA_BASE_URL}{PLACES_TRACT_ASSET.metadata_path}":
            return httpx.Response(200, content=metadata_bytes(), request=request)
        if url != f"{SOCRATA_BASE_URL}{PLACES_TRACT_ASSET.api_path}":
            return httpx.Response(404, content=b"{}", request=request)
        if parameters.get("$select") == "count(*)":
            body = json.dumps([{"count": str(self.count)}]).encode()
            return httpx.Response(200, content=body, request=request)
        offset = int(parameters["$offset"])
        limit = int(parameters["$limit"])
        if offset in self.overrides:
            response = self.overrides[offset]
            return httpx.Response(
                response.status_code, content=response.content, request=request
            )
        if limit == PAGE_SIZE and offset in PAGE_OFFSETS:
            return httpx.Response(200, content=page_bytes(offset), request=request)
        body = json.dumps(all_rows()[offset : offset + limit]).encode()
        return httpx.Response(200, content=body, request=request)

    def close(self) -> None:
        return None


def config(**overrides: object) -> CdcConfig:
    values: dict[str, object] = {
        "socrata_app_token": "",
        "socrata_bulk_page_size": PAGE_SIZE,
        "socrata_min_spacing_seconds": 0,
        "socrata_max_attempts": 1,
    }
    values.update(overrides)
    return CdcConfig(**values)


# -- database tiers ------------------------------------------------------------

from collections.abc import Callable  # noqa: E402
from typing import Any  # noqa: E402
from uuid import UUID  # noqa: E402

import pytest  # noqa: E402
from psycopg2.extensions import connection  # noqa: E402

from data_ingestion_toolbox.cdc.capture import (  # noqa: E402
    CapturedCdcRelease,
    capture_asset_release,
    persist_release_state,
)
from data_ingestion_toolbox.cdc.gold_cdc.publisher import publish_release  # noqa: E402
from data_ingestion_toolbox.cdc.metadata import (  # noqa: E402
    load_latest_accepted_metadata,
)
from data_ingestion_toolbox.cdc.silver_cdc.replay import (  # noqa: E402
    replay_and_persist_run,
)
from data_ingestion_toolbox.cdc.silver_cdc.transform import (  # noqa: E402
    transform_release,
)
from tests.support.capture_seed import delete_geography, seed_geography  # noqa: E402
from tests.support.warehouse_scope import (  # noqa: E402
    delete_capture_graph,
    source_run_ids,
)


def dane_tract_codes() -> tuple[str, ...]:
    return tuple(sorted({row["locationid"][5:] for row in all_rows()}))


def _seed_arguments() -> tuple[dict[str, Any], ...]:
    return (
        {"geo_type": "nation", "vintage": 2020, "name": "United States"},
        {"geo_type": "state", "state_fips": "55", "vintage": 2020, "name": "Wisconsin"},
        {
            "geo_type": "county",
            "state_fips": "55",
            "county_fips": "025",
            "vintage": 2020,
            "name": "Dane County",
        },
        *(
            {
                "geo_type": "tract",
                "state_fips": "55",
                "county_fips": "025",
                "tract_code": tract,
                "vintage": 2020,
                "name": f"Census Tract {tract}",
            }
            for tract in dane_tract_codes()
        ),
    )


def _canonical(arguments: dict[str, Any]) -> str:
    if arguments["geo_type"] == "nation":
        return "us:1"
    if arguments["geo_type"] == "state":
        return f"state:{arguments['state_fips']}"
    county = f"state:{arguments['state_fips']}|county:{arguments['county_fips']}"
    if arguments["geo_type"] == "county":
        return county
    return f"{county}|tract:{arguments['tract_code']}"


def tracked_geo_ids() -> tuple[str, ...]:
    return tuple(_canonical(arguments) for arguments in _seed_arguments())


def load_release(
    connection_factory: Callable[[], connection],
    *,
    client: FixtureClient | None = None,
    cdc_config: CdcConfig | None = None,
) -> tuple[CapturedCdcRelease, int]:
    """Capture, replay, conform and publish the tract release as the DAG does."""
    previous = load_latest_accepted_metadata(connection_factory, PLACES_TRACT_ASSET)
    release = capture_asset_release(
        connection_factory,
        PLACES_TRACT_ASSET,
        previous_metadata=previous,
        config=cdc_config or config(),
        client=client or FixtureClient(),
    )
    persist_release_state(connection_factory, release)
    if release.decision.value != "ingest":
        return release, 0
    watermark = release.metadata.release_version
    replay_and_persist_run(
        connection_factory,
        run_id=release.run_id,
        asset=PLACES_TRACT_ASSET,
        release_watermark=watermark,
    )
    transform_release(
        connection_factory,
        run_id=release.run_id,
        asset=PLACES_TRACT_ASSET,
        release_watermark=watermark,
    )
    published = publish_release(
        connection_factory,
        run_id=release.run_id,
        asset_id=PLACES_TRACT_ASSET.asset_id,
        release_watermark=watermark,
    )
    return release, published


def _cleanup(
    connection_factory: Callable[[], connection],
    preexisting: set[str],
    baseline_run_ids: frozenset[UUID],
) -> Callable[[], None]:
    def run() -> None:
        owned = sorted(source_run_ids(connection_factory, "CDC") - baseline_run_ids)
        database_connection = connection_factory()
        try:
            with database_connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM control.publisher_ready_event WHERE source_code = 'CDC'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution "
                    "WHERE provider_source = 'CDC' AND provider_dataset = 'places_tract'"
                )
                for table in (
                    "silver_cdc.fact_health_observation",
                    "silver_cdc.observation_revision",
                    "silver_cdc.observation_quarantine",
                    "silver_cdc.dim_measure",
                    "silver_cdc.dim_dataset_release",
                    "control.cdc_release_page",
                    "control.cdc_dataset_release",
                ):
                    cursor.execute(
                        f"DELETE FROM {table} WHERE asset_id = 'places_tract'"
                    )
                delete_capture_graph(cursor, owned)
                for geo_id in sorted(
                    set(tracked_geo_ids()) - preexisting, reverse=True
                ):
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
    """Seed the nation, Wisconsin, Dane County and its 123 tracts; clean up after."""
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(tracked_geo_ids()),),
            )
            preexisting = {row[0] for row in cursor.fetchall()}
    finally:
        reader.close()
    request.addfinalizer(
        _cleanup(
            connection_factory, preexisting, source_run_ids(connection_factory, "CDC")
        )
    )
    writer = connection_factory()
    try:
        with writer.cursor() as cursor:
            for arguments in _seed_arguments():
                if _canonical(arguments) not in preexisting:
                    seed_geography(cursor, **arguments)
        writer.commit()
    except BaseException:
        writer.rollback()
        raise
    finally:
        writer.close()
    return connection_factory
