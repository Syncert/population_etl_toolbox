"""Shared seeding and cleanup for NOAA climate normals database tests.

The checked-in ``tests/fixtures/noaa_normals/annualseasonal_by_station.tar.gz``
is NCEI's 1991-2020 annual/seasonal by-station archive (v1.0.1, c20230404)
trimmed to eleven station files copied verbatim: Delaware's eight stations
(standard, representative, and a precipitation-only estimated station), a
California station with ``X`` flags, a Puerto Rico station with provisional
temperature, and a Canadian station. It is played through the adapter's own
capture path by a scripted client.

Delaware's three counties are seeded with rectangular boundaries of boundary
vintage 2024 that split the state at latitudes 38.86 and 39.38 between
longitudes -75.8 and -75.0, so each Delaware station falls inside exactly
one of them and the other stations fall inside none.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.noaa_normals.capture import capture_archive
from data_ingestion_toolbox.noaa_normals.config import SOURCE_CODE, NormalsConfig
from data_ingestion_toolbox.noaa_normals.silver_noaa_normals.load import (
    publish_run,
    replay_run,
)
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.warehouse_scope import (
    delete_capture_graph,
    delete_harvested_glossary_rows,
    glossary_registration_exists,
    source_run_ids,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = (
    REPOSITORY_ROOT / "tests/fixtures/noaa_normals/annualseasonal_by_station.tar.gz"
)

KENT = "state:10|county:001"
NEW_CASTLE = "state:10|county:003"
SUSSEX = "state:10|county:005"
BOUNDARY_VINTAGE = 2024

#: (county FIPS, name, south latitude, north latitude).
_COUNTIES: tuple[tuple[str, str, float, float], ...] = (
    ("001", "Kent County", 38.86, 39.38),
    ("003", "New Castle County", 39.38, 39.85),
    ("005", "Sussex County", 38.45, 38.86),
)
_WEST, _EAST = -75.8, -75.0

TRACKED_GEO_IDS = ("state:10",) + tuple(
    f"state:10|county:{fips}" for fips, *_ in _COUNTIES
)


def fixture_bytes() -> bytes:
    return FIXTURE.read_bytes()


class FixtureClient:
    """Answers the archive path with the fixture or an override."""

    def __init__(self, override: bytes | None = None, *, status: int = 200) -> None:
        self.override = override
        self.status = status
        self.calls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        self.calls.append(url)
        request = httpx.Request("GET", url)
        content = self.override if self.override is not None else fixture_bytes()
        return httpx.Response(self.status, content=content, request=request)

    def close(self) -> None:
        return None


def run_to_gold(
    connection_factory: Callable[[], connection],
    *,
    client: FixtureClient | None = None,
) -> tuple[UUID, str, int, int]:
    """Capture, replay and publish the archive; return (run, status, normals, published)."""
    run_id, status = capture_archive(
        connection_factory,
        config=NormalsConfig(min_spacing_seconds=0, max_attempts=1),
        client=client or FixtureClient(),
    )
    normals = replay_run(connection_factory, run_id=run_id)
    published = publish_run(connection_factory, run_id=run_id)
    return run_id, status, normals, published


def run_all(connection_factory: Callable[[], connection]) -> None:
    run_to_gold(connection_factory)


def _seed_county_boundary(cursor: Any, geo_id: str, south: float, north: float) -> None:
    cursor.execute(
        """
        SELECT entity.geo_sk, version.source_snapshot_id
        FROM silver_ref.dim_geo_entity AS entity
        JOIN silver_ref.dim_geo_entity_version AS version ON version.geo_sk = entity.geo_sk
        WHERE entity.geo_id = %s
        ORDER BY version.geography_vintage DESC
        LIMIT 1
        """,
        (geo_id,),
    )
    geo_sk, snapshot = cursor.fetchone()
    wkt = (
        f"MULTIPOLYGON((({_WEST} {south},{_EAST} {south},{_EAST} {north},"
        f"{_WEST} {north},{_WEST} {south})))"
    )
    cursor.execute(
        """
        INSERT INTO silver_ref.dim_geo_geometry_version (
            geo_sk, boundary_vintage, geometry_source, resolution, source_snapshot_id,
            geom, geometry_checksum, is_valid
        ) VALUES (%s, %s, 'test_rectangle', 'test', %s, ST_GeomFromText(%s, 4326), %s, TRUE)
        ON CONFLICT DO NOTHING
        """,
        (
            geo_sk,
            BOUNDARY_VINTAGE,
            snapshot,
            wkt,
            hashlib.sha256(wkt.encode()).hexdigest(),
        ),
    )


def reviewed_warehouse(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    """Seed Delaware's counties with boundaries and remove all normals state afterwards."""
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(TRACKED_GEO_IDS),),
            )
            preexisting = {row[0] for row in cursor.fetchall()}
    finally:
        reader.close()
    preexisting_glossary = glossary_registration_exists(connection_factory, SOURCE_CODE)
    baseline = source_run_ids(connection_factory, SOURCE_CODE)

    def cleanup() -> None:
        owned_runs = sorted(source_run_ids(connection_factory, SOURCE_CODE) - baseline)
        database_connection = connection_factory()
        try:
            with database_connection.cursor() as cursor:
                delete_harvested_glossary_rows(
                    cursor, SOURCE_CODE, preexisting=preexisting_glossary
                )
                cursor.execute(
                    "DELETE FROM control.publisher_ready_event WHERE source_code = %s",
                    (SOURCE_CODE,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution WHERE provider_source = %s",
                    (SOURCE_CODE,),
                )
                for table in (
                    "silver_noaa_normals.station_normal",
                    "silver_noaa_normals.station",
                    "silver_noaa_normals.quarantine",
                    "control.noaa_normals_file",
                ):
                    cursor.execute(f"DELETE FROM {table}")
                cursor.execute(
                    "DELETE FROM silver_ref.dim_geo_geometry_version WHERE geometry_source = 'test_rectangle'"
                )
                delete_capture_graph(cursor, owned_runs)
                for geo_id in sorted(
                    set(TRACKED_GEO_IDS) - preexisting,
                    key=lambda value: (-len(value), value),
                ):
                    delete_geography(cursor, geo_id)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    request.addfinalizer(cleanup)
    writer = connection_factory()
    try:
        with writer.cursor() as cursor:
            if "state:10" not in preexisting:
                seed_geography(
                    cursor,
                    geo_type="state",
                    state_fips="10",
                    vintage=2024,
                    name="Delaware",
                )
            for fips, name, south, north in _COUNTIES:
                geo_id = f"state:10|county:{fips}"
                if geo_id not in preexisting:
                    seed_geography(
                        cursor,
                        geo_type="county",
                        state_fips="10",
                        county_fips=fips,
                        vintage=2024,
                        name=name,
                    )
                _seed_county_boundary(cursor, geo_id, south, north)
        writer.commit()
    except BaseException:
        writer.rollback()
        raise
    finally:
        writer.close()
    return connection_factory
