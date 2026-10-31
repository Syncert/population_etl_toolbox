"""Place and metro boundaries by vintage, and the tile functions that serve them.

Covers: ETL-105, DB-067 (tiles-places-and-metros)
"""

from __future__ import annotations

import io
import json
import zipfile
from collections.abc import Callable, Iterator

import mapbox_vector_tile
import pytest
import shapefile
from psycopg2.extensions import connection

from data_ingestion_toolbox.silver_ref.area_geography import publish_metro_boundaries
from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    GeographyRecord,
    GeographyRepository,
    GeometryRecord,
    parse_boundary_capture,
)
from tests.support.capture_seed import (
    delete_geography,
    delete_shared_geographies,
    preexisting_geographies,
    seed_capture,
    seed_geography,
)

pytestmark = [pytest.mark.integration, pytest.mark.database]

STATE = "state:56"
GROWING = "state:56|place:99901"
GONE = "state:56|place:99902"
METRO = "cbsa:99990"
UNLISTED = "cbsa:99991"
OWNED = (GROWING, GONE, METRO)

# Cheyenne-ish, small, and well inside one zoom-8 tile.
SMALL = [[-104.90, 41.10], [-104.80, 41.10], [-104.80, 41.20], [-104.90, 41.20]]
LARGER = [[-104.95, 41.05], [-104.75, 41.05], [-104.75, 41.25], [-104.95, 41.25]]
TILE_Z8 = (8, 53, 95)  # contains (-104.85, 41.15)


def _polygon(ring: list[list[float]]) -> str:
    closed = [*ring, ring[0]]
    return json.dumps(
        {"type": "Polygon", "coordinates": [closed]},
        sort_keys=True,
        separators=(",", ":"),
    )


def _cbsa_boundary_zip(codes: list[str]) -> bytes:
    shp, shx, dbf = io.BytesIO(), io.BytesIO(), io.BytesIO()
    writer = shapefile.Writer(shp=shp, shx=shx, dbf=dbf, shapeType=shapefile.POLYGON)
    writer.field("CBSAFP", "C", size=5)
    writer.field("NAME", "C", size=100)
    writer.field("NAMELSAD", "C", size=100)
    writer.field("LSAD", "C", size=2)
    for code in codes:
        # Clockwise: a shapefile's exterior ring.
        writer.poly([[*reversed([*LARGER, LARGER[0]])]])
        writer.record(code, "Testville, WY", "Testville, WY Metro Area", "M1")
    writer.close()
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("cbsa.shp", shp.getvalue())
        archive.writestr("cbsa.shx", shx.getvalue())
        archive.writestr("cbsa.dbf", dbf.getvalue())
    return payload.getvalue()


def _cleanup(factory: Callable[[], connection], preexisting: set[str]) -> None:
    cleanup = factory()
    try:
        with cleanup.cursor() as cursor:
            cursor.execute(
                "DELETE FROM gold_glossary.tile_boundary_version WHERE geo_id = ANY(%s)",
                (list(OWNED),),
            )
            cursor.execute(
                "DELETE FROM silver_ref.geography_resolution "
                "WHERE provider_dataset = 'census_cbsa_boundary' AND source_code = '99991'"
            )
            for geo_id in OWNED:
                delete_geography(cursor, geo_id)
            delete_shared_geographies(cursor, [STATE], preexisting)
        cleanup.commit()
    finally:
        cleanup.close()


@pytest.fixture
def seeded(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[Callable[[], connection]]:
    factory = postgres_connection_factory
    probe = factory()
    try:
        with probe.cursor() as cursor:
            preexisting = preexisting_geographies(cursor, [STATE])
    finally:
        probe.close()
    _cleanup(factory, preexisting)

    writer = factory()
    try:
        with writer.cursor() as cursor:
            if STATE not in preexisting:
                seed_geography(
                    cursor,
                    geo_type="state",
                    state_fips="56",
                    vintage=2020,
                    name="Wyoming",
                )
            # A place that grew between vintages, and one that is gone by 2020.
            seed_geography(
                cursor,
                geo_type="place",
                state_fips="56",
                place_fips="99901",
                vintage=2010,
                name="Growing town",
            )
            seed_geography(
                cursor,
                geo_type="place",
                state_fips="56",
                place_fips="99901",
                vintage=2020,
                name="Growing city",
            )
            seed_geography(
                cursor,
                geo_type="place",
                state_fips="56",
                place_fips="99902",
                vintage=2010,
                name="Gone village",
            )
            capture_id = seed_capture(cursor, "CENSUS_GEO")
        writer.commit()

        repository = GeographyRepository(factory)
        repository.load_geometries(
            [
                GeometryRecord(GROWING, 2010, _polygon(SMALL)),
                GeometryRecord(GROWING, 2020, _polygon(LARGER)),
                GeometryRecord(GONE, 2010, _polygon(SMALL)),
            ],
            capture_id=capture_id,
        )
        repository.load_attributes(
            [
                GeographyRecord(
                    "metro",
                    METRO,
                    "99990",
                    None,
                    None,
                    None,
                    "Testville, WY",
                    2023,
                    lsad="Metropolitan Statistical Area",
                    area_code="99990",
                )
            ],
            capture_id=capture_id,
        )
        boundaries = parse_boundary_capture(
            _cbsa_boundary_zip(["99990", "99991"]),
            geo_type="metro",
            boundary_vintage=2023,
        )
        publication = factory()
        try:
            counts = publish_metro_boundaries(
                repository,
                boundaries,
                listed_geo_ids={METRO},
                delineation_vintage=2023,
                capture_id=capture_id,
                connection=publication,
            )
            publication.commit()
        finally:
            publication.close()
        assert counts == {"metro_boundaries": 1, "unlisted_metro_boundaries": 1}
        with writer.cursor() as cursor:
            cursor.execute("CALL gold_glossary.refresh_tile_boundaries()")
        writer.commit()
        yield factory
    finally:
        writer.close()
        _cleanup(factory, preexisting)


def _rows(factory: Callable[[], connection]) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT geo_id, geo_level, boundary_vintage, geo_name, state_name,
                       area_code, lsad, refreshed_at
                FROM gold_glossary.tile_boundary_version
                WHERE geo_id = ANY(%s)
                ORDER BY geo_id, boundary_vintage
                """,
                (list(OWNED),),
            )
            return cursor.fetchall()
    finally:
        reader.close()


def _tile(factory: Callable[[], connection], layer: str, params: dict) -> dict:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                f"SELECT gold.tile_{layer}(%s, %s, %s, %s::json)",
                (*TILE_Z8, json.dumps(params)),
            )
            payload = cursor.fetchone()[0]
    finally:
        reader.close()
    if payload is None or len(bytes(payload)) == 0:
        return {}
    return mapbox_vector_tile.decode(bytes(payload))


def _ids(decoded: dict, layer: str) -> dict[str, int]:
    return {
        feature["properties"]["geo_id"]: feature["properties"]["boundary_vintage"]
        for feature in decoded.get(layer, {}).get("features", [])
        if feature["properties"]["geo_id"] in OWNED
    }


def test_metro_polygons_load_for_listed_cbsas_and_the_rest_are_ledgered(
    seeded: Callable[[], connection],
) -> None:
    """Covers: ETL-105 — a CBSA polygon attaches to a listed CBSA, never to an invented one."""
    reader = seeded()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT boundary.boundary_vintage
                FROM silver_ref.dim_geo_geometry_version AS boundary
                JOIN silver_ref.dim_geo_entity AS entity USING (geo_sk)
                WHERE entity.geo_id = %s
                """,
                (METRO,),
            )
            assert cursor.fetchall() == [(2023,)]
            cursor.execute(
                "SELECT COUNT(*) FROM silver_ref.dim_geo_entity WHERE geo_id = %s",
                (UNLISTED,),
            )
            assert cursor.fetchone() == (0,)
            cursor.execute(
                """
                SELECT status, reason_code, source_vintage
                FROM silver_ref.geography_resolution
                WHERE provider_dataset = 'census_cbsa_boundary' AND source_code = '99991'
                """
            )
            assert cursor.fetchall() == [
                ("unmapped", "boundary_not_in_delineation", 2023)
            ]
            # The delineation's name stands; the boundary file adds no version.
            cursor.execute(
                """
                SELECT version.name FROM silver_ref.dim_geo_entity_version AS version
                JOIN silver_ref.dim_geo_entity AS entity USING (geo_sk)
                WHERE entity.geo_id = %s
                """,
                (METRO,),
            )
            assert cursor.fetchall() == [("Testville, WY",)]
    finally:
        reader.close()


def test_every_vintage_is_held_with_the_name_it_had_then(
    seeded: Callable[[], connection],
) -> None:
    """Covers: DB-067 — one row per geography and boundary vintage, named as of that vintage."""
    rows = [row[:7] for row in _rows(seeded)]
    assert rows == [
        (
            METRO,
            "METRO",
            2023,
            "Testville, WY",
            None,
            "99990",
            "Metropolitan Statistical Area",
        ),
        (GROWING, "PLACE", 2010, "Growing town", "Wyoming", None, None),
        (GROWING, "PLACE", 2020, "Growing city", "Wyoming", None, None),
        (GONE, "PLACE", 2010, "Gone village", "Wyoming", None, None),
    ]
    reader = seeded()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT geo_level, boundary_vintage, is_default FROM gold.tile_layer_vintage "
                "WHERE geo_level IN ('PLACE', 'METRO') ORDER BY 1, 2"
            )
            held = cursor.fetchall()
    finally:
        reader.close()
    assert ("METRO", 2023, True) in held
    place_defaults = [
        vintage for level, vintage, default in held if level == "PLACE" and default
    ]
    assert len(place_defaults) == 1
    assert place_defaults[0] >= 2020


def test_a_tile_draws_the_vintage_it_is_asked_for_and_never_another(
    seeded: Callable[[], connection],
) -> None:
    """Covers: DB-067 — ?vintage= selects; unheld or malformed draws nothing; features carry no value."""
    asked_2010 = _tile(seeded, "places", {"vintage": "2010"})
    assert _ids(asked_2010, "places") == {GROWING: 2010, GONE: 2010}
    asked_2020 = _tile(seeded, "places", {"vintage": "2020"})
    assert _ids(asked_2020, "places") == {GROWING: 2020}
    assert _ids(_tile(seeded, "places", {"vintage": "1999"}), "places") == {}
    assert _ids(_tile(seeded, "places", {"vintage": "20x0"}), "places") == {}

    metros = _tile(seeded, "metros", {})
    assert _ids(metros, "metros") == {METRO: 2023}
    (feature,) = [
        feature
        for feature in metros["metros"]["features"]
        if feature["properties"]["geo_id"] == METRO
    ]
    # A property with no value is left out of the feature, which is how an
    # MVT spells NULL: the delineation file gives a metro no internal point.
    assert (
        {"geo_id", "geo_level", "cbsa_code", "metro_name", "boundary_vintage"}
        <= set(feature["properties"])
        <= {
            "geo_id",
            "geo_level",
            "cbsa_code",
            "metro_name",
            "lsad",
            "latitude",
            "longitude",
            "boundary_vintage",
        }
    )
    assert feature["properties"]["metro_name"] == "Testville, WY"
    place = next(
        feature
        for feature in asked_2010["places"]["features"]
        if feature["properties"]["geo_id"] == GROWING
    )
    assert set(place["properties"]) <= {
        "geo_id",
        "geo_level",
        "state_fips",
        "place_fips",
        "state_name",
        "place_name",
        "latitude",
        "longitude",
        "boundary_vintage",
    }
    assert place["properties"]["place_name"] == "Growing town"


def test_the_refresh_rewrites_only_what_changed_and_drops_what_was_withdrawn(
    seeded: Callable[[], connection],
) -> None:
    """Covers: DB-067 — an idle refresh writes nothing; a withdrawn vintage leaves."""
    before = {(row[0], row[2]): row[7] for row in _rows(seeded)}
    writer = seeded()
    try:
        with writer.cursor() as cursor:
            cursor.execute("CALL gold_glossary.refresh_tile_boundaries()")
        writer.commit()
        assert {(row[0], row[2]): row[7] for row in _rows(seeded)} == before

        with writer.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM silver_ref.dim_geo_geometry_version
                WHERE geo_sk = (SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = %s)
                """,
                (GONE,),
            )
            cursor.execute("CALL gold_glossary.refresh_tile_boundaries()")
        writer.commit()
    finally:
        writer.close()
    after = {(row[0], row[2]): row[7] for row in _rows(seeded)}
    assert (GONE, 2010) not in after
    assert after == {key: value for key, value in before.items() if key[0] != GONE}
