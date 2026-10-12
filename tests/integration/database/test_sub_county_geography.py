"""Tract and ZCTA geography replayed into the shared reference (sub-county-geography).

Covers: ETL-060
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.silver_ref import geography_pipeline
from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    GeographyRepository,
    parse_boundary_capture,
)
from tests.support.capture_seed import delete_geography, seed_capture
from tests.support.postgres import PostgresHookStub
from tests.support.warehouse_scope import delete_capture_graph, source_run_ids

pytestmark = [pytest.mark.integration, pytest.mark.database]

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "geography"
VINTAGE = 2024
ASSET_FIXTURES = {
    "2024_Gaz_tracts_national.zip": "2024_Gaz_tracts_DE.zip",
    "2024_Gaz_zcta_national.zip": "2024_Gaz_zcta_DE.zip",
    "cb_2024_us_tract_500k.zip": "cb_2024_10_tract_500k.zip",
    "cb_2020_us_zcta520_500k.zip": "cb_2020_zcta520_DE_500k.zip",
}


def _rows(
    factory: Callable[[], connection], sql: str, parameters: tuple | None = None
) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


@pytest.fixture
def delaware(
    postgres_connection_factory: Callable[[], connection],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Callable[[set[str]], None]]:
    """Load Delaware's counties and places, and serve its sub-county fixtures."""
    factory = postgres_connection_factory
    baseline = source_run_ids(factory, "CENSUS_GEO")
    created: set[str] = set()

    def download(_client, url: str, **_kwargs):  # noqa: ANN001, ANN202
        import httpx

        name = url.rsplit("/", 1)[1]
        return httpx.Response(
            200,
            content=(FIXTURES / ASSET_FIXTURES[name]).read_bytes(),
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(
        geography_pipeline, "_get_hook", lambda: PostgresHookStub(factory)
    )
    monkeypatch.setattr(geography_pipeline, "_download_with_retry", download)

    def load(counties: set[str]) -> None:
        writer = factory()
        try:
            with writer.cursor() as cursor:
                capture = seed_capture(cursor, "CENSUS_GEO", b"delaware-core-2024")
                existing = {
                    row[0]
                    for row in _query(
                        cursor,
                        "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id LIKE 'state:10|%%'",
                    )
                }
            writer.commit()
        finally:
            writer.close()
        repository = GeographyRepository(factory)
        for geo_type, name in (
            ("county", "cb_2024_county_DE_500k.zip"),
            ("place", "cb_2024_place_DE_500k.zip"),
        ):
            boundaries = [
                record
                for record in parse_boundary_capture(
                    (FIXTURES / name).read_bytes(),
                    geo_type=geo_type,
                    boundary_vintage=VINTAGE,
                )
                if geo_type != "county" or record.geo_id in counties
            ]
            fresh = [record for record in boundaries if record.geo_id not in existing]
            created.update(record.geo_id for record in fresh)
            repository.load_attributes(
                [record.geography for record in fresh], capture_id=capture
            )
            repository.load_geometries(fresh, capture_id=capture)

    yield load

    cleanup = factory()
    try:
        with cleanup.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_ref.geography_resolution WHERE provider_source = 'CENSUS_GEO' AND provider_dataset = 'tract'"
            )
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_type IN ('tract', 'zcta')"
            )
            for (geo_id,) in cursor.fetchall():
                delete_geography(cursor, geo_id)
            for geo_id in sorted(created):
                delete_geography(cursor, geo_id)
            delete_capture_graph(
                cursor, sorted(source_run_ids(factory, "CENSUS_GEO") - baseline)
            )
        cleanup.commit()
    finally:
        cleanup.close()


def _query(cursor, sql: str, parameters: tuple | None = None) -> list[tuple]:  # noqa: ANN001
    cursor.execute(sql, parameters)
    return list(cursor.fetchall())


ALL_COUNTIES = {"state:10|county:001", "state:10|county:003", "state:10|county:005"}


def test_tracts_nest_in_their_counties_and_zctas_overlap_by_area(
    postgres_connection_factory: Callable[[], connection], delaware
) -> None:
    """Covers: ETL-060 — every Delaware tract is contained by its county; ZCTA overlaps carry weights."""
    factory = postgres_connection_factory
    delaware(ALL_COUNTIES)
    counts = geography_pipeline.sync_sub_county_geography(source_year=VINTAGE)
    assert counts["refused"] == 0 and counts["attributes"] >= 262 + 157
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_ref.dim_geo_entity WHERE geo_type = 'tract'",
    ) == [(262,)]
    contained = _rows(
        factory,
        """
        SELECT county.geo_id, COUNT(*)
        FROM silver_ref.bridge_geo_relationship_version AS bridge
        JOIN silver_ref.dim_geo_entity AS county ON county.geo_sk = bridge.parent_geo_sk
        JOIN silver_ref.dim_geo_entity AS tract ON tract.geo_sk = bridge.related_geo_sk
        WHERE bridge.relationship_type = 'contains' AND tract.geo_type = 'tract'
        GROUP BY county.geo_id ORDER BY county.geo_id
        """,
    )
    assert sum(count for _geo, count in contained) == 262
    assert [geo for geo, _count in contained] == sorted(ALL_COUNTIES)
    current = _rows(
        factory,
        """
        SELECT county_name, area_name, tract_code, geom IS NOT NULL
        FROM silver_ref.dim_geo_current WHERE geo_id = 'state:10|county:001|tract:040100'
        """,
    )
    assert current == [("Kent County", "Census Tract 401", "040100", True)]
    dover = _rows(
        factory,
        """
        SELECT area.geo_id, ROUND(bridge.overlap_weight::NUMERIC, 2)
        FROM silver_ref.bridge_geo_relationship_version AS bridge
        JOIN silver_ref.dim_geo_entity AS zcta ON zcta.geo_sk = bridge.parent_geo_sk
        JOIN silver_ref.dim_geo_entity AS area ON area.geo_sk = bridge.related_geo_sk
        WHERE zcta.geo_id = 'zcta:19901' AND area.geo_type = 'county'
        """,
    )
    assert dover and dover[0][0] == "state:10|county:001" and dover[0][1] > 0.9
    sums = _rows(
        factory,
        """
        SELECT MAX(total) FROM (
            SELECT bridge.parent_geo_sk, SUM(bridge.overlap_weight) AS total
            FROM silver_ref.bridge_geo_relationship_version AS bridge
            JOIN silver_ref.dim_geo_entity AS zcta ON zcta.geo_sk = bridge.parent_geo_sk
            JOIN silver_ref.dim_geo_entity AS area ON area.geo_sk = bridge.related_geo_sk
            WHERE zcta.geo_type = 'zcta' AND area.geo_type = 'county'
            GROUP BY bridge.parent_geo_sk
        ) AS weights
        """,
    )
    assert sums[0][0] <= 1.0001
    again = geography_pipeline.sync_sub_county_geography(source_year=VINTAGE)
    assert again["refused"] == 0
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_ref.dim_geo_entity_version AS version JOIN silver_ref.dim_geo_entity AS entity USING (geo_sk) WHERE entity.geo_type = 'tract'",
    ) == [(262,)]


def test_a_tract_whose_county_is_absent_is_refused_not_guessed(
    postgres_connection_factory: Callable[[], connection], delaware
) -> None:
    """Covers: ETL-060 — Sussex's tracts are refused when Sussex is not in the reference."""
    factory = postgres_connection_factory
    delaware(ALL_COUNTIES - {"state:10|county:005"})
    counts = geography_pipeline.sync_sub_county_geography(source_year=VINTAGE)
    refused = _rows(
        factory,
        """
        SELECT COUNT(*), MIN(reason_code), MIN(status) FROM silver_ref.geography_resolution
        WHERE provider_source = 'CENSUS_GEO' AND provider_dataset = 'tract'
        """,
    )
    assert counts["refused"] == refused[0][0] > 0
    assert refused[0][1:] == ("parent_county_absent", "unmapped")
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_ref.dim_geo_entity WHERE geo_type = 'tract' AND county_fips = '005' AND state_fips = '10'",
    ) == [(0,)]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_ref.dim_geo_entity WHERE geo_type = 'tract'",
    ) == [(262 - counts["refused"],)]
