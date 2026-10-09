"""Regions, divisions and CBSAs reach the shared reference with their members.

Covers: ETL-075
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.silver_ref.area_geography import (
    parse_cbsa_delineation,
    parse_regions_and_divisions,
    publish_area_snapshot,
)
from data_ingestion_toolbox.silver_ref.geography_pipeline import GeographyRepository
from tests.support.capture_seed import delete_geography, seed_capture, seed_geography

pytestmark = [pytest.mark.integration, pytest.mark.database]

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "silver_ref" / "area"
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
AREA_IDS = [
    "region:1",
    "region:2",
    "region:3",
    "region:4",
    *(f"division:{code}" for code in range(1, 10)),
    "cbsa:10180",
    "cbsa:25540",
    "cbsa:31540",
]


def _cleanup(factory: Callable[[], connection]) -> None:
    cleanup = factory()
    try:
        with cleanup.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_ref.geography_resolution "
                "WHERE provider_dataset IN ('census_cbsa_delineation', 'census_region_division_codes')"
            )
            for geo_id in AREA_IDS:
                delete_geography(cursor, geo_id)
            for geo_id in ("state:55|county:025", "state:55"):
                delete_geography(cursor, geo_id)
        cleanup.commit()
    finally:
        cleanup.close()


def test_a_county_resolves_to_its_region_division_and_metro(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ETL-075 — membership by code, an absent member ledgered, a replay changing nothing."""
    _cleanup(postgres_connection_factory)
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            seed_geography(
                cursor,
                geo_type="state",
                state_fips="55",
                vintage=2023,
                name="Wisconsin",
            )
            seed_geography(
                cursor,
                geo_type="county",
                state_fips="55",
                county_fips="025",
                vintage=2023,
                name="Dane County",
            )
            capture_id = seed_capture(cursor, "CENSUS_GEO")
        writer.commit()

        repository = GeographyRepository(postgres_connection_factory)
        snapshots = [
            parse_regions_and_divisions(
                (FIXTURES / "NST-EST2024-ALLDATA.excerpt.csv").read_bytes(),
                vintage=2024,
            ),
            parse_cbsa_delineation(
                (FIXTURES / "cbsa-est2024-alldata.excerpt.csv").read_bytes(),
                delineation_vintage=2023,
            ),
        ]

        def publish() -> list[dict[str, int]]:
            publication = postgres_connection_factory()
            try:
                counts = [
                    publish_area_snapshot(
                        repository,
                        snapshot,
                        capture_id=capture_id,
                        connection=publication,
                    )
                    for snapshot in snapshots
                ]
                publication.commit()
                return counts
            finally:
                publication.close()

        first = publish()
        assert first[0]["areas"] == 13
        assert first[1]["areas"] == 3

        with writer.cursor() as cursor:
            cursor.execute(
                """
                SELECT parent.geo_type, parent.geo_id
                FROM silver_ref.bridge_geo_relationship_version AS edge
                JOIN silver_ref.dim_geo_entity AS parent ON parent.geo_sk = edge.parent_geo_sk
                JOIN silver_ref.dim_geo_entity AS member ON member.geo_sk = edge.related_geo_sk
                WHERE edge.relationship_type = 'contains'
                  AND (member.geo_id = 'state:55|county:025'
                       OR (member.geo_id = 'state:55'
                           AND parent.geo_type IN ('census_region', 'census_division')))
                ORDER BY 1, 2
                """
            )
            assert cursor.fetchall() == [
                ("census_division", "division:3"),
                ("census_region", "region:2"),
                ("metro", "cbsa:31540"),
            ]
            # Connecticut's planning regions are not in this reference: each
            # is ledgered with the delineation that named it, not dropped.
            cursor.execute(
                """
                SELECT source_geo_type, source_code, source_vintage, status, reason_code
                FROM silver_ref.geography_resolution
                WHERE provider_dataset = 'census_cbsa_delineation' AND source_code = '09110'
                """
            )
            assert cursor.fetchall() == [
                ("county", "09110", 2023, "unmapped", "member_not_in_reference")
            ]
            cursor.execute(
                "SELECT area_code, geo_type FROM silver_ref.dim_geo_entity WHERE geo_id = 'cbsa:31540'"
            )
            assert cursor.fetchone() == ("31540", "metro")
            # An area has no state, county or place name of its own, so its
            # name is `area_name`; without it the served catalog names the
            # area by its id ("cbsa:31540"), which a page cannot show.
            cursor.execute(
                "SELECT geo_id, area_name FROM silver_ref.dim_geo_current "
                "WHERE geo_id IN ('cbsa:31540', 'region:2', 'division:3') ORDER BY geo_id"
            )
            assert cursor.fetchall() == [
                ("cbsa:31540", "Madison, WI"),
                ("division:3", "East North Central"),
                ("region:2", "Midwest Region"),
            ]
            cursor.execute(
                "SELECT COUNT(*) FROM silver_ref.dim_geo_entity_version AS v "
                "JOIN silver_ref.dim_geo_entity AS e USING (geo_sk) WHERE e.geo_id = ANY(%s)",
                (AREA_IDS,),
            )
            versions = cursor.fetchone()[0]
            cursor.execute(
                "SELECT COUNT(*) FROM silver_ref.bridge_geo_relationship_version"
            )
            edges = cursor.fetchone()[0]
        writer.commit()

        second = publish()
        assert [c["memberships"] for c in second] == [0, 0]
        with writer.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM silver_ref.dim_geo_entity_version AS v "
                "JOIN silver_ref.dim_geo_entity AS e USING (geo_sk) WHERE e.geo_id = ANY(%s)",
                (AREA_IDS,),
            )
            assert cursor.fetchone()[0] == versions
            cursor.execute(
                "SELECT COUNT(*) FROM silver_ref.bridge_geo_relationship_version"
            )
            assert cursor.fetchone()[0] == edges
        writer.commit()
    finally:
        writer.close()
        _cleanup(postgres_connection_factory)


def test_an_area_identity_must_match_its_code(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ETL-075 — the warehouse refuses an area whose id and code disagree."""
    connection_ = postgres_connection_factory()
    try:
        with connection_.cursor() as cursor:
            for geo_type, geo_id, area_code in (
                ("census_region", "region:5", "5"),
                ("metro", "cbsa:31540", "31541"),
                ("provider_area", "area:bls_cpi:S12A", "S12A"),
            ):
                cursor.execute("SAVEPOINT attempt")
                with pytest.raises(Exception, match="dim_geo_entity_check1"):
                    cursor.execute(
                        """
                        INSERT INTO silver_ref.dim_geo_entity (
                            geo_id, geo_type, area_code, first_seen_version, last_seen_version
                        ) VALUES (%s, %s, %s, 2023, 2023)
                        """,
                        (geo_id, geo_type, area_code),
                    )
                cursor.execute("ROLLBACK TO SAVEPOINT attempt")
    finally:
        connection_.rollback()
        connection_.close()


def test_reapplying_the_reference_ddl_admits_the_area_types(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ETL-075 — a warehouse whose identity check predates areas is brought forward."""
    ddl = (
        REPOSITORY_ROOT / "src/data_ingestion_toolbox/silver_ref/DDL/silver_ref.sql"
    ).read_text(encoding="utf-8")
    connection_ = postgres_connection_factory()
    try:
        with connection_.cursor() as cursor:
            cursor.execute(
                "ALTER TABLE silver_ref.dim_geo_entity DROP CONSTRAINT dim_geo_entity_check1"
            )
            cursor.execute(
                """
                ALTER TABLE silver_ref.dim_geo_entity ADD CONSTRAINT dim_geo_entity_check1
                CHECK ((geo_type = 'nation' AND geo_id = 'us:1') OR geo_type = 'tract')
                NOT VALID
                """
            )
            cursor.execute(ddl)
            cursor.execute(ddl)
            cursor.execute(
                """
                SELECT pg_get_constraintdef(oid) FROM pg_constraint
                WHERE conname = 'dim_geo_entity_check1'
                  AND conrelid = 'silver_ref.dim_geo_entity'::REGCLASS
                """
            )
            definition = cursor.fetchone()[0]
            assert "provider_area" in definition and "census_region" in definition
            cursor.execute(
                """
                INSERT INTO silver_ref.dim_geo_entity (
                    geo_id, geo_type, area_code, first_seen_version, last_seen_version
                ) VALUES ('region:2', 'census_region', '2', 2024, 2024)
                """
            )
    finally:
        connection_.rollback()
        connection_.close()


def test_bls_cpi_metros_load_as_provider_areas(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ETL-076 — BLS's own metros are reference entities under BLS's codes."""
    from data_ingestion_toolbox.silver_ref.provider_areas import (
        parse_bls_cpi_areas,
        provider_area_records,
    )

    payload = (FIXTURES.parent / "provider_areas" / "bls_cu.area").read_bytes()
    records = provider_area_records(
        "bls_cpi", parse_bls_cpi_areas(payload), vintage=2026
    )
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            capture_id = seed_capture(cursor, "BLS")
        writer.commit()
        repository = GeographyRepository(postgres_connection_factory)
        assert repository.load_attributes(records, capture_id=capture_id) == 23
        assert repository.load_attributes(records, capture_id=capture_id) == 23
        with writer.cursor() as cursor:
            cursor.execute(
                """
                SELECT geo_level, name, COUNT(*) OVER ()
                FROM silver_ref.dim_geo_current WHERE geo_id = 'area:bls_cpi:S12A'
                """
            )
            assert cursor.fetchone() == (
                "provider_area",
                "New York-Newark-Jersey City, NY-NJ-PA",
                1,
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_ref.dim_geo_entity_version AS v "
                "JOIN silver_ref.dim_geo_entity AS e USING (geo_sk) "
                "WHERE e.geo_type = 'provider_area' AND e.area_code LIKE 'bls_cpi:%%'"
            )
            # A replay of the same list writes no second version.
            assert cursor.fetchone()[0] == 23
        writer.commit()
    finally:
        writer.close()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                for record in records:
                    delete_geography(cursor, record.geo_id)
            cleanup.commit()
        finally:
            cleanup.close()
