"""Versioned reference replay, geometry, relationship, and serving contracts."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from datetime import date

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.silver_ref import time_dim
from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    GeographyRecord,
    GeographyRepository,
    GeometryRecord,
)
from tests.support.capture_seed import seed_capture
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]

TEST_IDS = [
    "us:1",
    "state:98",
    "state:98|county:764",
    "state:98|county:765",
    "state:98|place:54321",
]


@pytest.fixture
def reference_dimension_scope(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[None]:
    try:
        yield
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution WHERE geo_sk IN "
                    "(SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s))",
                    (TEST_IDS,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.bridge_geo_relationship_version WHERE parent_geo_sk IN "
                    "(SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)) "
                    "OR related_geo_sk IN (SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s))",
                    (TEST_IDS, TEST_IDS),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_geo_geometry_version WHERE geo_sk IN "
                    "(SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s))",
                    (TEST_IDS,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_geo_entity_version WHERE geo_sk IN "
                    "(SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s))",
                    (TEST_IDS,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                    (TEST_IDS,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE date_key BETWEEN '2096-02-28' AND '2096-03-01'"
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_time_dimension_sync_replays_exact_leap_window(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    reference_dimension_scope: None,
) -> None:
    """Covers: ETL-024, ETL-025 — time replay is exact and idempotent."""
    monkeypatch.setattr(
        time_dim, "_get_hook", lambda: PostgresHookStub(postgres_connection_factory)
    )
    for _ in range(2):
        assert time_dim.sync_time_dim(date(2096, 2, 28), date(2096, 3, 1)) == 3
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """SELECT date_key::TEXT, is_month_start, is_month_end, day_of_week, quarter
                   FROM silver_ref.dim_time
                   WHERE date_key BETWEEN '2096-02-28' AND '2096-03-01' ORDER BY date_key"""
            )
            assert cursor.fetchall() == [
                ("2096-02-28", False, False, 2, 1),
                ("2096-02-29", False, True, 3, 1),
                ("2096-03-01", True, False, 4, 1),
            ]
    finally:
        reader.close()


def _polygon(x1: float, x2: float) -> str:
    return json.dumps(
        {
            "type": "Polygon",
            "coordinates": [[[x1, 40], [x2, 40], [x2, 41], [x1, 41], [x1, 40]]],
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def test_geography_replay_retains_versions_and_cross_county_place_relationships(
    postgres_connection_factory: Callable[[], connection],
    reference_dimension_scope: None,
) -> None:
    """Covers: DB-018 — versions, retirement, geometry, and intersections persist."""
    writer = postgres_connection_factory()
    with writer.cursor() as cursor:
        capture_2096 = seed_capture(cursor, "CENSUS_GEO", b"snapshot-2096")
    writer.commit()
    writer.close()

    repository = GeographyRepository(postgres_connection_factory)
    records = [
        GeographyRecord("nation", "us:1", "1", None, None, None, "United States", 2096),
        GeographyRecord(
            "state", "state:98", "98", "98", None, None, "Old State Name", 2096
        ),
        GeographyRecord(
            "county",
            "state:98|county:764",
            "98764",
            "98",
            "764",
            None,
            "West County",
            2096,
        ),
        GeographyRecord(
            "county",
            "state:98|county:765",
            "98765",
            "98",
            "765",
            None,
            "East County",
            2096,
        ),
        GeographyRecord(
            "place",
            "state:98|place:54321",
            "9854321",
            "98",
            None,
            "54321",
            "Crossing Place",
            2096,
        ),
    ]
    assert repository.load_attributes(records, capture_id=capture_2096) == 5
    geometries = [
        GeometryRecord("state:98", 2096, _polygon(-91, -88)),
        GeometryRecord("state:98|county:764", 2096, _polygon(-91, -89.5)),
        GeometryRecord("state:98|county:765", 2096, _polygon(-89.5, -88)),
        GeometryRecord("state:98|place:54321", 2096, _polygon(-90, -89)),
    ]
    assert repository.load_geometries(geometries, capture_id=capture_2096) == 4
    repository.reconcile_relationships(
        vintage=2096,
        capture_id=capture_2096,
        active_geo_ids={record.geo_id for record in records},
    )

    serving_reader = postgres_connection_factory()
    try:
        with serving_reader.cursor() as cursor:
            cursor.execute("CALL gold_glossary.refresh_dim_geo_latest()")
            cursor.execute(
                """SELECT place_fips, place_name, boundary_vintage,
                          ST_IsValid(geo_geom), ST_SRID(geo_geom)
                   FROM gold.dim_geo_latest
                   WHERE geo_id = 'state:98|place:54321'"""
            )
            assert cursor.fetchone() == (
                "54321",
                "Crossing Place",
                2096,
                True,
                4326,
            )
        serving_reader.rollback()
    finally:
        serving_reader.close()

    writer = postgres_connection_factory()
    with writer.cursor() as cursor:
        capture_2097 = seed_capture(cursor, "CENSUS_GEO", b"snapshot-2097")
    writer.commit()
    writer.close()
    renamed = GeographyRecord(
        "state", "state:98", "98", "98", None, None, "New State Name", 2097
    )
    repository.load_attributes([renamed], capture_id=capture_2097)
    repository.retire_missing(
        active_geo_ids={"state:98"}, vintage=2097, capture_id=capture_2097
    )

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT name, is_active FROM silver_ref.dim_geo_current WHERE geo_id = 'state:98'"
            )
            assert cursor.fetchone() == ("New State Name", True)
            cursor.execute(
                "SELECT name FROM silver_ref.dim_geo_entity_version v JOIN silver_ref.dim_geo_entity e USING (geo_sk) "
                "WHERE e.geo_id = 'state:98' ORDER BY geography_vintage"
            )
            assert cursor.fetchall() == [("Old State Name",), ("New State Name",)]
            cursor.execute(
                "SELECT is_active FROM silver_ref.dim_geo_current WHERE geo_id = 'state:98|place:54321'"
            )
            assert cursor.fetchone() == (False,)
            cursor.execute(
                """SELECT COUNT(*) FROM silver_ref.bridge_geo_relationship_version r
                   JOIN silver_ref.dim_geo_entity p ON p.geo_sk = r.related_geo_sk
                   WHERE p.geo_id = 'state:98|place:54321' AND r.relationship_type = 'intersects'"""
            )
            assert cursor.fetchone() == (2,)
            cursor.execute("CALL gold_glossary.refresh_dim_geo_latest()")
            cursor.execute(
                "SELECT state_name, ST_IsValid(geo_geom), ST_SRID(geo_geom) FROM gold.dim_geo_latest "
                "WHERE geo_id = 'state:98'"
            )
            assert cursor.fetchone() == ("New State Name", True, 4326)
        reader.rollback()
    finally:
        reader.close()

    writer = postgres_connection_factory()
    with writer.cursor() as cursor:
        capture_2095 = seed_capture(cursor, "CENSUS_GEO", b"snapshot-2095")
    writer.commit()
    writer.close()
    repository.load_attributes(
        [
            GeographyRecord(
                "state",
                "state:98",
                "98",
                "98",
                None,
                None,
                "Oldest State Name",
                2095,
            )
        ],
        capture_id=capture_2095,
    )
    bounds_reader = postgres_connection_factory()
    try:
        with bounds_reader.cursor() as cursor:
            cursor.execute(
                "SELECT first_seen_version, last_seen_version "
                "FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:98'"
            )
            assert cursor.fetchone() == (2095, 2097)
    finally:
        bounds_reader.close()


def test_batched_geography_replay_is_idempotent_and_guards_missing_entities(
    postgres_connection_factory: Callable[[], connection],
    reference_dimension_scope: None,
) -> None:
    """Covers: DB-018 — batched loading replays cleanly and still refuses orphans.

    The loaders write set-based now, so the properties that used to fall out of
    the per-row loop have to be proved against a real database: a rerun of the
    identical snapshot must publish nothing new, a repeated identity inside one
    batch must not break the entity upsert, and a boundary whose entity was
    never loaded must still be rejected.
    """
    writer = postgres_connection_factory()
    with writer.cursor() as cursor:
        capture = seed_capture(cursor, "CENSUS_GEO", b"batched-snapshot")
    writer.commit()
    writer.close()

    repository = GeographyRepository(postgres_connection_factory)
    records = [
        GeographyRecord("nation", "us:1", "1", None, None, None, "United States", 2096),
        GeographyRecord("state", "state:98", "98", "98", None, None, "State", 2096),
        GeographyRecord(
            "county", "state:98|county:764", "98764", "98", "764", None, "West", 2096
        ),
        GeographyRecord(
            "county", "state:98|county:765", "98765", "98", "765", None, "East", 2096
        ),
        GeographyRecord(
            "place",
            "state:98|place:54321",
            "9854321",
            "98",
            None,
            "54321",
            "City",
            2096,
        ),
    ]
    geometries = [
        GeometryRecord("state:98", 2096, _polygon(-91, -88)),
        GeometryRecord("state:98|county:764", 2096, _polygon(-91, -89.5)),
        GeometryRecord("state:98|county:765", 2096, _polygon(-89.5, -88)),
        GeometryRecord("state:98|place:54321", 2096, _polygon(-90, -89)),
    ]

    def publish() -> None:
        assert repository.load_attributes(records, capture_id=capture) == 5
        assert repository.load_geometries(geometries, capture_id=capture) == 4
        repository.reconcile_relationships(
            vintage=2096,
            capture_id=capture,
            active_geo_ids={record.geo_id for record in records},
        )

    def published_counts() -> tuple[int, int, int, int]:
        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """SELECT
                        (SELECT COUNT(*) FROM silver_ref.dim_geo_entity
                         WHERE geo_id = ANY(%s)),
                        (SELECT COUNT(*) FROM silver_ref.dim_geo_entity_version v
                         JOIN silver_ref.dim_geo_entity e USING (geo_sk)
                         WHERE e.geo_id = ANY(%s)),
                        (SELECT COUNT(*) FROM silver_ref.dim_geo_geometry_version g
                         JOIN silver_ref.dim_geo_entity e USING (geo_sk)
                         WHERE e.geo_id = ANY(%s)),
                        (SELECT COUNT(*) FROM silver_ref.bridge_geo_relationship_version r
                         JOIN silver_ref.dim_geo_entity e ON e.geo_sk = r.related_geo_sk
                         WHERE e.geo_id = ANY(%s))""",
                    (TEST_IDS, TEST_IDS, TEST_IDS, TEST_IDS),
                )
                return cursor.fetchone()
        finally:
            reader.close()

    publish()
    first = published_counts()
    assert first == (5, 5, 4, 6)

    publish()
    assert published_counts() == first, "replaying the snapshot published new rows"

    # A repeated identity inside one batch must not break ON CONFLICT DO UPDATE,
    # and the vintage bounds must merge exactly as the per-row loader merged them.
    assert (
        repository.load_attributes(
            [
                GeographyRecord(
                    "state", "state:98", "98", "98", None, None, "Earliest", 2090
                ),
                GeographyRecord(
                    "state", "state:98", "98", "98", None, None, "Latest", 2099
                ),
            ],
            capture_id=capture,
        )
        == 2
    )
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT first_seen_version, last_seen_version "
                "FROM silver_ref.dim_geo_entity WHERE geo_id = 'state:98'"
            )
            assert cursor.fetchone() == (2090, 2099)
            cursor.execute(
                "SELECT name FROM silver_ref.dim_geo_entity_version v "
                "JOIN silver_ref.dim_geo_entity e USING (geo_sk) "
                "WHERE e.geo_id = 'state:98' ORDER BY geography_vintage, name"
            )
            assert cursor.fetchall() == [
                ("Earliest",),
                ("State",),
                ("Latest",),
            ]
    finally:
        reader.close()

    # The orphan guard is a real integrity check, not an artefact of the loop.
    with pytest.raises(ValueError, match="boundary has no matching entity: state:99"):
        repository.load_geometries(
            [
                GeometryRecord("state:98", 2096, _polygon(-91, -88)),
                GeometryRecord("state:99", 2096, _polygon(-91, -88)),
            ],
            capture_id=capture,
        )


@pytest.fixture
def served_place_row_scope(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[None]:
    """Remove what this test publishes, however it ends.

    The geography catalog row matters as much as the reporting row: it now
    survives retirement by design, so leaving one behind would let a rerun
    start from a row the reference no longer explains -- and this tier is
    asserted to be repeatable (`test_tier_repeatability.py`).
    """
    try:
        yield
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_census.rpt_acs_observations "
                    "WHERE geo_id = ANY(%s)",
                    (TEST_IDS,),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_geo_latest WHERE geo_id = ANY(%s)",
                    (TEST_IDS,),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_a_retired_geography_stays_resolvable_and_keeps_naming_its_rows(
    postgres_connection_factory: Callable[[], connection],
    reference_dimension_scope: None,
    served_place_row_scope: None,
) -> None:
    """Covers: DB-038 — a retired geography is published as retired, not deleted.

    `refresh_dim_geo_latest` used to DELETE the catalog row for a geography the
    reference stopped listing, while `rpt_*_observations` kept that
    geography's rows. The two halves of one answer disagreed: a client that
    resolves geographies through `/catalog/geographies` could not reach those
    rows, and one that did not got rows the catalog would not name. Nothing
    asserted the containment either way.

    A place is the geography used here on purpose. It is also the case where
    the catalog and the observation views named the same geography
    differently -- place first in the catalog, county first in the views --
    so one test covers both halves of the divergence.
    """
    writer = postgres_connection_factory()
    with writer.cursor() as cursor:
        capture_listed = seed_capture(cursor, "CENSUS_GEO", b"place-listed")
    writer.commit()
    writer.close()

    repository = GeographyRepository(postgres_connection_factory)
    records = [
        GeographyRecord(
            "state", "state:98", "98", "98", None, None, "Stateville", 2096
        ),
        GeographyRecord(
            "place",
            "state:98|place:54321",
            "9854321",
            "98",
            None,
            "54321",
            "Crossing Place",
            2096,
        ),
    ]
    assert repository.load_attributes(records, capture_id=capture_listed) == 2
    repository.reconcile_relationships(
        vintage=2096,
        capture_id=capture_listed,
        active_geo_ids={record.geo_id for record in records},
    )

    connection_ = postgres_connection_factory()
    try:
        with connection_.cursor() as cursor:
            cursor.execute("CALL gold_glossary.refresh_dim_geo_latest()")
            connection_.commit()

            # One served row for the place, exactly as a reserve would leave it.
            cursor.execute(
                """
                INSERT INTO gold_census.rpt_acs_observations (
                    observation_date, as_of_date, updated_at, geo_id, geo_level,
                    state_fips, place_name, value, dataset_code, vintage_year,
                    table_id, variable_code, estimate_value, metric_code
                ) VALUES (
                    '2096-01-01', '2096-01-01', NOW(), 'state:98|place:54321',
                    'PLACE', '98', 'Crossing Place', 100, 'acs5', 2096,
                    'B01003', 'B01003_001', 100, 'CENSUS_ACS:acs5:B01003_001'
                )
                """
            )
            connection_.commit()

            cursor.execute(
                "SELECT geography_state, retired_at IS NULL, is_active, geo_name "
                "FROM gold_glossary.dim_geography WHERE geo_id = 'state:98|place:54321'"
            )
            assert cursor.fetchone() == ("current", True, True, "Crossing Place")

            # The observation routes name it the way the catalog does.
            cursor.execute(
                "SELECT geo_name FROM gold_census.fact_observation "
                "WHERE geo_id = 'state:98|place:54321'"
            )
            assert cursor.fetchone() == ("Crossing Place",)
    finally:
        connection_.close()

    # A new vintage lists the state and not the place.
    writer = postgres_connection_factory()
    with writer.cursor() as cursor:
        capture_dropped = seed_capture(cursor, "CENSUS_GEO", b"place-dropped")
    writer.commit()
    writer.close()
    repository.load_attributes(
        [
            GeographyRecord(
                "state", "state:98", "98", "98", None, None, "Stateville", 2097
            )
        ],
        capture_id=capture_dropped,
    )
    assert (
        repository.retire_missing(
            active_geo_ids={"state:98"}, vintage=2097, capture_id=capture_dropped
        )
        >= 1
    )

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT is_active FROM silver_ref.dim_geo_current "
                "WHERE geo_id = 'state:98|place:54321'"
            )
            assert cursor.fetchone() == (False,), "the reference must have retired it"

            cursor.execute("CALL gold_glossary.refresh_dim_geo_latest()")
            reader.commit()

            cursor.execute(
                "SELECT geography_state, retired_at IS NOT NULL, is_active, geo_name "
                "FROM gold_glossary.dim_geography "
                "WHERE geo_id = 'state:98|place:54321'"
            )
            resolved = cursor.fetchone()
            assert resolved == ("retired", True, False, "Crossing Place"), (
                "a retired geography must stay resolvable, and under the same "
                f"name its rows were served with: {resolved}"
            )

            # Criterion: every served geography resolves in the catalog.
            cursor.execute(
                """
                SELECT relation, geo_id FROM (
                    SELECT 'gold_census.rpt_acs_observations' AS relation, geo_id
                      FROM gold_census.rpt_acs_observations
                    UNION
                    SELECT 'gold_bls.rpt_bls_observations', geo_id
                      FROM gold_bls.rpt_bls_observations
                    UNION
                    SELECT 'gold_fred.rpt_fred_observations', geo_id
                      FROM gold_fred.rpt_fred_observations
                ) AS served
                WHERE NOT EXISTS (
                    SELECT 1 FROM gold_glossary.dim_geo_latest g
                    WHERE g.geo_id = served.geo_id
                )
                """
            )
            unresolvable = cursor.fetchall()
            assert not unresolvable, (
                "these served geographies do not resolve in the geography "
                f"catalog, so nothing can qualify their rows: {unresolvable}"
            )

            # `retired_at` records when it went, and a later sweep must not
            # move it: "when did this county go away" has one answer.
            cursor.execute(
                "SELECT retired_at FROM gold_glossary.dim_geo_latest "
                "WHERE geo_id = 'state:98|place:54321'"
            )
            first_retired_at = cursor.fetchone()[0]
            cursor.execute("CALL gold_glossary.refresh_dim_geo_latest()")
            reader.commit()
            cursor.execute(
                "SELECT retired_at FROM gold_glossary.dim_geo_latest "
                "WHERE geo_id = 'state:98|place:54321'"
            )
            assert cursor.fetchone()[0] == first_retired_at
    finally:
        reader.close()

    # A reference that lists it again un-retires it, with its attributes
    # unchanged -- which is the case the conflict clause has to notice.
    writer = postgres_connection_factory()
    with writer.cursor() as cursor:
        capture_relisted = seed_capture(cursor, "CENSUS_GEO", b"place-relisted")
    writer.commit()
    writer.close()
    # A newer vintage than the one that retired it, or `dim_geo_current`
    # keeps choosing the retirement version and the reference never says the
    # place is back.
    relisted = [
        GeographyRecord(
            "state", "state:98", "98", "98", None, None, "Stateville", 2098
        ),
        GeographyRecord(
            "place",
            "state:98|place:54321",
            "9854321",
            "98",
            None,
            "54321",
            "Crossing Place",
            2098,
        ),
    ]
    repository.load_attributes(relisted, capture_id=capture_relisted)
    repository.reconcile_relationships(
        vintage=2098,
        capture_id=capture_relisted,
        active_geo_ids={record.geo_id for record in relisted},
    )

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute("CALL gold_glossary.refresh_dim_geo_latest()")
            reader.commit()
            cursor.execute(
                "SELECT geography_state, retired_at FROM gold_glossary.dim_geo_latest "
                "WHERE geo_id = 'state:98|place:54321'"
            )
            assert cursor.fetchone() == ("current", None)
    finally:
        reader.close()
