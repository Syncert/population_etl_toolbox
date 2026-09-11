"""A publisher-contract change reaches the catalog, on real PostgreSQL.

Covers: ARC-004 — the harvest's skip guard read a publication time every
publisher derives from its own facts. A change to what a publisher *says* moves
none of them, so redefining a source's metric identities left the catalog
serving the old ones with no error anywhere: the task succeeded and wrote
nothing. These tests hold a fixture publisher's ``publication_time`` fixed
while its content changes, which is exactly the shape of that defect.
"""

from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

import pytest
from psycopg2 import sql
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher

pytestmark = [pytest.mark.integration, pytest.mark.database]

#: Held constant across every redefinition below. A real publisher's
#: publication time only moves when its facts are re-ingested, which an
#: identity change does not do.
FIXED_PUBLICATION = "2099-01-01T00:00:00Z"


def _define_publisher(
    factory: Callable[[], connection],
    schema: str,
    source_code: str,
    keys: list[tuple[str, str]],
    *,
    create_schema: bool = False,
) -> None:
    """(Re)define the fixture publisher to emit exactly ``keys``.

    ``keys`` is a list of (source_object_key, metric_display_name).
    """
    branches = sql.SQL(" UNION ALL ").join(
        sql.SQL(
            """
            SELECT
                {source_code}::TEXT AS source_code,
                '1.0'::TEXT AS publisher_contract_version,
                {key}::TEXT AS source_object_key,
                'measure'::TEXT AS source_object_type,
                {display}::TEXT AS metric_display_name,
                'widgets'::TEXT AS units,
                'level'::TEXT AS measure_kind,
                ARRAY['NATIONAL']::TEXT[] AS valid_geo_grains,
                ARRAY['MONTHLY']::TEXT[] AS valid_time_grains,
                NULL::TEXT AS aggregation_characteristic,
                '{{"schema":"fixture"}}'::JSONB AS physical_lineage,
                'watermark-1'::TEXT AS source_watermark,
                NULL::UUID AS source_run_id,
                {publication}::TIMESTAMPTZ AS publication_time,
                'Fixture provider'::TEXT AS source_name,
                'fixture'::TEXT AS source_type,
                'https://example.test/fixture'::TEXT AS reference_url
            """
        ).format(
            source_code=sql.Literal(source_code),
            key=sql.Literal(key),
            display=sql.Literal(display),
            publication=sql.Literal(FIXED_PUBLICATION),
        )
        for key, display in keys
    )
    writer = factory()
    try:
        with writer.cursor() as cursor:
            if create_schema:
                cursor.execute(
                    sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
                )
            cursor.execute(
                sql.SQL("CREATE OR REPLACE VIEW {}.metric_publisher AS {}").format(
                    sql.Identifier(schema), branches
                )
            )
        writer.commit()
    finally:
        writer.close()


def _catalog(
    factory: Callable[[], connection], source_code: str
) -> dict[str, tuple[str, str]]:
    """Each catalog key's display name and freshness state."""
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_object_key, metric_display_name, freshness_state
                FROM gold_glossary.dim_metric_catalog
                WHERE source_code = %s
                ORDER BY source_object_key
                """,
                (source_code,),
            )
            return {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
    finally:
        reader.close()


def _drop(factory: Callable[[], connection], schema: str, source_code: str) -> None:
    cleanup = factory()
    try:
        with cleanup.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )
            for statement in (
                "DELETE FROM gold_glossary.dim_metric_catalog WHERE source_code = %s",
                "DELETE FROM gold_glossary.publisher_harvest_state WHERE source_code = %s",
                "DELETE FROM gold_glossary.publisher_registry WHERE source_code = %s",
                "DELETE FROM control.publisher_ready_event WHERE source_code = %s",
                "DELETE FROM gold_glossary.dim_source_system WHERE source_code = %s",
            ):
                cursor.execute(statement, (source_code,))
        cleanup.commit()
    finally:
        cleanup.close()


def test_a_metric_identity_change_reaches_the_catalog_with_no_fact_movement(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ARC-004 — the defect, reproduced and closed on real PostgreSQL."""
    token = uuid4().hex[:10].lower()
    schema = f"gold_identity_{token}"
    source_code = f"IDENTITY_{token.upper()}"
    publisher = Publisher(schema)

    try:
        _define_publisher(
            postgres_connection_factory,
            schema,
            source_code,
            [("legacy-1", "Legacy metric")],
            create_schema=True,
        )
        assert harvest_publisher(postgres_connection_factory, publisher) == 1
        # Nothing published anything new, so the scheduled case stays cheap.
        assert harvest_publisher(postgres_connection_factory, publisher) == 0
        assert _catalog(postgres_connection_factory, source_code) == {
            "legacy-1": ("Legacy metric", "current")
        }

        # The identity change: a different key, same publication time. Before
        # the content fingerprint this harvested nothing and reported success.
        _define_publisher(
            postgres_connection_factory,
            schema,
            source_code,
            [("measure-1", "Measure metric")],
        )
        assert harvest_publisher(postgres_connection_factory, publisher) == 1

        catalog = _catalog(postgres_connection_factory, source_code)
        assert catalog["measure-1"] == ("Measure metric", "current")
        # The old key is kept and marked, never deleted, so an existing link
        # to it still resolves.
        assert catalog["legacy-1"][1] == "stale"
    finally:
        _drop(postgres_connection_factory, schema, source_code)


def test_a_renamed_metric_reaches_the_catalog_with_no_fact_movement(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ARC-004 — content, not only the set of keys, is followed."""
    token = uuid4().hex[:10].lower()
    schema = f"gold_rename_{token}"
    source_code = f"RENAME_{token.upper()}"
    publisher = Publisher(schema)

    try:
        _define_publisher(
            postgres_connection_factory,
            schema,
            source_code,
            [("metric-1", "Original name")],
            create_schema=True,
        )
        assert harvest_publisher(postgres_connection_factory, publisher) == 1

        _define_publisher(
            postgres_connection_factory,
            schema,
            source_code,
            [("metric-1", "Corrected name")],
        )
        assert harvest_publisher(postgres_connection_factory, publisher) == 1
        assert _catalog(postgres_connection_factory, source_code) == {
            "metric-1": ("Corrected name", "current")
        }
    finally:
        _drop(postgres_connection_factory, schema, source_code)


def test_retirement_completes_on_scheduled_harvests_alone(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ARC-004 — reaching `retired` needs no operator per grace step.

    The harvest that first sees a key disappear changes the content and so
    runs, marking it `stale`. Every harvest after that sees unchanged content;
    if a skip did nothing at all the key would wait at `stale` indefinitely.
    """
    token = uuid4().hex[:10].lower()
    schema = f"gold_retire_{token}"
    source_code = f"RETIRE_{token.upper()}"
    publisher = Publisher(schema)

    try:
        _define_publisher(
            postgres_connection_factory,
            schema,
            source_code,
            [("keep-1", "Kept metric"), ("drop-1", "Dropped metric")],
            create_schema=True,
        )
        assert harvest_publisher(postgres_connection_factory, publisher) == 2

        _define_publisher(
            postgres_connection_factory,
            schema,
            source_code,
            [("keep-1", "Kept metric")],
        )
        # First harvest after the drop: content changed, so it writes.
        assert harvest_publisher(postgres_connection_factory, publisher) == 1
        assert (
            _catalog(postgres_connection_factory, source_code)["drop-1"][1] == "stale"
        )

        # Second harvest: content is now unchanged, so nothing is written --
        # but the grace still advances and the key retires.
        assert harvest_publisher(postgres_connection_factory, publisher) == 0

        catalog = _catalog(postgres_connection_factory, source_code)
        assert catalog["drop-1"][1] == "retired"
        assert catalog["keep-1"] == ("Kept metric", "current")
    finally:
        _drop(postgres_connection_factory, schema, source_code)


def test_forcing_re_harvests_content_that_did_not_change(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ARC-004 — a repair can rewrite an unchanged catalog."""
    token = uuid4().hex[:10].lower()
    schema = f"gold_force_{token}"
    source_code = f"FORCE_{token.upper()}"
    publisher = Publisher(schema)

    try:
        _define_publisher(
            postgres_connection_factory,
            schema,
            source_code,
            [("metric-1", "Fixture metric")],
            create_schema=True,
        )
        assert harvest_publisher(postgres_connection_factory, publisher) == 1
        assert harvest_publisher(postgres_connection_factory, publisher) == 0
        assert (
            harvest_publisher(postgres_connection_factory, publisher, force=True) == 1
        )

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT last_harvest_forced, last_content_fingerprint IS NOT NULL
                    FROM gold_glossary.publisher_harvest_state
                    WHERE source_code = %s
                    """,
                    (source_code,),
                )
                assert cursor.fetchone() == (True, True)
        finally:
            reader.close()
    finally:
        _drop(postgres_connection_factory, schema, source_code)
