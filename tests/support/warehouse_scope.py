"""Foreign-key-safe ownership tracking for end-to-end warehouse fixtures.

Every product E2E node commits real rows into the shared disposable warehouse:
runs, requests, captures, payload blobs, geography resolutions, source silver
facts, gold projections, and publisher events. Left behind, those rows change
what the next test sees — a later release looks "unchanged", a plausibility
baseline shifts, a count assertion drifts with test order.

This module owns only the mechanics that are genuinely provider-neutral: the
capture/control/reference graph, the tracked geographies, and the residue
assertion. Source semantics — which silver relations exist and in what order
they must be emptied — stay with the source-owned test that understands them.

Cleanup is registered before the first committed row, so an assertion failure,
an application error, or a KeyboardInterrupt still tears the fixture down.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from uuid import UUID

import pytest
from psycopg2.errors import ForeignKeyViolation
from psycopg2.extensions import connection

from tests.support.capture_seed import delete_geography, seed_geography


@dataclass
class WarehouseScope:
    """Track and remove every warehouse row one product E2E node commits."""

    connection_factory: Callable[[], connection]
    source_code: str
    #: Source-owned DELETE statements, in foreign-key-safe order. They run
    #: before the shared capture/control graph is removed.
    silver_statements: tuple[str, ...] = ()
    #: Source-owned control tables (release/slice ledgers), same ordering rule.
    control_statements: tuple[str, ...] = ()
    run_ids: set[UUID] = field(default_factory=set)
    geo_ids: set[str] = field(default_factory=set)
    _preexisting_geo_ids: set[str] = field(default_factory=set)
    #: Whether the glossary already carried this source before the node ran. A
    #: node that harvests its publisher must remove the catalog rows it
    #: created, but never a registration another suite depends on.
    _preexisting_glossary: bool = False
    #: Runs the source already had. Production ingestion starts its own runs
    #: deep inside the pipeline, so a node that calls it cannot hand every
    #: run_id back; anything new for this source is this node's to remove.
    _baseline_run_ids: frozenset[UUID] = frozenset()

    def track_run(self, run_id: UUID) -> UUID:
        """Record a run whose requests, captures, and payloads this node owns."""
        self.run_ids.add(run_id)
        return run_id

    def owned_run_ids(self) -> list[UUID]:
        """Return every run this node is responsible for removing."""
        current = {
            row[0]
            for row in self.query(
                "SELECT run_id FROM control.ingestion_run WHERE source_code = %s",
                (self.source_code,),
            )
        }
        return sorted(self.run_ids | (current - self._baseline_run_ids))

    def track_runs(self, run_ids: Iterable[UUID]) -> None:
        for run_id in run_ids:
            self.track_run(run_id)

    def query(self, sql: str, parameters: Sequence[object] = ()) -> list[tuple]:
        """Run one read on its own connection, always closing it."""
        database_connection = self.connection_factory()
        try:
            with database_connection.cursor() as cursor:
                cursor.execute(sql, tuple(parameters))
                return cursor.fetchall()
        finally:
            database_connection.close()

    def scalar(self, sql: str, parameters: Sequence[object] = ()) -> object:
        rows = self.query(sql, parameters)
        assert rows and len(rows[0]) == 1, f"expected one scalar, got {rows}"
        return rows[0][0]

    def seed_geographies(self, geographies: Sequence[dict[str, object]]) -> None:
        """Seed canonical geographies, remembering which ones already existed.

        A geography another source published stays in place at teardown; only
        entities this node introduced are removed.
        """
        geo_ids = [_canonical_geo_id(entry) for entry in geographies]
        existing = {
            row[0]
            for row in self.query(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (geo_ids,),
            )
        }
        self._preexisting_geo_ids |= existing
        self.geo_ids.update(geo_ids)

        writer = self.connection_factory()
        try:
            with writer.cursor() as cursor:
                for entry in geographies:
                    seed_geography(cursor, **entry)  # type: ignore[arg-type]
            writer.commit()
        except BaseException:
            writer.rollback()
            raise
        finally:
            writer.close()

    def assert_absent(self, geo_ids: Sequence[str]) -> None:
        """Assert a geography is missing, for an inspectable geography-miss."""
        present = [
            row[0]
            for row in self.query(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(geo_ids),),
            )
        ]
        assert not present, (
            f"{present} must be absent for this contract; another suite left it "
            "behind or the fixture seeds it by mistake"
        )

    # -- teardown ---------------------------------------------------------

    def cleanup(self) -> None:
        """Delete every tracked row in foreign-key-safe order, then prove it."""
        run_ids = self.owned_run_ids()
        database_connection = self.connection_factory()
        try:
            with database_connection.cursor() as cursor:
                delete_harvested_glossary_rows(
                    cursor, self.source_code, preexisting=self._preexisting_glossary
                )
                cursor.execute(
                    "DELETE FROM control.publisher_ready_event WHERE source_code = %s",
                    (self.source_code,),
                )
                for statement in self.silver_statements:
                    cursor.execute(statement)
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution "
                    "WHERE provider_source = %s",
                    (self.source_code,),
                )
                for statement in self.control_statements:
                    cursor.execute(statement)
                checksums = self._delete_capture_graph(cursor, run_ids)
                self._delete_geographies(cursor)
                self._assert_no_residue(cursor, run_ids, checksums)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    def _delete_capture_graph(self, cursor, run_ids: list[UUID]) -> list[str]:
        return delete_capture_graph(cursor, run_ids)

    def _delete_geographies(self, cursor) -> None:
        for index, geo_id in enumerate(
            sorted(self.geo_ids - self._preexisting_geo_ids)
        ):
            # The reference dimension is shared. A geography another source
            # still references stays in place rather than aborting teardown.
            savepoint = f"scope_geo_{index}"
            cursor.execute(f"SAVEPOINT {savepoint}")
            try:
                delete_geography(cursor, geo_id)
            except ForeignKeyViolation:
                cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            else:
                cursor.execute(f"RELEASE SAVEPOINT {savepoint}")

    def _assert_no_residue(
        self, cursor, run_ids: list[UUID], checksums: list[str]
    ) -> None:
        if run_ids:
            cursor.execute(
                """
                SELECT (SELECT COUNT(*) FROM raw_capture.response_capture
                         WHERE run_id = ANY(%s))
                     + (SELECT COUNT(*) FROM control.ingestion_request
                         WHERE run_id = ANY(%s))
                     + (SELECT COUNT(*) FROM control.ingestion_run
                         WHERE run_id = ANY(%s))
                """,
                (run_ids, run_ids, run_ids),
            )
            residue = cursor.fetchone()[0]
            assert residue == 0, (
                f"{residue} capture/control rows survived teardown for "
                f"{self.source_code}"
            )
        if checksums:
            cursor.execute(
                "SELECT COUNT(*) FROM raw_capture.payload_blob AS payload "
                "WHERE payload.payload_checksum = ANY(%s) AND NOT EXISTS ("
                "SELECT 1 FROM raw_capture.response_capture AS capture "
                "WHERE capture.payload_checksum = payload.payload_checksum)",
                (checksums,),
            )
            orphans = cursor.fetchone()[0]
            assert orphans == 0, f"{orphans} orphaned payload blobs survived teardown"
        cursor.execute(
            "SELECT COUNT(*) FROM control.publisher_ready_event WHERE source_code = %s",
            (self.source_code,),
        )
        events = cursor.fetchone()[0]
        assert events == 0, (
            f"{events} publisher-ready events survived teardown for {self.source_code}"
        )


def delete_capture_graph(cursor, run_ids: list[UUID]) -> list[str]:
    """Delete the capture graph for a set of runs; return their payload hashes.

    Captures and payload blobs are append-only by trigger. The disposable test
    database is the only place that guard is lifted, and only for rows a
    fixture created.
    """
    if not run_ids:
        return []
    cursor.execute(
        "DELETE FROM control.capture_quarantine WHERE capture_id IN ("
        "SELECT capture_id FROM raw_capture.response_capture "
        "WHERE run_id = ANY(%s))",
        (run_ids,),
    )
    cursor.execute(
        "SELECT DISTINCT payload_checksum FROM raw_capture.response_capture "
        "WHERE run_id = ANY(%s)",
        (run_ids,),
    )
    checksums = [row[0] for row in cursor.fetchall()]
    cursor.execute(
        "ALTER TABLE raw_capture.response_capture "
        "DISABLE TRIGGER response_capture_reject_mutation"
    )
    cursor.execute(
        "DELETE FROM raw_capture.response_capture WHERE run_id = ANY(%s)",
        (run_ids,),
    )
    cursor.execute(
        "ALTER TABLE raw_capture.response_capture "
        "ENABLE TRIGGER response_capture_reject_mutation"
    )
    if checksums:
        cursor.execute(
            "ALTER TABLE raw_capture.payload_blob "
            "DISABLE TRIGGER payload_blob_reject_mutation"
        )
        cursor.execute(
            "DELETE FROM raw_capture.payload_blob AS payload "
            "WHERE payload.payload_checksum = ANY(%s) AND NOT EXISTS ("
            "SELECT 1 FROM raw_capture.response_capture AS capture "
            "WHERE capture.payload_checksum = payload.payload_checksum)",
            (checksums,),
        )
        cursor.execute(
            "ALTER TABLE raw_capture.payload_blob "
            "ENABLE TRIGGER payload_blob_reject_mutation"
        )
    cursor.execute(
        "DELETE FROM control.ingestion_request WHERE run_id = ANY(%s)", (run_ids,)
    )
    cursor.execute(
        "DELETE FROM control.ingestion_run WHERE run_id = ANY(%s)", (run_ids,)
    )
    return checksums


def source_run_ids(
    connection_factory: Callable[[], connection], source_code: str
) -> frozenset[UUID]:
    """Return the runs a source already has, as the baseline a node adds to."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT run_id FROM control.ingestion_run WHERE source_code = %s",
                (source_code,),
            )
            return frozenset(row[0] for row in cursor.fetchall())
    finally:
        database_connection.close()


def glossary_registration_exists(
    connection_factory: Callable[[], connection], source_code: str
) -> bool:
    """Report whether the glossary already knows this source.

    Harvesting a publisher registers the source and its measures in the shared
    glossary. A node that harvests must remove what it created, but a source
    another suite (or the bootstrap seed) already registered must survive.
    """
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM gold_glossary.dim_source_system "
                "WHERE source_code = %s",
                (source_code,),
            )
            return bool(cursor.fetchone()[0])
    finally:
        database_connection.close()


def delete_harvested_glossary_rows(
    cursor, source_code: str, *, preexisting: bool
) -> None:
    """Remove one source's harvested glossary rows in dependency order."""
    if preexisting:
        return
    cursor.execute(
        "DELETE FROM gold_glossary.dim_metric_catalog WHERE source_code = %s",
        (source_code,),
    )
    cursor.execute(
        "DELETE FROM gold_glossary.publisher_harvest_state WHERE source_code = %s",
        (source_code,),
    )
    cursor.execute(
        "DELETE FROM gold_glossary.publisher_registry WHERE source_code = %s",
        (source_code,),
    )
    cursor.execute(
        "DELETE FROM gold_glossary.dim_source_system WHERE source_code = %s",
        (source_code,),
    )


def _canonical_geo_id(entry: dict[str, object]) -> str:
    from data_ingestion_toolbox.silver_ref.geography_contract import canonical_geo_id

    return canonical_geo_id(
        str(entry["geo_type"]),
        state_fips=entry.get("state_fips"),  # type: ignore[arg-type]
        county_fips=entry.get("county_fips"),  # type: ignore[arg-type]
        place_fips=entry.get("place_fips"),  # type: ignore[arg-type]
    )


def warehouse_scope(
    connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
    *,
    source_code: str,
    silver_statements: Sequence[str] = (),
    control_statements: Sequence[str] = (),
) -> WarehouseScope:
    """Build a scope whose teardown is registered before any row is committed."""
    scope = WarehouseScope(
        connection_factory=connection_factory,
        source_code=source_code,
        silver_statements=tuple(silver_statements),
        control_statements=tuple(control_statements),
        _preexisting_glossary=glossary_registration_exists(
            connection_factory, source_code
        ),
        _baseline_run_ids=source_run_ids(connection_factory, source_code),
    )
    request.addfinalizer(scope.cleanup)
    return scope
