"""The shared geography guard, against a bootstrapped warehouse.

Covers: DAG-020 -- on a warehouse the manifest has bootstrapped and the
        geography DAG has not run, the guard refuses and names the counts it
        saw; with the reference loaded it passes.

This is the tier that reproduces the defect. `silver_ref.dim_geo_entity`
exists on every fresh bootstrap -- the manifest creates it in its `reference`
phase -- so the old `to_regclass` guard passed here, which is exactly where it
had to fail.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.silver_ref.geography_guard import (
    SharedGeographyNotLoaded,
    require_shared_geography_loaded,
)

pytestmark = [pytest.mark.integration, pytest.mark.database]


def _row(geo_type: str, index: int) -> tuple[str, str, str]:
    return (f"{geo_type}:{index:06d}", geo_type, f"{geo_type} {index}")


def test_a_bootstrapped_but_unloaded_warehouse_is_refused(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DAG-020 — the state the old guard passed on."""
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            # The table the old guard asked about exists here, which was the
            # whole problem: `to_regclass` answered a name and the guard
            # passed.
            cursor.execute("SELECT to_regclass('silver_ref.dim_geo_entity')")
            assert cursor.fetchone()[0] is not None, (
                "the bootstrap did not create dim_geo_entity, so this test is "
                "not reproducing the state it was written for"
            )

            # Count what is actually there, so the assertion below is about an
            # empty reference rather than about a fixture another test left.
            cursor.execute(
                "SELECT COUNT(*) FROM silver_ref.dim_geo_current WHERE is_active"
            )
            loaded = int(cursor.fetchone()[0])

        if loaded:
            pytest.skip(
                "this database already carries a geography reference; the "
                "refusal is proven on a fresh bootstrap"
            )

        with pytest.raises(SharedGeographyNotLoaded) as refusal:
            require_shared_geography_loaded(database)

        message = str(refusal.value)
        assert "nation=0" in message
        assert "state=0" in message
        assert "county=0" in message
    finally:
        database.close()


def test_a_loaded_reference_satisfies_the_guard(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DAG-020 — and it passes once silver_ref has run."""
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FILTER (WHERE geo_type = 'nation'), "
                "       COUNT(*) FILTER (WHERE geo_type = 'state'), "
                "       COUNT(*) FILTER (WHERE geo_type = 'county') "
                "FROM silver_ref.dim_geo_current WHERE is_active"
            )
            nation, states, counties = cursor.fetchone()

        if nation != 1 or states < 50 or counties < 3000:
            pytest.skip(
                "this database carries no loaded geography reference; run the "
                "geography pipeline fixture first"
            )

        observed = require_shared_geography_loaded(database)
        assert observed["nation"] == 1
        assert observed["state"] >= 50
        assert observed["county"] >= 3000
    finally:
        database.close()
