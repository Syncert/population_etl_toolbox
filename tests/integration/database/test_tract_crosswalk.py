"""Tract decade successions reach the reference by code and decade.

Covers: ETL-102
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.silver_ref.tract_crosswalk import (
    CrosswalkPart,
    parse_2000_2010,
    publish_crosswalk,
)
from tests.support.capture_seed import delete_geography, seed_capture, seed_geography

pytestmark = [pytest.mark.integration, pytest.mark.database]

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures/silver_ref/tract_crosswalk"
#: (tract code, vintages the reference holds it for). 040100 is the same
#: tract in both decades; 040500 split into 040501 and 040502 (and a sliver
#: of 043202, which this reference does not hold).
SEEDED = {
    "040100": (2000, 2010),
    "040500": (2000,),
    "040501": (2010,),
    "040502": (2010,),
}


def _geo_id(code: str) -> str:
    return f"state:10|county:001|tract:{code}"


@pytest.fixture
def decade_reference(postgres_connection_factory: Callable[[], connection]):
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            for code, vintages in SEEDED.items():
                for vintage in vintages:
                    seed_geography(
                        cursor,
                        geo_type="tract",
                        state_fips="10",
                        county_fips="001",
                        tract_code=code,
                        vintage=vintage,
                        name=f"Census Tract {code}",
                    )
            capture_id = seed_capture(cursor, "CENSUS_GEO")
        writer.commit()
    finally:
        writer.close()
    yield capture_id
    remover = postgres_connection_factory()
    try:
        with remover.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_ref.tract_decade_crosswalk WHERE source_snapshot_id = %s",
                (str(capture_id),),
            )
            cursor.execute(
                "DELETE FROM silver_ref.geography_resolution "
                "WHERE provider_dataset = 'census_tract_relationship'"
            )
            for code in SEEDED:
                delete_geography(cursor, _geo_id(code))
        remover.commit()
    finally:
        remover.close()


def _rows(factory, sql: str, parameters: tuple = ()) -> list:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def test_successions_are_matched_by_code_and_decade(
    postgres_connection_factory: Callable[[], connection], decade_reference
) -> None:
    """Covers: ETL-102 -- weights as published; a code from another decade is unmapped."""
    parts = [
        part
        for part in parse_2000_2010((FIXTURES / "us2010trf.excerpt.txt").read_bytes())
        if part.earlier_geo_id in {_geo_id("040100"), _geo_id("040500")}
    ]
    # 040501 is a 2010 tract; a part naming it as a 2000 tract must not match.
    parts.append(
        CrosswalkPart(2000, _geo_id("040501"), _geo_id("040502"), 1, 1, None, None)
    )
    database = postgres_connection_factory()
    try:
        counts = publish_crosswalk(database, parts, capture_id=decade_reference)
        database.commit()
    finally:
        database.close()
    assert counts == {"parts": 3, "unmapped_tracts": 2}

    stored = _rows(
        postgres_connection_factory,
        """
        SELECT earlier.geo_id, later.geo_id, crosswalk.later_vintage,
               crosswalk.earlier_population_share, crosswalk.later_population_part
        FROM silver_ref.tract_decade_crosswalk AS crosswalk
        JOIN silver_ref.dim_geo_entity AS earlier ON earlier.geo_sk = crosswalk.earlier_geo_sk
        JOIN silver_ref.dim_geo_entity AS later ON later.geo_sk = crosswalk.later_geo_sk
        WHERE crosswalk.source_snapshot_id = %s
        ORDER BY 1, 2
        """,
        (str(decade_reference),),
    )
    assert stored == [
        (_geo_id("040100"), _geo_id("040100"), 2010, Decimal("1.0000"), 6541),
        (_geo_id("040500"), _geo_id("040501"), 2010, Decimal("0.7131"), 4923),
        (_geo_id("040500"), _geo_id("040502"), 2010, Decimal("0.2869"), 1981),
    ]
    assert sorted(
        _rows(
            postgres_connection_factory,
            """
            SELECT source_code, source_vintage, status FROM silver_ref.geography_resolution
            WHERE provider_dataset = 'census_tract_relationship'
            """,
        )
    ) == [
        (_geo_id("040501"), 2000, "unmapped"),
        (_geo_id("043202"), 2010, "unmapped"),
    ]

    # A replay writes no second row.
    database = postgres_connection_factory()
    try:
        publish_crosswalk(database, parts, capture_id=decade_reference)
        database.commit()
    finally:
        database.close()
    assert (
        len(
            _rows(
                postgres_connection_factory,
                "SELECT 1 FROM silver_ref.tract_decade_crosswalk WHERE source_snapshot_id = %s",
                (str(decade_reference),),
            )
        )
        == 3
    )


def _serve(monkeypatch, factory, payloads: dict[str, bytes]) -> None:
    from data_ingestion_toolbox.silver_ref import geography_pipeline
    from tests.support.dag_pipeline import stub_response
    from tests.support.postgres import PostgresHookStub

    monkeypatch.setattr(
        geography_pipeline, "_get_hook", lambda: PostgresHookStub(factory)
    )
    monkeypatch.setattr(
        geography_pipeline,
        "_download_with_retry",
        lambda _client, url, **_kwargs: stub_response(payloads[url], url=url),
    )


@pytest.fixture
def owned_runs(postgres_connection_factory: Callable[[], connection]):
    from tests.support.warehouse_scope import delete_capture_graph, source_run_ids

    baseline = source_run_ids(postgres_connection_factory, "CENSUS_GEO")
    yield
    runs = sorted(source_run_ids(postgres_connection_factory, "CENSUS_GEO") - baseline)
    remover = postgres_connection_factory()
    try:
        with remover.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_ref.tract_decade_crosswalk WHERE source_snapshot_id IN "
                "(SELECT capture_id FROM raw_capture.response_capture WHERE run_id = ANY(%s::UUID[]))",
                ([str(run) for run in runs],),
            )
            cursor.execute(
                "DELETE FROM silver_ref.geography_resolution "
                "WHERE provider_dataset = 'census_tract_relationship'"
            )
            delete_capture_graph(cursor, runs)
        remover.commit()
    finally:
        remover.close()


def test_a_sync_captures_both_files_before_publishing_them(
    postgres_connection_factory: Callable[[], connection],
    decade_reference,
    owned_runs,
    monkeypatch,
) -> None:
    """Covers: ETL-102 -- capture first, then each decade's parts; an unreadable file quarantines."""
    from data_ingestion_toolbox.silver_ref.tract_crosswalk import (
        RELATIONSHIP_FILES,
        sync_tract_crosswalk,
    )

    payloads = {
        RELATIONSHIP_FILES[2000]: (FIXTURES / "us2010trf.excerpt.txt").read_bytes(),
        RELATIONSHIP_FILES[2010]: (
            FIXTURES / "tab20_tract20_tract10.excerpt.txt"
        ).read_bytes(),
    }
    _serve(monkeypatch, postgres_connection_factory, payloads)
    counts = sync_tract_crosswalk()
    # The 2000 parts land where this reference holds both decades; it holds no
    # 2020 tract, so every 2010-to-2020 part is ledgered rather than written.
    assert counts["2000-2010"]["parts"] == 3
    assert counts["2010-2020"]["parts"] == 0
    assert counts["2010-2020"]["unmapped_tracts"] > 0

    payloads[RELATIONSHIP_FILES[2010]] = b"not|the|file\n"
    with pytest.raises(ValueError, match="lacks its columns"):
        sync_tract_crosswalk()
    assert _rows(
        postgres_connection_factory,
        """
        SELECT run.status, quarantine.error_code
        FROM control.capture_quarantine AS quarantine
        JOIN control.ingestion_run AS run ON run.run_id = quarantine.run_id
        WHERE quarantine.error_code = 'tract_crosswalk_replay_failed'
        """,
    ) == [("failed", "tract_crosswalk_replay_failed")]
