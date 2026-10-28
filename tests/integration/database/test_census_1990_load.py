"""The 1990 files captured into raw capture and replayed into the revision table.

Covers: ETL-098
"""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_acs import load_1990
from data_ingestion_toolbox.census_acs.stf1990 import crosswalk_1990
from tests.unit.census.test_load_1990 import PLACE, STATE, TRACT_A, _disc

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_a_state_is_captured_byte_for_byte_and_replayed_as_1990(
    postgres_connection_factory: Callable[[], connection], monkeypatch
) -> None:
    """Covers: ETL-098 -- every file in raw capture; revision rows keyed to the header."""
    files = _disc([STATE, PLACE, TRACT_A])
    monkeypatch.setattr(
        load_1990, "discs_1990", lambda: {"dec1990stf1": {"vt": ["DISC_T"]}}
    )

    def get(self, url: str, **_kwargs) -> httpx.Response:
        name = url.rsplit("/", 1)[-1]
        return httpx.Response(
            200, content=files[name], request=httpx.Request("GET", url)
        )

    monkeypatch.setattr(httpx.Client, "get", get)

    database = postgres_connection_factory()
    database.autocommit = True
    try:
        results = load_1990.load(
            postgres_connection_factory, datasets=["dec1990stf1"], only_states=["vt"]
        )
        with database.cursor() as cursor:
            cursor.execute(
                """
                SELECT capture.request_parameters->>'file', blob.payload_size
                FROM raw_capture.response_capture AS capture
                JOIN raw_capture.payload_blob AS blob USING (payload_checksum)
                WHERE capture.endpoint LIKE %s
                ORDER BY 1
                """,
                (f"{load_1990.ARCHIVE_ROOT}/DISC_T/%",),
            )
            captured = cursor.fetchall()
            cursor.execute(
                """
                SELECT geo_level, COUNT(*), COUNT(DISTINCT capture_id)
                FROM silver_census.observation_revision
                WHERE dataset = 'dec1990stf1' AND year = 1990
                GROUP BY geo_level ORDER BY geo_level
                """
            )
            revisions = cursor.fetchall()
    finally:
        with database.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_census.observation_revision "
                "WHERE dataset = 'dec1990stf1' AND year = 1990"
            )
        database.close()

    assert results["dec1990stf1/vt"]["discs"] == 1
    assert [name for name, _ in captured] == [
        "stf1a0vt.dbf",
        "stf1a1vt.dbf",
        "tables.dbf",
    ]
    assert all(size == len(files[name]) for name, size in captured)
    measures = len(crosswalk_1990()["dec1990stf1"])
    assert revisions == [
        ("place", measures, 1),
        ("state", measures, 1),
        ("tract", measures, 1),
    ]
