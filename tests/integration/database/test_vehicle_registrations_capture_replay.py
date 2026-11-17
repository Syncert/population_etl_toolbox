"""Real PostgreSQL vehicle registrations capture-to-gold contract.

Covers: ETL-138
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.vehicle_registrations.config import (
    FHWA_BASE_URL,
    FHWA_MV1_PATH,
)
from tests.support import vehicle_registrations as vehicles

pytestmark = [pytest.mark.integration, pytest.mark.database]

MV1_2024 = f"{FHWA_BASE_URL}{FHWA_MV1_PATH.format(year=2024)}"


@pytest.fixture
def vehicle_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return vehicles.reviewed_warehouse(postgres_connection_factory, request)


def _rows(factory: Callable[[], connection], sql: str, parameters: tuple = ()) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def test_a_release_captures_every_document_and_records_unpublished_years(
    vehicle_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-138 — two index pages, one Report 28, MV-1 2023-2025 (2025 404)."""
    client = vehicles.FixtureClient()
    release_id, changed, published = vehicles.load_release(vehicle_warehouse, client=client)
    assert release_id is not None and published == 1 and changed > 0
    documents = dict(
        _rows(
            vehicle_warehouse,
            """
            SELECT document_key, status FROM control.vehicle_registrations_document
            WHERE release_id = %s
            """,
            (str(release_id),),
        )
    )
    assert documents == {
        "wisdot_index:/Pages/about-wisdot/newsroom/statistics/veh-info.aspx": "captured",
        "wisdot_index:/Pages/about-wisdot/newsroom/statistics/veh-info-prev.aspx": "captured",
        "wisdot_rpt28:cal:2025": "parsed",
        "fhwa_mv1:2023": "parsed",
        "fhwa_mv1:2024": "parsed",
    }
    assert len(client.calls) == 6
    statuses = _rows(
        vehicle_warehouse,
        """
        SELECT request.status, COUNT(*)
        FROM control.ingestion_request AS request
        JOIN control.vehicle_registrations_release AS release
          ON release.run_id = request.run_id
        WHERE release.release_id = %s
        GROUP BY request.status ORDER BY 1
        """,
        (str(release_id),),
    )
    assert statuses == [("captured", 5), ("empty", 1)]


def test_dane_county_and_wisconsin_reach_gold_as_published(
    vehicle_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-138 — Report 28 Dane by type sums to its total; MV-1 Wisconsin."""
    vehicles.load_release(vehicle_warehouse)
    dane = dict(
        _rows(
            vehicle_warehouse,
            """
            SELECT vehicle_type, value
            FROM gold_vehicle_registrations.wisdot_registration_latest
            WHERE data_year = 2025 AND table_part = 'county' AND geo_id = %s
            """,
            ("state:55|county:025",),
        )
    )
    assert dane == {
        "AUCY": 56,
        "AUTO": 152_184,
        "CYCL": 19_503,
        "TRLR": 39_988,
        "TRUK": 318_345,
        "SUM": 530_076,
    }
    assert sum(v for k, v in dane.items() if k != "SUM") == dane["SUM"]
    city = dict(
        _rows(
            vehicle_warehouse,
            """
            SELECT vehicle_type, value
            FROM gold_vehicle_registrations.wisdot_registration_latest
            WHERE data_year = 2025 AND table_part = 'cvt' AND county_code = '13'
              AND cvt_name = 'MADISON' AND cvt_kind = 'C'
            """,
        )
    )
    assert city["AUTO"] == 70_108 and city["TRUK"] == 122_249
    (state,) = _rows(
        vehicle_warehouse,
        """
        SELECT value, geo_type FROM gold_vehicle_registrations.wisdot_registration_latest
        WHERE data_year = 2025 AND geo_id = 'state:55' AND vehicle_type = 'SUM'
        """,
    )
    assert state == (6_384_450, "state")
    wisconsin = _rows(
        vehicle_warehouse,
        """
        SELECT value, source_label, footnote_marks, table_footnotes
        FROM gold_vehicle_registrations.fhwa_mv1_latest
        WHERE data_year = 2024 AND geo_id = 'state:55'
          AND vehicle_class = 'all_motor_vehicles' AND ownership = 'total'
        """,
    )
    assert len(wisconsin) == 1
    value, label, marks, notes = wisconsin[0]
    assert (int(value), label, marks) == (5_836_776, "Wisconsin (2)", ["2"])
    assert any(note["mark"] == "2" for note in notes)
    (nation,) = _rows(
        vehicle_warehouse,
        """
        SELECT value FROM gold_vehicle_registrations.fhwa_mv1_latest
        WHERE data_year = 2024 AND geo_id = 'us:1'
          AND vehicle_class = 'all_motor_vehicles' AND ownership = 'total'
        """,
    )
    assert int(nation[0]) == 297_525_836


def test_repeating_files_write_nothing_and_a_changed_file_only_its_changes(
    vehicle_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-138 — CHURN RULE: unchanged documents are not parsed again."""
    first, changed, _ = vehicles.load_release(vehicle_warehouse)
    assert first is not None and changed > 0
    assert vehicles.load_release(vehicle_warehouse) == (None, 0, 0)
    revised = vehicles.FixtureClient(
        overrides={MV1_2024: vehicles.fixture_bytes("mv1-2023.xlsx")}
    )
    second, changed_again, published = vehicles.load_release(
        vehicle_warehouse, client=revised
    )
    assert second is not None and published == 1
    documents = dict(
        _rows(
            vehicle_warehouse,
            """
            SELECT document_key, status FROM control.vehicle_registrations_document
            WHERE release_id = %s AND document_kind <> 'index'
            """,
            (str(second),),
        )
    )
    assert documents == {
        "wisdot_rpt28:cal:2025": "unchanged",
        "fhwa_mv1:2023": "unchanged",
        "fhwa_mv1:2024": "parsed",
    }
    written = _rows(
        vehicle_warehouse,
        """
        SELECT COUNT(*), COUNT(DISTINCT data_year)
        FROM silver_vehicle_registrations.fhwa_mv1_figure WHERE release_id = %s
        """,
        (str(second),),
    )[0]
    assert written[1] == 1 and 0 < written[0] == changed_again
    assert _rows(
        vehicle_warehouse,
        "SELECT COUNT(*) FROM silver_vehicle_registrations.wisdot_count WHERE release_id = %s",
        (str(second),),
    ) == [(0,)]


def test_a_report_in_another_layout_is_set_aside_by_name(
    vehicle_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-138 — the file writes nothing and its reason is recorded."""
    broken = vehicles.FixtureClient(
        overrides={
            f"https://wisconsindot.gov{vehicles.RPT28_PATH}": b"%PDF-1.4 not a report"
        }
    )
    release_id, _changed, published = vehicles.load_release(vehicle_warehouse, client=broken)
    assert release_id is not None and published == 1
    assert _rows(
        vehicle_warehouse,
        """
        SELECT document.status, quarantine.error_code
        FROM control.vehicle_registrations_document AS document
        JOIN silver_vehicle_registrations.quarantine AS quarantine
          ON quarantine.capture_id = document.capture_id
        WHERE document.release_id = %s AND document.document_key = 'wisdot_rpt28:cal:2025'
        """,
        (str(release_id),),
    ) == [("set_aside", "not_a_pdf")]
