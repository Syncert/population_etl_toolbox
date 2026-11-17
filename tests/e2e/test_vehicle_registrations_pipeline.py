"""Deterministic vehicle registrations flow from captured files to the API.

Every provider byte is WisDOT's or FHWA's own: the vehicle-information
pages, the 2025 Report 28 (cut to Dane County and the statewide tables) and
the 2023 and 2024 MV-1 workbooks read on 2026-11-17, played through the
adapter's own capture path by a scripted client; nothing here reaches the
network. The node proves what a reader must be able to see: Dane County's
and the City of Madison's registered vehicles by WisDOT type in the gold
views, and Dane County's and Wisconsin's figures through the neutral
`/api/v1/observations` route with the publisher's own cell text.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import vehicle_registrations as vehicles
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "VEHICLE_REGISTRATIONS"
PUBLISHER_SCHEMA = "gold_vehicle_registrations"
WISDOT_AUTO = f"{SOURCE_CODE}:wisdot_rpt28:auto"
MV1_ALL_TOTAL = f"{SOURCE_CODE}:fhwa_mv1:all_motor_vehicles:total"
DANE = "state:55|county:025"
WISCONSIN = "state:55"


@pytest.fixture
def vehicle_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return vehicles.reviewed_warehouse(postgres_connection_factory, request)


def _rows(factory: Callable[[], connection], sql: str) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql)
            return list(cursor.fetchall())
    finally:
        reader.close()


def test_dane_and_madison_fleet_by_type_reaches_the_neutral_api(
    vehicle_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-138 — Dane County's and Madison's fleet by WisDOT type in
    the gold views, and a glossary-discovered figure answering through
    `/api/v1/observations` for Dane County and Wisconsin, as published.

    Covers: E2E-004 — reading the frozen files again adds no row and the
        views answer the same rows.
    """
    factory = vehicle_warehouse
    release_id, changed, published = vehicles.load_release(factory)
    assert release_id is not None and changed > 0 and published == 1

    summary = """
        SELECT county_name, cvt_name, cvt_kind, vehicle_type, value, value_status
        FROM gold_vehicle_registrations.wisdot_registration_latest
        WHERE data_year = 2025 AND county_code = '13'
          AND (row_kind = 'county' OR (cvt_name = 'MADISON' AND cvt_kind = 'C'))
          AND vehicle_type IN ('AUTO', 'TRUK', 'CYCL')
        ORDER BY cvt_name, vehicle_type
    """
    rows = _rows(factory, summary)
    assert rows == [
        ("DANE", "", "", "AUTO", 152_184, "valid"),
        ("DANE", "", "", "CYCL", 19_503, "valid"),
        ("DANE", "", "", "TRUK", 318_345, "valid"),
        ("DANE", "MADISON", "C", "AUTO", 70_108, "valid"),
        ("DANE", "MADISON", "C", "CYCL", 6_613, "valid"),
        ("DANE", "MADISON", "C", "TRUK", 122_249, "valid"),
    ]
    state = """
        SELECT data_year, vehicle_class, value::BIGINT
        FROM gold_vehicle_registrations.fhwa_mv1_latest
        WHERE geo_id = 'state:55' AND ownership = 'total'
          AND vehicle_class IN ('automobiles', 'trucks', 'motorcycles')
        ORDER BY data_year DESC, vehicle_class
        LIMIT 3
    """
    newest = _rows(factory, state)
    assert newest == [
        (2024, "automobiles", 1_626_951),
        (2024, "motorcycles", 279_308),
        (2024, "trucks", 3_918_981),
    ]

    emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)
    assert harvest_publisher(factory, Publisher(PUBLISHER_SCHEMA)) >= 2

    with real_api_client() as client:
        catalog = client.get(
            "/api/v1/catalog/metrics", params={"source_code": SOURCE_CODE, "limit": 100}
        ).json()
        codes = {item["metric_code"] for item in catalog["items"]}
        assert {WISDOT_AUTO, MV1_ALL_TOTAL} <= codes

        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"] is True

        dane = client.get(
            "/api/v1/observations",
            params={"metric_code": WISDOT_AUTO, "geo_id": DANE, "limit": 100},
        )
        assert dane.status_code == 200, dane.text
        dane_items = dane.json()["items"]
        assert len(dane_items) == 1
        assert float(dane_items[0]["value"]) == 152_184
        assert dane_items[0]["geo_level"] == "COUNTY"
        assert dane_items[0]["period_start"][:10] == "2025-01-01"
        assert dane_items[0]["dimensions"]["value_source"] == "152,184"

        params = {"metric_code": MV1_ALL_TOTAL, "geo_id": WISCONSIN, "limit": 100}
        state_items = client.get("/api/v1/observations", params=params).json()["items"]
        assert [item["period_start"][:10] for item in state_items] == [
            "2023-01-01",
            "2024-01-01",
        ]
        assert float(state_items[-1]["value"]) == 5_836_776
        assert state_items[-1]["geo_level"] == "STATE"

        assert vehicles.load_release(factory) == (None, 0, 0)
        assert _rows(factory, summary) == rows
        assert _rows(factory, state) == newest
        repeat = client.get("/api/v1/observations", params=params).json()
        assert repeat["items"] == state_items
