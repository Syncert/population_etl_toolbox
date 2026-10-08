"""Regional and metro prices reach silver and gold under their own areas.

Covers: ETL-078, ETL-081
"""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls.gold_bls.transform import refresh_bls_elements
from data_ingestion_toolbox.bls.silver_bls import transform
from data_ingestion_toolbox.quality.sources import bls_bimonthly_cadence
from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    GeographyRecord,
    GeographyRepository,
)
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.capture_seed import delete_geography, seed_capture
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]

SERIES = {
    # series id: (program, title, base period, months published)
    "CUUR0200SAF11": (
        "cu",
        "Food at home in Midwest urban, all urban consumers, not seasonally adjusted",
        "1982-84=100",
        ("M01", "M02"),
    ),
    "CUURS35ASETB01": (
        "cu",
        "Gasoline (all types) in Washington-Arlington-Alexandria, DC-VA-MD-WV, all urban consumers, not seasonally adjusted",
        "NOVEMBER 1996=100",
        ("M01",),
    ),
    "APUS35A74714": (
        "ap",
        "Gasoline, unleaded regular, per gallon/3.785 liters in Washington-Arlington-Alexandria, DC-VA-MD-WV, average price, not seasonally adjusted",
        "",
        ("M01",),
    ),
    # Pittsburgh's discontinued area: no geography, so no silver row.
    "CUURA104SAF11": ("cu", "Food at home in Pittsburgh, PA", "1982-84=100", ("M01",)),
}
AREAS = ("region:2", "area:bls_cpi:S35A")


def _cleanup(factory: Callable[[], connection]) -> None:
    cleanup = factory()
    try:
        with cleanup.cursor() as cursor:
            for table in (
                "gold_bls.dim_bls_series",
                "silver_bls.fact_labor_statistics",
                "silver_bls.observation_revision",
                "raw_bls.bls_series",
            ):
                cursor.execute(
                    f"DELETE FROM {table} WHERE series_id = ANY(%s)", (list(SERIES),)
                )
            cursor.execute(
                "DELETE FROM silver_ref.geography_resolution WHERE provider_source = 'BLS' "
                "AND provider_dataset IN ('cu', 'ap') AND source_vintage = 2097"
            )
            for geo_id in AREAS:
                delete_geography(cursor, geo_id)
            cursor.execute(
                "DELETE FROM silver_ref.dim_time WHERE time_sk IN (20970101, 20970201)"
            )
        cleanup.commit()
    finally:
        cleanup.close()


def test_prices_land_on_their_region_and_metro_with_their_own_units(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ETL-078, ETL-081 — a region and a BLS metro by code, their own units, no invented month, and the cadence rule."""
    _cleanup(postgres_connection_factory)
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20970101, "2097-01-01")
            _seed_time(cursor, 20970201, "2097-02-01")
            capture_id = seed_capture(cursor, "BLS")
        writer.commit()
        GeographyRepository(postgres_connection_factory).load_attributes(
            [
                GeographyRecord(
                    "census_region",
                    "region:2",
                    "2",
                    None,
                    None,
                    None,
                    "Midwest Region",
                    2024,
                    area_code="2",
                ),
                GeographyRecord(
                    "provider_area",
                    "area:bls_cpi:S35A",
                    "S35A",
                    None,
                    None,
                    None,
                    "Washington-Arlington-Alexandria, DC-VA-MD-WV",
                    2026,
                    area_code="bls_cpi:S35A",
                ),
            ],
            capture_id=capture_id,
        )
        with writer.cursor() as cursor:
            index = 0
            for series_id, (program, title, base, months) in SERIES.items():
                cursor.execute(
                    """
                    INSERT INTO raw_bls.bls_series (
                        program, series_id, title, seasonal, measure, area_code, raw_metadata
                    ) VALUES (%s, %s, %s, 'U', %s, %s, %s)
                    """,
                    (
                        program,
                        series_id,
                        title,
                        series_id[-5:],
                        series_id[4:8] if program == "cu" else series_id[3:7],
                        json.dumps({"series_id": series_id, "base_period": base}),
                    ),
                )
                for month in months:
                    cursor.execute(
                        """INSERT INTO silver_bls.observation_revision (
                            capture_id, observation_index, program, series_id,
                            year_source, period_source, period_name_source, value_source,
                            year, period, period_name, value, value_status, is_latest
                        ) VALUES (%s, %s, %s, %s, '2097', %s, %s, '3.25',
                                  2097, %s, %s, 3.25, 'valid', TRUE)""",
                        (
                            capture_id,
                            index,
                            program,
                            series_id,
                            month,
                            month,
                            month,
                            month,
                        ),
                    )
                    index += 1
        writer.commit()

        monkeypatch.setattr(
            transform,
            "_get_hook",
            lambda: PostgresHookStub(postgres_connection_factory),
        )
        assert transform.transform_bls_to_silver("cu") == 3
        assert transform.transform_bls_to_silver("ap") == 1

        with writer.cursor() as cursor:
            cursor.execute(
                """
                SELECT f.series_id, g.geo_id, g.geo_level, f.period
                FROM silver_bls.fact_labor_statistics AS f
                JOIN silver_ref.dim_geo AS g ON g.geo_sk = f.geo_sk
                WHERE f.series_id = ANY(%s)
                ORDER BY 1, 4
                """,
                (list(SERIES),),
            )
            assert cursor.fetchall() == [
                ("APUS35A74714", "area:bls_cpi:S35A", "provider_area", "M01"),
                ("CUUR0200SAF11", "region:2", "census_region", "M01"),
                ("CUUR0200SAF11", "region:2", "census_region", "M02"),
                # Washington is published every other month: February has no
                # row because BLS published none, and none is invented.
                ("CUURS35ASETB01", "area:bls_cpi:S35A", "provider_area", "M01"),
            ]
        writer.commit()

        refresh_bls_elements(PostgresHookStub(postgres_connection_factory))
        with writer.cursor() as cursor:
            cursor.execute(
                """
                SELECT series_id, program_code, measure_category, unit_of_measure,
                       value_type, geographic_level, semantic_notes
                FROM gold_bls.dim_bls_series WHERE series_id = ANY(%s) ORDER BY 1
                """,
                (list(SERIES),),
            )
            rows = {row[0]: row[1:] for row in cursor.fetchall()}
        writer.commit()
        assert set(rows) == {"APUS35A74714", "CUUR0200SAF11", "CUURS35ASETB01"}
        assert rows["APUS35A74714"][:5] == (
            "AP",
            "AVERAGE_PRICE",
            "U.S. dollars per gallon/3.785 liters",
            "CURRENCY",
            "PROVIDER_AREA",
        )
        assert rows["CUUR0200SAF11"][2:5] == (
            "Index 1982-1984=100",
            "INDEX",
            "CENSUS_REGION",
        )
        assert rows["CUURS35ASETB01"][2] == "Index NOVEMBER 1996=100"
        assert "every other month" in rows["CUURS35ASETB01"][5]
        assert "not comparable across areas" in rows["CUURS35ASETB01"][5]
        assert rows["CUUR0200SAF11"][5].endswith("Published monthly.")

        # ETL-081: Washington is every other month. January alone is BLS's
        # cadence; a February value beside it is a finding, not a fact to
        # serve silently.
        with writer.cursor() as cursor:
            (on_cadence,) = bls_bimonthly_cadence(cursor, {})
            cursor.execute(
                """INSERT INTO silver_bls.observation_revision (
                    capture_id, observation_index, program, series_id,
                    year_source, period_source, period_name_source, value_source,
                    year, period, period_name, value, value_status, is_latest
                ) VALUES (%s, 99, 'cu', 'CUURS35ASETB01', '2097', 'M02', 'M02', '3.30',
                          2097, 'M02', 'M02', 3.30, 'valid', TRUE)""",
                (str(capture_id),),
            )
        writer.commit()
        transform.transform_bls_to_silver("cu")
        with writer.cursor() as cursor:
            (off_cadence,) = bls_bimonthly_cadence(cursor, {})
        writer.commit()
        assert on_cadence.result == "pass"
        assert off_cadence.result == "warn" and off_cadence.observed_count == 1
        assert off_cadence.evidence == ["CUURS35ASETB01|2097-02-01"]
    finally:
        writer.close()
        _cleanup(postgres_connection_factory)
