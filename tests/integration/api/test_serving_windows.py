"""Trailing and year-to-date windows over the real warehouse's served months.

Covers: API-169
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection

import apps.api.services.neutral_observations_service as service
from data_ingestion_toolbox.semantics.time_aggregation import TimeMethod
from tests.integration.api.test_catalog_serving_agreement import (  # noqa: F401
    api_client,
    published_bls_metric,
)

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]


def _month(cursor, metric_code: str, series_id: str, month: int, value) -> None:
    cursor.execute(
        """
        INSERT INTO gold_bls.rpt_bls_observations (
            source_code, observation_date, duration_start, duration_end,
            as_of_date, updated_at, geo_id, geo_level, series_id, program_code,
            value, value_status, units, metric_code
        ) VALUES (
            'BLS', (MAKE_DATE(2096, %(m)s, 1) + INTERVAL '1 month - 1 day')::DATE,
            MAKE_DATE(2096, %(m)s, 1),
            (MAKE_DATE(2096, %(m)s, 1) + INTERVAL '1 month - 1 day')::DATE,
            '2097-01-15', NOW(), 'state:93', 'STATE', %(series)s, 'SW', %(v)s,
            CASE WHEN %(v)s IS NULL THEN 'missing' ELSE 'valid' END,
            'persons', %(code)s
        )
        """,
        {"m": month, "v": value, "series": series_id, "code": metric_code},
    )


def test_windows_end_at_the_anchor_and_refuse_a_gap(
    api_client: TestClient,  # noqa: F811
    published_bls_metric: str,  # noqa: F811
    postgres_connection_factory: Callable[[], connection],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-169 — trailing and YTD means of served months, anchored, with a withheld month refused."""
    series_id = published_bls_metric.split(":", 1)[1]
    monkeypatch.setattr(
        service,
        "authorized_method",
        lambda code: TimeMethod(code, "mean", "approved", "Nick", 1)
        if code == published_bls_metric
        else None,
    )
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            for month in range(1, 13):
                # August is withheld: never zero, never averaged.
                _month(
                    cursor,
                    published_bls_metric,
                    series_id,
                    month,
                    None if month == 8 else month * 10,
                )
        writer.commit()
    finally:
        writer.close()

    def read(**params: str):
        return api_client.get(
            "/api/v1/observations",
            params={"metric_code": published_bls_metric, **params},
        )

    try:
        newest = read(window="trailing_3")
        ytd = read(window="ytd", period_start="2096-06-01")
        gap = read(window="trailing_12")
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_bls.rpt_bls_observations "
                    "WHERE series_id = %s AND observation_date < '2097-01-01'",
                    (series_id,),
                )
            cleanup.commit()
        finally:
            cleanup.close()

    assert newest.status_code == 200, newest.text
    assert newest.json()["window"] == "trailing_3"
    (row,) = newest.json()["items"]
    assert (row["geo_id"], row["period_start"], row["period_end"]) == (
        "state:93",
        "2096-10-01",
        "2096-12-31",
    )
    assert Decimal(row["value"]) == Decimal("110")
    assert row["derivation"]["kind"] == "derived"
    assert row["derivation"]["method"] == "mean"
    assert (row["derivation"]["expected_periods"], row["derivation"]["present_periods"]) == (3, 3)

    (row,) = ytd.json()["items"]
    assert (row["period_start"], row["period_end"]) == ("2096-01-01", "2096-06-30")
    assert Decimal(row["value"]) == Decimal("35")
    assert row["derivation"]["expected_periods"] == 6

    (row,) = gap.json()["items"]
    assert row["value"] is None and row["value_status"] == "incomplete_window"
    assert row["derivation"]["refusal_reason"] == (
        "incomplete_window: 11 of 12 periods reported"
    )

