"""Database contracts for the ACS gold serving refresh."""

from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

import pytest
from psycopg2.extensions import connection


pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_acs_latest_refresh_recomputes_each_affected_key_across_history(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ETL-037 — an old-year refresh retains the true latest ACS row."""
    token = uuid4().hex[:12].upper()
    geo_id = f"test:acs-latest:{token}"
    variable_code = f"B99999_{token}E"
    metric_code = f"ACS:acs5:{variable_code}"

    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute("SET LOCAL temp_file_limit = '1MB'")
            cursor.execute(
                """
                INSERT INTO gold_census.rpt_acs_observations (
                    observation_date,
                    as_of_date,
                    updated_at,
                    geo_id,
                    geo_level,
                    value,
                    dataset_code,
                    vintage_year,
                    table_id,
                    variable_code,
                    estimate_value,
                    metric_code
                )
                VALUES
                    ('2022-01-01', '2022-01-01', '2022-08-01 00:00:00+00',
                     %s, 'COUNTY', 10, 'acs5', 2022, 'B99999', %s, 10, %s),
                    ('2024-01-01', '2024-01-01', '2024-08-01 00:00:00+00',
                     %s, 'COUNTY', 20, 'acs5', 2024, 'B99999', %s, 20, %s)
                """,
                (
                    geo_id,
                    variable_code,
                    metric_code,
                    geo_id,
                    variable_code,
                    metric_code,
                ),
            )

            cursor.execute(
                "CALL gold_census.refresh_mv_acs_latest(%s, %s)",
                ("2022-01-01", "2022-12-31"),
            )
            cursor.execute(
                """
                SELECT observation_date::TEXT, vintage_year, estimate_value
                FROM gold_census.mv_acs_latest
                WHERE geo_id = %s
                  AND variable_code = %s
                  AND metric_code = %s
                """,
                (geo_id, variable_code, metric_code),
            )

            assert cursor.fetchall() == [("2024-01-01", 2024, 20)]
    finally:
        database_connection.rollback()
        database_connection.close()
