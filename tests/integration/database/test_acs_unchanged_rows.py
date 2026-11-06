"""A grain step rewrites only the served rows silver changed.

Covers: DB-069 -- `gold_census.refresh_rpt_acs_observations` called for a
        grain (and a state) used to delete every served row of its window and
        insert them all again, and hand every key to
        `gold_census.refresh_mv_acs_latest`, which rewrote those too. On the
        2024 tract step that was ~157k rows and ~450 seconds per state to add
        1,368 rows (2026-11-06).
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_acs.silver_census.transform import (
    _record_slice_fingerprint,
)
from data_ingestion_toolbox.utility.gold_schema import _changed_states
from data_ingestion_toolbox.utility.serving_reserve import ACS_CHUNK_CONFIG
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]

#: Outside the declared partition range, so the rows land in the default
#: partition this module can clear without touching a real vintage.
YEAR = 2093
STATE = "10"
TIME_SK = YEAR * 10000 + 101
WINDOW = (date(YEAR, 1, 1), date(YEAR, 12, 31))
TRACT = next(step for step in ACS_CHUNK_CONFIG.grain_steps if step.name == "tract")


def _tract_geo_id(code: str) -> str:
    return f"state:{STATE}|county:001|tract:{code}"


class _Seed:
    """One acs5 variable of YEAR with tract facts in STATE."""

    def __init__(self, factory: Callable[[], connection]) -> None:
        self.factory = factory
        token = uuid4().hex[:6].upper()
        self.table_id = f"B9{token[:4]}"
        self.variable_code = f"{self.table_id}_{token}E"
        self.geo_sk: dict[str, int] = {}

    def setup(self) -> None:
        database = self.factory()
        database.autocommit = True
        try:
            with database.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO silver_ref.dim_time (
                        time_sk, date_key, year, quarter, month, day, day_of_week,
                        day_name, month_name, week_of_year, is_weekend,
                        is_month_start, is_month_end, is_quarter_start,
                        is_quarter_end, is_year_start, is_year_end, ingested_at
                    )
                    SELECT %s, d, EXTRACT(YEAR FROM d)::INT, 1, 1, 1,
                           EXTRACT(ISODOW FROM d)::INT, TRIM(TO_CHAR(d, 'Day')),
                           'January', 1, EXTRACT(ISODOW FROM d) > 5, TRUE, FALSE,
                           TRUE, FALSE, TRUE, FALSE, NOW()
                    FROM (SELECT MAKE_DATE(%s, 1, 1) AS d) AS s
                    ON CONFLICT (time_sk) DO NOTHING
                    """,
                    (TIME_SK, YEAR),
                )
                cursor.execute(
                    """
                    INSERT INTO gold_census.dim_acs_table (
                        dataset_code, vintage_year, table_id, table_title,
                        concept, universe, survey_span_years, reference_url
                    ) VALUES ('acs5', %s, %s, 'Unchanged rows probe', 'Probe',
                              'Probe', 5, 'https://example.test/acs')
                    RETURNING acs_table_sk
                    """,
                    (YEAR, self.table_id),
                )
                table_sk = cursor.fetchone()[0]
                cursor.execute(
                    """
                    INSERT INTO gold_census.dim_acs_variable (
                        acs_table_sk, dataset_code, vintage_year, variable_code,
                        variable_label, concept, universe, value_role
                    ) VALUES (%s, 'acs5', %s, %s, 'Probe', 'Probe', 'Probe',
                              'ESTIMATE')
                    """,
                    (table_sk, YEAR, self.variable_code),
                )
        finally:
            database.close()

    def put(self, code: str, value: int) -> None:
        """Insert or change one tract's fact, moving `ingested_at` as silver does."""
        database = self.factory()
        database.autocommit = True
        try:
            with database.cursor() as cursor:
                if code not in self.geo_sk:
                    self.geo_sk[code] = seed_geography(
                        cursor,
                        geo_type="tract",
                        state_fips=STATE,
                        county_fips="001",
                        tract_code=code,
                        vintage=YEAR,
                        name=f"Probe tract {code}",
                    )
                cursor.execute(
                    """
                    INSERT INTO silver_census.fact_demographics (
                        geo_sk, geo_id, geo_level, state_fips, county_fips,
                        time_sk, dataset, estimate_year, duration_start,
                        duration_end, table_id, load_batch_id, variable_code,
                        estimate_value, ingested_at
                    ) VALUES (%s, %s, 'tract', %s, '001', %s, 'acs5', %s,
                              MAKE_DATE(%s, 1, 1), MAKE_DATE(%s, 12, 31), %s,
                              gen_random_uuid(), %s, %s, clock_timestamp())
                    ON CONFLICT (dataset, table_id, variable_code, geo_id, estimate_year)
                    DO UPDATE SET estimate_value = EXCLUDED.estimate_value,
                                  ingested_at = EXCLUDED.ingested_at
                    """,
                    (
                        self.geo_sk[code],
                        _tract_geo_id(code),
                        STATE,
                        TIME_SK,
                        YEAR,
                        YEAR,
                        YEAR,
                        self.table_id,
                        self.variable_code,
                        value,
                    ),
                )
        finally:
            database.close()

    def drop(self, code: str) -> None:
        database = self.factory()
        database.autocommit = True
        try:
            with database.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM silver_census.fact_demographics "
                    "WHERE geo_id = %s AND variable_code = %s",
                    (_tract_geo_id(code), self.variable_code),
                )
        finally:
            database.close()

    def serve(self) -> dict[str, int]:
        """One state's tract step, as the chunk driver runs it; the counts it logged."""
        database = self.factory()
        try:
            with database.cursor() as cursor:
                cursor.execute(
                    f"CALL {ACS_CHUNK_CONFIG.report_procedure}(%s, %s, %s, %s)",
                    (*WINDOW, ["TRACT"], STATE),
                )
                cursor.execute(
                    f"CALL {ACS_CHUNK_CONFIG.latest_procedure}(%s, %s)", WINDOW
                )
            database.commit()
            notice = next(
                text
                for text in database.notices
                if "[ACS RPT CHUNK] status=COMPLETE" in text
            )
        finally:
            database.close()
        return {
            name: int(value)
            for name, value in re.findall(
                r"(deleted_rows|inserted_rows|unchanged_rows|affected_keys)=(\d+)",
                notice,
            )
        }

    def versions(self, relation: str) -> dict[str, tuple[str, str]]:
        """Each served tract's (value, row version) in ``relation``."""
        database = self.factory()
        try:
            with database.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT geo_id, estimate_value::TEXT, xmin::TEXT
                    FROM {relation}
                    WHERE variable_code = %s
                    """,
                    (self.variable_code,),
                )
                return {
                    geo_id.rsplit(":", 1)[1]: (value, version)
                    for geo_id, value, version in cursor.fetchall()
                }
        finally:
            database.close()

    def teardown(self) -> None:
        database = self.factory()
        database.autocommit = True
        try:
            with database.cursor() as cursor:
                for relation in (
                    "gold_census.mv_acs_latest",
                    "gold_census.rpt_acs_observations",
                    "silver_census.fact_demographics",
                    "gold_census.dim_acs_variable",
                ):
                    cursor.execute(
                        f"DELETE FROM {relation} WHERE variable_code = %s",
                        (self.variable_code,),
                    )
                cursor.execute(
                    "DELETE FROM gold_census.dim_acs_table "
                    "WHERE vintage_year = %s AND table_id = %s",
                    (YEAR, self.table_id),
                )
                for code in self.geo_sk:
                    delete_geography(cursor, _tract_geo_id(code))
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = %s", (TIME_SK,)
                )
        finally:
            database.close()


@pytest.fixture
def seed(postgres_connection_factory: Callable[[], connection]) -> Iterator[_Seed]:
    probe = _Seed(postgres_connection_factory)
    probe.setup()
    try:
        yield probe
    finally:
        probe.teardown()


def test_a_replay_with_nothing_changed_rewrites_no_row(seed: _Seed) -> None:
    """Covers: DB-069 -- no delete, no insert, no latest key; the rows keep their versions."""
    for code, value in (("000100", 1), ("000200", 2), ("000300", 3)):
        seed.put(code, value)
    first = seed.serve()
    assert first["inserted_rows"] == 3 and first["deleted_rows"] == 0
    served = seed.versions("gold_census.rpt_acs_observations")
    latest = seed.versions("gold_census.mv_acs_latest")
    assert set(served) == set(latest) == {"000100", "000200", "000300"}

    again = seed.serve()

    assert again == {
        "deleted_rows": 0,
        "inserted_rows": 0,
        "unchanged_rows": 3,
        "affected_keys": 0,
    }
    assert seed.versions("gold_census.rpt_acs_observations") == served
    assert seed.versions("gold_census.mv_acs_latest") == latest


def test_only_changed_added_and_removed_rows_are_written(seed: _Seed) -> None:
    """Covers: DB-069 -- a changed value, a new tract and a withdrawn tract; the untouched row stays."""
    for code, value in (("000100", 1), ("000200", 2), ("000300", 3)):
        seed.put(code, value)
    seed.serve()
    served = seed.versions("gold_census.rpt_acs_observations")
    latest = seed.versions("gold_census.mv_acs_latest")

    seed.put("000100", 10)  # revised
    seed.drop("000200")  # withdrawn
    seed.put("000400", 4)  # new
    counts = seed.serve()

    # The revised row is deleted and inserted; the withdrawn one deleted; the
    # new one inserted; one row is left as it was.
    assert counts == {
        "deleted_rows": 2,
        "inserted_rows": 2,
        "unchanged_rows": 1,
        "affected_keys": 3,
    }
    after = seed.versions("gold_census.rpt_acs_observations")
    assert {code: value for code, (value, _) in after.items()} == {
        "000100": "10",
        "000300": "3",
        "000400": "4",
    }
    assert after["000300"] == served["000300"]
    latest_after = seed.versions("gold_census.mv_acs_latest")
    assert {code: value for code, (value, _) in latest_after.items()} == {
        "000100": "10",
        "000300": "3",
        "000400": "4",
    }
    assert latest_after["000300"] == latest["000300"]


# -- Which states a tract or place step publishes ----------------------------

LEDGER_YEAR = 2094


@pytest.fixture
def ledger(postgres_connection_factory: Callable[[], connection]):
    """Write slice-ledger rows for LEDGER_YEAR with the transform's own writer."""

    def record(slice_key: str, rows_changed: int) -> None:
        _record_slice_fingerprint(
            PostgresHookStub(postgres_connection_factory),
            LEDGER_YEAR,
            slice_key,
            "f",
            rows_changed,
        )

    try:
        yield record
    finally:
        database = postgres_connection_factory()
        database.autocommit = True
        try:
            with database.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM silver_census.transform_slice_state WHERE year = %s",
                    (LEDGER_YEAR,),
                )
        finally:
            database.close()


def _database_now(factory: Callable[[], connection]) -> datetime:
    database = factory()
    try:
        with database.cursor() as cursor:
            cursor.execute("SELECT clock_timestamp()")
            return cursor.fetchone()[0]
    finally:
        database.close()


def _states(factory, since: datetime | None, states: list[str]) -> list[str]:
    chunk = {
        "start": date(LEDGER_YEAR, 1, 1),
        "completed_at": since,
        "completed": since,
    }
    return _changed_states(
        PostgresHookStub(factory), ACS_CHUNK_CONFIG, TRACT, chunk, states
    )


def test_only_the_states_whose_slices_changed_publish(
    postgres_connection_factory: Callable[[], connection], ledger
) -> None:
    """Covers: DB-069 -- Delaware's tracts changed; Wisconsin's replayed with none; Ohio is unknown."""
    ledger("acs5/tract/10", 4)
    ledger("acs5/tract/55/[,550250001)", 6)
    ledger("acs5/tract/55/[550250001,)", 6)
    served = _database_now(postgres_connection_factory)
    ledger("acs5/tract/10", 2)
    ledger("acs5/tract/55/[,550250001)", 0)
    ledger("acs5/tract/55/[550250001,)", 0)
    ledger("acs5/place/55", 9)  # another grain changing never publishes tracts

    assert _states(postgres_connection_factory, served, ["10", "39", "55"]) == [
        "10",
        "39",
    ]


def test_every_state_publishes_when_the_ledger_cannot_say(
    postgres_connection_factory: Callable[[], connection], ledger
) -> None:
    """Covers: DB-069 -- no ledger row, a never-served chunk, or a changed whole-year slice."""
    everyone = ["10", "55"]
    served = datetime.now(timezone.utc)
    assert _states(postgres_connection_factory, served, everyone) == everyone
    ledger("acs5/tract/10", 0)
    ledger("acs5/tract/55", 0)
    assert _states(postgres_connection_factory, None, everyone) == everyone
    assert (
        _states(postgres_connection_factory, served + timedelta(days=1), everyone) == []
    )
    ledger("acs5/*", 3)
    assert _states(postgres_connection_factory, served, everyone) == everyone
