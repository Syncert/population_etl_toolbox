"""Real PostgreSQL LEHD LODES history load: resumable, all-or-nothing publish.

Covers: ETL-108
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_lodes.capture import capture_state_year
from data_ingestion_toolbox.census_lodes.config import LodesConfig
from data_ingestion_toolbox.census_lodes.history import (
    LOCK_KEY,
    HistoryLoadBusy,
    load_history,
)
from data_ingestion_toolbox.quality.sources import (
    lodes_served_sums,
    lodes_state_jobs_continuity,
    lodes_year_coverage,
)
from tests.support import census_lodes as lodes

pytestmark = [pytest.mark.integration, pytest.mark.database]

CONFIG = LodesConfig(min_spacing_seconds=0, max_attempts=1)


@pytest.fixture
def lodes_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return lodes.reviewed_warehouse(postgres_connection_factory, request)


def _rows(
    factory: Callable[[], connection], sql: str, parameters: tuple | None = None
) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def _execute(
    factory: Callable[[], connection], sql: str, parameters: tuple = ()
) -> None:
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(sql, parameters)
        writer.commit()
    finally:
        writer.close()


def _load(factory, client=None, **kwargs):
    return load_history(
        factory,
        kwargs.pop("years", [2023, 2008]),
        states=["de"],
        config=CONFIG,
        client=client or lodes.FixtureClient(),
        **kwargs,
    )


def _outcome(factory, executor):
    reader = factory()
    try:
        with reader.cursor() as cursor:
            return executor(cursor, {})[0]
    finally:
        reader.close()


def test_every_year_loads_then_publishes_and_a_rerun_fetches_nothing(
    lodes_warehouse,
) -> None:
    """Covers: ETL-108 — two years load and publish together; a rerun is a no-op."""
    factory = lodes_warehouse
    summary = _load(factory)
    assert summary["published"] is True and summary["failed"] == []
    assert summary["published_slices"] == 2 and summary["served_rows_written"] > 0
    assert [(y["year"], y["states"]) for y in summary["years"]] == [
        (2023, {"silver_ready": 1}),
        (2008, {"silver_ready": 1}),
    ]
    served = _rows(
        factory,
        """
        SELECT year, COUNT(*) FROM gold_census_lodes.observation_latest
        WHERE geo_id = %s GROUP BY year ORDER BY year
        """,
        (lodes.KENT,),
    )
    # 2023 has the five measures; 2008 has no origin-destination file.
    assert served == [(2008, 2), (2023, 5)]
    # The view serves every computed row whose geography the reference holds
    # (New Castle is not seeded here, so its outbound row stays unserved).
    assert _rows(
        factory, "SELECT COUNT(*) FROM gold_census_lodes.observation_revision"
    ) == _rows(
        factory,
        """
        SELECT COUNT(*) FROM gold_census_lodes.observation_fact AS fact
        JOIN silver_ref.dim_geo_entity AS entity USING (geo_id)
        """,
    )

    client = lodes.FixtureClient()
    again = _load(factory, client)
    assert client.calls == []
    assert [(y["year"], y["states"]) for y in again["years"]] == [
        (2023, {"already_loaded": 1}),
        (2008, {"already_loaded": 1}),
    ]
    assert again["published_slices"] == 0 and again["served_rows_written"] == 0


def test_a_failed_year_publishes_nothing_and_the_rerun_retries_only_it(
    lodes_warehouse,
) -> None:
    """Covers: ETL-108 — one bad file holds the publish; the rerun fetches only that year."""
    factory = lodes_warehouse
    corrupt = lodes.FixtureClient(
        {
            "de_wac_S000_JT00_2008.csv.gz": lodes.fixture_bytes(
                "de_rac_S000_JT00_2008.csv.gz"
            )
        }
    )
    summary = _load(factory, corrupt)
    assert summary["published"] is False
    assert [(f["state"], f["year"], f["status"]) for f in summary["failed"]] == [
        ("de", 2008, "failed")
    ]
    assert _rows(
        factory,
        "SELECT year, status FROM control.census_lodes_slice ORDER BY year",
    ) == [(2023, "silver_ready")]
    assert _rows(
        factory, "SELECT COUNT(*) FROM gold_census_lodes.observation_fact"
    ) == [(0,)]

    client = lodes.FixtureClient()
    rerun = _load(factory, client)
    assert rerun["published"] is True and rerun["published_slices"] == 2
    assert all(
        call.endswith(("2008.csv.gz", ".txt", ".sha256sum")) for call in client.calls
    )
    assert _rows(
        factory,
        "SELECT year, status FROM control.census_lodes_slice ORDER BY year",
    ) == [(2008, "published"), (2023, "published")]


def test_a_capture_whose_replay_never_finished_is_replayed_not_refetched(
    lodes_warehouse,
) -> None:
    """Covers: ETL-108 — files already captured are replayed from the capture."""
    factory = lodes_warehouse
    capture_state_year(factory, "de", 2023, config=CONFIG, client=lodes.FixtureClient())
    client = lodes.FixtureClient()
    summary = _load(factory, client, years=[2023])
    assert client.calls == []
    assert summary["published"] is True and summary["published_slices"] == 1
    assert _rows(
        factory,
        "SELECT status, COUNT(*) FROM control.census_lodes_slice GROUP BY status",
    ) == [("published", 1)]


def test_a_second_history_load_refuses_while_one_runs(lodes_warehouse) -> None:
    """Covers: ETL-108 — one history load at a time; the second refuses, it does not wait."""
    factory = lodes_warehouse
    holder = factory()
    holder.autocommit = True
    try:
        with holder.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
        client = lodes.FixtureClient()
        with pytest.raises(HistoryLoadBusy):
            _load(factory, client)
        assert client.calls == []
    finally:
        holder.close()


def test_history_quality_rules_pass_then_catch_a_gap(lodes_warehouse) -> None:
    """Covers: ETL-108 — DQ-LODES-005, -006 and -007 pass a full load and name what breaks."""
    factory = lodes_warehouse
    assert _outcome(factory, lodes_served_sums).result == "not_applicable"
    _load(factory)
    assert _outcome(factory, lodes_served_sums).result == "pass"
    # Delaware is the only state loaded: every other registered state is
    # missing from both years.
    coverage = _outcome(factory, lodes_year_coverage)
    assert coverage.result == "warn" and coverage.observed_count == 100
    # 2008 and 2023 are not consecutive, so no step is measured.
    assert _outcome(factory, lodes_state_jobs_continuity).result == "pass"

    _execute(
        factory,
        """
        UPDATE gold_census_lodes.observation_fact SET year = 2022, value = value * 3
        WHERE year = 2008 AND geo_type = 'state' AND metric_key = 'jobs'
        """,
    )
    step = _outcome(factory, lodes_state_jobs_continuity)
    assert step.result == "warn" and step.observed_count == 1

    _execute(
        factory,
        """
        DELETE FROM gold_census_lodes.observation_fact
        WHERE run_id = (SELECT run_id FROM control.census_lodes_slice WHERE year = 2023)
        """,
    )
    lost = _outcome(factory, lodes_served_sums)
    assert lost.result == "fail" and lost.observed_count == 1
