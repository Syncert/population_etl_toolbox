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


def _served(factory, relation="observation_latest"):
    return _rows(
        factory,
        f"""
        SELECT year, metric_key FROM gold_census_lodes.{relation}
        WHERE geo_id = %s ORDER BY year, metric_key
        """,
        (lodes.KENT,),
    )


def test_a_state_year_lehd_did_not_cover_is_not_served(lodes_warehouse) -> None:
    """Covers: ETL-108 — a file holding a handful of jobs is a coverage gap; the measures it feeds are not served."""
    factory = lodes_warehouse
    _load(factory)
    assert _rows(factory, "SELECT COUNT(*) FROM gold_census_lodes.coverage_gap") == [
        (0,)
    ]
    computed = _rows(factory, "SELECT COUNT(*) FROM gold_census_lodes.observation_fact")
    state_totals = _rows(
        factory,
        """
        SELECT run_id, metric_key, value FROM gold_census_lodes.observation_fact
        WHERE geo_type = 'state'
        """,
    )
    everything = _served(factory)
    assert everything == [
        (2008, "jobs"),
        (2008, "resident_workers"),
        (2023, "inbound"),
        (2023, "jobs"),
        (2023, "live_and_work"),
        (2023, "outbound_in_state"),
        (2023, "resident_workers"),
    ]

    # Delaware's 2008 workplace file covers a dozen jobs, as Massachusetts'
    # did before LEHD covered it: the refresh names the file's gap and serves
    # no 2008 jobs, state or county, while the residence counts stay.
    _execute(
        factory,
        """
        UPDATE gold_census_lodes.observation_fact SET value = 13
        WHERE year = 2008 AND geo_type = 'state' AND metric_key = 'jobs'
        """,
    )
    _execute(factory, "SELECT gold_census_lodes.refresh_coverage_gap()")
    gaps = _rows(
        factory,
        "SELECT year, metric_key, state_value FROM gold_census_lodes.coverage_gap",
    )
    assert gaps == [(2008, "jobs", 13)]
    for relation in ("observation_revision", "observation_latest"):
        assert _served(factory, relation) == [
            row for row in everything if row != (2008, "jobs")
        ]

    # A residence file that covers a sliver of its workers (Michigan 2022-2023)
    # hides the residence counts and the commuting measures that pair a home
    # with a workplace, never the jobs located there.
    _execute(
        factory,
        """
        UPDATE gold_census_lodes.observation_fact SET value = 13
        WHERE year = 2023 AND geo_type = 'state' AND metric_key = 'resident_workers'
        """,
    )
    _execute(factory, "SELECT gold_census_lodes.refresh_coverage_gap()")
    assert _rows(
        factory,
        "SELECT year, metric_key FROM gold_census_lodes.coverage_gap ORDER BY 1",
    ) == [(2008, "jobs"), (2023, "resident_workers")]
    assert _served(factory) == [(2008, "resident_workers"), (2023, "jobs")]

    # The sums stay computed, so DQ-LODES-005 still sees every state-year.
    assert (
        _rows(factory, "SELECT COUNT(*) FROM gold_census_lodes.observation_fact")
        == computed
    )
    assert _outcome(factory, lodes_served_sums).result == "pass"

    # A jump out of a year whose jobs were not covered is not a step to explain.
    _execute(
        factory,
        "UPDATE gold_census_lodes.observation_fact SET year = 2022 WHERE year = 2008",
    )
    assert _outcome(factory, lodes_state_jobs_continuity).result == "pass"

    # A refresh after the files are complete serves every measure again.
    for run_id, metric_key, value in state_totals:
        _execute(
            factory,
            """
            UPDATE gold_census_lodes.observation_fact SET value = %s
            WHERE run_id = %s AND metric_key = %s AND geo_type = 'state'
            """,
            (value, run_id, metric_key),
        )
    _execute(factory, "SELECT gold_census_lodes.refresh_coverage_gap()")
    assert _rows(factory, "SELECT COUNT(*) FROM gold_census_lodes.coverage_gap") == [
        (0,)
    ]
    assert _served(factory) == [
        (2022 if year == 2008 else year, metric) for year, metric in everything
    ]


def test_a_capture_stopped_part_way_is_withdrawn_and_captured_again(
    lodes_warehouse,
) -> None:
    """Covers: ETL-108 — a killed capture is never replayed with files missing."""
    factory = lodes_warehouse
    stopped, _status = capture_state_year(
        factory, "de", 2023, config=CONFIG, client=lodes.FixtureClient()
    )
    # The worker was killed after two of the four files.
    _execute(
        factory,
        "DELETE FROM control.census_lodes_file WHERE run_id = %s AND family LIKE 'od_%%'",
        (str(stopped),),
    )
    client = lodes.FixtureClient()
    summary = _load(factory, client, years=[2023])
    assert summary["published"] is True and summary["stuck"] == []
    assert "de_od_main_JT00_2023.csv.gz" in client.calls
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM control.census_lodes_slice WHERE run_id = %s",
        (str(stopped),),
    ) == [(0,)]
    assert _rows(
        factory,
        "SELECT status FROM control.ingestion_run WHERE run_id = %s",
        (str(stopped),),
    ) == [("failed",)]
    measures = {
        row[0]
        for row in _rows(
            factory,
            "SELECT metric_key FROM gold_census_lodes.observation_latest WHERE geo_id = %s",
            (lodes.KENT,),
        )
    }
    assert "live_and_work" in measures
