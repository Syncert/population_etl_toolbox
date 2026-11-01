"""Load the LODES history, every registered year, one state-year at a time.

Run inside the Airflow image on the internal stack::

    docker exec docker-airflow-scheduler-1 \\
        python -m data_ingestion_toolbox.census_lodes.history --years 2023-2002

The monthly DAG keeps only the newest year current; this command fills the
years before it (build-out W-9). What a run does:

1. Plans every (state, year) asked for. A state-year that already has a
   replayed (``silver_ready``) or ``published`` slice is skipped without a
   request, so a run that stopped part way resumes where it stopped and a
   rerun fetches nothing. ``--refresh`` asks the Bureau again; the capture
   then fetches data files only when the state's vintage is new.
2. Captures and replays each pending state-year through the same capture
   and replay code the DAG runs (checksums verified, blocks summed to
   counties, one transaction per state-year), ``--workers`` at a time, each
   worker with its own connection. A state-year that fails or is
   quarantined is recorded and the run carries on with the rest.
3. Publishes once every requested state-year is in: the replayed slices
   turn ``published`` and their county and state sums are computed into
   ``gold_census_lodes.observation_fact`` in one transaction. Until then
   the site keeps serving what it served. A run with any failed state-year
   publishes nothing and exits 1; rerun it and only the failures are
   retried.

Only one history load runs at a time (a Postgres advisory lock); a second
refuses to start rather than wait.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from typing import Any
from uuid import UUID

from .capture import capture_state_year
from .config import LodesConfig
from .registry import STATES, YEARS
from .silver_census_lodes.load import refresh_observation_fact, replay_run

logger = logging.getLogger(__name__)

LOCK_KEY = 0x4C4F4445  # "LODE": one LODES history load at a time.
#: Slice statuses that mean a state-year needs no further work.
DONE_STATUSES = ("silver_ready", "published")


class HistoryLoadBusy(RuntimeError):
    """Another history load holds the lock."""


@dataclass
class SliceResult:
    state: str
    year: int
    status: str
    run_id: str | None = None
    rows: int = 0
    seconds: float = 0.0
    error: str | None = None


def parse_years(text: str) -> list[int]:
    """``2023-2002`` or ``2010,2011`` -> years in the order given, registered only."""
    years: list[int] = []
    for part in text.split(","):
        part = part.strip()
        if "-" in part:
            first, last = (int(value) for value in part.split("-", 1))
            step = -1 if last < first else 1
            years.extend(range(first, last + step, step))
        elif part:
            years.append(int(part))
    unregistered = sorted(set(years) - set(YEARS))
    if unregistered:
        raise ValueError(
            f"not registered LODES years: {unregistered} "
            f"(registered {min(YEARS)}-{max(YEARS)})"
        )
    return list(dict.fromkeys(years))


def parse_states(text: str | None) -> list[str]:
    if not text:
        return [state for state, _fips in STATES]
    states = [part.strip().lower() for part in text.split(",") if part.strip()]
    known = {state for state, _fips in STATES}
    unknown = sorted(set(states) - known)
    if unknown:
        raise ValueError(f"not registered LODES states: {unknown}")
    return states


def _done(
    connection_factory: Callable[[], Any], years: Sequence[int]
) -> set[tuple[str, int]]:
    """State-years that already have a replayed or published slice."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT state, year FROM control.census_lodes_slice
                WHERE year = ANY(%s) AND status = ANY(%s)
                """,
                (list(years), list(DONE_STATUSES)),
            )
            return {(state, int(year)) for state, year in cursor.fetchall()}
    finally:
        connection.close()


def _captured(
    connection_factory: Callable[[], Any], years: Sequence[int]
) -> dict[tuple[str, int], str]:
    """State-years whose files are captured but whose replay never finished."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (state, year) state, year, run_id::TEXT
                FROM control.census_lodes_slice
                WHERE year = ANY(%s) AND status = 'captured'
                ORDER BY state, year, created_at DESC
                """,
                (list(years),),
            )
            return {
                (state, int(year)): run_id for state, year, run_id in cursor.fetchall()
            }
    finally:
        connection.close()


def load_state_year(
    connection_factory: Callable[[], Any],
    state: str,
    year: int,
    *,
    config: LodesConfig | None = None,
    client: Any | None = None,
    captured_run_id: str | None = None,
) -> SliceResult:
    """Capture and replay one state-year; never raises for a source failure.

    Given the run of an earlier capture whose replay never finished, it
    replays that capture instead of downloading the files again.
    """
    started = time.monotonic()
    try:
        if captured_run_id is not None:
            run_id, status = UUID(captured_run_id), "captured"
        else:
            run_id, status = capture_state_year(
                connection_factory, state, year, config=config, client=client
            )
        rows = replay_run(connection_factory, run_id=UUID(str(run_id)))
        if status == "captured":
            status = _slice_status(connection_factory, run_id)
        return SliceResult(
            state,
            year,
            status,
            run_id=str(run_id),
            rows=rows,
            seconds=round(time.monotonic() - started, 1),
        )
    except Exception as error:  # noqa: BLE001 - recorded, the run carries on
        logger.warning("LODES %s %s failed: %s", state, year, error)
        return SliceResult(
            state,
            year,
            "failed",
            seconds=round(time.monotonic() - started, 1),
            error=repr(error),
        )


def _slice_status(connection_factory: Callable[[], Any], run_id: UUID) -> str:
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT status FROM control.census_lodes_slice WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            return row[0] if row else "failed"
    finally:
        connection.close()


# -- worker processes ----------------------------------------------------------
# A worker builds its own connection factory once, from a picklable callable,
# and keeps it for every state-year it is handed: one connection source per
# worker, never one shared across processes.
_WORKER_FACTORY: Callable[[], Any] | None = None


def _init_worker(factory_builder: Callable[[], Callable[[], Any]]) -> None:
    global _WORKER_FACTORY
    logging.basicConfig(level=logging.INFO)
    _WORKER_FACTORY = factory_builder()


def _work(state: str, year: int, captured_run_id: str | None) -> SliceResult:
    assert _WORKER_FACTORY is not None, "worker was not initialised"
    return load_state_year(
        _WORKER_FACTORY, state, year, captured_run_id=captured_run_id
    )


def airflow_connection_factory() -> Callable[[], Any]:
    """The warehouse connection the DAG uses, for a worker process."""
    from .schema import _get_hook

    return _get_hook().get_conn


def _publish(
    connection_factory: Callable[[], Any], years: Sequence[int], states: Sequence[str]
) -> tuple[int, int]:
    """Publish every replayed slice of these state-years and compute its sums."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.census_lodes_slice
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE status = 'silver_ready' AND year = ANY(%s) AND state = ANY(%s)
                RETURNING run_id
                """,
                (list(years), list(states)),
            )
            run_ids = [UUID(str(row[0])) for row in cursor.fetchall()]
            written = refresh_observation_fact(cursor, run_ids) if run_ids else 0
            # Any published state-year still without served sums (published
            # before the table existed, or by an interrupted refresh).
            written += refresh_observation_fact(cursor, None)
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return len(run_ids), written


def load_history(
    connection_factory: Callable[[], Any],
    years: Sequence[int],
    *,
    states: Iterable[str] | None = None,
    workers: int = 1,
    worker_factory: Callable[[], Callable[[], Any]] | None = None,
    config: LodesConfig | None = None,
    client: Any | None = None,
    refresh: bool = False,
    publish: bool = True,
) -> dict[str, Any]:
    """Capture, replay and publish the requested years; resumable and idempotent.

    ``workers`` above 1 runs state-years in worker processes, each building
    its connections with ``worker_factory`` (required then). With one
    worker everything runs here, with ``client`` and ``config`` if given.
    """
    state_list = list(states) if states is not None else parse_states(None)
    lock = connection_factory()
    lock.autocommit = True
    try:
        with lock.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,))
            if not cursor.fetchone()[0]:
                raise HistoryLoadBusy(
                    "another LODES history load is running; wait for it to finish"
                )
        done = set() if refresh else _done(connection_factory, years)
        captured = {} if refresh else _captured(connection_factory, years)
        pending = [
            (state, year)
            for year in years
            for state in state_list
            if (state, year) not in done
        ]
        skipped = [
            SliceResult(state, year, "already_loaded")
            for year in years
            for state in state_list
            if (state, year) in done
        ]
        logger.info(
            "LODES history: %s state-years to load, %s already loaded",
            len(pending),
            len(skipped),
        )
        results: list[SliceResult] = []
        if workers > 1 and pending:
            if worker_factory is None:
                raise ValueError("workers > 1 needs a worker_factory")
            with ProcessPoolExecutor(
                max_workers=workers,
                initializer=_init_worker,
                initargs=(worker_factory,),
            ) as pool:
                futures = [
                    pool.submit(_work, state, year, captured.get((state, year)))
                    for state, year in pending
                ]
                for future in as_completed(futures):
                    result = future.result()
                    _log(result, len(results) + 1, len(pending))
                    results.append(result)
        else:
            for state, year in pending:
                result = load_state_year(
                    connection_factory,
                    state,
                    year,
                    config=config,
                    client=client,
                    captured_run_id=captured.get((state, year)),
                )
                _log(result, len(results) + 1, len(pending))
                results.append(result)

        failed = [r for r in results if r.status not in ("silver_ready", "unchanged")]
        published_slices = written = 0
        published = False
        if publish and not failed:
            published_slices, written = _publish(connection_factory, years, state_list)
            published = True
    finally:
        lock.close()
    if published and published_slices:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_census_lodes"
        )
    every = sorted(results + skipped, key=lambda r: (-r.year, r.state))
    return {
        "published": published,
        "published_slices": published_slices,
        "served_rows_written": written,
        "years": _by_year(every),
        "failed": [asdict(r) for r in sorted(failed, key=lambda r: (-r.year, r.state))],
    }


def _log(result: SliceResult, index: int, total: int) -> None:
    logger.info(
        "LODES %s %s: %s (%s rows, %.1f s) [%s/%s]",
        result.state,
        result.year,
        result.status,
        result.rows,
        result.seconds,
        index,
        total,
    )


def _by_year(results: Sequence[SliceResult]) -> list[dict[str, Any]]:
    summary: dict[int, dict[str, Any]] = {}
    for result in results:
        entry = summary.setdefault(
            result.year, {"year": result.year, "rows": 0, "seconds": 0.0, "states": {}}
        )
        entry["states"][result.status] = entry["states"].get(result.status, 0) + 1
        entry["rows"] += result.rows
        entry["seconds"] = round(entry["seconds"] + result.seconds, 1)
    return [summary[year] for year in sorted(summary, reverse=True)]


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Load the LEHD LODES history.")
    parser.add_argument(
        "--years",
        default=f"{max(YEARS)}-{min(YEARS)}",
        help=f"e.g. 2022-2002 or 2010,2011 (default {max(YEARS)}-{min(YEARS)})",
    )
    parser.add_argument(
        "--states", default=None, help="e.g. de,wi (default every registered state)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        help="state-years captured and replayed at once (default 6)",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="ask the Bureau again even for state-years already loaded",
    )
    parser.add_argument(
        "--no-publish", action="store_true", help="load without publishing"
    )
    parser.add_argument(
        "--plan",
        action="store_true",
        help="print what would load and exit without fetching anything",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    years = parse_years(args.years)
    states = parse_states(args.states)

    from .schema import _get_hook, ensure_census_lodes_schema

    hook = _get_hook()
    if args.plan:
        done = set() if args.refresh else _done(hook.get_conn, years)
        plan = [
            {
                "year": year,
                "to_load": sorted(s for s in states if (s, year) not in done),
                "already_loaded": sum(1 for s in states if (s, year) in done),
            }
            for year in years
        ]
        print(json.dumps(plan, indent=2))
        return 0
    ensure_census_lodes_schema(hook)
    try:
        summary = load_history(
            hook.get_conn,
            years,
            states=states,
            workers=max(1, args.workers),
            worker_factory=airflow_connection_factory,
            refresh=args.refresh,
            publish=not args.no_publish,
        )
    except HistoryLoadBusy as error:
        print(str(error))
        return 2
    print(json.dumps(summary, indent=2, default=str))
    # Exit non-zero so the operator reruns; loaded state-years are skipped then.
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
