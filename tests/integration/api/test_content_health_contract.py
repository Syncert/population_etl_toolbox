"""The content report reads the warehouse relations it names.

Covers: API-137 — ``/api/v1/health/content`` answers from the real glossary
        catalog and publication state, counts the freshness vocabulary the
        warehouse's own CHECK constraint declares, and names a registered
        source the catalog publishes nothing for.

The unit tier proves the grading rule; nothing there proves the query. A
statement naming ``gold_glossary.dim_metric_catalog``, its ``freshness_state``
words, and ``gold_glossary.publisher_harvest_state.last_publication_time`` is
only right against relations that actually carry those columns -- and this
resource exists precisely so an operator can trust what it says about an
empty deployment. A report that failed to read the warehouse and answered
"empty" would be indistinguishable from the condition it is built to detect.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.registry import OBSERVATION_DISPATCH
from apps.api.services.content_health import DEGRADED, EMPTY, SERVING
from apps.api.services.metric_freshness import (
    FRESHNESS_RETIRED,
    PUBLISHED_FRESHNESS_STATES,
)
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

CONTENT_PATH = "/api/v1/health/content"

#: The source the fixtures publish into. FRED is identified by a metric-code
#: column and publishes no geography dimension, so a catalog row for it
#: disturbs nothing else this tier asserts.
FIXTURE_SOURCE = "FRED"

CATALOG_INSERT = """
    INSERT INTO gold_glossary.dim_metric_catalog (
        metric_code, metric_display_name, source_code,
        source_object_type, source_object_key,
        valid_geo_grains, valid_time_grains, freshness_state
    ) VALUES (
        %s, 'Content health fixture', %s, 'FRED_SERIES', %s,
        ARRAY['NATIONAL'], ARRAY['MONTHLY'], %s
    )
"""


def _read_report(settings: PostgresTestConfig) -> dict:
    """One reading of the resource, over a session on the real warehouse."""
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": settings.host,
            "port": settings.port,
            "user": settings.user,
            "password": settings.password,
            "dbname": settings.database,
        },
        pool_pre_ping=True,
    )

    def override_db() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_db_session_dep] = override_db
    try:
        response = TestClient(app).get(CONTENT_PATH)
        assert response.status_code == 200, response.text
        return response.json()
    finally:
        app.dependency_overrides.clear()
        engine.dispose()


@pytest.fixture
def settings() -> PostgresTestConfig:
    configured = PostgresTestConfig.from_environment()
    assert configured is not None
    return configured


@pytest.fixture
def one_measure_per_state(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[list[str]]:
    """One ``current``, one ``stale`` and one ``retired`` measure for FRED.

    The three states come from the module the content query binds them from,
    so this fixture cannot drift from the vocabulary being counted.
    """
    token = uuid4().hex[:12].upper()
    codes = [
        f"{FIXTURE_SOURCE}:CONTENT_{token}_{state.upper()}"
        for state in PUBLISHED_FRESHNESS_STATES
    ]

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            for metric_code, state in zip(codes, PUBLISHED_FRESHNESS_STATES):
                cursor.execute(
                    CATALOG_INSERT, (metric_code, FIXTURE_SOURCE, metric_code, state)
                )
        writer.commit()
    finally:
        writer.close()

    try:
        yield codes
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog "
                    "WHERE metric_code = ANY(%s)",
                    (codes,),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def _source(payload: dict, source_code: str) -> dict:
    for entry in payload["sources"]:
        if entry["source_code"] == source_code:
            return entry
    raise AssertionError(
        f"{source_code} is absent from the content report: "
        f"{[entry['source_code'] for entry in payload['sources']]}"
    )


def test_the_report_counts_the_warehouses_own_freshness_vocabulary(
    one_measure_per_state: list[str], settings: PostgresTestConfig
) -> None:
    """Covers: API-137 — the counted states are the states the catalog stores.

    A shared warehouse may carry more measures than this fixture seeded, so
    the per-state counts are lower bounds. ``counts_are_complete`` is not: it
    says every measure the relation holds for this source fell into one of
    the three counted buckets, which is the claim that would break first if
    the warehouse gained a fourth state.
    """
    payload = _read_report(settings)
    fred = _source(payload, FIXTURE_SOURCE)

    assert fred["metrics_current"] >= 1
    assert fred["metrics_stale"] >= 1
    assert fred["metrics_retired"] >= 1
    assert fred["status"] == SERVING
    assert fred["registered"] is True

    assert fred["counts_are_complete"] is True, fred
    assert (
        fred["metrics_current"] + fred["metrics_stale"] + fred["metrics_retired"]
        == fred["metrics_total"]
    )

    # This fixture writes catalog rows directly rather than running a harvest,
    # so the source has no `publisher_harvest_state` row and its publication
    # time is legitimately null -- which is the documented answer for a source
    # the catalog holds no publication row for. The field's *shape* is asserted
    # by the test below, which writes one.
    assert fred["last_publication_time"] is None


@pytest.fixture
def a_recorded_publication_time(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[datetime]:
    """Record one publication time for the fixture source, and restore what was there.

    This tier may run against a warehouse a real harvest has already written,
    so the teardown puts the previous value back rather than deleting the row:
    a fixture that removed a publication state it did not create would leave
    the next reader's source looking as though it had never published.
    """
    published = datetime(2026, 9, 1, 4, 11, 22, tzinfo=timezone.utc)
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "SELECT last_publication_time FROM "
                "gold_glossary.publisher_harvest_state WHERE source_code = %s",
                (FIXTURE_SOURCE,),
            )
            existing = cursor.fetchone()
            cursor.execute(
                """
                INSERT INTO gold_glossary.publisher_harvest_state (
                    source_code, publisher_contract_version,
                    last_publication_time, status
                ) VALUES (%s, '1.0', %s, 'success')
                ON CONFLICT (source_code) DO UPDATE
                    SET last_publication_time = EXCLUDED.last_publication_time
                """,
                (FIXTURE_SOURCE, published),
            )
        writer.commit()
    finally:
        writer.close()

    try:
        yield published
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                if existing is None:
                    cleanup_sql = (
                        "DELETE FROM gold_glossary.publisher_harvest_state "
                        "WHERE source_code = %s"
                    )
                    cursor.execute(cleanup_sql, (FIXTURE_SOURCE,))
                else:
                    cursor.execute(
                        "UPDATE gold_glossary.publisher_harvest_state "
                        "SET last_publication_time = %s WHERE source_code = %s",
                        (existing[0], FIXTURE_SOURCE),
                    )
            cleanup.commit()
        finally:
            cleanup.close()


def test_a_publication_time_is_served_as_the_guide_documents_it(
    one_measure_per_state: list[str],
    a_recorded_publication_time: datetime,
    settings: PostgresTestConfig,
) -> None:
    """Covers: API-137 — the served field is the ISO-8601 the guide promises.

    The unit tier proves the conversion from a datetime a test constructed.
    Only a warehouse can prove the value reaching it *is* a datetime: the
    statement used to cast the column with ``::TEXT``, so by the time any
    Python saw it the value was already a string in Postgres's own rendering
    -- ``2026-09-10 20:27:49.130325+00``, a space where the ``T`` belongs --
    and every layer above passed it through unchanged. A test that built its
    own datetime would have gone on passing throughout.

    So this reads back a real ``TIMESTAMPTZ`` through the whole stack and
    checks it against the instant written, offset included.
    """
    payload = _read_report(settings)
    served = _source(payload, FIXTURE_SOURCE)["last_publication_time"]

    assert served is not None, (
        "a publication time was recorded for this source and the report served "
        "none, so the resource is not reading the relation it documents"
    )
    assert datetime.fromisoformat(served) == a_recorded_publication_time, (
        f"last_publication_time is {served!r}, which is not the offset-aware "
        "ISO-8601 API_CONSUMER_GUIDE.md documents"
    )


def test_every_registered_source_is_reported_whatever_the_catalog_holds(
    one_measure_per_state: list[str], settings: PostgresTestConfig
) -> None:
    """Covers: API-137 — a source with no catalog row still appears, as empty.

    A grouped scan reports the sources that have rows. The sources that have
    none are the ones an operator most needs named, and on a warehouse seeded
    only by this tier that is most of them.
    """
    payload = _read_report(settings)

    reported = {
        entry["source_code"] for entry in payload["sources"] if entry["registered"]
    }
    assert reported == set(OBSERVATION_DISPATCH), (
        "the report must name every source the observation registry declares; "
        f"missing {sorted(set(OBSERVATION_DISPATCH) - reported)}"
    )

    # The summary list is what an operator reads, so it must agree with the
    # rows it is derived from, in both directions.
    for entry in payload["sources"]:
        if not entry["registered"]:
            continue
        silent = entry["source_code"] in payload["silent_sources"]
        assert silent is (entry["status"] == EMPTY), entry
        assert silent is (entry["metrics_current"] == 0), entry


def test_a_source_whose_measures_have_all_retired_is_reported_empty(
    postgres_connection_factory: Callable[[], connection],
    settings: PostgresTestConfig,
) -> None:
    """Covers: API-137 — the failure that looks exactly like a healthy source.

    BLS carries 13,261 retired codes against 63 active ones. A source whose
    active measures all retire keeps a large catalog and answers no
    observation request, and a deployment reporting it healthy is the state
    this resource exists to correct. Proved end to end rather than reasoned
    about: the query's ``FILTER`` clauses are what decide the counts, and the
    counts are what decide the word.

    Every pre-existing measure for the source is retired for the duration and
    restored afterwards, so no other row can quietly supply the ``current``
    measure that would make this pass.
    """
    token = uuid4().hex[:12].upper()
    retired_code = f"{FIXTURE_SOURCE}:CONTENT_{token}_ONLY_RETIRED"
    writer = postgres_connection_factory()

    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "SELECT metric_code, freshness_state "
                "FROM gold_glossary.dim_metric_catalog WHERE source_code = %s",
                (FIXTURE_SOURCE,),
            )
            saved = cursor.fetchall()
            cursor.execute(
                CATALOG_INSERT,
                (retired_code, FIXTURE_SOURCE, retired_code, FRESHNESS_RETIRED),
            )
            cursor.execute(
                "UPDATE gold_glossary.dim_metric_catalog SET freshness_state = %s "
                "WHERE source_code = %s",
                (FRESHNESS_RETIRED, FIXTURE_SOURCE),
            )
        writer.commit()

        payload = _read_report(settings)
        fred = _source(payload, FIXTURE_SOURCE)

        assert fred["metrics_total"] >= 1
        assert fred["metrics_retired"] == fred["metrics_total"]
        assert fred["metrics_current"] == 0
        # A full catalog, and nothing a client can ask for.
        assert fred["status"] == EMPTY
        assert FIXTURE_SOURCE in payload["silent_sources"]
        assert payload["status"] in {EMPTY, DEGRADED}
    finally:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code = %s",
                (retired_code,),
            )
            for metric_code, state in saved:
                cursor.execute(
                    "UPDATE gold_glossary.dim_metric_catalog "
                    "SET freshness_state = %s WHERE metric_code = %s",
                    (state, metric_code),
                )
        writer.commit()
        writer.close()


def test_a_drifting_source_is_named_from_the_warehouses_own_state(
    one_measure_per_state: list[str], settings: PostgresTestConfig
) -> None:
    """Covers: API-137 — `stale_sources` reads the stored state, not a guess.

    The fixture seeds one measure in each of the three states, so the source
    carries a stale measure and a current one at the same time. That is the
    condition the warning exists for: the source answers rows, nothing about
    the served answer looks wrong, and a publisher has already stopped
    emitting one of its measures.
    """
    payload = _read_report(settings)
    fred = _source(payload, FIXTURE_SOURCE)

    assert fred["metrics_stale"] >= 1
    assert FIXTURE_SOURCE in payload["stale_sources"], payload["stale_sources"]
    # Still serving, and therefore not silent: the two lists answer different
    # questions and a drifting source must not be reported as an outage.
    assert fred["status"] == SERVING
    assert FIXTURE_SOURCE not in payload["silent_sources"]

    # Every name in the list is a registered source that really carries one,
    # in both directions, so the summary cannot drift from the rows.
    for entry in payload["sources"]:
        if not entry["registered"]:
            continue
        drifting = entry["source_code"] in payload["stale_sources"]
        assert drifting is (entry["metrics_stale"] > 0), entry


def test_the_catalog_refuses_a_state_outside_the_counted_vocabulary(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: API-137 — counting exactly three states is a complete tally.

    The content query has one ``FILTER`` clause per word in
    ``PUBLISHED_FRESHNESS_STATES``. That is only a whole count of the catalog
    while the catalog cannot hold a fourth word, so the constraint is asked
    rather than assumed: a warehouse that started accepting one would make
    every per-source total silently partial.
    """
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute("SAVEPOINT vocabulary_probe")
            try:
                cursor.execute(
                    CATALOG_INSERT,
                    (
                        f"{FIXTURE_SOURCE}:CONTENT_VOCABULARY_PROBE",
                        FIXTURE_SOURCE,
                        "probe",
                        "not_a_declared_state",
                    ),
                )
            except Exception:
                refused = True
            else:
                refused = False
            cursor.execute("ROLLBACK TO SAVEPOINT vocabulary_probe")

        assert refused, (
            "the catalog accepted a freshness state outside "
            f"{list(PUBLISHED_FRESHNESS_STATES)}; every count built from those "
            "words is now a partial tally of the catalog"
        )
    finally:
        writer.rollback()
        writer.close()
