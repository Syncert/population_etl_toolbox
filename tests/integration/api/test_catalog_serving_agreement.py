"""A metric code taken from the catalog is answerable by the API.

The catalog at ``/api/v1/catalog/metrics`` is the documented discovery surface
for metric codes. Until this module existed nothing crossed it: the API's own
tests built their fixtures from one side or the other, and the web explorer
reads metrics and observations from the same endpoint family without ever
round-tripping a catalog code. Census ACS published 4,447 catalog codes that
the serving layer had never heard of, and nothing in the stack reported it.

These checks are deliberately source-agnostic. They read the reviewed
observation-dispatch registry rather than a source list, so a fifth source
cannot reintroduce the defect, and they go through the published HTTP surfaces
rather than the services behind them, because a consumer only ever has those.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.registry import OBSERVATION_DISPATCH
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

#: Bounds the sweep below. The development warehouse publishes tens of
#: thousands of catalog rows, and one HTTP round trip per row would turn a
#: contract check into a load test. The sample is the source's first codes by
#: ``metric_code``, so it is deterministic rather than lucky.
SWEEP_SAMPLE = 15

ACS_TABLE = "B99997"
ACS_DATASET = "acs5"
ACS_VINTAGE = 2093
ACS_PERIOD = "2093-01-01"


def _seed_time(database_cursor, time_sk: int, value: str) -> None:
    database_cursor.execute(
        """
        INSERT INTO silver_ref.dim_time (
            time_sk, date_key, year, quarter, month, day, day_of_week,
            day_name, month_name, week_of_year, is_weekend,
            is_month_start, is_month_end, is_quarter_start,
            is_quarter_end, is_year_start, is_year_end, ingested_at
        ) VALUES (
            %s, %s, EXTRACT(YEAR FROM %s::DATE), EXTRACT(QUARTER FROM %s::DATE),
            EXTRACT(MONTH FROM %s::DATE), EXTRACT(DAY FROM %s::DATE), 4,
            'Thursday', TO_CHAR(%s::DATE, 'Month'), 1, FALSE,
            TRUE, FALSE, TRUE, FALSE, TRUE, FALSE, NOW()
        ) ON CONFLICT (time_sk) DO NOTHING
        """,
        (time_sk, value, value, value, value, value, value),
    )


#: Relations the glossary harvest writes for the source it registers, beyond
#: the catalog rows themselves. Order matters for the foreign keys.
_REGISTRATION_RELATIONS = (
    "control.publisher_ready_event",
    "gold_glossary.publisher_harvest_state",
    "gold_glossary.publisher_registry",
    "gold_glossary.dim_source_system",
)


def _registration_state(
    factory: Callable[[], connection],
) -> dict[str, bool]:
    """Which registration rows for CENSUS_ACS already exist before the harvest."""
    reader = factory()
    try:
        with reader.cursor() as database_cursor:
            state = {}
            for relation in _REGISTRATION_RELATIONS:
                database_cursor.execute(
                    f"SELECT EXISTS (SELECT 1 FROM {relation} "
                    "WHERE source_code = 'CENSUS_ACS')"
                )
                state[relation] = bool(database_cursor.fetchone()[0])
            return state
    finally:
        reader.close()


def _remove_registration(database_cursor, existed_before: dict[str, bool]) -> None:
    """Remove the registration rows the harvest created, and only those."""
    for relation in _REGISTRATION_RELATIONS:
        if existed_before[relation]:
            continue
        if relation == "gold_glossary.dim_source_system":
            # Another suite's catalog rows may still reference the source
            # system; it is only this fixture's to remove when nothing does.
            database_cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM gold_glossary.dim_metric_catalog "
                "WHERE source_code = 'CENSUS_ACS')"
            )
            if database_cursor.fetchone()[0]:
                continue
        database_cursor.execute(
            f"DELETE FROM {relation} WHERE source_code = 'CENSUS_ACS'"
        )


@pytest.fixture
def api_client(
    postgres_test_config: PostgresTestConfig,
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[TestClient]:
    """The real application over the real warehouse."""
    engine = create_engine(
        "postgresql+psycopg2://"
        f"{postgres_test_config.user}:{postgres_test_config.password}"
        f"@{postgres_test_config.host}:{postgres_test_config.port}"
        f"/{postgres_test_config.database}"
    )

    def _session() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_db_session_dep] = _session
    try:
        yield TestClient(app, raise_server_exceptions=False)
    finally:
        app.dependency_overrides.clear()
        engine.dispose()


@pytest.fixture
def published_acs_metric(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Publish one ACS metric the way production does, end to end.

    Silver rows go through the real gold refresh procedures, and the real
    glossary harvest reads ``gold_census.metric_publisher`` into the catalog.
    Neither side is told what the other spelled, which is the whole point: the
    code this fixture yields is the catalog's own, and the serving rows are
    whatever the refresh procedure composed.
    """
    token = uuid4().hex[:8].upper()
    variable_code = f"{ACS_TABLE}_{token}E"
    geo_id = "state:95"

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as database_cursor:
            _seed_time(database_cursor, 20930101, ACS_PERIOD)
            geo_sk = seed_geography(
                database_cursor,
                geo_type="state",
                state_fips="95",
                vintage=ACS_VINTAGE,
                name="Catalog agreement state",
            )
            database_cursor.execute(
                """
                INSERT INTO gold_census.dim_acs_table (
                    dataset_code, vintage_year, table_id, table_title,
                    concept, universe, survey_span_years, reference_url
                ) VALUES (%s, %s, %s, 'Catalog agreement table',
                          'Total population', 'Total population', 5,
                          'https://example.test/acs')
                ON CONFLICT (dataset_code, vintage_year, table_id) DO NOTHING
                RETURNING acs_table_sk
                """,
                (ACS_DATASET, ACS_VINTAGE, ACS_TABLE),
            )
            row = database_cursor.fetchone()
            if row is None:
                database_cursor.execute(
                    """
                    SELECT acs_table_sk FROM gold_census.dim_acs_table
                    WHERE dataset_code = %s AND vintage_year = %s AND table_id = %s
                    """,
                    (ACS_DATASET, ACS_VINTAGE, ACS_TABLE),
                )
                row = database_cursor.fetchone()
            acs_table_sk = row[0]
            database_cursor.execute(
                """
                INSERT INTO gold_census.dim_acs_variable (
                    acs_table_sk, dataset_code, vintage_year, variable_code,
                    variable_label, concept, universe, value_role
                ) VALUES (%s, %s, %s, %s, 'Total population',
                          'Total population', 'Total population', 'ESTIMATE')
                """,
                (acs_table_sk, ACS_DATASET, ACS_VINTAGE, variable_code),
            )
            database_cursor.execute(
                """
                INSERT INTO silver_census.fact_demographics (
                    time_sk, geo_sk, duration_start, duration_end,
                    estimate_year, dataset, table_id, variable_code,
                    geo_level, geo_id, state_fips, estimate_value,
                    margin_of_error, variable_label, load_batch_id
                ) VALUES (20930101, %s, %s, '2093-12-31', %s, %s, %s, %s,
                          'state', %s, '95', 1234, 12,
                          'Total population', gen_random_uuid())
                """,
                (
                    geo_sk,
                    ACS_PERIOD,
                    ACS_VINTAGE,
                    ACS_DATASET,
                    ACS_TABLE,
                    variable_code,
                    geo_id,
                ),
            )
            database_cursor.execute(
                "CALL gold_census.refresh_dashboard_serving_layer_acs(%s, %s, TRUE)",
                (ACS_PERIOD, "2093-12-31"),
            )
        writer.commit()
    finally:
        writer.close()

    # The real harvest registers the source as a side effect: a
    # publisher_registry row, a publisher_harvest_state row, a dim_source_system
    # upsert, and any ready events. On a bootstrapped warehouse none of those
    # exist yet, and a later suite asserts on that emptiness (DB-024), so the
    # cleanup below removes exactly the rows this fixture caused to exist.
    registered_before = _registration_state(postgres_connection_factory)
    harvest_publisher(postgres_connection_factory, Publisher("gold_census"))

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as database_cursor:
            database_cursor.execute(
                """
                SELECT metric_code FROM gold_glossary.dim_metric_catalog
                WHERE source_code = 'CENSUS_ACS' AND source_object_key = %s
                """,
                (f"{ACS_DATASET}:{variable_code}",),
            )
            published = database_cursor.fetchone()
    finally:
        reader.close()
    assert published is not None, "the harvest published no catalog row to check"

    try:
        yield published[0]
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as database_cursor:
                database_cursor.execute(
                    "DELETE FROM gold_census.mv_acs_latest WHERE variable_code = %s",
                    (variable_code,),
                )
                database_cursor.execute(
                    "DELETE FROM gold_census.rpt_acs_observations "
                    "WHERE variable_code = %s",
                    (variable_code,),
                )
                database_cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog "
                    "WHERE source_object_key = %s AND source_code = 'CENSUS_ACS'",
                    (f"{ACS_DATASET}:{variable_code}",),
                )
                database_cursor.execute(
                    "DELETE FROM gold_census.dim_acs_variable WHERE variable_code = %s",
                    (variable_code,),
                )
                database_cursor.execute(
                    "DELETE FROM silver_census.fact_demographics "
                    "WHERE variable_code = %s",
                    (variable_code,),
                )
                database_cursor.execute(
                    "DELETE FROM gold_census.dim_acs_table WHERE table_id = %s",
                    (ACS_TABLE,),
                )
                delete_geography(database_cursor, geo_id)
                database_cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20930101"
                )
                _remove_registration(database_cursor, registered_before)
            cleanup.commit()
        finally:
            cleanup.close()


def _current_catalog_codes(client: TestClient, source_code: str) -> list[str]:
    response = client.get(
        "/api/v1/catalog/metrics",
        params={"source_code": source_code, "limit": 1000},
    )
    assert response.status_code == 200, response.text
    return sorted(
        item["metric_code"]
        for item in response.json()["items"]
        if item.get("freshness_state") == "current"
    )


def _answers(client: TestClient, metric_code: str) -> int:
    response = client.get(
        "/api/v1/observations", params={"metric_code": metric_code, "limit": 1}
    )
    assert response.status_code == 200, f"{metric_code}: {response.text}"
    return int(response.json()["total"])


def test_a_catalog_code_is_answerable_by_the_observation_resource(
    api_client: TestClient, published_acs_metric: str
) -> None:
    """Covers: API-067 — a code read from the catalog answers with rows.

    Covers: ARC-005 — this is the ACS defect end to end. The catalog published
    ``CENSUS_ACS:<dataset>:<variable>`` and the serving relations stored
    ``ACS:<dataset>:<variable>``, so this request answered ``{"total": 0}``
    while the same rows were sitting in ``gold_census.mv_acs_latest``.
    """
    assert published_acs_metric.startswith("CENSUS_ACS:")
    assert _answers(api_client, published_acs_metric) >= 1


def test_the_serving_relation_stores_the_code_the_catalog_publishes(
    postgres_connection_factory: Callable[[], connection],
    published_acs_metric: str,
) -> None:
    """Covers: DB-025 — the two published surfaces spell one identity."""
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as database_cursor:
            database_cursor.execute(
                "SELECT COUNT(*) FROM gold_census.mv_acs_latest WHERE metric_code = %s",
                (published_acs_metric,),
            )
            latest = database_cursor.fetchone()[0]
            database_cursor.execute(
                "SELECT COUNT(*) FROM gold_census.rpt_acs_observations "
                "WHERE metric_code = %s",
                (published_acs_metric,),
            )
            history = database_cursor.fetchone()[0]
    finally:
        database_connection.close()

    assert latest >= 1, (
        f"the catalog publishes {published_acs_metric} but "
        "gold_census.mv_acs_latest holds no row under that code"
    )
    assert history >= 1


def test_no_acs_serving_row_survives_under_the_abandoned_spelling(
    postgres_connection_factory: Callable[[], connection],
    published_acs_metric: str,
) -> None:
    """Covers: DB-026 — a re-serve leaves no row under the retired spelling.

    The catalog's published codes do not change under this identity decision,
    so nothing in the catalog retires. What must not survive is a serving row
    still keyed on the abandoned ``ACS:`` spelling: the refresh deletes and
    re-inserts by ``metric_code``, so a partially re-served warehouse would
    carry both identities and answer differently depending on which one a
    consumer happened to hold.
    """
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as database_cursor:
            for relation in (
                "gold_census.mv_acs_latest",
                "gold_census.rpt_acs_observations",
            ):
                database_cursor.execute(
                    f"SELECT COUNT(*) FROM {relation} WHERE metric_code LIKE 'ACS:%%'"
                )
                stale = database_cursor.fetchone()[0]
                assert stale == 0, (
                    f"{relation} still holds {stale} row(s) under the abandoned "
                    "'ACS:' spelling; the catalog advertises none of them"
                )
    finally:
        database_connection.close()


def test_every_registered_source_answers_each_current_catalog_code(
    api_client: TestClient, published_acs_metric: str
) -> None:
    """Covers: DB-025 — no registered source advertises a code it cannot serve.

    Source-agnostic by construction: the sources come from the reviewed
    observation-dispatch registry, never from a list written here.

    A source whose catalog holds no ``current`` code contributes nothing, not
    because an unresolvable code is tolerable but because an unpopulated
    warehouse publishes no codes at all -- that is a warehouse with no content
    for the source, not a source advertising something it cannot serve. The
    final assertion is what stops that from becoming a vacuous pass: at least
    one registered source must actually have been exercised, and the fixture
    above guarantees Census ACS is one of them on any warehouse.
    """
    unresolvable: list[str] = []
    exercised: list[str] = []

    for source_code in sorted(OBSERVATION_DISPATCH):
        codes = _current_catalog_codes(api_client, source_code)
        if not codes:
            continue
        for metric_code in codes[:SWEEP_SAMPLE]:
            exercised.append(f"{source_code}:{metric_code}")
            if _answers(api_client, metric_code) < 1:
                unresolvable.append(
                    f"{source_code} publishes current catalog code "
                    f"'{metric_code}', which /api/v1/observations answers with "
                    "no rows"
                )

    assert not unresolvable, "\n".join(unresolvable)
    assert exercised, (
        "no registered source published a current catalog code, so this guard "
        "proved nothing; the warehouse under test carries no catalog content"
    )
