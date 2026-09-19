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
from urllib.parse import quote
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.registry import OBSERVATION_DISPATCH
from data_ingestion_toolbox.fred.gold_fred import transform as fred_gold_transform
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from data_ingestion_toolbox.usda_nass.registry import get_product as get_nass_product
from tests.support import fbi_release
from tests.support import usda_nass as nass_support
from tests.support.capture_seed import (
    delete_geography,
    delete_shared_geographies,
    preexisting_geographies,
    seed_geography,
)
from tests.support.postgres import PostgresHookStub, PostgresTestConfig
from tests.support.source_grains import (
    ADVERTISED_GEO_GRAINS as SOURCE_ADVERTISED_GEO_GRAINS,
)

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

#: Bounds the sweep below. The development warehouse publishes tens of
#: thousands of catalog rows, and one HTTP round trip per row would turn a
#: contract check into a load test. The sample is the source's first codes by
#: ``metric_code``, so it is deterministic rather than lucky.
SWEEP_SAMPLE = 15

ACS_DATASET = "acs5"

#: The narrow measure: one state row, for DB-028's derived-not-declared proof.
ACS_TABLE = "B99997"
ACS_VINTAGE = 2093
ACS_PERIOD = "2093-01-01"
ACS_PERIOD_END = "2093-12-31"
ACS_TIME_SK = 20930101

#: The wide measure: one row at every grain Census ACS ingests (DB-044). Its
#: own table, vintage and time row, because it runs beside the narrow one and
#: two fixtures sharing either would make the first teardown the second's
#: foreign-key violation.
ACS_GRAIN_TABLE = "B99996"
ACS_GRAIN_VINTAGE = 2092
ACS_GRAIN_PERIOD = "2092-01-01"
ACS_GRAIN_PERIOD_END = "2092-12-31"
ACS_GRAIN_TIME_SK = 20920101

#: Each row's ``geo_level`` is the source's own spelling -- ``census_acs``
#: requests ``us``, ``state`` and ``county`` and stores what it requested.
#: ``gold_glossary.geo_grain`` is what turns those into the vocabulary words
#: the catalog publishes, so writing NATIONAL here instead would test this
#: fixture rather than that function.
ACS_STATE_ROWS: tuple[dict[str, object], ...] = (
    {
        "geo_id": "state:95",
        "geo_level": "state",
        "geography": {
            "geo_type": "state",
            "state_fips": "95",
            "name": "Catalog agreement state",
        },
        "state_fips": "95",
        "county_fips": None,
        "estimate_value": 1234,
    },
)
ACS_GRAIN_ROWS: tuple[dict[str, object], ...] = (
    {
        "geo_id": "us:1",
        "geo_level": "us",
        "geography": {"geo_type": "nation", "name": "Catalog agreement nation"},
        "state_fips": None,
        "county_fips": None,
        "estimate_value": 331449281,
    },
    {
        "geo_id": "state:93",
        "geo_level": "state",
        "geography": {
            "geo_type": "state",
            "state_fips": "93",
            "name": "Catalog agreement grain state",
        },
        "state_fips": "93",
        "county_fips": None,
        "estimate_value": 4321,
    },
    {
        "geo_id": "state:93|county:001",
        "geo_level": "county",
        "geography": {
            "geo_type": "county",
            "state_fips": "93",
            "county_fips": "001",
            "name": "Catalog agreement grain county",
        },
        "state_fips": "93",
        "county_fips": "001",
        "estimate_value": 567,
    },
)

FRED_VINTAGE = 2094
FRED_PERIOD = "2094-01-01"
FRED_PERIOD_END = "2094-01-31"
FRED_TIME_SK = 20940101


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
    source_code: str = "CENSUS_ACS",
) -> dict[str, bool]:
    """Which registration rows for ``source_code`` exist before the harvest."""
    reader = factory()
    try:
        with reader.cursor() as database_cursor:
            state = {}
            for relation in _REGISTRATION_RELATIONS:
                database_cursor.execute(
                    f"SELECT EXISTS (SELECT 1 FROM {relation} WHERE source_code = %s)",
                    (source_code,),
                )
                state[relation] = bool(database_cursor.fetchone()[0])
            return state
    finally:
        reader.close()


def _remove_registration(
    database_cursor,
    existed_before: dict[str, bool],
    source_code: str = "CENSUS_ACS",
) -> None:
    """Remove the registration rows the harvest created, and only those."""
    for relation in _REGISTRATION_RELATIONS:
        if existed_before[relation]:
            continue
        if relation == "gold_glossary.dim_source_system":
            # Another suite's catalog rows may still reference the source
            # system; it is only this fixture's to remove when nothing does.
            database_cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM gold_glossary.dim_metric_catalog "
                "WHERE source_code = %s)",
                (source_code,),
            )
            if database_cursor.fetchone()[0]:
                continue
        database_cursor.execute(
            f"DELETE FROM {relation} WHERE source_code = %s", (source_code,)
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


def _publish_acs_variable(
    postgres_connection_factory: Callable[[], connection],
    *,
    table_id: str,
    vintage: int,
    period_start: str,
    period_end: str,
    time_sk: int,
    grain_rows: tuple[dict[str, object], ...],
) -> Iterator[str]:
    """Publish one ACS variable at the given grains, the way production does.

    Silver rows go through the real gold refresh procedures, and the real
    glossary harvest reads ``gold_census.metric_publisher`` into the catalog.
    Neither side is told what the other spelled, which is the whole point: the
    code this yields is the catalog's own, and the serving rows are whatever
    the refresh procedure composed.

    Every value that identifies state is a parameter rather than a constant,
    because two of these run in the same test: sharing a table, a vintage or a
    time row would make one fixture's teardown the other's foreign-key
    violation.
    """
    token = uuid4().hex[:8].upper()
    variable_code = f"{table_id}_{token}E"
    geo_ids = tuple(str(row["geo_id"]) for row in grain_rows)

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as database_cursor:
            _seed_time(database_cursor, time_sk, period_start)
            preexisting_geo = preexisting_geographies(database_cursor, geo_ids)
            geo_sk_by_id = {
                str(row["geo_id"]): seed_geography(
                    database_cursor,
                    vintage=vintage,
                    **row["geography"],  # type: ignore[arg-type]
                )
                for row in grain_rows
            }
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
                (ACS_DATASET, vintage, table_id),
            )
            row = database_cursor.fetchone()
            if row is None:
                database_cursor.execute(
                    """
                    SELECT acs_table_sk FROM gold_census.dim_acs_table
                    WHERE dataset_code = %s AND vintage_year = %s AND table_id = %s
                    """,
                    (ACS_DATASET, vintage, table_id),
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
                (acs_table_sk, ACS_DATASET, vintage, variable_code),
            )
            for grain_row in grain_rows:
                database_cursor.execute(
                    """
                    INSERT INTO silver_census.fact_demographics (
                        time_sk, geo_sk, duration_start, duration_end,
                        estimate_year, dataset, table_id, variable_code,
                        geo_level, geo_id, state_fips, county_fips,
                        estimate_value, margin_of_error, variable_label,
                        load_batch_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, 12,
                              'Total population', gen_random_uuid())
                    """,
                    (
                        time_sk,
                        geo_sk_by_id[str(grain_row["geo_id"])],
                        period_start,
                        period_end,
                        vintage,
                        ACS_DATASET,
                        table_id,
                        variable_code,
                        grain_row["geo_level"],
                        grain_row["geo_id"],
                        grain_row["state_fips"],
                        grain_row["county_fips"],
                        grain_row["estimate_value"],
                    ),
                )
            database_cursor.execute(
                "CALL gold_census.refresh_dashboard_serving_layer_acs(%s, %s, TRUE)",
                (period_start, period_end),
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
                    (table_id,),
                )
                delete_shared_geographies(database_cursor, geo_ids, preexisting_geo)
                database_cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = %s", (time_sk,)
                )
                _remove_registration(database_cursor, registered_before)
            cleanup.commit()
        finally:
            cleanup.close()


@pytest.fixture
def published_acs_metric(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Publish one ACS metric from a single state row.

    Deliberately narrower than the source: ACS ingests us, state and county,
    and this seeds one state. That is what lets DB-028 tell a derived
    ``valid_geo_grains`` from a declared one -- a view declaring its grains
    from the dataset code would advertise NATIONAL and COUNTY here, and the
    catalog would carry two grains nothing serves. The fixture that covers the
    source's whole range is ``published_acs_grain_metric``.
    """
    yield from _publish_acs_variable(
        postgres_connection_factory,
        table_id=ACS_TABLE,
        vintage=ACS_VINTAGE,
        period_start=ACS_PERIOD,
        period_end=ACS_PERIOD_END,
        time_sk=ACS_TIME_SK,
        grain_rows=ACS_STATE_ROWS,
    )


@pytest.fixture
def published_acs_grain_metric(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Publish one ACS metric at every grain the source ingests (DB-044)."""
    yield from _publish_acs_variable(
        postgres_connection_factory,
        table_id=ACS_GRAIN_TABLE,
        vintage=ACS_GRAIN_VINTAGE,
        period_start=ACS_GRAIN_PERIOD,
        period_end=ACS_GRAIN_PERIOD_END,
        time_sk=ACS_GRAIN_TIME_SK,
        grain_rows=ACS_GRAIN_ROWS,
    )


@pytest.fixture
def published_fred_metric(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Publish one FRED metric the way production does, end to end.

    The ACS fixture above guarantees this guard is exercised on a source whose
    grains have always been derived. FRED is the second, and until 2026-09-12
    it was the counter-case: ``gold_fred.metric_publisher`` declared
    ``ARRAY['NATIONAL']`` for every series, so the sweep would have reported a
    national grain for a series serving nothing without ever reading a row.
    """
    series_id = f"TEST_AGREEMENT_FRED_{uuid4().hex[:8].upper()}"

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as database_cursor:
            _seed_time(database_cursor, FRED_TIME_SK, FRED_PERIOD)
            seed_geography(
                database_cursor,
                geo_type="nation",
                vintage=FRED_VINTAGE,
                name="Catalog agreement nation",
            )
            database_cursor.execute(
                """
                INSERT INTO silver_fred.fact_economic_indicators (
                    time_sk, duration_start, duration_end, observation_date,
                    series_id, domain, value, is_missing, series_title,
                    unit_of_measure, frequency, seasonal_adjustment,
                    source_system, load_batch_id, ingested_at
                ) VALUES (
                    %s, %s, %s, %s, %s, 'fixture', 7.5, FALSE,
                    'Catalog agreement series', 'Index', 'Monthly',
                    'Not Adjusted', 'FRED', gen_random_uuid(), NOW()
                )
                """,
                (FRED_TIME_SK, FRED_PERIOD, FRED_PERIOD_END, FRED_PERIOD, series_id),
            )
        writer.commit()
    finally:
        writer.close()

    fred_gold_transform.refresh_fred_elements(
        PostgresHookStub(postgres_connection_factory)
    )

    # Serve before harvesting. The grain is read from the served relation, so
    # the reverse order publishes the empty array -- which the ingest DAG
    # already gets right and a fixture is free to get wrong.
    refresher = postgres_connection_factory()
    try:
        with refresher.cursor() as database_cursor:
            database_cursor.execute(
                "CALL gold_fred.refresh_dashboard_serving_layer_fred(%s, %s, TRUE)",
                (FRED_PERIOD, FRED_PERIOD_END),
            )
        refresher.commit()
    finally:
        refresher.close()

    registered_before = _registration_state(postgres_connection_factory, "FRED")
    harvest_publisher(postgres_connection_factory, Publisher("gold_fred"))

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as database_cursor:
            database_cursor.execute(
                """
                SELECT metric_code FROM gold_glossary.dim_metric_catalog
                WHERE source_code = 'FRED' AND source_object_key = %s
                """,
                (series_id,),
            )
            published = database_cursor.fetchone()
    finally:
        reader.close()
    assert published is not None, "the harvest published no FRED catalog row"

    try:
        yield published[0]
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as database_cursor:
                for statement in (
                    "DELETE FROM gold_fred.mv_fred_latest WHERE series_id = %s",
                    "DELETE FROM gold_fred.rpt_fred_observations WHERE series_id = %s",
                    "DELETE FROM gold_glossary.dim_metric_catalog "
                    "WHERE source_code = 'FRED' AND source_object_key = %s",
                    # gold_fred.fact_fred_observation is a view over silver, so
                    # the silver delete below is what removes the fact rows.
                    "DELETE FROM gold_fred.dim_fred_series WHERE series_id = %s",
                    "DELETE FROM silver_fred.fact_economic_indicators WHERE series_id = %s",
                ):
                    database_cursor.execute(statement, (series_id,))
                database_cursor.execute(
                    "DELETE FROM control.serving_refresh_chunk_state "
                    "WHERE source_code = 'FRED'"
                )
                database_cursor.execute(
                    "DELETE FROM control.serving_refresh_state WHERE source_code = 'FRED'"
                )
                _remove_registration(database_cursor, registered_before, "FRED")
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


# ---------------------------------------------------------------------------
# The three sources the sweep below could not reach (DB-043)
#
# `test_every_registered_source_answers_each_current_catalog_code` walked the
# reviewed dispatch registry and skipped every source whose catalog held no
# current code. On the warehouse CI builds that was BLS, FBI UCR and USDA
# NASS -- three of seven, and between them two of the three identity
# strategies' widest cases: FBI's `identity_columns` pair over a participation
# basis, and NASS's five-column tuple. The sweep read green having exercised
# neither, and the skip was silent, so nothing said which sources it had not
# checked.
#
# These publish one current code each, so the sweep can require every
# registered source rather than accepting whichever happened to be seeded.
# FBI UCR and USDA NASS run their real pipelines from reviewed captures --
# both already have support modules that replay, transform, publish, and
# remove all of their own state -- because a fixture that hand-wrote gold
# rows for them would be asserting agreement between two things this file
# wrote. BLS has no such module, so its rows are seeded here, through the
# publisher's series arm (the branch that publishes a series whose program no
# measure identity claims).
# ---------------------------------------------------------------------------


#: One served row at each grain BLS publishes (DB-044). ``bls/geography.py``
#: parses a LAUS area code to ``state`` or ``county`` and to nothing else --
#: "LAUS has no national series" -- while the national CPS/CES series carry
#: ``us:1``, which ``gold_bls.fact_bls_observation`` reads as NATIONAL. These
#: rows go into the serving relations already carrying the vocabulary word,
#: which is what the gold view puts there in production; the publisher still
#: derives the catalog's grains from them rather than being told.
BLS_GRAIN_ROWS: tuple[tuple[str, str, int], ...] = (
    ("us:1", "NATIONAL", 1234),
    ("state:93", "STATE", 567),
    ("state:93|county:001", "COUNTY", 89),
)


@pytest.fixture
def published_bls_metric(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Publish one BLS metric, and serve one row under the code it publishes."""
    token = uuid4().hex[:8].upper()
    program_code = f"S{token[:2]}"
    series_id = f"SWEEP{token}"
    metric_code = f"BLS:{series_id}"
    registered_before = _registration_state(postgres_connection_factory, "BLS")

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as database_cursor:
            database_cursor.execute(
                """
                INSERT INTO gold_bls.dim_bls_survey (
                    program_code, survey_name, observation_basis
                ) VALUES (%s, 'Catalog agreement survey', 'JOBS')
                """,
                (program_code,),
            )
            database_cursor.execute(
                """
                INSERT INTO gold_bls.dim_bls_series (
                    bls_survey_sk, program_code, series_id, series_title,
                    measure_category, value_type, unit_of_measure,
                    seasonal_adjustment_status
                )
                SELECT bls_survey_sk, %s, %s, 'Catalog agreement series',
                       'EMPLOYMENT', 'LEVEL', 'persons', 'Seasonally Adjusted'
                FROM gold_bls.dim_bls_survey WHERE program_code = %s
                """,
                (program_code, series_id, program_code),
            )
            # The reporting table and the latest projection carry the same
            # rows: the publisher reads the latest projection for the grains it
            # advertises, and the sweep reads whichever the dispatch selects.
            for relation in ("rpt_bls_observations", "mv_bls_latest"):
                for geo_id, geo_level, value in BLS_GRAIN_ROWS:
                    database_cursor.execute(
                        f"""
                        INSERT INTO gold_bls.{relation} (
                            source_code, observation_date, duration_start,
                            duration_end, time_sk, as_of_date, updated_at,
                            geo_id, geo_level, series_id, program_code,
                            series_title, value, units,
                            seasonal_adjustment_status, metric_code,
                            metric_display_name
                        ) VALUES (
                            'BLS', %s, %s, %s, 20970101, %s, %s, %s,
                            %s, %s, %s, 'Catalog agreement series', %s,
                            'persons', 'Seasonally Adjusted', %s,
                            'Catalog agreement series'
                        )
                        """,
                        (
                            BLS_PERIOD_START,
                            BLS_PERIOD_START,
                            BLS_PERIOD_END,
                            BLS_PERIOD_END,
                            f"{BLS_PERIOD_END} 00:00:00+00",
                            geo_id,
                            geo_level,
                            series_id,
                            program_code,
                            value,
                            metric_code,
                        ),
                    )
        writer.commit()
    finally:
        writer.close()

    harvest_publisher(postgres_connection_factory, Publisher("gold_bls"))
    _assert_catalog_published(postgres_connection_factory, "BLS", series_id)

    try:
        yield metric_code
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as database_cursor:
                database_cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog "
                    "WHERE source_code = 'BLS' AND source_object_key = %s",
                    (series_id,),
                )
                for relation in ("mv_bls_latest", "rpt_bls_observations"):
                    database_cursor.execute(
                        f"DELETE FROM gold_bls.{relation} WHERE series_id = %s",
                        (series_id,),
                    )
                database_cursor.execute(
                    "DELETE FROM gold_bls.dim_bls_series WHERE series_id = %s",
                    (series_id,),
                )
                database_cursor.execute(
                    "DELETE FROM gold_bls.dim_bls_survey WHERE program_code = %s",
                    (program_code,),
                )
                _remove_registration(database_cursor, registered_before, "BLS")
            cleanup.commit()
        finally:
            cleanup.close()


@pytest.fixture
def published_fbi_metric(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Publish one FBI UCR metric by running its real release pipeline."""
    for factory in fbi_release.reviewed_warehouse(postgres_connection_factory):
        captured = fbi_release.persist_fixture_release(factory)
        fbi_release.run_pipeline(factory, captured)
        harvest_publisher(factory, Publisher("gold_fbi"))
        yield _one_published_code(factory, "FBI_UCR")


@pytest.fixture
def published_nass_metric(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> str:
    """Publish one USDA NASS metric by running its real release pipeline."""
    factory = nass_support.reviewed_warehouse(postgres_connection_factory, request)
    product = get_nass_product("corn_survey_annual")
    nass_support.run_to_gold(
        factory, product, nass_support.load_product_fixture(product.product_id)
    )
    harvest_publisher(factory, Publisher("gold_nass"))
    return _one_published_code(factory, "USDA_NASS")


def _assert_catalog_published(
    factory: Callable[[], connection], source_code: str, source_object_key: str
) -> None:
    """Fail loudly when a harvest published nothing for a seeded measure."""
    reader = factory()
    try:
        with reader.cursor() as database_cursor:
            database_cursor.execute(
                """
                SELECT metric_code FROM gold_glossary.dim_metric_catalog
                WHERE source_code = %s AND source_object_key = %s
                """,
                (source_code, source_object_key),
            )
            published = database_cursor.fetchone()
    finally:
        reader.close()
    assert published is not None, (
        f"the harvest published no {source_code} catalog row for "
        f"{source_object_key!r}; the fixture proves nothing about the sweep"
    )


def _one_published_code(factory: Callable[[], connection], source_code: str) -> str:
    """The first current catalog code the harvest published for a source."""
    reader = factory()
    try:
        with reader.cursor() as database_cursor:
            database_cursor.execute(
                """
                SELECT metric_code FROM gold_glossary.dim_metric_catalog
                WHERE source_code = %s AND freshness_state = 'current'
                ORDER BY metric_code LIMIT 1
                """,
                (source_code,),
            )
            published = database_cursor.fetchone()
    finally:
        reader.close()
    assert published is not None, (
        f"the harvest published no current {source_code} catalog row, so the "
        "sweep would skip that source exactly as it used to"
    )
    return str(published[0])


def test_every_registered_source_answers_each_current_catalog_code(
    api_client: TestClient,
    published_acs_metric: str,
    published_cdc_metric: str,
    published_fred_metric: str,
    published_pep_metric: str,
    published_bls_metric: str,
    published_fbi_metric: str,
    published_nass_metric: str,
) -> None:
    """Covers: DB-025 — no registered source advertises a code it cannot serve.

    Source-agnostic by construction: the sources come from the reviewed
    observation-dispatch registry, never from a list written here.

    Covers: DB-043 -- and every registered source is exercised, not whichever
    the warehouse happened to carry.

    This sweep used to `continue` past a source whose catalog held no current
    code, on the reasoning that an unpopulated warehouse is not a source
    advertising what it cannot serve. That reasoning is sound and the
    behaviour was not: the skip was silent, so a sweep that checked three of
    seven sources reported the same green as one that checked all seven, and
    nothing named the four it had not looked at. On the warehouse CI builds,
    BLS, FBI UCR and USDA NASS were skipped every run -- including FBI's
    `identity_columns` pair over a participation basis and NASS's five-column
    tuple, the two widest cases of the strategy DB-032 had already caught once.

    Every registered source now has a fixture that publishes one current code,
    so a source contributing nothing is a failure that names it rather than a
    silent pass.
    """
    unresolvable: list[str] = []
    exercised: dict[str, list[str]] = {}

    for source_code in sorted(OBSERVATION_DISPATCH):
        for metric_code in _current_catalog_codes(api_client, source_code)[
            :SWEEP_SAMPLE
        ]:
            exercised.setdefault(source_code, []).append(metric_code)
            if _answers(api_client, metric_code) < 1:
                unresolvable.append(
                    f"{source_code} publishes current catalog code "
                    f"'{metric_code}', which /api/v1/observations answers with "
                    "no rows"
                )

    assert not unresolvable, "\n".join(unresolvable)

    # The registry is the list, so a source added to the dispatch without a
    # fixture fails here rather than quietly joining the ones nobody checks.
    unexercised = sorted(set(OBSERVATION_DISPATCH) - set(exercised))
    assert not unexercised, (
        f"these registered sources published no current catalog code, so the "
        f"sweep proved nothing about them: {unexercised}. Every source needs a "
        "fixture that publishes one; a source that cannot get one is a source "
        "this guard does not cover."
    )

    # And the codes the fixtures published are among the codes swept: a source
    # whose catalog carried thousands of other rows could otherwise satisfy
    # the check above without the identity strategy under test being read.
    for source_code, metric_code in (
        ("CENSUS_ACS", published_acs_metric),
        ("CDC", published_cdc_metric),
        ("FRED", published_fred_metric),
        ("CENSUS_PEP", published_pep_metric),
        ("BLS", published_bls_metric),
        ("FBI_UCR", published_fbi_metric),
        ("USDA_NASS", published_nass_metric),
    ):
        assert _answers(api_client, metric_code) >= 1, (
            f"{source_code}'s fixture published '{metric_code}', which "
            "/api/v1/observations answers with no rows"
        )


def _current_catalog_grains(
    client: TestClient, source_code: str
) -> dict[str, list[str]]:
    response = client.get(
        "/api/v1/catalog/metrics",
        params={"source_code": source_code, "active_only": "true", "limit": 1000},
    )
    assert response.status_code == 200, response.text
    return {
        item["metric_code"]: list(item.get("valid_geo_grains") or [])
        for item in sorted(
            response.json()["items"], key=lambda item: item["metric_code"]
        )
    }


def test_every_published_grain_of_a_current_code_answers_in_the_vocabulary(
    api_client: TestClient,
    published_acs_metric: str,
    published_fred_metric: str,
    published_cdc_metric: str,
) -> None:
    """Covers: DB-028 — a grain read from the catalog can be sent straight back.

    The consumer guide promises one geography-grain vocabulary on served rows
    and in ``valid_geo_grains``. Four sources broke it four ways, and no tier
    saw any of them: CDC and PEP published and served ``nation`` where the
    contract says ``NATIONAL``; USDA NASS published ``NATION`` from one column
    and filtered on another whose word is ``NATIONAL``, so every national
    statistic in its catalog was unanswerable; FBI projected a source
    identifier as the row's grain; and Census ACS declared grains from its
    dataset code, advertising 2,487 metric/grain pairs nothing served.

    Source-agnostic like DB-025: the sources come from the reviewed dispatch
    registry. For each sampled current code, every grain the catalog
    publishes for it must answer at least one row when sent as ``geo_level``,
    and every row that comes back must carry that grain, in the vocabulary.
    The ACS fixture publishes exactly the grain it seeded, so the guard is
    exercised on every warehouse rather than only where content happens to
    exist.
    """
    from apps.api.registry import GEO_GRAINS

    unanswered: list[str] = []
    off_vocabulary: list[str] = []
    exercised: list[str] = []

    for source_code in sorted(OBSERVATION_DISPATCH):
        grains_by_code = _current_catalog_grains(api_client, source_code)
        for metric_code, grains in list(grains_by_code.items())[:SWEEP_SAMPLE]:
            for grain in grains:
                exercised.append(f"{metric_code}@{grain}")
                if grain not in GEO_GRAINS:
                    off_vocabulary.append(
                        f"{source_code} publishes grain '{grain}' for '{metric_code}', "
                        f"which is not in the vocabulary {GEO_GRAINS}"
                    )
                response = api_client.get(
                    "/api/v1/observations",
                    params={"metric_code": metric_code, "geo_level": grain, "limit": 5},
                )
                assert response.status_code == 200, (
                    f"{metric_code}@{grain}: {response.text}"
                )
                payload = response.json()
                if int(payload["total"]) < 1:
                    unanswered.append(
                        f"{source_code} publishes grain '{grain}' for '{metric_code}', "
                        "which /api/v1/observations answers with no rows"
                    )
                for row in payload["items"]:
                    if row.get("geo_level") != grain:
                        off_vocabulary.append(
                            f"{metric_code}@{grain} served a row whose geo_level is "
                            f"{row.get('geo_level')!r}"
                        )

    assert not unanswered, "\n".join(unanswered)
    assert not off_vocabulary, "\n".join(off_vocabulary)
    # The fixture seeded one state row, so the derived grain is exactly STATE:
    # a declared grain would have advertised NATIONAL and COUNTY here too.
    assert f"{published_acs_metric}@STATE" in exercised
    assert f"{published_acs_metric}@NATIONAL" not in exercised
    assert f"{published_acs_metric}@COUNTY" not in exercised
    # And FRED, whose grain was declared rather than derived until 2026-09-12.
    # Its fixture serves one national row, so NATIONAL is the whole set: the
    # assertion is that the grain came from that row, not from the view.
    assert f"{published_fred_metric}@NATIONAL" in exercised
    assert f"{published_fred_metric}@STATE" not in exercised
    assert f"{published_fred_metric}@COUNTY" not in exercised
    # And CDC, the source this sweep's docstring names first and had never
    # actually asked: it is identified by `identity_columns`, and nothing
    # published a code under that strategy until DB-032 seeded one.
    assert f"{published_cdc_metric}@STATE" in exercised


# ---------------------------------------------------------------------------
# DB-044 — the fixture corpus reaches every grain each source can publish
# ---------------------------------------------------------------------------

#: What each registered source's pipeline can put in ``valid_geo_grains``.
#:
#: Declared in ``tests/support/source_grains``, which carries the per-source
#: evidence for every entry. Two tiers read it now -- this one, and the web
#: visualization coverage matrix, which asks whether any grain a source
#: publishes is one the tile boundary can draw -- and a second copy would be a
#: second answer to the same question.
ADVERTISED_GEO_GRAINS = SOURCE_ADVERTISED_GEO_GRAINS


def _published_grains(client: TestClient, source_code: str) -> dict[str, list[str]]:
    """Every grain the source's current catalog publishes, and a code for each.

    Unlike ``_current_catalog_grains`` this does not sample: a grain seeded by
    one fixture among many would fall outside the first ``SWEEP_SAMPLE`` codes
    and the coverage question would answer itself wrongly.
    """
    codes_by_grain: dict[str, list[str]] = {}
    for metric_code, grains in _current_catalog_grains(client, source_code).items():
        for grain in grains:
            codes_by_grain.setdefault(grain, []).append(metric_code)
    return codes_by_grain


def test_every_source_fixture_corpus_reaches_every_grain_its_pipeline_publishes(
    api_client: TestClient,
    published_acs_grain_metric: str,
    published_cdc_metric: str,
    published_cdc_county_metric: str,
    published_fred_metric: str,
    published_pep_metrics: list[str],
    published_bls_metric: str,
    published_fbi_metric: str,
    published_nass_metric: str,
) -> None:
    """Covers: DB-044 — every grain a source can publish has a fixture row.

    ``valid_geo_grains`` is derived, not declared: every publisher aggregates
    it out of the rows that exist (migration 018 and the six views that call
    ``gold_glossary.geo_grain``). That is the right design and it makes the
    grain sweeps self-limiting. A fixture that seeds one county publishes a
    catalog whose only grain is COUNTY, so DB-028's "every published grain
    answers" passes over a single word, and DB-030's route sweep asks every
    route about that one word. Both report the same green they would report
    for a corpus covering all five. Nothing in the stack could tell the two
    apart, which is how the smoke seed ran a seventh of the surface its own
    summary line named until 2026-09-14.

    So the fixture corpus is measured against what each source's own reviewed
    declaration says it can publish, and a grain no fixture reaches fails
    naming the source and the word. This is the one check that must not read
    its expectation from the warehouse.
    """
    from apps.api.registry import GEO_GRAINS

    # A source added to the dispatch without declaring its grains would
    # otherwise join the set nobody measures -- DB-043's defect, one level up.
    assert set(ADVERTISED_GEO_GRAINS) == set(OBSERVATION_DISPATCH), (
        "every registered source must declare the grains its pipeline can "
        f"publish; declared {sorted(ADVERTISED_GEO_GRAINS)}, registered "
        f"{sorted(OBSERVATION_DISPATCH)}"
    )
    for source_code, advertised in sorted(ADVERTISED_GEO_GRAINS.items()):
        assert advertised, f"{source_code} declares no grain at all"
        off_vocabulary = sorted(advertised - set(GEO_GRAINS))
        assert not off_vocabulary, (
            f"{source_code} declares {off_vocabulary}, which the vocabulary "
            f"{GEO_GRAINS} does not contain"
        )

    # And every word in the vocabulary is some source's to publish. A grain
    # the API accepts that no source owns is a filter that can only ever
    # answer empty.
    ownerless = sorted(set(GEO_GRAINS) - set().union(*ADVERTISED_GEO_GRAINS.values()))
    assert not ownerless, (
        f"the vocabulary carries {ownerless}, which no registered source "
        "declares; a grain with no publisher answers every request empty"
    )

    unreached: list[str] = []
    unanswered: list[str] = []
    for source_code, advertised in sorted(ADVERTISED_GEO_GRAINS.items()):
        codes_by_grain = _published_grains(api_client, source_code)
        missing = sorted(advertised - set(codes_by_grain))
        if missing:
            unreached.append(
                f"{source_code} can publish {sorted(advertised)} but its "
                f"fixtures publish only {sorted(codes_by_grain)}; nothing "
                f"exercises {missing}"
            )
        for grain in sorted(advertised & set(codes_by_grain)):
            metric_code = sorted(codes_by_grain[grain])[0]
            response = api_client.get(
                "/api/v1/observations",
                params={"metric_code": metric_code, "geo_level": grain, "limit": 5},
            )
            assert response.status_code == 200, (
                f"{metric_code}@{grain}: {response.text}"
            )
            if int(response.json()["total"]) < 1:
                unanswered.append(
                    f"{source_code} publishes grain '{grain}' for "
                    f"'{metric_code}', which /api/v1/observations answers with "
                    "no rows"
                )

    assert not unreached, "\n".join(unreached)
    assert not unanswered, "\n".join(unanswered)


# ---------------------------------------------------------------------------
# DB-030 — the published-grain sweep reaches every route that accepts a grain
# ---------------------------------------------------------------------------

PEP_VINTAGE = 2095
PEP_YEAR = 2095
PEP_ESTIMATE_DATE = "2095-07-01"  # make_date(year, 7, 1), per the fact's own check
PEP_DATASET = "pep_agreement_test"

#: One row at each grain Census PEP publishes (DB-044).
#: ``census_pep/silver_pep/transform.py`` maps summary levels 010, 040, 050
#: and 162 to nation, state, county and place, and every other level to
#: ``unsupported`` -- which resolves to no geography and reaches no served
#: row. The summary level is the source's own code and ``geo_type`` is what
#: the transform derives from it; both are written here because this fixture
#: seeds silver directly, and the publisher still derives the catalog's grains
#: from the rows through ``gold_glossary.geo_grain``.
PEP_GRAIN_ROWS: tuple[dict[str, object], ...] = (
    {
        "summary_level": "010",
        "geo_type": "nation",
        "geo_id": "us:1",  # PEP_NATION_GEO_ID, defined below these rows
        "source_geo_code": "1",
        "name": "United States",
        "geography": {"geo_type": "nation", "name": "Grain sweep nation"},
        "state_fips_source": None,
        "county_fips_source": None,
        "place_fips_source": None,
        "value": 331000000,
    },
    {
        "summary_level": "040",
        "geo_type": "state",
        "geo_id": "state:92",
        "source_geo_code": "92",
        "name": "Grain sweep state",
        "geography": {
            "geo_type": "state",
            "state_fips": "92",
            "name": "Grain sweep state",
        },
        "state_fips_source": "92",
        "county_fips_source": None,
        "place_fips_source": None,
        "value": 5200000,
    },
    {
        "summary_level": "050",
        "geo_type": "county",
        "geo_id": "state:92|county:001",
        "source_geo_code": "92001",
        "name": "Grain sweep County",
        "geography": {
            "geo_type": "county",
            "state_fips": "92",
            "county_fips": "001",
            "name": "Grain sweep County",
        },
        "state_fips_source": "92",
        "county_fips_source": "001",
        "place_fips_source": None,
        "value": 61000,
    },
    {
        "summary_level": "162",
        "geo_type": "place",
        "geo_id": "state:92|place:00100",
        "source_geo_code": "9200100",
        "name": "Grain sweep city",
        "geography": {
            "geo_type": "place",
            "state_fips": "92",
            "place_fips": "00100",
            "name": "Grain sweep city",
        },
        "state_fips_source": "92",
        "county_fips_source": None,
        "place_fips_source": "00100",
        "value": 2700,
    },
)
PEP_GEO_IDS: tuple[str, ...] = tuple(str(row["geo_id"]) for row in PEP_GRAIN_ROWS)
#: The canonical national identity, which is what ``canonical_geo_id``
#: composes and what ``seed_geography`` therefore writes. The fixture used to
#: seed that geography and then label its fact row ``nation:us`` -- a spelling
#: nothing else in the warehouse uses -- and the route test below filtered on
#: the same invented string, so the pair agreed with each other and with
#: nothing.
PEP_NATION_GEO_ID = "us:1"


@pytest.fixture
def published_pep_metrics(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Iterator[list[str]]:
    """Publish one or more Census PEP metrics at the national grain, end to end.

    Parameterised on how many to publish, because `/comparison/matrix` needs
    two **distinct** comparable measures and refuses a repeat — a measure
    against itself correlates 1 with no information in it. Two PEP measures in
    one dataset share their units, their time grain and their geography grain,
    so the pair is comparable by the policy's own rules rather than by a
    fixture pretending it is.

    `published_pep_metric` below is this fixture publishing one, kept as its
    own name so every existing caller reads the same.

    PEP is the source the grain mapping exists for: its reporting view
    projects ``revision.geo_type AS geo_level``, so the served relation stores
    ``nation`` under a column named for the vocabulary. The catalog publishes
    ``NATIONAL`` for it, and the two must still meet -- which is what nothing
    exercised, because no fixture published a PEP metric at all.

    Seeded through the real views: silver rows, then `population_estimate_revision`
    and `_latest` and `rpt_pep_observations` and `mv_pep_latest` compose
    themselves, then the real glossary harvest reads `gold_pep.metric_publisher`.
    Neither side is told what the other spelled.
    """
    from tests.support.capture_seed import seed_capture

    count = int(getattr(request, "param", 1))
    token = uuid4().hex[:8].upper()
    # POPESTIMATE-family codes carry a non-negative check; any other code
    # is unconstrained, and the sweep cares about the grain, not the measure.
    measure_codes = [
        f"AGREEMENT_{token}" if index == 0 else f"AGREEMENT_{token}_{index}"
        for index in range(count)
    ]

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as database_cursor:
            capture_id = seed_capture(database_cursor, "CENSUS_PEP")
            preexisting_geo = preexisting_geographies(database_cursor, PEP_GEO_IDS)
            geo_sk_by_id = {
                str(row["geo_id"]): seed_geography(
                    database_cursor,
                    vintage=PEP_VINTAGE,
                    **row["geography"],  # type: ignore[arg-type]
                )
                for row in PEP_GRAIN_ROWS
            }
            database_cursor.execute(
                """
                INSERT INTO silver_pep.pep_dataset (
                    dataset_code, title, transport, geography_levels,
                    summary_levels, variable_families, parser_version,
                    text_encoding, release_page_url, decennial_base, is_active,
                    created_at, updated_at, series_kind, era, native_grain
                ) VALUES (
                    %s, 'Grain sweep dataset', 'bulk_csv',
                    ARRAY['nation', 'state', 'county', 'place'],
                    ARRAY['010', '040', '050', '162'], ARRAY['POP'], '1', 'utf-8',
                    'https://www.census.gov/grain-sweep', 2090, TRUE, NOW(), NOW(),
                    'postcensal', 'test', '010'
                ) ON CONFLICT (dataset_code) DO NOTHING
                """,
                (PEP_DATASET,),
            )
            for measure_code in measure_codes:
                database_cursor.execute(
                    """
                    INSERT INTO silver_pep.dim_measure (
                        metric_code, display_name, unit, value_type, is_component,
                        allows_negative, population_universe, updated_at
                    ) VALUES (%s, 'Grain sweep population', 'persons', 'count',
                              FALSE, FALSE, 'resident population', NOW())
                    """,
                    (measure_code,),
                )
            database_cursor.execute(
                """
                INSERT INTO silver_pep.pep_release (
                    dataset_code, vintage_year, product_code, data_url,
                    layout_url, release_date, observation_start_year,
                    observation_end_year, geography_basis_date, schema_version,
                    status, media_type, created_at, updated_at, series_kind
                ) VALUES (
                    %s, %s, 'agreement_test_alldata',
                    'https://www2.census.gov/grain-sweep/data.csv',
                    'https://www2.census.gov/grain-sweep/layout.txt',
                    %s, %s, %s, %s, '1', 'published', 'text/csv',
                    NOW(), NOW(), 'postcensal'
                )
                -- The conflict target is named, and naming it is the point.
                -- A bare `ON CONFLICT DO NOTHING` absorbs *any* unique
                -- violation, including ones that say nothing about this row's
                -- identity: `silver_pep.pep_release` also carries a global
                -- `UNIQUE (product_code)` (migration 009), so when a
                -- neighbouring seed took `alldata` this insert did nothing,
                -- raised nothing, and failed as a foreign key violation on
                -- `release_load` one statement later -- ten tests erroring at
                -- setup for a reason no message named. Re-running this
                -- fixture is the only conflict it should tolerate.
                ON CONFLICT (dataset_code, vintage_year, product_code) DO NOTHING
                """,
                (
                    PEP_DATASET,
                    PEP_VINTAGE,
                    PEP_ESTIMATE_DATE,
                    PEP_YEAR,
                    PEP_YEAR,
                    PEP_ESTIMATE_DATE,
                ),
            )
            database_cursor.execute(
                """
                INSERT INTO silver_pep.release_load (
                    capture_id, dataset_code, release_vintage, product_code,
                    source_record_count, observation_count, completeness_status,
                    validated_at
                ) VALUES (%s, %s, %s, 'agreement_test_alldata', 1, 1, 'complete', NOW())
                """,
                (capture_id, PEP_DATASET, PEP_VINTAGE),
            )
            row_index = 0
            for measure_code in measure_codes:
                for grain_row in PEP_GRAIN_ROWS:
                    row_index += 1
                    # The fact keys back to the revision it was parsed from, so
                    # the parsed row exists first -- the same order the loader
                    # writes in.
                    database_cursor.execute(
                        """
                        INSERT INTO silver_pep.observation_revision (
                            capture_id, source_row_index, source_column_index,
                            source_header, dataset_code, release_vintage,
                            product_code, observation_year, metric_code, unit,
                            summary_level, state_fips_source, county_fips_source,
                            place_fips_source, name_source, value_source, value,
                            value_status, parser_version, parsed_at
                        ) VALUES (
                            %s, %s, 1, %s, %s, %s, 'agreement_test_alldata', %s, %s, 'persons',
                            %s, %s, %s, %s, %s, %s, %s,
                            'valid', '1', NOW()
                        )
                        """,
                        (
                            capture_id,
                            row_index,
                            measure_code,
                            PEP_DATASET,
                            PEP_VINTAGE,
                            PEP_YEAR,
                            measure_code,
                            grain_row["summary_level"],
                            grain_row["state_fips_source"],
                            grain_row["county_fips_source"],
                            grain_row["place_fips_source"],
                            grain_row["name"],
                            str(grain_row["value"]),
                            grain_row["value"],
                        ),
                    )
                    database_cursor.execute(
                        """
                        INSERT INTO silver_pep.fact_population_estimate (
                            capture_id, source_row_index, source_column_index,
                            dataset_code, release_vintage, product_code,
                            metric_code, observation_year, estimate_date, geo_id,
                            geo_sk, geo_type, geography_basis_date,
                            resolution_status, summary_level, source_geo_code,
                            source_name, value_source, value, unit,
                            transformed_at
                        ) VALUES (
                            %s, %s, 1, %s, %s, 'agreement_test_alldata', %s, %s, %s,
                            %s, %s, %s, %s, 'resolved', %s, %s, %s,
                            %s, %s, 'persons', NOW()
                        )
                        """,
                        (
                            capture_id,
                            row_index,
                            PEP_DATASET,
                            PEP_VINTAGE,
                            measure_code,
                            PEP_YEAR,
                            PEP_ESTIMATE_DATE,
                            grain_row["geo_id"],
                            geo_sk_by_id[str(grain_row["geo_id"])],
                            grain_row["geo_type"],
                            PEP_ESTIMATE_DATE,
                            grain_row["summary_level"],
                            grain_row["source_geo_code"],
                            grain_row["name"],
                            str(grain_row["value"]),
                            grain_row["value"],
                        ),
                    )
        writer.commit()
    finally:
        writer.close()

    registered_before = _registration_state(postgres_connection_factory, "CENSUS_PEP")
    harvest_publisher(postgres_connection_factory, Publisher("gold_pep"))

    reader = postgres_connection_factory()
    published: list[str] = []
    try:
        with reader.cursor() as database_cursor:
            for measure_code in measure_codes:
                database_cursor.execute(
                    """
                    SELECT metric_code FROM gold_glossary.dim_metric_catalog
                    WHERE source_code = 'CENSUS_PEP' AND source_object_key = %s
                    """,
                    (measure_code,),
                )
                row = database_cursor.fetchone()
                assert row is not None, (
                    f"the harvest published no PEP catalog row for {measure_code}"
                )
                published.append(row[0])
    finally:
        reader.close()
    assert len(published) == len(measure_codes)

    try:
        yield published
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as database_cursor:
                for measure_code in measure_codes:
                    database_cursor.execute(
                        "DELETE FROM gold_glossary.dim_metric_catalog "
                        "WHERE source_object_key = %s AND source_code = 'CENSUS_PEP'",
                        (measure_code,),
                    )
                    database_cursor.execute(
                        "DELETE FROM silver_pep.fact_population_estimate "
                        "WHERE metric_code = %s",
                        (measure_code,),
                    )
                database_cursor.execute(
                    "DELETE FROM silver_pep.observation_revision "
                    "WHERE dataset_code = %s",
                    (PEP_DATASET,),
                )
                database_cursor.execute(
                    "DELETE FROM silver_pep.release_load WHERE dataset_code = %s",
                    (PEP_DATASET,),
                )
                database_cursor.execute(
                    "DELETE FROM silver_pep.pep_release WHERE dataset_code = %s",
                    (PEP_DATASET,),
                )
                for measure_code in measure_codes:
                    database_cursor.execute(
                        "DELETE FROM silver_pep.dim_measure WHERE metric_code = %s",
                        (measure_code,),
                    )
                database_cursor.execute(
                    "DELETE FROM silver_pep.pep_dataset WHERE dataset_code = %s",
                    (PEP_DATASET,),
                )
                delete_shared_geographies(database_cursor, PEP_GEO_IDS, preexisting_geo)
                _remove_registration(database_cursor, registered_before, "CENSUS_PEP")
            cleanup.commit()
        finally:
            cleanup.close()


def test_every_published_grain_answers_on_every_route_that_accepts_one(
    api_client: TestClient, published_pep_metric: str
) -> None:
    """Covers: DB-030 — the sweep asks every route, not only the neutral one.

    DB-028 sweeps `/api/v1/observations` and nothing else, and its docstring
    says of the four defects it was written for that "no tier saw any of
    them". API-092 was the fifth, on a route it does not ask: the
    source-scoped latest route filtered the caller's vocabulary word against
    a relation storing the source's own, so `geo_level=NATIONAL` reached none
    of Census PEP's national rows. Two gaps let it through -- the sweep never
    asked that route, and no fixture published a PEP metric, so even the
    neutral sweep was vacuous for the source the mapping exists for.
    """
    from apps.api.registry import GEO_GRAINS, SERVING_CONTRACTS

    segments = {
        contract.source_code: segment for segment, contract in SERVING_CONTRACTS.items()
    }
    unanswered: list[str] = []
    off_vocabulary: list[str] = []
    exercised: list[str] = []

    for source_code, segment in sorted(segments.items()):
        grains_by_code = _current_catalog_grains(api_client, source_code)
        for metric_code, grains in list(grains_by_code.items())[:SWEEP_SAMPLE]:
            for grain in grains:
                assert grain in GEO_GRAINS, (
                    f"{source_code} publishes grain '{grain}', which is not in "
                    f"the vocabulary {GEO_GRAINS}"
                )
                route = f"/api/v1/{segment}/observations/latest"
                exercised.append(f"{segment}:{metric_code}@{grain}")
                response = api_client.get(
                    route,
                    params={
                        "metric_code": metric_code,
                        "geo_level": grain,
                        "limit": 5,
                    },
                )
                assert response.status_code == 200, (
                    f"{route} {metric_code}@{grain}: {response.text}"
                )
                payload = response.json()
                if int(payload["total"]) < 1:
                    unanswered.append(
                        f"{source_code} publishes grain '{grain}' for "
                        f"'{metric_code}', which {route} answers with no rows"
                    )
                for row in payload["items"]:
                    if row.get("geo_level") != grain:
                        off_vocabulary.append(
                            f"{route} {metric_code}@{grain} served a row whose "
                            f"geo_level is {row.get('geo_level')!r}"
                        )

    assert not unanswered, "\n".join(unanswered)
    assert not off_vocabulary, "\n".join(off_vocabulary)
    # The PEP fixture seeds one national row, and PEP is the source whose
    # relation stores `nation` under a column named `geo_level`. If this pair
    # is absent the sweep proved nothing about the case it exists for.
    assert f"pep:{published_pep_metric}@NATIONAL" in exercised, exercised


def test_every_catalog_code_answers_on_every_route_that_accepts_one(
    api_client: TestClient, published_pep_metric: str
) -> None:
    """Covers: DB-031 — the other half of the same route family.

    DB-025 asks this question of `/api/v1/observations`; DB-030 asks the grain
    question of each source's own latest route. `/{segment}/observations/timeseries`
    accepts no `geo_level`, so no grain sweep reaches it -- and it accepts the
    same `metric_code`, and was equally unanswerable for every Census PEP
    metric until the serving contracts learned how their relations compose an
    identity.

    The geography it asks for is one the source actually published, taken from
    the latest route's own answer: asking for a geography nothing covers would
    pass while proving nothing.
    """
    from apps.api.registry import SERVING_CONTRACTS

    unresolvable: list[str] = []
    exercised: list[str] = []

    for segment, contract in sorted(SERVING_CONTRACTS.items()):
        for metric_code in _current_catalog_codes(api_client, contract.source_code)[
            :SWEEP_SAMPLE
        ]:
            latest = api_client.get(
                f"/api/v1/{segment}/observations/latest",
                params={"metric_code": metric_code, "limit": 1},
            )
            assert latest.status_code == 200, latest.text
            published = latest.json()["items"]
            if not published:
                unresolvable.append(
                    f"{contract.source_code} publishes current catalog code "
                    f"'{metric_code}', which /api/v1/{segment}/observations/latest "
                    "answers with no rows"
                )
                continue
            geo_id = published[0]["geo_id"]
            exercised.append(f"{segment}:{metric_code}@{geo_id}")
            history = api_client.get(
                f"/api/v1/{segment}/observations/timeseries",
                params={"metric_code": metric_code, "geo_id": geo_id, "limit": 5},
            )
            assert history.status_code == 200, history.text
            if int(history.json()["total"]) < 1:
                unresolvable.append(
                    f"{contract.source_code} publishes '{metric_code}' for "
                    f"'{geo_id}', which "
                    f"/api/v1/{segment}/observations/timeseries answers with no rows"
                )

    assert not unresolvable, "\n".join(unresolvable)
    assert exercised, (
        "no serving contract's source published a current catalog code, so "
        "this guard proved nothing; the warehouse under test carries no "
        "catalog content for them"
    )
    # Census PEP is the source whose relations compose an identity the catalog
    # does not publish. If it was not exercised the sweep proved nothing about
    # the case it exists for.
    assert any(entry.startswith("pep:") for entry in exercised), exercised


# ---------------------------------------------------------------------------
# DB-032 — the sweeps reach a source identified by `identity_columns`
# ---------------------------------------------------------------------------

CDC_ASSET = "cdi"
#: Socrata publishes a release watermark as epoch seconds, and both the
#: publisher view and ``gold_cdc.latest_release_observation`` order releases by
#: ``release_watermark::BIGINT``. A far-future value keeps this fixture's
#: release the newest one for its asset whatever else the database holds, so
#: the seeded row is the one the latest surface serves.
CDC_WATERMARK = "3975004800"
#: CDC periods are years, not dates: the fact table stores them as integers.
CDC_PERIOD = 2096
#: The PLACES county asset, and its own watermark: the two releases are keyed
#: by ``(asset_id, release_watermark)`` and the latest surface orders on the
#: watermark within an asset, so each fixture's release stays the newest of its
#: own.
CDC_COUNTY_ASSET = "places_county"
CDC_COUNTY_WATERMARK = "3975004801"

#: The grains each CDC asset declares in ``cdc/registry.py``: CDI publishes
#: ``("us", "state")`` and PLACES county publishes ``("us", "county")``. The
#: ``geo_type`` each row carries is the source's own word -- the publisher
#: sends it through ``gold_glossary.geo_grain`` to get the catalog's (DB-044).
CDC_CDI_GRAIN_ROWS: tuple[dict[str, object], ...] = (
    {
        "geo_id": "us:1",
        "geo_type": "nation",
        "geography": {"geo_type": "nation", "name": "Sweep nation"},
        "value": 11.5,
    },
    {
        "geo_id": "state:94",
        "geo_type": "state",
        "geography": {
            "geo_type": "state",
            "state_fips": "94",
            "name": "Sweep state",
        },
        "value": 12.5,
    },
)
CDC_PLACES_GRAIN_ROWS: tuple[dict[str, object], ...] = (
    {
        "geo_id": "us:1",
        "geo_type": "nation",
        "geography": {"geo_type": "nation", "name": "Sweep nation"},
        "value": 13.5,
    },
    {
        "geo_id": "state:94|county:001",
        "geo_type": "county",
        "geography": {
            "geo_type": "county",
            "state_fips": "94",
            "county_fips": "001",
            "name": "Sweep county",
        },
        "value": 14.5,
    },
)


def _digest_token() -> str:
    """A 64-character lowercase hex token.

    ``dim_stratum.stratum_id`` and ``fact_health_observation.source_record_id``
    are both constrained to ``^[0-9a-f]{64}$`` -- they are content digests in
    production, and the schema says so. A fixture that invented a readable
    identifier would be rejected by the same constraint that keeps a
    hand-edited row out of the warehouse.
    """
    return uuid4().hex + uuid4().hex


@pytest.fixture
def published_pep_metric(published_pep_metrics: list[str]) -> str:
    """One published PEP metric — the shape every existing caller reads."""
    return published_pep_metrics[0]


def _publish_cdc_measure(
    postgres_connection_factory: Callable[[], connection],
    *,
    asset_id: str,
    release_watermark: str,
    socrata_id: str,
    grain_rows: tuple[dict[str, object], ...],
) -> Iterator[str]:
    """Publish one CDC measure end to end, at the grains its asset declares.

    The asset is a parameter because CDC's two registered assets publish
    different geographies -- ``cdi`` at ``("us", "state")`` and
    ``places_county`` at ``("us", "county")`` -- and a fixture that gave one
    asset the other's grain would be publishing a row the source never does.
    """
    from tests.support.capture_seed import seed_capture

    token = uuid4().hex[:8].upper()
    measure_id = f"SWEEP_{token}"
    value_type_id = "crude"
    stratum_id = _digest_token()
    geo_ids = tuple(str(row["geo_id"]) for row in grain_rows)

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as database_cursor:
            capture_id = seed_capture(database_cursor, "CDC")
            # Both silver_cdc tables carry a foreign key to the ingestion run,
            # so the run the capture was recorded under is the one to cite --
            # a fresh uuid would name a run that never happened.
            database_cursor.execute(
                "SELECT run_id FROM raw_capture.response_capture WHERE capture_id = %s",
                (capture_id,),
            )
            run_id = database_cursor.fetchone()[0]
            preexisting_geo = preexisting_geographies(database_cursor, geo_ids)
            geo_sk_by_id = {
                str(row["geo_id"]): seed_geography(
                    database_cursor,
                    vintage=CDC_PERIOD,
                    **row["geography"],  # type: ignore[arg-type]
                )
                for row in grain_rows
            }
            database_cursor.execute(
                """
                INSERT INTO silver_cdc.dim_dataset_release (
                    asset_id, release_watermark, socrata_id, title,
                    methodology_url, geography_basis, parser_contract_version,
                    estimate_method, population_basis, metadata_capture_id,
                    source_run_id, source_record_count, quarantine_count,
                    status, reconciled_at, published_at, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, 'Sweep dataset',
                    'https://www.cdc.gov/sweep', 'state', '1',
                    'model-based', 'adults', %s, %s, %s, 0,
                    'published', NOW(), NOW(), NOW(), NOW()
                )
                """,
                (
                    asset_id,
                    release_watermark,
                    socrata_id,
                    capture_id,
                    run_id,
                    len(grain_rows),
                ),
            )
            database_cursor.execute(
                """
                INSERT INTO silver_cdc.dim_measure (
                    asset_id, measure_id, value_type_id, measure_label, topic,
                    value_type_label, unit, adjustment_status, estimate_method,
                    population_basis, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, 'Sweep measure', 'Sweep topic',
                    'Crude prevalence', 'percent', 'crude', 'model-based',
                    'adults', NOW(), NOW()
                )
                """,
                (asset_id, measure_id, value_type_id),
            )
            database_cursor.execute(
                """
                INSERT INTO silver_cdc.dim_stratum (stratum_id, strata, created_at)
                VALUES (
                    %s,
                    '[["OVERALL", "Overall", "OVR", "Overall"]]'::jsonb,
                    NOW()
                )
                """,
                (stratum_id,),
            )
            for row_index, grain_row in enumerate(grain_rows):
                database_cursor.execute(
                    """
                    INSERT INTO silver_cdc.fact_health_observation (
                        asset_id, release_watermark, source_record_id,
                        source_run_id, capture_id, source_row_index, measure_id,
                        value_type_id, stratum_id, period_start, period_end,
                        geo_id, geo_sk, geo_type, geography_status,
                        value_source, value, value_status, unit,
                        adjustment_status, estimate_method, population_basis,
                        transformation_version
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, 'resolved',
                        %s, %s, 'valid', 'percent', 'crude', 'model-based',
                        'adults', '1'
                    )
                    """,
                    (
                        asset_id,
                        release_watermark,
                        _digest_token(),
                        run_id,
                        capture_id,
                        row_index,
                        measure_id,
                        value_type_id,
                        stratum_id,
                        CDC_PERIOD,
                        CDC_PERIOD,
                        grain_row["geo_id"],
                        geo_sk_by_id[str(grain_row["geo_id"])],
                        grain_row["geo_type"],
                        str(grain_row["value"]),
                        grain_row["value"],
                    ),
                )
        writer.commit()
    finally:
        writer.close()

    registered_before = _registration_state(postgres_connection_factory, "CDC")
    harvest_publisher(postgres_connection_factory, Publisher("gold_cdc"))

    source_object_key = f"{asset_id}:{measure_id}:{value_type_id}"
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as database_cursor:
            database_cursor.execute(
                """
                SELECT metric_code FROM gold_glossary.dim_metric_catalog
                WHERE source_code = 'CDC' AND source_object_key = %s
                """,
                (source_object_key,),
            )
            published = database_cursor.fetchone()
    finally:
        reader.close()
    assert published is not None, "the harvest published no CDC catalog row"

    try:
        yield published[0]
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as database_cursor:
                # `cdi` is one of two asset ids the schema permits, so every
                # delete names this fixture's own release, measure or stratum.
                # A delete scoped to the asset alone would take a neighbouring
                # fixture's rows with it.
                database_cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog "
                    "WHERE source_object_key = %s AND source_code = 'CDC'",
                    (source_object_key,),
                )
                database_cursor.execute(
                    "DELETE FROM silver_cdc.fact_health_observation "
                    "WHERE asset_id = %s AND release_watermark = %s",
                    (asset_id, release_watermark),
                )
                database_cursor.execute(
                    "DELETE FROM silver_cdc.dim_measure "
                    "WHERE asset_id = %s AND measure_id = %s",
                    (asset_id, measure_id),
                )
                database_cursor.execute(
                    "DELETE FROM silver_cdc.dim_dataset_release "
                    "WHERE asset_id = %s AND release_watermark = %s",
                    (asset_id, release_watermark),
                )
                database_cursor.execute(
                    "DELETE FROM silver_cdc.dim_stratum WHERE stratum_id = %s",
                    (stratum_id,),
                )
                delete_shared_geographies(database_cursor, geo_ids, preexisting_geo)
                _remove_registration(database_cursor, registered_before, "CDC")
            cleanup.commit()
        finally:
            cleanup.close()


@pytest.fixture
def published_cdc_metric(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Publish one CDC metric end to end, for the third identity strategy.

    The registry identifies a metric's serving rows three ways, and
    ``identity_columns`` -- where the service binds ``lineage.get(field)`` for
    each declared column and refuses the metric if the lineage publishes no
    such key -- had never been asked to answer a code the catalog published.
    CDC is the smallest source that uses it, and its rows carry a stratum and
    an adjustment status, so the sweeps exercise more of the envelope than a
    single-series source does.

    The rows cover ``us`` and ``state``, which is what ``CDI_ASSET`` declares
    it publishes. CDC's county grain belongs to the other asset, and
    ``published_cdc_county_metric`` is where it is seeded (DB-044).
    """
    yield from _publish_cdc_measure(
        postgres_connection_factory,
        asset_id=CDC_ASSET,
        release_watermark=CDC_WATERMARK,
        socrata_id="abcd-1234",
        grain_rows=CDC_CDI_GRAIN_ROWS,
    )


@pytest.fixture
def published_cdc_county_metric(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Publish one PLACES county metric, the only CDC asset with that grain."""
    yield from _publish_cdc_measure(
        postgres_connection_factory,
        asset_id=CDC_COUNTY_ASSET,
        release_watermark=CDC_COUNTY_WATERMARK,
        socrata_id="swc5-untb",
        grain_rows=CDC_PLACES_GRAIN_ROWS,
    )


def test_a_source_identified_by_its_lineage_columns_answers_its_catalog_code(
    api_client: TestClient, published_cdc_metric: str
) -> None:
    """Covers: DB-032 — the third identity strategy, actually exercised.

    `identity_columns` binds `lineage.get(field)` for each declared column and
    refuses a metric whose lineage publishes no such key. DB-025 and DB-028
    sweep every registered source, but a source publishing no catalog row
    contributes nothing -- and until now none of the three sources using this
    strategy published one, so the strategy had never answered a code the
    catalog composed. Seeding Census PEP is what revealed DB-030; this is the
    same gap, one strategy over.
    """
    from apps.api.registry import GEO_GRAINS

    catalog = api_client.get(
        "/api/v1/catalog/metrics",
        params={"source_code": "CDC", "active_only": "true", "limit": 50},
    )
    assert catalog.status_code == 200, catalog.text
    published = {
        item["metric_code"]: list(item.get("valid_geo_grains") or [])
        for item in catalog.json()["items"]
    }
    assert published_cdc_metric in published, published

    grains = published[published_cdc_metric]
    assert grains, "the publisher derived no grain from the seeded row"
    for grain in grains:
        assert grain in GEO_GRAINS, grain
        answer = api_client.get(
            "/api/v1/observations",
            params={
                "metric_code": published_cdc_metric,
                "geo_level": grain,
                "limit": 5,
            },
        )
        assert answer.status_code == 200, answer.text
        payload = answer.json()
        assert int(payload["total"]) >= 1, (
            f"CDC publishes grain '{grain}' for '{published_cdc_metric}', "
            "which /api/v1/observations answers with no rows"
        )
        for row in payload["items"]:
            assert row["geo_level"] == grain, row
            # The stratified envelope this source is served through: its own
            # stratum and adjustment status travel with the value.
            assert row["dimensions"].get("stratum_id"), row


# ---------------------------------------------------------------------------
# DB-033 — a stratum the warehouse accepts is a stratum the API can serve
# ---------------------------------------------------------------------------


def test_the_stratum_the_warehouse_accepts_is_the_stratum_the_api_serves(
    api_client: TestClient, published_cdc_metric: str
) -> None:
    """Covers: DB-033 — the CDC source-explorer route serves its own rows.

    `CdcObservation.strata` is `list[Any]`, and `jsonb` is not. A stratum
    stored as an object was accepted by every write path and then crashed
    `CdcObservation.model_validate`, which the caller sees as
    `500 The API failed to complete this request` -- with no way to tell which
    row is unserveable, and the row still there for the next page that
    includes it. Migration 019 refuses the shape at the write; this is the
    other half, and the only tier that can see it: the row the fixture
    publishes is fetched back through the route that declares the list.

    It is also what keeps the DB-032 fixture honest. A fixture is only
    evidence while the rows it seeds are rows the warehouse could hold, and
    this one seeded a shape no parser produces until the route it feeds was
    asked to serve it.
    """
    answer = api_client.get(
        "/api/v1/cdc/observations", params={"dataset": CDC_ASSET, "limit": 50}
    )
    assert answer.status_code == 200, answer.text
    payload = answer.json()
    seeded = [
        item for item in payload["items"] if item["release_watermark"] == CDC_WATERMARK
    ]
    assert seeded, (
        f"the CDC source-explorer route answers no row for release "
        f"{CDC_WATERMARK}, which the catalog publishes as "
        f"'{published_cdc_metric}'"
    )
    for item in seeded:
        # The published shape, not merely a truthy one: a mapping validates as
        # neither, and that is exactly what used to reach the response model.
        assert isinstance(item["strata"], list), item["strata"]
        assert all(isinstance(entry, list) for entry in item["strata"]), item["strata"]


# ---------------------------------------------------------------------------
# DB-034 — a route answers the metric code it publishes
# ---------------------------------------------------------------------------


def test_every_route_answers_the_metric_code_it_published(
    api_client: TestClient,
    published_pep_metric: str,
    published_acs_metric: str,
    published_fred_metric: str,
    published_bls_metric: str,
) -> None:
    """Covers: DB-034 — a route's own answer is a request it accepts.

    The source-scoped routes project `metric_code` from the source's own
    serving relation, and Census PEP's relation composes its identity from a
    dataset and a measure. So a page answered for the catalog's
    `CENSUS_PEP:<measure>` comes back carrying `CENSUS_PEP:<dataset>:<measure>`
    -- and asking for more of that metric, by the only identity the page gave,
    used to be an empty 200. A route refusing an identity it published in its
    own response is the route disagreeing with itself.

    Covers: DB-043 -- and every source-scoped contract is exercised.

    Source-agnostic: the contracts come from the reviewed registry and the
    codes from the served catalog, so a contract added later is covered
    without an edit here. It used to be possible for a contract to contribute
    nothing and say so to nobody -- one PEP assertion stood in for all four --
    so every segment the registry declares now has a fixture publishing a
    code, and a segment that reaches no row fails naming itself.
    """
    from apps.api.registry import SERVING_CONTRACTS

    disagreements: list[str] = []
    exercised: list[str] = []

    for segment, contract in sorted(SERVING_CONTRACTS.items()):
        for catalog_code in _current_catalog_codes(api_client, contract.source_code)[
            :SWEEP_SAMPLE
        ]:
            first = api_client.get(
                f"/api/v1/{segment}/observations/latest",
                params={"metric_code": catalog_code, "limit": 1},
            )
            assert first.status_code == 200, first.text
            published = first.json()["items"]
            if not published:
                continue
            served_code = published[0]["metric_code"]
            geo_id = published[0]["geo_id"]
            exercised.append(f"{segment}:{catalog_code}->{served_code}")
            for route, params in (
                (
                    f"/api/v1/{segment}/observations/latest",
                    {"metric_code": served_code, "limit": 1},
                ),
                (
                    f"/api/v1/{segment}/observations/timeseries",
                    {"metric_code": served_code, "geo_id": geo_id, "limit": 1},
                ),
            ):
                answer = api_client.get(route, params=params)
                assert answer.status_code == 200, answer.text
                if int(answer.json()["total"]) < 1:
                    disagreements.append(
                        f"{route} published metric_code '{served_code}' for "
                        f"catalog code '{catalog_code}', and answers no rows "
                        "when asked for it"
                    )

    assert not disagreements, "\n".join(disagreements)

    # Census PEP is the source whose published identity and catalog identity
    # differ at all. If it was not exercised the sweep proved nothing.
    assert any(
        entry.startswith(f"pep:{published_pep_metric}->") for entry in exercised
    ), exercised

    # And every other source-scoped contract too: a segment whose catalog code
    # reached no row was skipped silently, so three of the four could have
    # stopped answering with this sweep green on PEP alone.
    reached = {entry.split(":", 1)[0] for entry in exercised}
    unreached = sorted(set(SERVING_CONTRACTS) - reached)
    assert not unreached, (
        f"these source-scoped contracts answered no row for any current "
        f"catalog code, so the sweep proved nothing about them: {unreached}"
    )


# ---------------------------------------------------------------------------
# API-094 — every route that takes a grain takes the same grain words
# ---------------------------------------------------------------------------


def _grain_requests(
    fred_metric: str, pep_metric: str, acs_metric: str, pep_pair: list[str]
) -> dict[str, dict[str, object]]:
    """One request per route that declares ``geo_level``, minus the grain.

    Keyed by the path the served document publishes, so a route that grows a
    ``geo_level`` parameter without an entry here fails the sweep instead of
    quietly skipping the rule.
    """
    return {
        "/api/v1/observations": {"metric_code": pep_metric, "limit": 5},
        "/api/v1/observations/latest": {"metric_code": fred_metric, "limit": 5},
        "/api/v1/comparison": {
            "metric_code_a": pep_metric,
            "metric_code_b": pep_metric,
            "limit": 5,
        },
        "/api/v1/distribution/bins": {"metric_code": pep_metric, "bin_count": 3},
        "/api/v1/catalog/geographies": {"limit": 5},
        "/api/v1/pep/observations/latest": {"metric_code": pep_metric, "limit": 5},
        "/api/v1/fred/observations/latest": {"metric_code": fred_metric, "limit": 5},
        "/api/v1/census/observations/latest": {"metric_code": acs_metric, "limit": 5},
        "/api/v1/bls/observations/latest": {"metric_code": "BLS:UNUSED", "limit": 5},
        "/api/v1/comparison/correlation": {
            "metric_code_a": pep_metric,
            "metric_code_b": pep_metric,
        },
        "/api/v1/comparison/matrix": {
            # Two distinct measures, because the matrix refuses a repeat: a
            # measure against itself correlates 1 with no information in it.
            # Both are PEP, so the pair is comparable by the policy's own
            # rules rather than by this fixture asserting that it is.
            "metric_codes": ",".join(pep_pair),
            "limit": 5,
        },
    }


#: The field each route answers its row count in. `total` everywhere except
#: the correlation, which is one statistic over a set of pairs rather than a
#: page of rows and counts them in `n`.
_GRAIN_COUNT_FIELD: dict[str, str] = {
    "/api/v1/comparison/correlation": "n",
}


@pytest.mark.parametrize("published_pep_metrics", [2], indirect=True)
def test_every_route_that_takes_a_grain_takes_the_same_grain_words(
    api_client: TestClient,
    published_pep_metrics: list[str],
    published_fred_metric: str,
    published_acs_metric: str,
) -> None:
    """Covers: API-094 — an alias answers what its vocabulary word answers.

    API-092 promised more than the words: the words they replaced keep
    answering, so a shared link or a saved configuration holding the catalog's
    earlier `NATION` still resolves. `normalize_geo_level` is that promise,
    and four of the nine routes declaring a `geo_level` never called it.

    `/comparison` was the sharpest -- not merely alias-blind but
    case-sensitive, because the value each side's filter had normalized was
    overwritten afterwards with the caller's own text. `/observations/latest`
    and `/catalog/geographies` compare `UPPER(geo_level)`, so they survived a
    case difference and failed on an alias.

    Driven by the served document: a route that declares `geo_level` and has
    no request here fails rather than skipping the rule.
    """
    from apps.api.main import app
    from apps.api.registry import GEO_GRAIN_ALIASES

    declared = {
        path
        for path, operations in app.openapi()["paths"].items()
        for operation in operations.values()
        if any(
            parameter.get("name") == "geo_level" and parameter.get("in") == "query"
            for parameter in operation.get("parameters") or []
        )
    }
    requests = _grain_requests(
        published_fred_metric,
        published_pep_metrics[0],
        published_acs_metric,
        published_pep_metrics,
    )
    assert declared == set(requests), (
        "routes declaring geo_level with no request in this sweep: "
        f"{sorted(declared - set(requests))}; requests for routes that no "
        f"longer declare one: {sorted(set(requests) - declared)}"
    )

    disagreements: list[str] = []
    exercised: list[str] = []

    for path, base in sorted(requests.items()):
        for alias, word in sorted(GEO_GRAIN_ALIASES.items()):
            count_field = _GRAIN_COUNT_FIELD.get(path, "total")
            canonical = api_client.get(path, params={**base, "geo_level": word})
            assert canonical.status_code == 200, canonical.text
            expected = int(canonical.json()[count_field])
            if expected:
                exercised.append(f"{path}@{word}")
            for spelling in (alias, alias.lower(), word.lower()):
                answer = api_client.get(path, params={**base, "geo_level": spelling})
                assert answer.status_code == 200, answer.text
                actual = int(answer.json()[count_field])
                if actual != expected:
                    disagreements.append(
                        f"{path} answers {expected} row(s) for geo_level "
                        f"'{word}' and {actual} for '{spelling}', which names "
                        "the same grain"
                    )

    assert not disagreements, "\n".join(disagreements)
    # The fixtures publish one national row each through two different
    # dispatch paths. Without them every comparison above is 0 == 0.
    assert "/api/v1/comparison@NATIONAL" in exercised, exercised
    assert "/api/v1/observations@NATIONAL" in exercised, exercised
    assert "/api/v1/distribution/bins@NATIONAL" in exercised, exercised
    assert "/api/v1/pep/observations/latest@NATIONAL" in exercised, exercised
    assert "/api/v1/observations/latest@NATIONAL" in exercised, exercised


def test_the_distribution_reports_the_grain_it_binned(
    api_client: TestClient, published_pep_metric: str
) -> None:
    """Covers: API-094 — the bins are labelled with the grain they describe.

    The response's `geo_level` is the analysis's own statement of what it
    measured, and it is what a saved analysis and an evidence packet record.
    Echoing the caller's text labelled a set of bins over `NATIONAL` rows as
    `us`.
    """
    for spelling in ("NATIONAL", "nation", "NATION", "us", "US"):
        answer = api_client.get(
            "/api/v1/distribution/bins",
            params={
                "metric_code": published_pep_metric,
                "geo_level": spelling,
                "bin_count": 3,
            },
        )
        assert answer.status_code == 200, answer.text
        payload = answer.json()
        assert int(payload["total"]) >= 1, payload
        assert payload["geo_level"] == "NATIONAL", payload["geo_level"]


def test_the_geography_catalog_is_its_own_refresh(
    api_client: TestClient, published_acs_metric: str
) -> None:
    """Covers: API-107 — an absent geography is "not projected yet".

    `/catalog/geographies` answers `gold_glossary.dim_geo_latest`, which
    `gold_glossary.refresh_dim_geo_latest()` fills from one Airflow task whose
    own docstring says it refreshes "the glossary-owned geography projection
    independently" -- in the glossary reconciliation DAG, on that DAG's
    schedule, not with the publishers that make observations available.

    So one geography can have two correct answers: the observation surface
    serves it and attributes it from the relation it reads, and the projection
    does not list it yet. The guide says exactly this for
    `/observations/latest` and said nothing here, while `apps/web` builds its
    geography pickers from this resource.

    Pinned so that a change making this resource read the durable relation
    fails here -- at which point the guide's caveat should be removed
    deliberately rather than left behind as a stale warning.
    """
    served = api_client.get(
        "/api/v1/observations", params={"metric_code": published_acs_metric}
    )
    assert served.status_code == 200, served.text
    items = served.json()["items"]
    assert items, served.text
    geo_id = items[0]["geo_id"]
    assert items[0]["geo_level"] == "STATE", items[0]

    # The observation surface knows the geography's attribution, not only its
    # identity: this is a geography the API can name, in full.
    compared = api_client.get(
        "/api/v1/comparison",
        params={
            "metric_code_a": published_acs_metric,
            "metric_code_b": published_acs_metric,
            "geo_level": "STATE",
        },
    )
    assert compared.status_code == 200, compared.text
    attributed = compared.json()["items"]
    assert attributed and attributed[0]["geo_id"] == geo_id, compared.text
    assert attributed[0]["state_name"], compared.text

    # And the projection has not been refreshed, so the catalog does not list
    # it -- which is a different fact from "no such geography".
    listed = api_client.get("/api/v1/catalog/geographies", params={"q": geo_id})
    assert listed.status_code == 200, listed.text
    assert listed.json()["total"] == 0, (
        "this node's premise is that the geography projection is refreshed "
        "on its own schedule; if the catalog now answers for a geography the "
        "publishers just served, the guide's caveat should be removed "
        "deliberately rather than left standing"
    )


def test_a_source_scoped_row_names_the_catalogs_code(
    api_client: TestClient, published_pep_metric: str
) -> None:
    """Covers: API-108 — the row's identity is one the API recognises.

    The source-scoped column list selected the serving relation's own
    `metric_code`. For BLS, Census ACS and FRED that column is the catalog's
    code; Census PEP's relation composes its identity from the dataset, so
    `/pep/observations/latest` answered rows whose `metric_code` 404s on
    `/catalog/metrics/{metric_code}`, 404s on `/observations`, and is refused
    by the stored-document validation as "not a published metric".

    The guide already settles it: a provider's own key travels beside the
    catalog's code rather than as it -- "the BLS series id is still on every
    row, under `dimensions.series_id`" -- and every PEP row already carries
    the composed code's one extra component as `dataset_code`.
    """
    for route in (
        "/api/v1/pep/observations/latest",
        "/api/v1/pep/observations/timeseries",
    ):
        params = {"metric_code": published_pep_metric, "limit": 5}
        if route.endswith("timeseries"):
            params["geo_id"] = PEP_NATION_GEO_ID
        answer = api_client.get(route, params=params)
        assert answer.status_code == 200, answer.text
        items = answer.json()["items"]
        assert items, f"{route} answered no rows; the node would prove nothing"
        for item in items:
            assert item["metric_code"] == published_pep_metric, (
                f"{route} labelled a row with the relation's composed code, "
                "which no other resource in this API recognises"
            )
            # The component the composed form interpolated is still published,
            # under its own name.
            assert item["dataset_code"], item

    # An identity every other resource answers for.
    detail = api_client.get(f"/api/v1/catalog/metrics/{published_pep_metric}")
    assert detail.status_code == 200, detail.text
    neutral = api_client.get(
        "/api/v1/observations", params={"metric_code": published_pep_metric, "limit": 1}
    )
    assert neutral.status_code == 200, neutral.text
    assert neutral.json()["items"][0]["metric_code"] == published_pep_metric


def test_the_relations_composed_spelling_still_addresses_one_dataset(
    api_client: TestClient, published_pep_metric: str
) -> None:
    """Covers: API-108 — the relabel does not widen what a request answers.

    The relation's composed spelling is a *narrower* address than the catalog
    code: one dataset's rows of a measure the catalog publishes across
    several. The first version of this fix resolved that spelling back to the
    catalog row so the answer could be labelled with it, which bound the
    lineage key as well and widened the match from one dataset to all of
    them -- the end-to-end tier caught it, on a request that answered six
    rows and began answering twelve.

    So the catalog's code labels the rows only where the request named it,
    and a request naming the composed spelling answers exactly what it
    always did.
    """
    served = api_client.get(
        "/api/v1/pep/observations/latest",
        params={"metric_code": published_pep_metric, "limit": 100},
    )
    assert served.status_code == 200, served.text
    by_catalog_code = served.json()

    composed = (
        f"CENSUS_PEP:{by_catalog_code['items'][0]['dataset_code']}:"
        f"{published_pep_metric.split(':', 1)[1]}"
    )
    assert composed != published_pep_metric, composed

    answer = api_client.get(
        "/api/v1/pep/observations/latest",
        params={"metric_code": composed, "limit": 100},
    )
    assert answer.status_code == 200, answer.text
    by_composed = answer.json()
    assert by_composed["items"], "the composed spelling stopped answering"
    # One dataset, and no more rows than the catalog code's own answer.
    assert {item["dataset_code"] for item in by_composed["items"]} == {
        by_catalog_code["items"][0]["dataset_code"]
    }
    assert by_composed["total"] <= by_catalog_code["total"], (
        "resolving the composed spelling widened the lineage match from one "
        "dataset to every dataset publishing the measure"
    )


def test_a_rows_dimensions_are_what_capabilities_declares(
    api_client: TestClient,
    published_acs_metric: str,
    published_pep_metric: str,
    published_cdc_metric: str,
) -> None:
    """Covers: API-109 — the declared dimension set is the served one.

    `dimensions` was registry-derived and unpublished: the guide claimed the
    row carried "everything the source publishes" and gestured at four
    examples, so a consumer coding against it read one row and hard-coded
    whatever it held. `/catalog/capabilities` now answers the set, from the
    same declaration the rows are built from.

    Asserted against real published rows for three sources, including the two
    whose relations are widest -- `gold_nass.latest_release_observation` has
    53 columns and 14 of them ride here -- so the claim is about what the
    warehouse actually serves and not about a fixture's shape.
    """
    capabilities = api_client.get("/api/v1/catalog/capabilities")
    assert capabilities.status_code == 200, capabilities.text
    declared = {
        item["source_code"]: set(item["observation_dimensions"])
        for item in capabilities.json()["items"]
    }
    assert declared, capabilities.text
    # Not a vacuous agreement: these sources publish dimensions.
    assert len(declared["CDC"]) >= 10, declared["CDC"]
    assert len(declared["USDA_NASS"]) >= 10, declared["USDA_NASS"]

    for metric_code in (
        published_acs_metric,
        published_pep_metric,
        published_cdc_metric,
    ):
        answer = api_client.get(
            "/api/v1/observations", params={"metric_code": metric_code, "limit": 5}
        )
        assert answer.status_code == 200, answer.text
        payload = answer.json()
        assert payload["items"], f"{metric_code} answered no rows"
        expected = declared[payload["source_code"]]
        for item in payload["items"]:
            assert set(item["dimensions"]) == expected, (
                f"{payload['source_code']} served dimensions "
                f"{sorted(set(item['dimensions']))} while the capability map "
                f"declares {sorted(expected)}"
            )


#: A LAUS measure code `gold_bls.dim_bls_measure` does not seed. The seven it
#: seeds are 03-09; ingest accepts any two digits, and the serving refresh
#: falls through to the series identity for anything it does not hold.
UNSEEDED_LA_MEASURE = "10"
BLS_PERIOD_START = "2097-01-01"
BLS_PERIOD_END = "2097-12-31"


@pytest.fixture
def served_bls_series_identity(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    """Serve one BLS row whose measure code no measure identity holds.

    The real refresh procedures compose the identity
    (`COALESCE('BLS:' || measure.metric_key, 'BLS:' || series.series_id)`) and
    the real harvest reads `gold_bls.metric_publisher` into the catalog.
    Neither side is told what the other spelled, which is the point.
    """
    token = uuid4().hex[:6].upper()
    series_id = f"LAUCN9599{token}{UNSEEDED_LA_MEASURE}"
    geo_id = "state:95|county:997"
    registered_before = _registration_state(postgres_connection_factory, "BLS")

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20970101, BLS_PERIOD_START)
            geo_sk = seed_geography(
                cursor,
                geo_type="county",
                state_fips="95",
                county_fips="997",
                vintage=2097,
                name="Catalog agreement county",
            )
            # The shipped LA program: a survey row and measure identities the
            # serving refresh matches on. Seeded here because the DDL creates
            # the tables and the gold transform fills them.
            cursor.execute(
                """
                INSERT INTO gold_bls.dim_bls_survey (
                    program_code, survey_name, survey_universe, observation_basis,
                    primary_concept, id_construction_type, reference_url
                ) VALUES ('LA', 'Local Area Unemployment Statistics',
                          'Residence-based civilian labor force', 'PEOPLE',
                          'Local labor market conditions', 'Program+Area+Measure',
                          'https://www.bls.gov/lau/')
                ON CONFLICT (program_code) DO NOTHING
                """
            )
            cursor.execute(
                """
                INSERT INTO gold_bls.dim_bls_measure (
                    program_code, measure_code, metric_key,
                    metric_display_name, unit_of_measure, value_type
                ) VALUES ('LA', '03', 'LAU:UNEMP_RATE', 'Unemployment rate',
                          'Percent', 'RATE')
                ON CONFLICT (program_code, measure_code) DO NOTHING
                """
            )
            cursor.execute(
                "SELECT bls_survey_sk FROM gold_bls.dim_bls_survey "
                "WHERE program_code = 'LA'"
            )
            survey_sk = cursor.fetchone()[0]
            cursor.execute(
                """
                INSERT INTO gold_bls.dim_bls_series (
                    bls_survey_sk, program_code, series_id, series_title,
                    measure_name, measure_category, unit_of_measure, value_type,
                    seasonal_adjustment_status, geographic_level
                ) VALUES (%s, 'LA', %s, 'Catalog agreement measure',
                          'Catalog agreement measure', 'OTHER', 'Percent',
                          'RATE', 'U', 'county')
                """,
                (survey_sk, series_id),
            )
            cursor.execute(
                """
                INSERT INTO silver_bls.fact_labor_statistics (
                    series_id, program, measure_code, geo_sk, geo_id, geo_level,
                    state_fips, county_fips, time_sk, period_date,
                    duration_start, duration_end, year, period, period_name,
                    value, seasonal_adjustment, load_batch_id, ingested_at
                ) VALUES (%s, 'la', %s, %s, %s, 'county', '95', '997',
                          20970101, '2097-01-31', %s, '2097-01-31', 2097,
                          'M01', 'January', 4.2, 'U', gen_random_uuid(), NOW())
                """,
                (series_id, UNSEEDED_LA_MEASURE, geo_sk, geo_id, BLS_PERIOD_START),
            )
            cursor.execute(
                "CALL gold_bls.refresh_rpt_bls_observations(%s, %s)",
                (BLS_PERIOD_START, BLS_PERIOD_END),
            )
            cursor.execute(
                "CALL gold_bls.refresh_mv_bls_latest(%s, %s)",
                (BLS_PERIOD_START, BLS_PERIOD_END),
            )
        writer.commit()
    finally:
        writer.close()

    harvest_publisher(postgres_connection_factory, Publisher("gold_bls"))
    try:
        yield series_id
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                for relation in (
                    "gold_bls.mv_bls_latest",
                    "gold_bls.rpt_bls_observations",
                ):
                    cursor.execute(
                        f"DELETE FROM {relation} WHERE series_id = %s", (series_id,)
                    )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog "
                    "WHERE source_code = 'BLS' AND source_object_key = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM silver_bls.fact_labor_statistics WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM gold_bls.dim_bls_series WHERE series_id = %s",
                    (series_id,),
                )
                delete_geography(cursor, geo_id)
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20970101"
                )
                _remove_registration(cursor, registered_before, "BLS")
            cleanup.commit()
        finally:
            cleanup.close()


def test_every_served_bls_code_is_a_catalog_code(
    postgres_connection_factory: Callable[[], connection],
    served_bls_series_identity: str,
) -> None:
    """Covers: DB-036 — serving to catalog, the direction DB-025 did not check.

    The serving refresh assigns identity per `(program_code, measure_code)`:
    a row whose pair `dim_bls_measure` does not hold keeps its series
    identity. The publisher's series arm excluded whole *programs*, so an LA
    measure code nobody seeded served `BLS:LAU...` rows the publisher never
    published: `/observations?metric_code=...` answered "unknown metric"
    while `/bls/observations/timeseries` paged the rows.
    """
    expected = f"BLS:{served_bls_series_identity}"
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT served.metric_code
                  FROM gold_bls.rpt_bls_observations AS served
                  LEFT JOIN gold_glossary.dim_metric_catalog AS catalog
                    ON catalog.metric_code = served.metric_code
                 WHERE served.metric_code IS NOT NULL
                   AND catalog.metric_code IS NULL
                 ORDER BY 1
                """
            )
            unpublished = [row[0] for row in cursor.fetchall()]
            assert unpublished == [], (
                f"these served BLS codes resolve in no catalog row: {unpublished}"
            )

            cursor.execute(
                "SELECT COUNT(*) FROM gold_bls.rpt_bls_observations "
                "WHERE metric_code = %s",
                (expected,),
            )
            assert cursor.fetchone()[0] > 0, (
                "the fixture served no row under the series identity, so the "
                "sweep above proves nothing"
            )

            # And the catalog row's grains are the grains the *served*
            # relation carries, not silver's.
            cursor.execute(
                "SELECT valid_geo_grains FROM gold_glossary.dim_metric_catalog "
                "WHERE metric_code = %s",
                (expected,),
            )
            published_grains = cursor.fetchone()
            assert published_grains is not None
            assert published_grains[0] == ["COUNTY"], published_grains
    finally:
        reader.close()


def test_re_serving_a_chunk_publishes_no_new_release(
    postgres_connection_factory: Callable[[], connection],
    served_bls_series_identity: str,
) -> None:
    """Covers: DB-039 — a release is the row's ingestion, not the refresh's day.

    The BLS and FRED fact views published `CURRENT_DATE AS as_of_date` and the
    chunked refresh materialised that literal, so a "release" was the calendar
    day a chunk was last written. The driver re-serves only changed years:
    re-serve 2019 on Monday and 2020 on Tuesday and
    `/observations/releases?metric_code=BLS:...` lists two published releases
    with row counts, `newest_release_per_period=true` settles on "the most
    recently re-served chunk", and a full re-serve collapses every release
    into one. BLS published nothing on any of those days.

    The silver row's `ingested_at` is set back before the refresh runs, which
    is what makes this failing-first: with the clock literal the served date
    was today's regardless, so a test that ingested and served in one session
    could not tell the two apart.
    """
    metric_code = f"BLS:{served_bls_series_identity}"
    ingested_on = "2097-06-15"

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                UPDATE silver_bls.fact_labor_statistics
                   SET ingested_at = %s::TIMESTAMPTZ
                 WHERE series_id = %s
                """,
                (f"{ingested_on} 12:00:00+00", served_bls_series_identity),
            )
            assert cursor.rowcount == 1
            cursor.execute(
                "CALL gold_bls.refresh_rpt_bls_observations(%s, %s)",
                (BLS_PERIOD_START, BLS_PERIOD_END),
            )
            cursor.execute(
                """
                SELECT DISTINCT as_of_date::TEXT, updated_at::DATE::TEXT
                  FROM gold_bls.rpt_bls_observations WHERE metric_code = %s
                """,
                (metric_code,),
            )
            first = cursor.fetchall()
            assert first == [(ingested_on, ingested_on)], (
                f"the served release date is not the row's own ingestion: {first}"
            )

            # The chunk is re-served with nothing changed in silver, which is
            # what the year-chunk driver does on every run of a year it has
            # already served.
            cursor.execute(
                "CALL gold_bls.refresh_rpt_bls_observations(%s, %s)",
                (BLS_PERIOD_START, BLS_PERIOD_END),
            )
            cursor.execute(
                """
                SELECT DISTINCT as_of_date::TEXT
                  FROM gold_bls.rpt_bls_observations WHERE metric_code = %s
                """,
                (metric_code,),
            )
            assert cursor.fetchall() == [(ingested_on,)]

            # What `/observations/releases` pages: one release per distinct
            # ingestion, and re-serving does not add one.
            cursor.execute(
                """
                SELECT COUNT(DISTINCT as_of_date)
                  FROM gold_bls.rpt_bls_observations WHERE metric_code = %s
                """,
                (metric_code,),
            )
            assert cursor.fetchone() == (1,)
        writer.commit()
    finally:
        writer.close()


def test_a_source_that_publishes_no_value_state_serves_only_numbers(
    api_client: TestClient,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: API-127, DB-061 — `publishes_value_status` says which shape a row takes.

    Two contracts, and a client has to code for one of them. Where a source
    publishes a value state, an unpublished figure arrives as a row with
    `value: null` and a reason. Where it does not, the serving relation
    carries only published numbers -- the gold view selects on the value
    being present -- so `value` is never null and a period the source
    published without one is **absent from the series** rather than present
    and marked.

    The capability is derived from the dispatch entry's `value_status_column`,
    and this holds the warehouse to it in both directions: a source declaring
    one has the column on both served relations, and a source declaring none
    has it on neither. Without that, `publishes_value_status: false` would be
    a claim about a projection rather than about the rows.
    """
    capabilities = api_client.get("/api/v1/catalog/capabilities").json()["items"]
    declared = {
        entry["source_code"]: entry["publishes_value_status"]
        for entry in capabilities
        if entry["source_code"] in OBSERVATION_DISPATCH
    }
    assert declared, "no source declares a value-state capability"

    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            for source_code, publishes in sorted(declared.items()):
                dispatch = OBSERVATION_DISPATCH[source_code]
                for relation in (dispatch.latest_relation, dispatch.released_relation):
                    schema, name = relation.split(".", 1)
                    cursor.execute(
                        """
                        SELECT count(*) FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                          AND column_name = 'value_status'
                        """,
                        (schema, name),
                    )
                    carries = bool(cursor.fetchone()[0])
                    assert carries == publishes, (
                        f"/catalog/capabilities says {source_code} "
                        f"publishes_value_status={publishes} and {relation} "
                        f"{'carries' if carries else 'does not carry'} a "
                        f"value_status column"
                    )
    finally:
        database_connection.close()


def test_a_metric_carries_the_same_value_state_declaration_as_its_source(
    api_client: TestClient,
    published_acs_metric: str,
) -> None:
    """Covers: API-127 — the declaration is on both resources.

    The reason API-119 records for the dimensions: a client that discovered a
    metric should not have to enumerate sources to learn the shape of its own
    rows.

    `/catalog/capabilities` is composed from the registry and answers on an
    empty warehouse; the metric catalog is rows, and the disposable database
    this suite runs against is bootstrapped with the DDL and no data. So the
    sweep needs a metric to have been published, and every fixture here
    publishes one for the length of a test and takes it away again. ACS is
    the source whose own fixture already carries a value state, which is the
    half of the agreement a bare catalog could not exercise at all.
    """
    capabilities = {
        entry["source_code"]: entry["publishes_value_status"]
        for entry in api_client.get("/api/v1/catalog/capabilities").json()["items"]
    }
    metrics = api_client.get(
        "/api/v1/catalog/metrics", params={"limit": SWEEP_SAMPLE}
    ).json()["items"]
    assert metrics, (
        "the catalog published no metric; the fixture's metric should be "
        "here, so the publish did not land rather than the rule being wrong"
    )
    checked = 0
    for metric in metrics:
        detail = api_client.get(
            f"/api/v1/catalog/metrics/{quote(metric['metric_code'], safe='')}"
        )
        assert detail.status_code == 200, detail.text
        body = detail.json()
        source_code = body["source_code"]
        if source_code not in capabilities:
            continue
        assert body["publishes_value_status"] == capabilities[source_code], (
            f"{metric['metric_code']} and its source {source_code} disagree "
            f"about whether a row can arrive with a null value"
        )
        checked += 1
    assert checked, "no sampled metric belonged to a discovered source"
